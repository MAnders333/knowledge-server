import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { config } from "../config.js";
import { logger } from "../logger.js";
import {
	BertWordPieceTokenizer,
	MAX_SEQUENCE_LENGTH,
} from "./bert-tokenizer.js";
import {
	RerankSkippedError,
	createRemoteRerankerFromConfig,
} from "./rerank-remote.js";

// Onnxruntime-web WASM runtime (plain build, not asyncify), imported with Bun's
// file loader:
// - source/dev runs: resolves to the real node_modules path
// - `bun build --compile` binaries: embedded into the single-file binary and
//   exposed under the virtual /$bunfs/ filesystem (readable via node:fs)
//
// Why WASM and not onnxruntime-node (native)? The native .node binding does
// not survive single-file compilation: its dependent libonnxruntime dylib is
// not embedded, so dlopen fails (verified by binary smoke test). The WASM
// backend is platform-agnostic and embeds cleanly. Measured on an M4 with the
// default model: ~37ms per pair at ~80 tokens, one thread. WASM threading is
// not used: worker spawning fails under Bun (fetch() of local worker URL), so
// numThreads is pinned to 1. The asyncify variant was dropped: it added ~270MB
// RSS and ~150ms session creation, and inference speed was unchanged.
//
// Paths are relative because onnxruntime-web's package `exports` does not
// expose ./dist/* — the bundler resolves and embeds them identically.
// @ts-ignore — path module declared in src/assets.d.ts
import wasmRuntimeMjs from "../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.mjs" with {
	type: "file",
};
// @ts-ignore — path module declared in src/assets.d.ts
import wasmRuntimeBin from "../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.wasm" with {
	type: "file",
};

/**
 * Local in-process cross-encoder reranker.
 *
 * Second stage of the retrieve & re-rank pipeline: dense retrieval
 * (embeddings + pgvector/cosine) overfetches candidates for recall, then this
 * cross-encoder jointly encodes each (query, document) pair for a much more
 * accurate relevance signal. Runs fully in-process — no network roundtrip,
 * no vendor, no per-call cost, and knowledge content never leaves the machine.
 *
 * Architecture (shaped by single-file-binary constraints):
 * - Tokenizer: vendored BERT WordPiece tokenizer (bert-tokenizer.ts) driven
 *   by the model's tokenizer.json. Every packaged tokenizer runtime pulls in
 *   native code (@huggingface/transformers statically imports
 *   onnxruntime-node; @huggingface/tokenizers uses napi prebuilds) that
 *   crashes `bun build --compile` binaries at module load.
 * - Inference: onnxruntime-web WASM backend directly (the transformers.js
 *   node build does not register the "wasm" execution provider, and its web
 *   build cannot read local model files under Bun — both verified by spike).
 * - Model: int8-quantized ONNX weights (~23MB) + tokenizer.json, downloaded
 *   once from Hugging Face into config.activation.rerank.modelsDir (pinned to
 *   a revision and checksum for the default model), then fully offline.
 *
 * Default model: Xenova/ms-marco-MiniLM-L-6-v2 — the classic English MS MARCO
 * passage reranker. Set RERANK_MODEL to a multilingual ONNX cross-encoder
 * (with an onnx/model_quantized.onnx artifact) for non-English knowledge
 * bases, or to a smaller member of the family (e.g. ms-marco-MiniLM-L-4-v2).
 *
 * Latency bounds: each pair is capped (query tail, document head), pairs run
 * one at a time, and the whole call is bounded by RERANK_TIMEOUT_MS (see
 * scoreBatch).
 *
 * Failure philosophy: reranking is a precision enhancement, never a hard
 * dependency. Any failure (model download, load, inference, timeout) throws
 * from scoreBatch(), and the caller (ActivationEngine) falls back to
 * dense-only ordering — activation must never break because of the reranker.
 */

/** Where a scorer's numbers come from. Score-dependent settings are keyed by it. */
export type RerankProvider = "local" | "remote";

export interface RerankIdentity {
	provider: RerankProvider;
	/** Hugging Face repo for the local model, gateway model name for remote. */
	model: string;
}

/**
 * Minimal scorer interface used by ActivationEngine. Kept separate from
 * LocalReranker so tests can inject deterministic fakes without loading a
 * model (and without network access).
 */
export interface RerankScorer {
	/** Provider and model that produced the scores. */
	readonly identity: RerankIdentity;
	/**
	 * Score each document's relevance to the query.
	 * Returns one score per document (same order), normalized to [0, 1].
	 */
	scoreBatch(query: string, documents: string[]): Promise<number[]>;
}

// biome-ignore lint/suspicious/noExplicitAny: ort session type via dynamic import
type OrtSession = any;

interface LoadedModel {
	tokenizer: BertWordPieceTokenizer;
	session: OrtSession;
}

/** The model the reranker ships with (English-only). */
const DEFAULT_RERANK_MODEL = "Xenova/ms-marco-MiniLM-L-6-v2";

/**
 * Hugging Face revisions and sha256 checksums for models we pin. Artifacts
 * are cached at the same path as before (<modelsDir>/<model>/<file>), so an
 * existing cache is reused when its checksum matches. Other models use `main`
 * without a checksum: their revision is the user's choice.
 */
const PINNED_MODELS: Record<
	string,
	{ revision: string; sha256: Record<string, string> }
> = {
	[DEFAULT_RERANK_MODEL]: {
		revision: "a09144355adeed5f58c8ed011d209bf8ee5a1fec",
		sha256: {
			"tokenizer.json":
				"d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
			"onnx/model_quantized.onnx":
				"e9d8ebf845c413e981c175bfe49a3bfa9b3dcce2a3ba54875ee5df5a58639fbe",
		},
	},
};

/** Budget for one model download (headers and body). The model is ~23 MB. */
const MODEL_DOWNLOAD_TIMEOUT_MS = 300_000;

/** Calls that may wait behind the running scoring pass; more are refused (see scoreBatch). */
const MAX_QUEUED_CALLS = 2;

export class LocalReranker implements RerankScorer {
	private readonly modelId: string;
	private readonly timeoutMs: number;
	private readonly downloadTimeoutMs: number;

	/**
	 * Lazy load state. Tokenizer + WASM session are loaded on first
	 * scoreBatch() (or via preload() at startup) and kept warm for the
	 * process lifetime. `failed` latches permanent load failures so we don't
	 * retry a broken download on every activation call.
	 */
	private loadPromise: Promise<LoadedModel> | null = null;
	private failed = false;

	/**
	 * Scoring is single-flight: all callers share one ONNX session, and two runs
	 * interleaved on it each take N times as long, so every caller would miss its
	 * deadline together. `tail` chains the runs in FIFO order; `admitted` counts
	 * calls whose scoring has not settled (the running one and the queued ones).
	 */
	private tail: Promise<void> = Promise.resolve();
	private admitted = 0;
	/** Calls refused because the queue was full, since this instance was created. */
	skipped = 0;

	constructor(opts?: {
		model?: string;
		timeoutMs?: number;
		downloadTimeoutMs?: number;
	}) {
		this.modelId = opts?.model ?? config.activation.rerank.model;
		this.timeoutMs = opts?.timeoutMs ?? config.activation.rerank.timeoutMs;
		this.downloadTimeoutMs =
			opts?.downloadTimeoutMs ?? MODEL_DOWNLOAD_TIMEOUT_MS;
	}

	get identity(): RerankIdentity {
		return { provider: "local", model: this.modelId };
	}

	/**
	 * Start loading the model in the background without awaiting it.
	 * Intended for server startup so the first activation doesn't pay the
	 * cold-load cost. Failures are logged and latch `failed` — subsequent
	 * scoreBatch() calls throw immediately and the caller falls back.
	 */
	preload(): void {
		this.load().catch(() => {
			// Error already logged inside load()'s catch handler.
		});
	}

	private async load(): Promise<LoadedModel> {
		if (this.failed) {
			throw new Error("reranker model failed to load previously");
		}
		if (!this.loadPromise) {
			this.loadPromise = this.doLoad();
			this.loadPromise.catch((err) => {
				this.failed = true;
				logger.warn(
					`[rerank] Model load failed — reranking disabled for this process (dense-only ordering). Reason: ${err instanceof Error ? err.message : String(err)}`,
				);
			});
		}
		return this.loadPromise;
	}

	private async doLoad(): Promise<LoadedModel> {
		const start = Date.now();
		logger.log(
			`[rerank] Loading cross-encoder model "${this.modelId}" (first run downloads ~23MB into ${config.activation.rerank.modelsDir})...`,
		);

		// ── Tokenizer (vendored WordPiece, pure JS) + ONNX weights ──
		// Sequential: the tokenizer is small, and a failure here must not leave
		// the model download running in the background.
		const tokenizerBytes = await this.readArtifact("tokenizer.json");
		const modelBytes = await this.readArtifact("onnx/model_quantized.onnx");
		const tokenizer = BertWordPieceTokenizer.fromString(
			tokenizerBytes.toString("utf8"),
		);

		// ── WASM inference session (external onnxruntime-web) ──
		const ort = await import("onnxruntime-web");
		try {
			// biome-ignore lint/suspicious/noExplicitAny: env typing is partial
			const wasmEnv = (ort.env as any).wasm;
			wasmEnv.numThreads = 1; // worker spawn fails under Bun (fetch of local URL)
			// Bun's `with { type: "file" }` import attribute caches the module
			// as a path string, which breaks ORT's internal
			// `await import(mjsPath).default` in source/dev runs — it gets the
			// path string instead of the ortWasmThreaded factory, producing
			// "TypeError: c is not a function". In compiled binaries the
			// /$bunfs/ virtual path imports correctly, so we only need a
			// cache-busting query suffix for real filesystem paths.
			const mjsPath = wasmRuntimeMjs.includes("/$bunfs/")
				? wasmRuntimeMjs
				: `${wasmRuntimeMjs}?import`;
			wasmEnv.wasmPaths = { mjs: mjsPath, wasm: wasmRuntimeBin };
		} catch {
			// onnxruntime-web version differences — non-fatal.
		}
		const session = await ort.InferenceSession.create(modelBytes, {
			executionProviders: ["wasm"],
		});

		logger.log(
			`[rerank] Model loaded in ${((Date.now() - start) / 1000).toFixed(1)}s — reranking active.`,
		);
		return { tokenizer, session };
	}

	/**
	 * Bytes of one model artifact: the cached copy when it passes its pinned
	 * checksum, otherwise a fresh download. A cached copy that fails the
	 * checksum (for example a file cut short by an older version) is replaced
	 * instead of being loaded forever.
	 */
	private async readArtifact(relPath: string): Promise<Buffer> {
		const localPath = join(
			config.activation.rerank.modelsDir,
			this.modelId,
			relPath,
		);
		const expected = PINNED_MODELS[this.modelId]?.sha256[relPath];
		const cached = await readFile(localPath).catch(() => null);
		if (
			cached &&
			(!expected ||
				createHash("sha256").update(cached).digest("hex") === expected)
		) {
			return cached;
		}
		if (cached) {
			logger.warn(
				`[rerank] Cached ${relPath} does not match its pinned checksum — re-downloading.`,
			);
			await rm(localPath, { force: true });
		}
		return this.download(relPath, localPath);
	}

	private async download(relPath: string, localPath: string): Promise<Buffer> {
		const pin = PINNED_MODELS[this.modelId];
		const revision = pin?.revision ?? "main";
		const url = `https://huggingface.co/${this.modelId}/resolve/${revision}/${relPath}`;
		logger.log(`[rerank] Downloading ${relPath}: ${url}`);
		const response = await fetch(url, {
			redirect: "follow",
			signal: AbortSignal.timeout(this.downloadTimeoutMs),
		});
		if (!response.ok) {
			throw new Error(
				`model artifact download failed: ${response.status} ${response.statusText} (${url})`,
			);
		}
		const bytes = Buffer.from(await response.arrayBuffer());
		const expected = pin?.sha256[relPath];
		if (
			expected &&
			createHash("sha256").update(bytes).digest("hex") !== expected
		) {
			throw new Error(
				`checksum mismatch for ${relPath} at revision ${revision} — not cached`,
			);
		}
		await mkdir(dirname(localPath), { recursive: true });
		// Write beside the target, then rename: a write cut short (disk full,
		// killed process) never leaves a partial file at the cache path.
		const tmpPath = `${localPath}.${randomUUID()}.tmp`;
		try {
			await writeFile(tmpPath, bytes);
			await rename(tmpPath, localPath);
		} catch (err) {
			await rm(tmpPath, { force: true });
			throw err;
		}
		return bytes;
	}

	/**
	 * Score each (query, document) pair; returns one sigmoid score per document,
	 * in input order.
	 *
	 * Pairs run one at a time: batching gave no throughput on single-threaded
	 * WASM and padded every pair to the longest one. The query is its last
	 * `maxQueryTokens` tokens (the end of the prompt) and each document its
	 * first `maxDocTokens` tokens, so no pair exceeds the model window.
	 *
	 * The whole call is bounded by timeoutMs. Before each run the call checks
	 * a cooperative deadline, and after each run it yields to the event loop,
	 * so timers and other requests are served during scoring. A timer also
	 * races the call, so a slow model download cannot outlast the timeout.
	 *
	 * On timeout the call rejects (no partial scores). Scores for scored and
	 * unscored candidates are not on a comparable scale, so the caller falls
	 * back to dense ordering for the whole batch.
	 *
	 * Throws on timeout and on any load/inference failure — callers are
	 * expected to catch and fall back to dense-only ordering.
	 *
	 * Runs are single-flight and FIFO: one scoring pass at a time. At most
	 * MAX_QUEUED_CALLS calls wait behind the running one; a newcomer beyond that
	 * is refused at once with RerankSkippedError. The timeout covers queue wait
	 * too, and a queued call whose deadline has passed never starts scoring.
	 */
	async scoreBatch(query: string, documents: string[]): Promise<number[]> {
		if (documents.length === 0) return [];
		// Admission is decided before any await, so refusals follow call order.
		// `admitted` includes the running call, hence the comparison.
		if (this.admitted > MAX_QUEUED_CALLS) {
			this.skipped++;
			throw new RerankSkippedError(
				`rerank busy: ${MAX_QUEUED_CALLS} queued; ${this.skipped} skipped since start`,
			);
		}

		const deadline = performance.now() + this.timeoutMs;
		const work = this.runQueued(() =>
			this.scorePairs(query, documents, deadline),
		);
		const { promise: expired, reject } = Promise.withResolvers<never>();
		const timer = setTimeout(
			() => reject(new Error(`rerank timeout after ${this.timeoutMs}ms`)),
			this.timeoutMs,
		);
		try {
			return await Promise.race([work, expired]);
		} finally {
			clearTimeout(timer);
		}
	}

	/**
	 * Run `run` once every earlier admitted run has settled. The slot is released
	 * when the run itself settles, not when its caller gives up, so a timed-out
	 * run never overlaps the next one on the shared session.
	 */
	private runQueued<T>(run: () => Promise<T>): Promise<T> {
		this.admitted++;
		const result = this.tail.then(run).finally(() => {
			this.admitted--;
		});
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private async scorePairs(
		query: string,
		documents: string[],
		deadline: number,
	): Promise<number[]> {
		// Turn check: a call that waited past its deadline must not occupy the session.
		if (performance.now() >= deadline) {
			throw new Error(`rerank timeout after ${this.timeoutMs}ms`);
		}
		const { tokenizer, session } = await this.load();
		const ort = await import("onnxruntime-web");
		const { maxQueryTokens, maxDocTokens } = config.activation.rerank;
		const queryIds = tokenizer.encodeTail(query, maxQueryTokens);

		const scores: number[] = [];
		for (const document of documents) {
			if (performance.now() >= deadline) {
				throw new Error(`rerank timeout after ${this.timeoutMs}ms`);
			}
			const features = tokenizer.batchFromIds(
				[queryIds],
				[tokenizer.encodeHead(document, maxDocTokens)],
				MAX_SEQUENCE_LENGTH,
			);
			const inputs: Record<string, unknown> = {
				input_ids: new ort.Tensor("int64", features.inputIds, features.dims),
				attention_mask: new ort.Tensor(
					"int64",
					features.attentionMask,
					features.dims,
				),
				token_type_ids: new ort.Tensor(
					"int64",
					features.tokenTypeIds,
					features.dims,
				),
			};

			// biome-ignore lint/suspicious/noExplicitAny: ort run signature with our tensor map
			const out = await session.run(inputs as any);
			const logit = (out.logits.data as Float32Array)[0];
			scores.push(1 / (1 + Math.exp(-logit)));

			// Macrotask yield: timers and other requests run between pairs.
			const { promise: yielded, resolve: yieldDone } =
				Promise.withResolvers<void>();
			setImmediate(yieldDone);
			await yielded;
		}
		return scores;
	}
}

/**
 * Composition-root factory: create the reranker when enabled in config.
 * Returns null when disabled — ActivationEngine then keeps dense-only
 * ordering with zero behavior change.
 *
 * The model load is kicked off in the background (preload) so server startup
 * is not blocked by the first-download case.
 */
export function createRerankerFromConfig(): RerankScorer | null {
	const rerank = config.activation.rerank;
	if (!rerank.enabled) {
		logger.log(
			"[rerank] Disabled (RERANK_ENABLED=false) — dense-only ordering.",
		);
		return null;
	}
	if (rerank.provider === "remote") {
		const remote = createRemoteRerankerFromConfig();
		if (remote) {
			logger.log(
				`[rerank] Enabled: provider "remote", model "${remote.identity.model}" via LLM_BASE_ENDPOINT. Each activation sends the query tail and candidate text to the gateway. Timeout ${rerank.remote.timeoutMs}ms; candidates ${rerank.candidates}; document ≤${rerank.maxDocTokens} tokens.`,
			);
			return remote;
		}
		logger.warn(
			"[rerank] RERANK_PROVIDER=remote needs LLM_BASE_ENDPOINT and LLM_API_KEY (unset or still the template placeholder) — using the local reranker.",
		);
	}
	const pin = PINNED_MODELS[rerank.model];
	logger.log(
		`[rerank] Enabled: provider "local", model "${rerank.model}" (${pin ? `pinned to ${pin.revision.slice(0, 12)}` : "unpinned, revision main"}${rerank.model === DEFAULT_RERANK_MODEL ? ", English-only" : ""}); mode: WASM, one pair per run, timeout ${rerank.timeoutMs}ms; candidates ${rerank.candidates}, query ≤${rerank.maxQueryTokens} tokens, document ≤${rerank.maxDocTokens} tokens.`,
	);
	const reranker = new LocalReranker();
	reranker.preload();
	return reranker;
}
