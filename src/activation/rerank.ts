import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { config } from "../config.js";
import { logger } from "../logger.js";
import { BertWordPieceTokenizer } from "./bert-tokenizer.js";

// Onnxruntime-web WASM runtime, imported with Bun's file loader:
// - source/dev runs: resolves to the real node_modules path
// - `bun build --compile` binaries: embedded into the single-file binary and
//   exposed under the virtual /$bunfs/ filesystem (readable via node:fs)
//
// Why WASM and not onnxruntime-node (native)? The native .node binding does
// not survive single-file compilation: its dependent libonnxruntime dylib is
// not embedded, so dlopen fails (verified by binary smoke test). The WASM
// backend is platform-agnostic and embeds cleanly. Trade-off: ~10x slower
// than native (~23ms/pair single-threaded vs ~2ms), i.e. ~0.9s for a 40-entry
// candidate pool — inside the activation latency budget (5s plugin timeout,
// 2s rerank timeout). WASM threading is not used: worker spawning fails under
// Bun (fetch() of local worker URL), so numThreads is pinned to 1.
//
// Paths are relative because onnxruntime-web's package `exports` does not
// expose ./dist/* — the bundler resolves and embeds them identically.
// @ts-ignore — path module declared in src/assets.d.ts
import wasmRuntimeMjs from "../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.asyncify.mjs" with { type: "file" };
// @ts-ignore — path module declared in src/assets.d.ts
import wasmRuntimeBin from "../../node_modules/onnxruntime-web/dist/ort-wasm-simd-threaded.asyncify.wasm" with { type: "file" };

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
 *   once from Hugging Face into config.activation.rerank.modelsDir, then
 *   fully offline.
 *
 * Default model: Xenova/ms-marco-MiniLM-L-6-v2 — the classic English MS MARCO
 * passage reranker. Set RERANK_MODEL to a multilingual ONNX cross-encoder
 * (with an onnx/model_quantized.onnx artifact) for non-English knowledge
 * bases, or to a smaller member of the family (e.g. ms-marco-MiniLM-L-4-v2)
 * for lower latency.
 *
 * Failure philosophy: reranking is a precision enhancement, never a hard
 * dependency. Any failure (model download, load, inference, timeout) throws
 * from scoreBatch(), and the caller (ActivationEngine) falls back to
 * dense-only ordering — activation must never break because of the reranker.
 */

/**
 * Minimal scorer interface used by ActivationEngine. Kept separate from
 * LocalReranker so tests can inject deterministic fakes without loading a
 * model (and without network access).
 */
export interface RerankScorer {
	/**
	 * Score each document's relevance to the query.
	 * Returns one score per document (same order), normalized to [0, 1].
	 */
	scoreBatch(query: string, documents: string[]): Promise<number[]>;
}

// biome-ignore lint/suspicious/noExplicitAny: ort session type via dynamic import
type OrtSession = any;

export class LocalReranker implements RerankScorer {
	private readonly modelId: string;
	private readonly timeoutMs: number;

	/**
	 * Lazy load state. Tokenizer + WASM session are loaded on first
	 * scoreBatch() (or via preload() at startup) and kept warm for the
	 * process lifetime. `failed` latches permanent load failures so we don't
	 * retry a broken download on every activation call.
	 */
	private loadPromise: Promise<{
		tokenizer: BertWordPieceTokenizer;
		session: OrtSession;
	}> | null = null;
	private failed = false;

	constructor(opts?: { model?: string; timeoutMs?: number }) {
		this.modelId = opts?.model ?? config.activation.rerank.model;
		this.timeoutMs = opts?.timeoutMs ?? config.activation.rerank.timeoutMs;
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

	private async load(): Promise<{ tokenizer: BertWordPieceTokenizer; session: OrtSession }> {
		if (this.failed) {
			throw new Error("reranker model failed to load previously");
		}
		if (!this.loadPromise) {
			this.loadPromise = this.doLoad();
			this.loadPromise.catch((err) => {
				this.failed = true;
				logger.warn(
					`[rerank] Model load failed — reranking disabled for this process (dense-only ordering). ` +
						`Reason: ${err instanceof Error ? err.message : String(err)}`,
				);
			});
		}
		return this.loadPromise;
	}

	private async doLoad(): Promise<{ tokenizer: BertWordPieceTokenizer; session: OrtSession }> {
		const start = Date.now();
		logger.log(
			`[rerank] Loading cross-encoder model "${this.modelId}" (first run downloads ~23MB into ${config.activation.rerank.modelsDir})...`,
		);

		// ── Tokenizer (vendored WordPiece, pure JS) + ONNX weights ──
		const [tokenizer, onnxPath] = await Promise.all([
			this.ensureTokenizerFile().then(async (p) =>
				BertWordPieceTokenizer.fromString(await readFile(p, "utf8")),
			),
			this.ensureOnnxFile(),
		]);

		// ── WASM inference session (external onnxruntime-web) ──
		const ort = await import("onnxruntime-web");
		try {
			// biome-ignore lint/suspicious/noExplicitAny: env typing is partial
			const wasmEnv = (ort.env as any).wasm;
			wasmEnv.numThreads = 1; // worker spawn fails under Bun (fetch of local URL)
			wasmEnv.wasmPaths = { mjs: wasmRuntimeMjs, wasm: wasmRuntimeBin };
		} catch {
			// onnxruntime-web version differences — non-fatal.
		}
		const modelBytes = await readFile(onnxPath);
		const session = await ort.InferenceSession.create(modelBytes, {
			executionProviders: ["wasm"],
		});

		logger.log(
			`[rerank] Model loaded in ${((Date.now() - start) / 1000).toFixed(1)}s — reranking active.`,
		);
		return { tokenizer, session };
	}

	/**
	 * Locate a model artifact (tokenizer.json / ONNX weights), downloading it
	 * from Hugging Face on first use. Follows the transformers.js v4 cache
	 * layout (<modelsDir>/<modelId>/<rel>) so a cache populated by other
	 * transformers.js usage is reused.
	 */
	private async ensureModelArtifact(relPath: string): Promise<string> {
		const localPath = join(
			config.activation.rerank.modelsDir,
			this.modelId,
			relPath,
		);
		if (existsSync(localPath)) return localPath;

		const url = `https://huggingface.co/${this.modelId}/resolve/main/${relPath}`;
		logger.log(`[rerank] Downloading ${relPath}: ${url}`);
		const response = await fetch(url, { redirect: "follow" });
		if (!response.ok) {
			throw new Error(
				`model artifact download failed: ${response.status} ${response.statusText} (${url})`,
			);
		}
		await mkdir(dirname(localPath), { recursive: true });
		await writeFile(localPath, Buffer.from(await response.arrayBuffer()));
		return localPath;
	}

	private ensureTokenizerFile(): Promise<string> {
		return this.ensureModelArtifact("tokenizer.json");
	}

	private ensureOnnxFile(): Promise<string> {
		return this.ensureModelArtifact("onnx/model_quantized.onnx");
	}

	/**
	 * Score all (query, document) pairs in a single batched forward pass.
	 *
	 * The model emits one logit per pair; sigmoid maps it to [0, 1].
	 *
	 * Throws on timeout (the underlying WASM inference keeps running but its
	 * result is abandoned) and on any load/inference failure — callers are
	 * expected to catch and fall back to dense-only ordering.
	 */
	async scoreBatch(query: string, documents: string[]): Promise<number[]> {
		if (documents.length === 0) return [];

		const work = (async () => {
			const { tokenizer, session } = await this.load();
			const queries = new Array(documents.length).fill(query);
			const features = tokenizer.batchEncodePairs(queries, documents);

			const ort = await import("onnxruntime-web");
			const inputs: Record<string, unknown> = {
				input_ids: new ort.Tensor(
					"int64",
					features.inputIds,
					features.dims,
				),
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
			const logits = out.logits.data as Float32Array;
			return Array.from(logits, (x) => 1 / (1 + Math.exp(-x)));
		})();

		return await new Promise<number[]>((resolve, reject) => {
			const timer = setTimeout(
				() => reject(new Error(`rerank timeout after ${this.timeoutMs}ms`)),
				this.timeoutMs,
			);
			work.then(
				(scores) => {
					clearTimeout(timer);
					resolve(scores);
				},
				(err) => {
					clearTimeout(timer);
					reject(err);
				},
			);
		});
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
export function createRerankerFromConfig(): LocalReranker | null {
	if (!config.activation.rerank.enabled) {
		logger.log("[rerank] Disabled (RERANK_ENABLED=false) — dense-only ordering.");
		return null;
	}
	const reranker = new LocalReranker();
	reranker.preload();
	return reranker;
}
