import { config } from "../config.js";
import { logger } from "../logger.js";

/**
 * Local in-process cross-encoder reranker.
 *
 * Second stage of the retrieve & re-rank pipeline: dense retrieval
 * (embeddings + pgvector/cosine) overfetches candidates for recall, then this
 * cross-encoder jointly encodes each (query, document) pair for a much more
 * accurate relevance signal. Runs fully in-process via onnxruntime-web (WASM)
 * through @huggingface/transformers — no network roundtrip, no vendor, no
 * per-call cost, and knowledge content never leaves the machine.
 *
 * Default model: Xenova/ms-marco-MiniLM-L-6-v2 (int8-quantized ONNX, ~23MB) —
 * the classic MS MARCO passage reranker. Measured on Apple Silicon: ~2ms/pair
 * in a single batched pass (40 candidates ≈ 90ms), well inside the activation
 * latency budget. English-biased; set RERANK_MODEL to a multilingual
 * cross-encoder (e.g. a jina/bge multilingual reranker ONNX export) if the
 * knowledge base contains substantial non-English content.
 *
 * The model is downloaded from Hugging Face on first use into
 * config.activation.rerank.modelsDir (one-time, ~23MB) and cached there.
 * After that the reranker works fully offline.
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

export class LocalReranker implements RerankScorer {
	private readonly modelId: string;
	private readonly timeoutMs: number;

	/**
	 * Lazy load state. The model+tokenizer are loaded on first scoreBatch()
	 * (or via preload() at startup) and kept warm for the process lifetime.
	 * `failed` latches permanent load failures so we don't retry a broken
	 * download on every activation call.
	 */
	private loadPromise: Promise<{
		// biome-ignore lint/suspicious/noExplicitAny: transformers.js pipeline types are heavy; runtime-validated
		tokenizer: any;
		// biome-ignore lint/suspicious/noExplicitAny: see above
		model: any;
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

	private async load() {
		if (this.failed) {
			throw new Error("reranker model failed to load previously");
		}
		if (!this.loadPromise) {
			this.loadPromise = (async () => {
				// Configured here (not at module top) so config.activation.rerank
				// is fully resolved and tests can construct the class freely.
				const { AutoTokenizer, AutoModelForSequenceClassification, env } =
					await import("@huggingface/transformers");
				env.cacheDir = config.activation.rerank.modelsDir;
				env.allowRemoteModels = true; // first run downloads from Hugging Face
				// WASM threading is unreliable under Bun — SIMD single-thread is
				// stable and fast enough (~2ms/pair measured).
				try {
					// biome-ignore lint/suspicious/noExplicitAny: env.backends typing is partial
					(env.backends as any).onnx.wasm.numThreads = 1;
				} catch {
					// Older/newer transformers.js versions may differ — non-fatal.
				}

				logger.log(
					`[rerank] Loading cross-encoder model "${this.modelId}" (first run downloads ~23MB into ${config.activation.rerank.modelsDir})...`,
				);
				const start = Date.now();
				const [tokenizer, model] = await Promise.all([
					AutoTokenizer.from_pretrained(this.modelId),
					AutoModelForSequenceClassification.from_pretrained(this.modelId, {
						// int8-quantized ONNX weights (~23MB). transformers.js v4:
						// `dtype` replaced the old `quantized: true` option.
						dtype: "q8",
					}),
				]);
				logger.log(
					`[rerank] Model loaded in ${((Date.now() - start) / 1000).toFixed(1)}s — reranking active.`,
				);
				return { tokenizer, model };
			})();

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

	/**
	 * Score all (query, document) pairs in a single batched forward pass.
	 *
	 * The model emits one logit per pair; sigmoid maps it to [0, 1]. Note the
	 * high-level transformers.js pipeline() is NOT used — it softmaxes the
	 * single logit to a useless 1.0 for every document (verified by spike).
	 *
	 * Throws on timeout (the underlying WASM inference keeps running but its
	 * result is abandoned) and on any load/inference failure — callers are
	 * expected to catch and fall back to dense-only ordering.
	 */
	async scoreBatch(query: string, documents: string[]): Promise<number[]> {
		if (documents.length === 0) return [];

		const work = (async () => {
			const { tokenizer, model } = await this.load();
			const queries = new Array(documents.length).fill(query);
			const features = await tokenizer(queries, {
				text_pair: documents,
				padding: true,
				truncation: true,
			});
			const out = await model(features);
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
