import { config } from "../config.js";
import { logger } from "../logger.js";
import type { KnowledgeType } from "../types.js";

/** Texts per /embeddings request. */
const CHUNK_SIZE = 100;
/** Minimum per-attempt timeout for background bulk embedding (re-embed, backfill). */
export const BULK_EMBED_MIN_TIMEOUT_MS = 60_000;
const DEFAULT_RETRY_BASE_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;

export interface EmbedBatchOptions {
	/** Additional attempts per chunk after a retryable failure. Default 0 (interactive). */
	maxRetries?: number;
	/** Per-attempt timeout in ms. Default: the client's timeout (EMBEDDING_TIMEOUT_MS). */
	timeoutMs?: number;
}

/** Options for background bulk paths. A function, so tests that mutate config are honoured. */
export function bulkEmbedOptions(): Required<EmbedBatchOptions> {
	return {
		maxRetries: config.embedding.maxRetries,
		timeoutMs: Math.max(config.embedding.timeoutMs, BULK_EMBED_MIN_TIMEOUT_MS),
	};
}

export class EmbeddingError extends Error {
	readonly status: number | undefined;
	readonly retryable: boolean;

	constructor(
		message: string,
		opts: { retryable: boolean; status?: number; cause?: unknown },
	) {
		super(
			message,
			opts.cause !== undefined ? { cause: opts.cause } : undefined,
		);
		this.name = "EmbeddingError";
		this.status = opts.status;
		this.retryable = opts.retryable;
	}
}

/** "Name: message" for Errors (DOMException included), String() otherwise. */
export function describeError(err: unknown): string {
	return err instanceof Error ? `${err.name}: ${err.message}` : String(err);
}

/**
 * Whether a failed embedding attempt is worth repeating. Transient transport
 * failures and malformed or truncated responses are retried; other HTTP 4xx
 * and configuration mismatches are not.
 */
export function isRetryableEmbeddingError(err: unknown): boolean {
	if (err instanceof EmbeddingError) return err.retryable;
	if (!(err instanceof Error)) return false;
	// AbortSignal.timeout() rejects with TimeoutError, aborts with AbortError.
	if (err.name === "TimeoutError" || err.name === "AbortError") return true;
	// Bun fetch network failure (ECONNREFUSED, ENOTFOUND, ...) surfaces as TypeError.
	if (err instanceof TypeError) return true;
	// Truncated or non-JSON response body.
	if (err instanceof SyntaxError) return true;
	// System errors such as ECONNRESET carry a string code.
	if ("code" in err && typeof err.code === "string") return true;
	return false;
}

function isRetryableStatus(status: number): boolean {
	return status === 408 || status === 429 || status >= 500;
}

/**
 * Delay before retry number `attempt` (1-based): base * 2^(attempt-1), capped
 * at MAX_RETRY_DELAY_MS. With the default base this gives 1s, 2s, 4s, ... 30s.
 */
export function retryDelayMs(attempt: number, baseMs: number): number {
	return Math.min(baseMs * 2 ** (attempt - 1), MAX_RETRY_DELAY_MS);
}

/**
 * Resolve the sort key for one /embeddings response item. An integer `index`,
 * or a string that Number() turns into an integer ("1"), is used as given.
 * Anything else (missing on Google's item 0, fractional) falls back to the
 * array position. The value is not range-checked here: servers that count
 * from 1, or from any offset, are ordered by relative value.
 */
function resolveItemIndex(
	item: { index?: unknown } | null | undefined,
	position: number,
): number {
	const raw = item?.index;
	if (Number.isInteger(raw)) return raw as number;
	if (typeof raw === "string" && Number.isInteger(Number(raw))) {
		return Number(raw);
	}
	return position;
}

/**
 * Validate one /embeddings response chunk before its vectors are used.
 * A truncated or otherwise inconsistent response must not be allowed to
 * assign vectors to the wrong texts.
 *
 * Count and vector shape are always enforced. Ordering: when the resolved
 * indices are distinct, vectors are sorted by ascending index (relative
 * order, like the pre-#100 sort). When they are not distinct (for example
 * every item says index 0), array order is used and `indicesDistinct` is
 * false so the caller can warn.
 */
function parseEmbeddingResponse(
	body: unknown,
	expectedCount: number,
	configuredDims: number | undefined,
	batchDims: number | undefined,
): { embeddings: number[][]; indicesDistinct: boolean } {
	const data =
		typeof body === "object" &&
		body !== null &&
		Array.isArray((body as { data?: unknown }).data)
			? ((body as { data: unknown[] }).data as unknown[])
			: undefined;
	if (!data) {
		throw new EmbeddingError("Embedding API returned no data array", {
			retryable: true,
		});
	}
	if (data.length !== expectedCount) {
		throw new EmbeddingError(
			`Embedding API returned ${data.length} embeddings for ${expectedCount} inputs`,
			{ retryable: true },
		);
	}

	const items: { index: number; embedding: number[] }[] = [];
	for (let i = 0; i < expectedCount; i++) {
		const item = data[i] as
			| { index?: unknown; embedding?: unknown }
			| null
			| undefined;
		const idx = resolveItemIndex(item, i);
		const emb = item?.embedding;
		if (
			!Array.isArray(emb) ||
			emb.length === 0 ||
			!emb.every((v) => typeof v === "number" && Number.isFinite(v))
		) {
			throw new EmbeddingError(
				`Embedding API returned an invalid embedding at position ${i} (index ${idx})`,
				{ retryable: true },
			);
		}
		items.push({ index: idx, embedding: emb as number[] });
	}

	const indicesDistinct =
		new Set(items.map((it) => it.index)).size === items.length;
	if (indicesDistinct) {
		// Stable sort: equal keys cannot occur here, so this is a plain ascending order.
		items.sort((a, b) => a.index - b.index);
	}
	const embeddings = items.map((it) => it.embedding);

	// Every vector must share one length, whatever the order.
	const dims = embeddings[0].length;
	for (const emb of embeddings) {
		if (emb.length !== dims) {
			throw new EmbeddingError(
				`Embedding API returned inconsistent embedding lengths (${dims} vs ${emb.length})`,
				{ retryable: true },
			);
		}
	}
	if (configuredDims !== undefined && dims !== configuredDims) {
		throw new EmbeddingError(
			`Embedding API returned ${dims}-dimensional embeddings but EMBEDDING_DIMENSIONS=${configuredDims}. Unset EMBEDDING_DIMENSIONS or use a provider/model that honours the dimensions parameter.`,
			{ retryable: false },
		);
	}
	if (batchDims !== undefined && dims !== batchDims) {
		throw new EmbeddingError(
			`Embedding API returned ${dims}-dimensional embeddings, earlier chunks had ${batchDims}`,
			{ retryable: true },
		);
	}
	return { embeddings, indicesDistinct };
}

/**
 * Embedding client.
 *
 * All embedding APIs must be OpenAI-compatible (/v1/embeddings).
 *
 * Endpoint resolution priority:
 *   1. EMBEDDING_BASE_URL / EMBEDDING_API_KEY  — dedicated embedding endpoint
 *      (e.g. Ollama: EMBEDDING_BASE_URL=http://localhost:11434/v1)
 *   2. OPENAI_BASE_URL / OPENAI_API_KEY        — reuse OpenAI-compatible provider if set
 *   3. LLM_BASE_ENDPOINT / LLM_API_KEY         — unified proxy fallback (/openai/v1 appended)
 */
export class EmbeddingClient {
	private endpoint: string;
	private apiKey: string;
	private model: string;
	private dimensions: number | undefined;
	private timeoutMs: number;
	private retryBaseDelayMs: number;
	/** Set once a response with non-distinct indices has been warned about. */
	private duplicateIndexWarned = false;

	constructor(opts?: { timeoutMs?: number; retryBaseDelayMs?: number }) {
		// Resolve endpoint and key using the priority chain.
		if (config.embedding.baseURL) {
			// Dedicated embedding endpoint — use as-is (user provides full base URL).
			// Key: dedicated embedding key, or generic unified key. Do NOT fall back to
			// OPENAI_API_KEY here — a custom endpoint (e.g. Ollama) won't accept it.
			this.endpoint = config.embedding.baseURL;
			this.apiKey = config.embedding.apiKey || config.llm.apiKey;
		} else if (config.llm.openai.baseURL) {
			// Reuse OpenAI-compatible provider base URL + key.
			this.endpoint = config.llm.openai.baseURL;
			this.apiKey = config.llm.openai.apiKey || config.llm.apiKey;
		} else if (config.llm.baseEndpoint) {
			// Unified proxy fallback — append OpenAI-compatible path.
			this.endpoint = `${config.llm.baseEndpoint}/openai/v1`;
			this.apiKey = config.llm.apiKey;
		} else {
			// No base URL configured at all — fall back to OpenAI's public API.
			// This covers the case where OPENAI_API_KEY is set without OPENAI_BASE_URL.
			this.endpoint = "https://api.openai.com/v1";
			this.apiKey = config.llm.openai.apiKey || config.llm.apiKey;
		}
		this.model = config.embedding.model;
		this.dimensions = config.embedding.dimensions;
		this.timeoutMs = opts?.timeoutMs ?? config.embedding.timeoutMs;
		this.retryBaseDelayMs =
			opts?.retryBaseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS;
	}

	/**
	 * Generate an embedding for a single text.
	 */
	async embed(text: string): Promise<number[]> {
		const results = await this.embedBatch([text]);
		return results[0];
	}

	/**
	 * Perform one /embeddings request for a single chunk and validate the response.
	 */
	private async requestChunk(
		chunk: string[],
		timeoutMs: number,
		batchDims: number | undefined,
	): Promise<number[][]> {
		const response = await fetch(`${this.endpoint}/embeddings`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${this.apiKey}`,
			},
			body: JSON.stringify({
				model: this.model,
				input: chunk,
				// Only sent when explicitly configured - `dimensions` is only valid for
				// text-embedding-3-* models; omitting it lets the model use its default.
				...(this.dimensions !== undefined && { dimensions: this.dimensions }),
			}),
			// Hard per-call timeout. Without this, a slow or rate-limited
			// embedding proxy causes the fetch to hang indefinitely and
			// holds the TCP connection open - silently breaking /activate
			// (HTTP route + MCP tool) for the duration. See config.embedding.timeoutMs.
			signal: AbortSignal.timeout(timeoutMs),
		});

		if (!response.ok) {
			const body = await response.text();
			throw new EmbeddingError(
				`Embedding API error ${response.status}: ${body.slice(0, 500)}`,
				{
					status: response.status,
					retryable: isRetryableStatus(response.status),
				},
			);
		}

		const body: unknown = await response.json();
		const parsed = parseEmbeddingResponse(
			body,
			chunk.length,
			this.dimensions,
			batchDims,
		);
		if (!parsed.indicesDistinct && !this.duplicateIndexWarned) {
			this.duplicateIndexWarned = true;
			logger.warn(
				"[embedding] Embedding API returned duplicate indices (for example every item index 0); array order is used. Further occurrences on this client are not logged.",
			);
		}
		return parsed.embeddings;
	}

	/**
	 * Generate embeddings for multiple texts.
	 * The API may have limits on batch size - we chunk at CHUNK_SIZE.
	 *
	 * Each chunk's response is validated (count, vector shape) and ordered by
	 * index; duplicate indices fall back to array order with a warning. Retries
	 * are per chunk and off by default; pass `maxRetries` for background bulk
	 * work. Only retryable failures are repeated, with exponential backoff.
	 */
	async embedBatch(
		texts: string[],
		opts?: EmbedBatchOptions,
	): Promise<number[][]> {
		if (texts.length === 0) return [];

		const maxAttempts = 1 + Math.max(0, opts?.maxRetries ?? 0);
		const timeoutMs = opts?.timeoutMs ?? this.timeoutMs;
		const totalChunks = Math.ceil(texts.length / CHUNK_SIZE);
		const results: number[][] = [];
		let batchDims: number | undefined;

		for (let i = 0; i < texts.length; i += CHUNK_SIZE) {
			const chunk = texts.slice(i, i + CHUNK_SIZE);
			const chunkNo = i / CHUNK_SIZE + 1;
			let embeddings: number[][] | undefined;
			let lastError: unknown;
			let attemptsMade = 0;

			for (let attempt = 1; attempt <= maxAttempts; attempt++) {
				attemptsMade = attempt;
				try {
					embeddings = await this.requestChunk(chunk, timeoutMs, batchDims);
					break;
				} catch (err) {
					lastError = err;
					if (attempt >= maxAttempts || !isRetryableEmbeddingError(err)) break;
					const delay = retryDelayMs(attempt, this.retryBaseDelayMs);
					logger.warn(
						`[embedding] Chunk ${chunkNo}/${totalChunks} (${chunk.length} texts) failed on attempt ${attempt}/${maxAttempts} - retrying in ${delay / 1000}s. Error: ${describeError(err)}`,
					);
					await new Promise((resolve) => setTimeout(resolve, delay));
				}
			}

			if (!embeddings) {
				throw new EmbeddingError(
					`Embedding chunk ${chunkNo}/${totalChunks} (${chunk.length} texts) failed after ${attemptsMade} attempt(s): ${describeError(lastError)}`,
					{
						retryable: isRetryableEmbeddingError(lastError),
						status:
							lastError instanceof EmbeddingError
								? lastError.status
								: undefined,
						cause: lastError,
					},
				);
			}

			batchDims ??= embeddings[0].length;
			results.push(...embeddings);
		}

		return results;
	}
}

/**
 * Format an entry's fields into the canonical text string used for embedding.
 *
 * Single source of truth — used by both:
 * - `reconsolidate()` when embedding immediately after a merge
 * - `ensureEmbeddings()` when regenerating embeddings for entries missing one
 *
 * Keeping both paths in sync here prevents silent vector drift between a
 * freshly-merged entry and the same entry re-embedded on a later run.
 */
export function formatEmbeddingText(
	type: KnowledgeType,
	content: string,
	topics: string[],
): string {
	return `[${type}] ${content} (topics: ${topics.join(", ")})`;
}

/**
 * Compute cosine similarity between two vectors.
 * Returns a value between -1 and 1 (1 = identical, 0 = orthogonal, -1 = opposite).
 */
export function cosineSimilarity(a: number[], b: number[]): number {
	if (a.length !== b.length) {
		throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
	}

	let dotProduct = 0;
	let normA = 0;
	let normB = 0;

	for (let i = 0; i < a.length; i++) {
		dotProduct += a[i] * b[i];
		normA += a[i] * a[i];
		normB += b[i] * b[i];
	}

	const denominator = Math.sqrt(normA) * Math.sqrt(normB);
	if (denominator === 0) return 0;

	return dotProduct / denominator;
}
