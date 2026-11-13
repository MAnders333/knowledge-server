import { z } from "zod";
import {
	PLACEHOLDER_API_KEY,
	PLACEHOLDER_ENDPOINT,
	config,
} from "../config.js";
import { logger } from "../logger.js";
import type { RerankIdentity, RerankScorer } from "./rerank.js";

/**
 * Remote second-stage reranker: one listwise decision-model call per activation,
 * sent through the LLM gateway behind LLM_BASE_ENDPOINT (the same credentials as
 * the LLM and embedding clients).
 *
 * Privacy: every call sends the query tail and the candidate text to the gateway,
 * which forwards them to the hosting provider of the model. This backend runs only
 * when RERANK_PROVIDER=remote is set explicitly.
 *
 * Failure philosophy matches the local reranker: any failure throws from
 * scoreBatch() and the caller keeps dense ordering. No retries. A circuit breaker
 * stops calls after repeated failures, so a dead gateway costs one timeout per
 * cooldown rather than one per activation.
 */

/** Largest candidate list one listwise call carries. */
export const REMOTE_RERANK_MAX_DOCUMENTS = 40;
/** Consecutive failures that open the breaker. */
const BREAKER_FAILURE_THRESHOLD = 3;
/** Time the breaker stays open before one probe call is allowed. */
const BREAKER_COOLDOWN_MS = 60_000;
/** Approximate characters per token, mapping the token caps to characters. */
const CHARS_PER_TOKEN = 4;
/** Gateway error bodies are logged only up to this length. */
const ERROR_BODY_MAX_CHARS = 200;

/** Thrown while the breaker is open. The call never reached the gateway. */
export class RerankSkippedError extends Error {}

// Question wording from the decision-model benchmark template. Changing it changes the scores.
const TRUE_CRITERION =
	"The document contains information that answers the query or directly addresses what it asks about.";
const FALSE_CRITERION =
	"The document is only loosely related, on a similar topic, or does not address what the query asks.";

const completionSchema = z.object({
	choices: z
		.array(
			z.object({
				message: z.object({
					content: z.union([z.string(), z.record(z.string(), z.unknown())]),
				}),
			}),
		)
		.min(1),
	usage: z
		.object({
			prompt_tokens: z.number().nullish(),
			cost: z.number().nullish(),
		})
		.nullish(),
});

/** One answer per document id. Extra keys are ignored; scores must be probabilities. */
const answersSchema = z.record(
	z.string(),
	z.object({ noul: z.number().min(0).max(1) }),
);

interface ListwiseResult {
	scores: number[];
	promptTokens: number | null;
	costUsd: number | null;
}

/** Build the request: the benchmark's questions template, one Noul per document id. */
function buildRequestBody(
	model: string,
	query: string,
	documents: string[],
	ids: string[],
	maxQueryChars: number,
	maxDocChars: number,
) {
	const docs: Record<string, { title: string; text: string }> = {};
	const questions: Record<string, unknown> = {};
	ids.forEach((id, i) => {
		docs[id] = { title: "", text: headChars(documents[i], maxDocChars) };
		questions[id] = {
			type: "noul",
			instructions: `Document \`documents.${id}\` is relevant to \`query\`: it contains information that answers or directly addresses it.`,
			criteria: { true: TRUE_CRITERION, false: FALSE_CRITERION },
		};
	});
	return {
		model,
		messages: [
			{
				role: "user",
				content: JSON.stringify({
					query: tailChars(query, maxQueryChars),
					documents: docs,
				}),
			},
		],
		response_format: { type: "questions", questions },
	};
}

/** First `max` characters, without splitting a surrogate pair. */
function headChars(text: string, max: number): string {
	if (text.length <= max) return text;
	const end = isHighSurrogate(text.charCodeAt(max - 1)) ? max - 1 : max;
	return text.slice(0, end);
}

/** Last `max` characters, without splitting a surrogate pair. */
function tailChars(text: string, max: number): string {
	if (text.length <= max) return text;
	const start = text.length - max;
	return text.slice(isLowSurrogate(text.charCodeAt(start)) ? start + 1 : start);
}

function isHighSurrogate(code: number): boolean {
	return code >= 0xd800 && code <= 0xdbff;
}

function isLowSurrogate(code: number): boolean {
	return code >= 0xdc00 && code <= 0xdfff;
}

/**
 * Read the per-document scores from a gateway response. Lenient about the
 * wrapper (code fences around the JSON, an already-parsed message object, extra
 * fields) and strict about the scores: every id must have a probability in [0, 1].
 * A partial answer is an error, because scores for a subset cannot be ranked
 * against dense order.
 */
function parseListwiseResponse(body: unknown, ids: string[]): ListwiseResult {
	const completion = completionSchema.safeParse(body);
	if (!completion.success) {
		throw new Error(
			`malformed gateway response: ${describeIssue(completion.error)}`,
		);
	}
	const { content } = completion.data.choices[0].message;
	let raw: unknown = content;
	if (typeof content === "string") {
		try {
			raw = JSON.parse(content.trim().replace(/^```(?:json)?\s*|\s*```$/g, ""));
		} catch {
			throw new Error(
				"malformed gateway response: message content is not JSON",
			);
		}
	}
	const answers = answersSchema.safeParse(raw);
	if (!answers.success) {
		throw new Error(
			`malformed gateway response: ${describeIssue(answers.error)}`,
		);
	}
	const scores = ids.map((id) => {
		const answer = answers.data[id];
		if (!answer) throw new Error(`gateway response has no score for ${id}`);
		return answer.noul;
	});
	return {
		scores,
		promptTokens: completion.data.usage?.prompt_tokens ?? null,
		costUsd: completion.data.usage?.cost ?? null,
	};
}

function describeIssue(error: z.ZodError): string {
	const issue = error.issues[0];
	return issue
		? `${issue.path.join(".") || "(root)"}: ${issue.message}`
		: "unexpected shape";
}

export interface RemoteRerankerOptions {
	/** Gateway root, as in LLM_BASE_ENDPOINT. Defaults to config. */
	baseEndpoint?: string;
	apiKey?: string;
	model?: string;
	timeoutMs?: number;
	maxDocTokens?: number;
	maxQueryTokens?: number;
	/** Clock for the breaker cooldown. Tests inject one. */
	now?: () => number;
}

export class RemoteReranker implements RerankScorer {
	readonly identity: RerankIdentity;
	private readonly url: string;
	private readonly apiKey: string;
	private readonly timeoutMs: number;
	private readonly maxDocChars: number;
	private readonly maxQueryChars: number;
	private readonly now: () => number;

	private consecutiveFailures = 0;
	private breakerOpen = false;
	private breakerOpenUntil = 0;
	private lastError = "";

	constructor(opts: RemoteRerankerOptions = {}) {
		const base = (opts.baseEndpoint ?? config.llm.baseEndpoint)
			.trim()
			.replace(/\/+$/, "");
		this.url = `${base}/openai/v1/chat/completions`;
		this.apiKey = (opts.apiKey ?? config.llm.apiKey).trim();
		this.identity = {
			provider: "remote",
			model: opts.model ?? config.activation.rerank.remote.model,
		};
		this.timeoutMs =
			opts.timeoutMs ?? config.activation.rerank.remote.timeoutMs;
		this.maxDocChars =
			(opts.maxDocTokens ?? config.activation.rerank.maxDocTokens) *
			CHARS_PER_TOKEN;
		this.maxQueryChars =
			(opts.maxQueryTokens ?? config.activation.rerank.maxQueryTokens) *
			CHARS_PER_TOKEN;
		this.now = opts.now ?? Date.now;
	}

	/**
	 * Score every document in one gateway call. Returns probabilities in input
	 * order. Throws on timeout, HTTP error, malformed or partial response, and
	 * while the breaker is open (RerankSkippedError).
	 */
	async scoreBatch(query: string, documents: string[]): Promise<number[]> {
		if (documents.length === 0) return [];
		if (documents.length > REMOTE_RERANK_MAX_DOCUMENTS) {
			throw new Error(
				`remote rerank scores at most ${REMOTE_RERANK_MAX_DOCUMENTS} candidates per call (got ${documents.length})`,
			);
		}
		if (this.breakerOpen) {
			const now = this.now();
			if (now < this.breakerOpenUntil) {
				throw new RerankSkippedError(
					`remote rerank paused after repeated failures (last error: ${this.lastError})`,
				);
			}
			// Cooldown over: this call is the probe. Hold other calls off until it settles.
			this.breakerOpenUntil = now + BREAKER_COOLDOWN_MS;
		}

		const started = performance.now();
		try {
			const result = await this.request(query, documents);
			this.recordSuccess();
			logger.log(
				`[rerank] provider=remote model=${this.identity.model} gate_mode=rerank candidates=${documents.length} prompt_tokens=${result.promptTokens ?? "unknown"} cost_usd=${result.costUsd ?? "unknown"} elapsed_ms=${Math.round(performance.now() - started)}`,
			);
			return result.scores;
		} catch (err) {
			this.recordFailure(err);
			throw err;
		}
	}

	private async request(
		query: string,
		documents: string[],
	): Promise<ListwiseResult> {
		const ids = documents.map((_, i) => `D${String(i).padStart(2, "0")}`);
		const body = buildRequestBody(
			this.identity.model,
			query,
			documents,
			ids,
			this.maxQueryChars,
			this.maxDocChars,
		);
		try {
			const response = await fetch(this.url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${this.apiKey}`,
				},
				body: JSON.stringify(body),
				signal: AbortSignal.timeout(this.timeoutMs),
			});
			if (!response.ok) {
				// The gateway wraps upstream errors in the body; the status alone is not enough.
				const detail = await response.text().catch(() => "");
				throw new Error(
					`gateway returned HTTP ${response.status}: ${detail.replace(/\s+/g, " ").trim().slice(0, ERROR_BODY_MAX_CHARS)}`,
				);
			}
			return parseListwiseResponse(await response.json(), ids);
		} catch (err) {
			if (
				err instanceof Error &&
				(err.name === "TimeoutError" || err.name === "AbortError")
			) {
				throw new Error(`timed out after ${this.timeoutMs}ms`);
			}
			throw err;
		}
	}

	private recordSuccess(): void {
		this.consecutiveFailures = 0;
		if (this.breakerOpen) {
			this.breakerOpen = false;
			this.breakerOpenUntil = 0;
			logger.log(
				`[rerank] Remote rerank recovered; scoring resumed (model "${this.identity.model}").`,
			);
		}
	}

	private recordFailure(err: unknown): void {
		this.lastError = err instanceof Error ? err.message : String(err);
		this.consecutiveFailures++;
		const now = this.now();
		if (this.breakerOpen) {
			// A failed probe restarts the cooldown. The state did not change, so no new warning.
			this.breakerOpenUntil = now + BREAKER_COOLDOWN_MS;
		} else if (this.consecutiveFailures >= BREAKER_FAILURE_THRESHOLD) {
			this.breakerOpen = true;
			this.breakerOpenUntil = now + BREAKER_COOLDOWN_MS;
			logger.warn(
				`[rerank] Remote rerank paused for ${BREAKER_COOLDOWN_MS / 1000}s after ${this.consecutiveFailures} consecutive failures; dense ordering meanwhile. Last error: ${this.lastError}`,
			);
		}
	}
}

/**
 * Remote reranker from the unified gateway settings (LLM_BASE_ENDPOINT and
 * LLM_API_KEY). Returns null when either is missing or still a template placeholder.
 */
export function createRemoteRerankerFromConfig(): RemoteReranker | null {
	const endpoint = config.llm.baseEndpoint.trim();
	const apiKey = config.llm.apiKey.trim();
	if (
		!endpoint ||
		!apiKey ||
		endpoint === PLACEHOLDER_ENDPOINT ||
		apiKey === PLACEHOLDER_API_KEY
	) {
		return null;
	}
	const { candidates } = config.activation.rerank;
	if (candidates > REMOTE_RERANK_MAX_DOCUMENTS) {
		logger.warn(
			`[rerank] RERANK_CANDIDATES=${candidates} is above the remote limit of ${REMOTE_RERANK_MAX_DOCUMENTS}; larger candidate pools fall back to dense ordering.`,
		);
	}
	return new RemoteReranker();
}
