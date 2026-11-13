import { z } from "zod";
import {
	REMOTE_RERANK_MAX_CANDIDATES,
	config,
	remoteRerankIsActive,
} from "../config.js";
import { logger } from "../logger.js";
import type { RerankIdentity, RerankScorer } from "./rerank.js";

/**
 * Remote second-stage reranker: one System One call per activation (TypeSafe's
 * decision-model API, reached through the LLM gateway unless RERANK_REMOTE_URL
 * points elsewhere).
 *
 * The request follows hev/reranker (Apache-2.0; hev_rerank/prompt.yaml, version
 * generic-1, and schema/state.schema.json): the query and every candidate travel in
 * one `state`, and each candidate gets one Noul question, answered with the
 * probability that it is relevant.
 *
 * Privacy: each call sends the query tail and the head of every candidate to the
 * endpoint, which forwards them to the hosting provider of the model. This backend
 * runs only when RERANK_PROVIDER=remote is set explicitly.
 *
 * Failure philosophy matches the local reranker: any failure throws from
 * scoreBatch() and the caller keeps dense ordering. Retries: at most one, and only
 * after a 429 whose Retry-After fits in the remaining budget. A circuit breaker stops
 * calls after repeated failures, so a dead endpoint costs one timeout per cooldown
 * rather than one per activation. Health is recorded by the caller (activate.ts) only.
 */

/** Consecutive failures that open the breaker. */
const BREAKER_FAILURE_THRESHOLD = 3;
/** Time the breaker stays open before one probe call is allowed. */
const BREAKER_COOLDOWN_MS = 60_000;
/** Approximate characters per token, mapping the token caps to characters. */
const CHARS_PER_TOKEN = 4;
/** Error bodies are logged only up to this length. */
const ERROR_BODY_MAX_CHARS = 200;

/** Thrown while the breaker is open. The call never reached the endpoint. */
export class RerankSkippedError extends Error {}

/**
 * Question wording from hev/reranker prompt.yaml, version generic-1. Changing any of
 * it changes the scores, and with them the calibrated floors.
 */
const QUESTION_TEMPLATE =
	"Document `documents.{id}` is relevant to `query`: it contains information that answers or directly addresses it.";
const TRUE_CRITERION =
	"The document contains information that answers the query or directly addresses what it asks about.";
const FALSE_CRITERION =
	"The document is only loosely related, on a similar topic, or does not address what the query asks.";

/**
 * A System One answer set. Every answer must be a noul with a probability in [0, 1];
 * an answer of any other type fails the whole response. Extra noul answers are ignored.
 */
const responseSchema = z.object({
	model: z.string().nullish(),
	answers: z.record(
		z.string(),
		z.object({
			type: z.literal("noul"),
			noul: z.number().min(0).max(1),
		}),
	),
	usage: z.object({ input_tokens: z.number().nullish() }).nullish(),
});

interface RemoteResult {
	scores: number[];
	/** Model the gateway answered with (the resolved version), when it said. */
	model: string | null;
	inputTokens: number | null;
}

/** The System One body: one state with the query and the documents, one Noul per document. */
function buildRequestBody(
	model: string,
	query: string,
	documents: string[],
	maxQueryChars: number,
	maxDocChars: number,
): { ids: string[]; body: unknown } {
	// Document ids are D00, D01, ... in the order the documents were given.
	const ids = documents.map((_, i) => `D${String(i).padStart(2, "0")}`);
	const docs: Record<string, { text: string }> = {};
	const questions: Record<string, unknown> = {};
	ids.forEach((id, i) => {
		docs[id] = { text: headChars(documents[i], maxDocChars) };
		questions[id] = {
			type: "noul",
			instructions: QUESTION_TEMPLATE.replace("{id}", id),
			criteria: { true: TRUE_CRITERION, false: FALSE_CRITERION },
		};
	});
	return {
		ids,
		body: {
			model,
			state: { query: tailChars(query, maxQueryChars), documents: docs },
			questions,
		},
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
 * Read the per-document scores from a System One response. Every candidate id must
 * have a noul probability in [0, 1]. A partial answer is an error, because scores for
 * a subset cannot be ranked against dense order.
 */
function parseResponse(body: unknown, ids: string[]): RemoteResult {
	const parsed = responseSchema.safeParse(body);
	if (!parsed.success) {
		throw new Error(
			`malformed gateway response: ${describeIssue(parsed.error)}`,
		);
	}
	const { answers } = parsed.data;
	const scores = ids.map((id) => {
		const answer = answers[id];
		if (!answer) throw new Error(`gateway response has no answer for ${id}`);
		return answer.noul;
	});
	return {
		scores,
		model: parsed.data.model || null,
		inputTokens: parsed.data.usage?.input_tokens ?? null,
	};
}

function describeIssue(error: z.ZodError): string {
	const issue = error.issues[0];
	return issue
		? `${issue.path.join(".") || "(root)"}: ${issue.message}`
		: "unexpected shape";
}

/**
 * Milliseconds in a Retry-After header (delay-seconds or an HTTP-date), or null when
 * the header is absent or unreadable. Never negative.
 */
export function parseRetryAfterMs(
	value: string | null,
	nowMs: number,
): number | null {
	if (value === null) return null;
	const trimmed = value.trim();
	if (/^\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed) * 1000;
	const date = Date.parse(trimmed);
	if (Number.isNaN(date)) return null;
	return Math.max(0, date - nowMs);
}

/**
 * Bounded, content-free summary of an error body. The `detail` text is used when the
 * gateway sends one. Validation entries keep their location and message only, because
 * the rejected input they echo can be candidate text.
 */
async function summarizeErrorBody(response: Response): Promise<string> {
	const text = await response.text().catch(() => "");
	let summary = text;
	try {
		const { detail } = JSON.parse(text) as { detail?: unknown };
		if (typeof detail === "string") {
			summary = detail;
		} else if (Array.isArray(detail)) {
			summary = (detail as Array<{ loc?: unknown[]; msg?: unknown }>)
				.map(
					(item) => `${(item.loc ?? []).join(".")}: ${String(item.msg ?? "")}`,
				)
				.join("; ");
		}
	} catch {
		// Not JSON: the raw text, bounded below.
	}
	return summary.replace(/\s+/g, " ").trim().slice(0, ERROR_BODY_MAX_CHARS);
}

export interface RemoteRerankerOptions {
	/** Full System One URL, as in RERANK_REMOTE_URL. Defaults to config. */
	url?: string;
	apiKey?: string;
	model?: string;
	timeoutMs?: number;
	maxDocTokens?: number;
	maxQueryTokens?: number;
	/** Clock for the breaker cooldown and Retry-After dates. Tests inject one. */
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
		this.url = (opts.url ?? config.activation.rerank.remote.url).trim();
		this.apiKey = (
			opts.apiKey ?? config.activation.rerank.remote.apiKey
		).trim();
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
	 * Score every document in one call. Returns probabilities in input order. Throws
	 * on timeout, HTTP error, malformed or partial response, and while the breaker is
	 * open (RerankSkippedError).
	 */
	async scoreBatch(query: string, documents: string[]): Promise<number[]> {
		if (documents.length === 0) return [];
		if (documents.length > REMOTE_RERANK_MAX_CANDIDATES) {
			throw new Error(
				`remote rerank scores at most ${REMOTE_RERANK_MAX_CANDIDATES} candidates per call (got ${documents.length})`,
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
			const result = await this.request(
				query,
				documents,
				started + this.timeoutMs,
			);
			this.recordSuccess();
			logger.log(
				`[rerank] provider=remote model=${this.identity.model} resolved_model=${result.model ?? "unknown"} gate_mode=rerank candidates=${documents.length} input_tokens=${result.inputTokens ?? "unknown"} elapsed_ms=${Math.round(performance.now() - started)}`,
			);
			return result.scores;
		} catch (err) {
			this.recordFailure(err);
			throw err;
		}
	}

	/** One call, plus one retry after a 429 whose Retry-After fits in the budget. */
	private async request(
		query: string,
		documents: string[],
		deadline: number,
	): Promise<RemoteResult> {
		const { ids, body } = buildRequestBody(
			this.identity.model,
			query,
			documents,
			this.maxQueryChars,
			this.maxDocChars,
		);
		try {
			let response = await this.post(body, deadline);
			if (response.status === 429) {
				const waitMs = parseRetryAfterMs(
					response.headers.get("retry-after"),
					this.now(),
				);
				if (waitMs !== null && waitMs < deadline - performance.now()) {
					await response.text().catch(() => "");
					const { promise, resolve } = Promise.withResolvers<void>();
					setTimeout(resolve, waitMs);
					await promise;
					response = await this.post(body, deadline);
				}
			}
			if (!response.ok) {
				const detail = await summarizeErrorBody(response);
				throw new Error(
					`gateway returned HTTP ${response.status}${detail ? `: ${detail}` : ""}`,
				);
			}
			// Read as text so a timeout while reading the body surfaces as a timeout.
			const text = await response.text();
			let raw: unknown;
			try {
				raw = JSON.parse(text);
			} catch {
				throw new Error("malformed gateway response: body is not JSON");
			}
			return parseResponse(raw, ids);
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

	/** One POST. The budget is whatever remains of the call's deadline. */
	private async post(body: unknown, deadline: number): Promise<Response> {
		const remaining = deadline - performance.now();
		if (remaining <= 0) throw new Error(`timed out after ${this.timeoutMs}ms`);
		return fetch(this.url, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${this.apiKey}`,
			},
			body: JSON.stringify(body),
			signal: AbortSignal.timeout(remaining),
		});
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
 * Remote reranker from the resolved settings, or null when the remote reranker is not
 * the active scorer (see remoteRerankIsActive).
 */
export function createRemoteRerankerFromConfig(): RemoteReranker | null {
	return remoteRerankIsActive() ? new RemoteReranker() : null;
}
