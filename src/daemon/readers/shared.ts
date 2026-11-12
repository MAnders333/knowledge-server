import { config } from "../../config.js";
import { logger } from "../../logger.js";
import type { Episode, EpisodeMessage, ProcessedRange } from "../../types.js";

/**
 * Shared constants and utilities used by all episode readers.
 *
 * Centralised here so changes to token budgets, message formatting and the
 * incremental-upload rules only need to be made in one place.
 */

/**
 * Maximum tokens per episode segment (soft limit — see chunkByTokenBudget).
 * The LLM sees: system prompt + episode batch (no existing knowledge context —
 * deduplication is handled by the reconsolidation step, not extraction).
 * Keeping each episode under 50K tokens means a chunk of 10 typical episodes
 * stays well within the 200K context limit for average content.
 *
 * Fat-tail protection: consolidate.ts checks the formatted chunk size before
 * each LLM call (MAX_CHUNK_TOKENS = 150K) and splits the chunk in half if it
 * exceeds the budget. A single oversized episode is processed alone.
 *
 * Note: a single message capped at MAX_MESSAGE_CHARS (~15K tokens) can
 * occupy up to 30% of this budget on its own; the chunker places oversized
 * messages alone in their own chunk (soft-limit behaviour, intentional).
 */
export const MAX_TOKENS_PER_EPISODE = 50_000;

/**
 * Maximum characters for a fully assembled message (text + all tool outputs
 * combined). 60K chars ≈ 15K tokens — keeps the episode batch sent to the
 * extraction LLM within limits that most LLM providers can handle reliably.
 *
 * There is no separate per-tool-output cap: individual tool outputs (e.g.
 * long Confluence pages) pass through untruncated. The chunker places any
 * message that individually exceeds this limit alone in its own chunk, so
 * no content is silently dropped — it simply generates more, smaller chunks.
 */
export const MAX_MESSAGE_CHARS = 60_000;

/**
 * A session whose newest message is older than this is "idle". An unuploaded
 * tail smaller than `minSessionMessages` is held back while the session is
 * active (it may still grow into a full episode) and flushed once it is idle.
 */
export const SESSION_IDLE_FLUSH_MS = 30 * 60 * 1000;

/** True for a parsed JSON object: not null, not an array, not a scalar. */
export function isJsonObject(value: unknown): value is object {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Approximate token count from character count (1 token ~ 4 chars for ASCII).
 *
 * Note: this underestimates for CJK / emoji content where each character is
 * worth more than one token. Acceptable for a soft-limit heuristic — the LLM
 * request will succeed regardless, it will simply be slightly larger than expected.
 */
export function approxTokens(text: string): number {
	return Math.ceil(text.length / 4);
}

/**
 * Chunk messages into groups that fit within a token budget.
 *
 * Note: `maxTokens` is a soft limit. A single message that individually exceeds
 * the budget is never split — it is placed alone in its own chunk. This means
 * an individual chunk may exceed `maxTokens` when a single message is very large.
 */
export function chunkByTokenBudget(
	messages: EpisodeMessage[],
	maxTokens: number,
): EpisodeMessage[][] {
	const chunks: EpisodeMessage[][] = [];
	let currentChunk: EpisodeMessage[] = [];
	let currentTokens = 0;

	for (const msg of messages) {
		const msgTokens = approxTokens(msg.content);
		if (currentTokens + msgTokens > maxTokens && currentChunk.length > 0) {
			chunks.push(currentChunk);
			currentChunk = [];
			currentTokens = 0;
		}
		currentChunk.push(msg);
		currentTokens += msgTokens;
	}

	if (currentChunk.length > 0) chunks.push(currentChunk);
	return chunks;
}

/** Label per role. Tool output is marked as third-party so it is never read as a user turn. */
const ROLE_LABELS: Record<EpisodeMessage["role"], string> = {
	user: "user",
	assistant: "assistant",
	tool: "tool output (third-party text)",
};

/**
 * Format messages into a plain-text block for LLM extraction.
 * Per-message size is bounded by MAX_MESSAGE_CHARS applied by the callers.
 */
export function formatMessages(messages: EpisodeMessage[]): string {
	return messages
		.map((m) => `  ${ROLE_LABELS[m.role]}: ${m.content}`)
		.join("\n");
}

// ── Incremental upload ────────────────────────────────────────────────────────

/** Identity fields shared by every episode produced from one session. */
export interface EpisodeBase {
	source: string;
	sessionId: string;
	sessionTitle: string;
	projectName: string;
	directory: string;
	timeCreated: number;
}

/**
 * Build an Episode from the shared session identity plus per-episode fields.
 * The only place an Episode literal is assembled inside the readers.
 */
export function episodeFrom(
	base: EpisodeBase,
	fields: Pick<
		Episode,
		| "startMessageId"
		| "endMessageId"
		| "maxMessageTime"
		| "content"
		| "contentType"
	>,
): Episode {
	return {
		...base,
		...fields,
		approxTokens: approxTokens(fields.content),
	};
}

/**
 * Index of the last message of `messages` that is already covered by
 * `processedRanges`, or -1 when none is.
 *
 * A range covers the messages up to its `endMessageId`. Ranges whose end is not
 * in `messages` (compaction summaries, messages removed from the source) are
 * ignored here; the maximum index among the rest is the watermark.
 */
export function watermarkIndex(
	messages: EpisodeMessage[],
	processedRanges: ProcessedRange[],
): number {
	if (processedRanges.length === 0) return -1;
	const indexById = new Map<string, number>();
	messages.forEach((m, i) => {
		indexById.set(m.messageId, i);
	});
	let best = -1;
	for (const range of processedRanges) {
		const i = indexById.get(range.endMessageId);
		if (i !== undefined && i > best) best = i;
	}
	return best;
}

/**
 * Chunk the part of one session that has not been uploaded yet.
 *
 * - Messages up to the watermark (see `watermarkIndex`) are dropped; only the
 *   tail is chunked. Chunks are cut from the tail, so re-running on a grown
 *   session never regenerates an already-uploaded chunk.
 * - Min size: the tail must hold at least `minMessages` messages (default
 *   `config.consolidation.minSessionMessages`). A smaller tail is held back
 *   while the session is active. Once the session is idle, a tail that has
 *   prior uploads is flushed at any size. A session with no prior uploads keeps
 *   the whole-session minimum, so ineligible sessions stay ineligible.
 * - No overlap context: each episode is extracted on its own, so repeating
 *   earlier messages would only duplicate extraction work.
 *
 * Returns message-content episodes only. Callers add their own compaction
 * episodes and run `dropProcessed` over the combined list.
 */
export function episodesFromMessages(
	base: EpisodeBase,
	messages: EpisodeMessage[],
	options: {
		processedRanges?: ProcessedRange[];
		minMessages?: number;
		now?: number;
	} = {},
): Episode[] {
	const processedRanges = options.processedRanges ?? [];
	const minMessages =
		options.minMessages ?? config.consolidation.minSessionMessages;

	const cut = watermarkIndex(messages, processedRanges);
	const tail = messages.slice(cut + 1);
	if (tail.length === 0) return [];

	const continuing = cut >= 0;
	const lastActivity = tail[tail.length - 1].timestamp;
	const now = options.now ?? Date.now();
	const flushIdle = continuing && now - lastActivity >= SESSION_IDLE_FLUSH_MS;
	if (tail.length < minMessages && !flushIdle) return [];

	const episodes: Episode[] = [];
	for (const chunk of chunkByTokenBudget(tail, MAX_TOKENS_PER_EPISODE)) {
		const content = formatMessages(chunk);
		if (!content.trim()) continue;
		episodes.push(
			episodeFrom(base, {
				startMessageId: chunk[0].messageId,
				endMessageId: chunk[chunk.length - 1].messageId,
				maxMessageTime: chunk[chunk.length - 1].timestamp,
				content,
				contentType: "messages",
			}),
		);
	}
	return episodes;
}

/**
 * Remove episodes whose exact (startMessageId, endMessageId) key is already
 * processed. Covers compaction-summary episodes, which are keyed by their
 * summary message and are regenerated on every run.
 */
export function dropProcessed(
	episodes: Episode[],
	processedRanges: ProcessedRange[],
): Episode[] {
	if (processedRanges.length === 0) return episodes;
	const keys = new Set(
		processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`),
	);
	return episodes.filter(
		(ep) => !keys.has(`${ep.startMessageId}::${ep.endMessageId}`),
	);
}

// ── Skip accounting ───────────────────────────────────────────────────────────

const skipCounts = new Map<string, number>();
const reportedSkips = new Set<string>();

/**
 * Record data a reader could not use: an unreadable session, a malformed
 * record, or a poison-pill value. The skip is counted for the daemon's cycle
 * summary; the reason is logged once per (source, item) per process so a
 * session that stays unreadable does not flood the log.
 */
export function recordSkip(
	source: string,
	item: string,
	reason: string,
	count = 1,
): void {
	skipCounts.set(source, (skipCounts.get(source) ?? 0) + count);
	const key = `${source}\u0000${item}`;
	if (reportedSkips.has(key)) return;
	reportedSkips.add(key);
	logger.warn(`[daemon/${source}] Skipped ${item}: ${reason}`);
}

/** Return and reset the number of skips recorded for `source` since the last call. */
export function takeSkipCount(source: string): number {
	const n = skipCounts.get(source) ?? 0;
	skipCounts.delete(source);
	return n;
}
