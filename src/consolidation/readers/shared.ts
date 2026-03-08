import type { EpisodeMessage } from "../../types.js";

/**
 * Shared constants and utilities used by all episode readers.
 *
 * Centralised here so changes to token budgets or message formatting
 * only need to be made in one place.
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

/**
 * Format messages into a plain-text block for LLM extraction.
 * Per-message size is bounded by MAX_MESSAGE_CHARS applied by the callers.
 */
export function formatMessages(messages: EpisodeMessage[]): string {
	return messages.map((m) => `  ${m.role}: ${m.content}`).join("\n");
}
