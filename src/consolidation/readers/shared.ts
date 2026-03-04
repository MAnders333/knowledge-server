import type { EpisodeMessage } from "../../types.js";

/**
 * Shared constants and utilities used by all episode readers.
 *
 * Centralised here so changes to token budgets or message formatting
 * only need to be made in one place.
 */

/**
 * Maximum tokens per episode segment (soft limit — see chunkByTokenBudget).
 * The LLM sees: system prompt + existing knowledge + episode batch.
 * Keeping each episode under 50K tokens means a chunk of ~10 typical episodes
 * stays well within context limits even with a large existing knowledge base.
 *
 * Note: a single message capped at MAX_MESSAGE_CHARS (~30K tokens) can
 * occupy up to 60% of this budget on its own; the chunker places oversized
 * messages alone in their own chunk (soft-limit behaviour, intentional).
 */
export const MAX_TOKENS_PER_EPISODE = 50_000;

/**
 * Maximum characters to include from a single tool output.
 * ~40K chars ≈ 10K tokens. Real Confluence pages regularly run 30–80K chars;
 * 20K was too aggressive and silently truncated most of the page content.
 * MAX_MESSAGE_CHARS caps the fully assembled message as a second guard.
 */
export const MAX_TOOL_OUTPUT_CHARS = 40_000;

/**
 * Maximum characters for a fully assembled message (text + all tool outputs).
 * Derived as 3 × MAX_TOOL_OUTPUT_CHARS (~120K chars ≈ 30K tokens) so the cap
 * automatically tracks the per-output limit. The 3× factor gives room for two
 * full tool outputs plus surrounding text without silent truncation.
 * Applied unconditionally — the guard covers both the multi-output path and
 * plain oversized user/assistant messages.
 */
export const MAX_MESSAGE_CHARS = MAX_TOOL_OUTPUT_CHARS * 3;

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
