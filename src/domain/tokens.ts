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
