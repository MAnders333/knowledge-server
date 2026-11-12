/**
 * Size bounds for caller-supplied activation input (/activate, the Claude Code
 * hook, and the MCP activate tool). Oversized text is truncated, not rejected:
 * the head and tail are kept so the start and the most recent part both count.
 */

/** Maximum characters of a full prompt or query after truncation. */
export const MAX_PROMPT_CHARS = 20_000;

/** Maximum characters of a single segment cue (every cue except the last). */
export const MAX_CUE_CHARS = 2_000;

/**
 * Maximum number of cues embedded per request. The last cue is the holistic
 * query and is always kept; the others are taken from the front.
 */
export const MAX_CUE_COUNT = 16;

/** Maximum JSON body size accepted by POST routes. */
export const MAX_BODY_BYTES = 1024 * 1024;

const TRUNCATION_MARKER = "\n…\n";

/** Keep the head and tail of `text` so its result is at most `max` characters. */
export function capText(text: string, max = MAX_PROMPT_CHARS): string {
	if (text.length <= max) return text;
	const keep = max - TRUNCATION_MARKER.length;
	const headLen = Math.ceil(keep / 2);
	const tailLen = keep - headLen;
	let head = text.slice(0, headLen);
	let tail = text.slice(text.length - tailLen);
	// Never split a UTF-16 surrogate pair at either cut.
	if (/[\uD800-\uDBFF]$/.test(head)) head = head.slice(0, -1);
	if (/^[\uDC00-\uDFFF]/.test(tail)) tail = tail.slice(1);
	return `${head}${TRUNCATION_MARKER}${tail}`;
}

/**
 * Bound a cue list: at most MAX_CUE_COUNT entries (keeping the last one), each
 * segment capped at MAX_CUE_CHARS, and the last (holistic) cue capped at
 * MAX_PROMPT_CHARS.
 */
export function boundCues(cues: readonly string[]): string[] {
	if (cues.length === 0) return [];
	const kept =
		cues.length <= MAX_CUE_COUNT
			? [...cues]
			: [...cues.slice(0, MAX_CUE_COUNT - 1), cues[cues.length - 1]];
	const last = kept.length - 1;
	return kept.map((cue, i) =>
		capText(cue, i === last ? MAX_PROMPT_CHARS : MAX_CUE_CHARS),
	);
}
