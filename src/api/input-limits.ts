/**
 * Size bounds for caller-supplied activation input (/activate, the Claude Code
 * hook, and the MCP activate tool). Oversized text is truncated, not rejected:
 * the head and tail are kept so the start and the most recent part both count.
 */

/** Maximum characters of a full prompt or query after truncation. */
const MAX_PROMPT_CHARS = 20_000;

/** Maximum characters of a single segment cue (every cue except the last). */
const MAX_CUE_CHARS = 2_000;

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
 * Cap each cue's length: a segment at MAX_CUE_CHARS, the last (holistic) cue at
 * MAX_PROMPT_CHARS. The cue count and the duplicate check are not done here: the
 * engine's normalizeCues (activation/activate.ts, MAX_CUES) applies them to every
 * cue list, so there is one count limit.
 */
export function boundCues(cues: readonly string[]): string[] {
	const last = cues.length - 1;
	return cues.map((cue, i) =>
		capText(cue, i === last ? MAX_PROMPT_CHARS : MAX_CUE_CHARS),
	);
}
