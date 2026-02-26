/**
 * Shared formatting helpers for activated knowledge entry annotations.
 * Used by both the MCP tool (src/mcp/index.ts) and the passive plugin
 * (plugin/knowledge.ts) to keep tag strings consistent.
 */

/** Exported so plugin/knowledge.ts can reference the same value in its local copy. */
export const CONFLICT_TRUNCATE_LEN = 100;

/**
 * Truncate a string to `maxLen` characters, appending an ellipsis only when
 * the string actually exceeds the limit.
 */
export function truncate(text: string, maxLen: number): string {
  return text.length > maxLen ? `${text.slice(0, maxLen)}…` : text;
}

/**
 * Returns a staleness annotation tag, or an empty string if not stale.
 * e.g. " [may be outdated — last accessed 47d ago]"
 */
export function staleTag(staleness: { mayBeStale: boolean; lastAccessedDaysAgo: number }): string {
  return staleness.mayBeStale
    ? ` [may be outdated — last accessed ${staleness.lastAccessedDaysAgo}d ago]`
    : "";
}

/**
 * Returns a contradiction annotation, formatted for inline use (plugin).
 * e.g. " [CONFLICTED — conflicts with: "…". <caveat>]"
 */
export function contradictionTagInline(
  contradiction: { conflictingContent: string; caveat: string } | undefined
): string {
  if (!contradiction) return "";
  return ` [CONFLICTED — conflicts with: "${truncate(contradiction.conflictingContent, CONFLICT_TRUNCATE_LEN)}". ${contradiction.caveat}]`;
}

/**
 * Returns a contradiction annotation, formatted as a block (MCP tool).
 * e.g. "\n   ⚠ CONFLICTED — conflicts with: "…"\n   Caveat: <caveat>"
 */
export function contradictionTagBlock(
  contradiction: { conflictingContent: string; caveat: string } | undefined
): string {
  if (!contradiction) return "";
  return `\n   ⚠ CONFLICTED — conflicts with: "${truncate(contradiction.conflictingContent, CONFLICT_TRUNCATE_LEN)}"\n   Caveat: ${contradiction.caveat}`;
}
