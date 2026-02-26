/**
 * Shared formatting helpers for activated knowledge entry annotations.
 * Used by both the MCP tool (src/mcp/index.ts) and the passive plugin
 * (plugin/knowledge.ts) to keep tag strings consistent.
 */

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
 * The full conflicting content is always shown — truncation would risk
 * hiding the part of the conflict that matters most to the LLM.
 * e.g. " [CONFLICTED — conflicts with: "<full content>". <caveat>]"
 */
export function contradictionTagInline(
  contradiction: { conflictingContent: string; caveat: string } | undefined
): string {
  if (!contradiction) return "";
  return ` [CONFLICTED — conflicts with: "${contradiction.conflictingContent}". ${contradiction.caveat}]`;
}

/**
 * Returns a contradiction annotation, formatted as a block (MCP tool).
 * The full conflicting content is always shown — truncation would risk
 * hiding the part of the conflict that matters most to the LLM.
 * e.g. "\n   ⚠ CONFLICTED — conflicts with: "<full content>"\n   Caveat: <caveat>"
 */
export function contradictionTagBlock(
  contradiction: { conflictingContent: string; caveat: string } | undefined
): string {
  if (!contradiction) return "";
  return `\n   ⚠ CONFLICTED — conflicts with: "${contradiction.conflictingContent}"\n   Caveat: ${contradiction.caveat}`;
}
