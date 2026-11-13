/**
 * Client tag for /activate. Plugins, the claude-code hook, the stdio MCP server
 * and the CLI send it as the `client` query parameter or the `x-knowledge-client`
 * header. A missing or unknown value is recorded as "unknown" (never rejected), so
 * old plugins keep working and the stored value set stays bounded.
 */
export const CLIENT_TAGS = [
	"opencode",
	"claude-code",
	"pi",
	"omp",
	"codex",
	"mcp",
	"cli",
] as const;

export type ClientTag = (typeof CLIENT_TAGS)[number] | "unknown";

export const CLIENT_HEADER = "x-knowledge-client";

/** Normalise a raw query/header value to a known client tag. */
export function parseClientTag(raw: string | null | undefined): ClientTag {
	const v = raw?.trim().toLowerCase();
	if (!v) return "unknown";
	return (CLIENT_TAGS as readonly string[]).includes(v)
		? (v as ClientTag)
		: "unknown";
}
