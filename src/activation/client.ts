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

/** Time budget of one activation request, from request entry (see activate.ts). */
export const DEFAULT_ACTIVATION_BUDGET_MS = 4_500;

/**
 * Budgets that differ from the default, by client. Codex's hook curl gives up at 4 s,
 * so codex is answered within 3.5 s. The plugins abort at 5 s and keep the default.
 */
const ACTIVATION_BUDGET_MS_BY_CLIENT: Partial<Record<ClientTag, number>> = {
	codex: 3_500,
};

/** The request budget, in ms, for a client's /activate or hook call. */
export function activationBudgetMs(client: ClientTag): number {
	return ACTIVATION_BUDGET_MS_BY_CLIENT[client] ?? DEFAULT_ACTIVATION_BUDGET_MS;
}
