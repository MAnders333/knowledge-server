import { config } from "./config.js";

/**
 * Secret scrubbing for transcript and extracted-entry text.
 *
 * Rules run in a fixed order: provider-specific token formats first, so a token
 * is labelled by its provider, and generic key/value rules last. Every rule
 * replaces only the secret itself with `[REDACTED:<kind>]`. Surrounding context
 * (a header name, an env var name, a URL user) is kept. Prefix-anchored rules use
 * lookbehind so the prefix is never consumed.
 */

export type RedactionResult = { text: string; counts: Record<string, number> };

const RULES: ReadonlyArray<{ kind: string; re: RegExp }> = [
	{
		kind: "pem",
		re: /-----BEGIN (?:[A-Z0-9 ]*)PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|$)/g,
	},
	{
		kind: "bearer",
		re: /(?<=Authorization:\s*(?:Bearer|Basic|Token)\s+)\S+/gi,
	},
	{ kind: "bearer", re: /(?<=\bBearer\s+)[A-Za-z0-9._~+/=-]{16,}/g },
	{ kind: "anthropic_key", re: /(?<![A-Za-z0-9_-])sk-ant-[A-Za-z0-9_-]{20,}/g },
	{
		kind: "openai_key",
		re: /(?<![A-Za-z0-9_-])sk-(?:proj-)?[A-Za-z0-9_-]{20,}/g,
	},
	{
		kind: "github_token",
		re: /(?<![A-Za-z0-9])(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})/g,
	},
	{ kind: "gcp_api_key", re: /(?<![A-Za-z0-9])AIza[0-9A-Za-z_-]{35}/g },
	{ kind: "slack_token", re: /(?<![A-Za-z0-9])xox[abpors]-[A-Za-z0-9-]{10,}/g },
	{
		kind: "stripe_key",
		re: /(?<![A-Za-z0-9])(?:sk|pk|rk)_(?:live|test)_[A-Za-z0-9]{16,}/g,
	},
	{ kind: "npm_token", re: /(?<![A-Za-z0-9])npm_[A-Za-z0-9]{36,}/g },
	{ kind: "aws_access_key", re: /(?<![A-Za-z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}/g },
	{
		kind: "aws_secret",
		re: /(?<=aws_?secret(?:_?access)?_?key["']?\s*[:=]\s*["']?)[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])/gi,
	},
	{
		kind: "jwt",
		re: /(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\.eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}/g,
	},
	{
		kind: "url_credentials",
		re: /(?<=[a-z][a-z0-9+.-]*:\/\/[^\s:/@]+:)[^\s@/]+(?=@)/gi,
	},
	{
		kind: "env_secret",
		re: /(?<=^[ \t]*(?:export[ \t]+)?[A-Za-z0-9_]*(?:SECRET|TOKEN|PASSWORD|PASSWD|API_?KEY|PRIVATE)[A-Za-z0-9_]*=)(?!\[REDACTED:)[^\r\n]+/gim,
	},
	{
		kind: "keyed_entropy",
		re: /(?<=(?:api[_-]?key|secret|token|password|credential|auth)["']?[ \t]*[:=][ \t]*["']?)[A-Za-z0-9+/=_-]{32,}/gi,
	},
];

/**
 * Replace secret values in `text`. Pure. Returns the input unchanged with empty
 * counts when `config.redactSecrets` is false. `counts` holds only kinds with at
 * least one match.
 */
export function redactSecrets(text: string): RedactionResult {
	if (!config.redactSecrets) return { text, counts: {} };
	const counts: Record<string, number> = {};
	let out = text;
	for (const { kind, re } of RULES) {
		out = out.replace(re, () => {
			counts[kind] = (counts[kind] ?? 0) + 1;
			return `[REDACTED:${kind}]`;
		});
	}
	return { text: out, counts };
}

export function totalRedactions(counts: Record<string, number>): number {
	return Object.values(counts).reduce((sum, n) => sum + n, 0);
}

/** Counts-only summary for logs, e.g. `env_secret:2, pem:1`. Never includes content. */
export function formatRedactionCounts(counts: Record<string, number>): string {
	return Object.entries(counts)
		.sort(([a], [b]) => a.localeCompare(b))
		.map(([kind, n]) => `${kind}:${n}`)
		.join(", ");
}
