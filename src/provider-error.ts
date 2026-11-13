/**
 * Summaries of provider error responses for logs, health state and MCP callers.
 *
 * A provider's error body can echo the rejected request (a 422 that lists the
 * input), so messages carry only the HTTP status and an error type or code. The
 * body itself belongs in debug logs. Callers keep the status for retry decisions.
 */

/** A plain identifier (error type or code); anything else is free text and is dropped. */
const ERROR_LABEL = /^[A-Za-z0-9_.-]{1,64}$/;

/**
 * The error type or code in a JSON error body, or null. Reads the OpenAI and
 * Anthropic shape (`error.code`, `error.type`), a top-level `type`, and the
 * FastAPI shape (`detail`, or the first item of a `detail` array).
 */
export function providerErrorLabel(body: string): string | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(body);
	} catch {
		return null;
	}
	if (typeof parsed !== "object" || parsed === null) return null;
	const top = parsed as Record<string, unknown>;
	const detail = top.detail;
	const scopes: unknown[] = [
		top.error,
		Array.isArray(detail) ? detail[0] : detail,
		top,
	];
	for (const scope of scopes) {
		if (typeof scope !== "object" || scope === null) continue;
		const { code, type } = scope as Record<string, unknown>;
		for (const value of [code, type]) {
			if (typeof value === "string" && ERROR_LABEL.test(value)) return value;
		}
	}
	return null;
}

/** ` (error: <label>)` for a provider error body, or "" when it has no label. */
export function providerErrorSuffix(body: string): string {
	const label = providerErrorLabel(body);
	return label ? ` (error: ${label})` : "";
}
