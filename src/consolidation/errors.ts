/**
 * Classify a per-chunk failure for the attempt limit.
 *
 * Deterministic failures recur for the same episodes, so they count toward
 * dead-lettering: a rejected request (HTTP 400/413/422, e.g. an over-long prompt)
 * and data a store refuses (Postgres SQLSTATE class 22, e.g. NUL bytes). Malformed
 * model output (SyntaxError) is deterministic too. Everything else is transient:
 * rate limits, timeouts, dropped connections, expired credentials and provider
 * outages. Transient failures never dead-letter an episode; the chunk is retried
 * on a later run.
 */
export function isDeterministicFailure(err: unknown): boolean {
	if (err instanceof SyntaxError) return true;
	if (
		typeof err === "object" &&
		err !== null &&
		"code" in err &&
		typeof err.code === "string" &&
		err.code.startsWith("22")
	) {
		return true;
	}
	const status = httpStatusOf(err);
	if (status === 400 || status === 413 || status === 422) {
		// Billing and quota rejections also come back as 400; they say nothing about the content.
		return !/credit|billing|quota|balance/i.test(messageOf(err));
	}
	return false;
}

/** Find the HTTP status on the error or on the provider error it wraps (Vercel AI SDK). */
function httpStatusOf(err: unknown): number | undefined {
	let current: unknown = err;
	for (let depth = 0; depth < 4; depth++) {
		if (typeof current !== "object" || current === null) return undefined;
		if ("statusCode" in current && typeof current.statusCode === "number") {
			return current.statusCode;
		}
		current =
			"lastError" in current
				? current.lastError
				: "cause" in current
					? current.cause
					: undefined;
	}
	return undefined;
}

function messageOf(err: unknown): string {
	if (!(err instanceof Error)) return String(err);
	const inner =
		"lastError" in err && err.lastError instanceof Error
			? err.lastError.message
			: "";
	return `${err.message} ${inner}`;
}
