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
 *
 * A failed model call arrives as LlmCallError, whose message is sanitized. The
 * provider's own error is on its cause chain, and these functions read it there.
 * That text is only matched, never stored or shown.
 */
export function isDeterministicFailure(err: unknown): boolean {
	const chain = errorChain(err);
	if (chain.some((e) => e instanceof SyntaxError)) return true;
	if (
		chain.some(
			(e) =>
				"code" in e && typeof e.code === "string" && e.code.startsWith("22"),
		)
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

/**
 * A short label for a transient provider failure, shown in /status. It names the
 * failure class and the HTTP status only: never the provider's message or body,
 * which can echo request ids or credentials.
 */
export function providerFailureClass(err: unknown): string {
	const status = httpStatusOf(err);
	if (status === 401 || status === 403) {
		return `authentication rejected (HTTP ${status})`;
	}
	if (status === 429) return "rate limited (HTTP 429)";
	if (status === 400) return "billing or quota rejection (HTTP 400)";
	if (status !== undefined && status >= 500) {
		return `provider error (HTTP ${status})`;
	}
	const timedOut = errorChain(err).some(
		(e) =>
			e instanceof Error &&
			(e.name === "AbortError" ||
				e.name === "TimeoutError" ||
				/timed? ?out|aborted/i.test(e.message)),
	);
	if (timedOut) return "request timed out";
	return "provider unreachable";
}

/** Message/body markers of a request that will fail the same way on retry. */
const NON_RETRYABLE_MARKERS = [
	"context_length_exceeded",
	"invalid_request_error",
	"insufficient_quota",
	"invalid_argument",
	"maximum context length",
	"prompt is too long",
] as const;

/** 4xx statuses that are transient (timeout, conflict, rate limit). */
const TRANSIENT_4XX = new Set([408, 409, 429]);

/**
 * Whether a failed model call cannot succeed on an immediate retry, for complete()'s
 * retry loop. Separate from isDeterministicFailure (dead-lettering): a 401 or a billing
 * 400 is pointless to retry within seconds, but may succeed on a later run.
 *
 * Permanent: a 4xx other than 408/409/429, and any error whose message or response body
 * carries a NON_RETRYABLE_MARKERS entry, also under a 5xx status (gateways wrap an
 * upstream 400 context_length_exceeded as 503). A 408/409/429, given directly or
 * embedded in a 5xx body (e.g. {"status":429}), stays retryable unless the text says
 * insufficient_quota. Aborts, timeouts, network errors and unknown errors are retryable.
 * The provider text is only matched, never stored or shown.
 */
export function isNonRetryableLlmError(err: unknown): boolean {
	const chain = errorChain(err);
	if (chain.length === 0) return false;
	if (
		chain.some(
			(e) =>
				e instanceof Error &&
				(e.name === "AbortError" || e.name === "TimeoutError"),
		)
	) {
		return false;
	}
	const body = responseBodyOf(err);
	const text = `${messageOf(err)}\n${body}`.toLowerCase();
	const status = httpStatusOf(err);
	const embedded =
		/"(?:status|statusCode|status_code|code)"\s*:\s*"?(4\d{2})\b/.exec(body);
	const embeddedStatus = embedded ? Number(embedded[1]) : undefined;
	const effectiveStatus =
		status !== undefined && status < 500 ? status : embeddedStatus;
	if (effectiveStatus !== undefined && TRANSIENT_4XX.has(effectiveStatus)) {
		return text.includes("insufficient_quota");
	}
	if (status !== undefined && status >= 400 && status < 500) return true;
	if (NON_RETRYABLE_MARKERS.some((m) => text.includes(m))) return true;
	return embeddedStatus !== undefined;
}

/**
 * The error and the errors it wraps, outermost first: the AI SDK's lastError, then
 * cause. Depth-limited.
 */
function errorChain(err: unknown): object[] {
	const chain: object[] = [];
	let current: unknown = err;
	while (typeof current === "object" && current !== null && chain.length < 4) {
		chain.push(current);
		current =
			"lastError" in current
				? current.lastError
				: "cause" in current
					? current.cause
					: undefined;
	}
	return chain;
}

/** The HTTP status on the error or on the provider error it wraps. */
function httpStatusOf(err: unknown): number | undefined {
	for (const e of errorChain(err)) {
		if ("statusCode" in e && typeof e.statusCode === "number") {
			return e.statusCode;
		}
	}
	return undefined;
}

/** The messages down the chain, for matching only. */
function messageOf(err: unknown): string {
	const chain = errorChain(err);
	if (chain.length === 0) return String(err);
	return chain
		.filter((e): e is Error => e instanceof Error)
		.map((e) => e.message)
		.join(" ");
}

/** The provider response bodies down the chain, for matching only. */
function responseBodyOf(err: unknown): string {
	return errorChain(err)
		.map((e) =>
			"responseBody" in e && typeof e.responseBody === "string"
				? e.responseBody
				: "",
		)
		.filter(Boolean)
		.join("\n");
}
