/**
 * Refuses to attach an API key to a request that would leave the machine over plain
 * http. A key sent in clear text to a non-loopback host can be read in transit, so
 * the request is not made. The error names neither the URL nor the key.
 */
export function assertKeyTransport(url: string, apiKey: string): void {
	if (!apiKey || !isPlainHttpRemote(url)) return;
	throw new Error(
		"refusing to send an API key over plain http to a non-loopback host; use https",
	);
}

/** True for an http:// URL whose host is not loopback (localhost, 127.0.0.0/8, ::1). */
function isPlainHttpRemote(url: string): boolean {
	try {
		const parsed = new URL(url);
		if (parsed.protocol !== "http:") return false;
		const host = parsed.hostname.replace(/^\[|\]$/g, "");
		return !(
			host === "localhost" ||
			host.endsWith(".localhost") ||
			host === "::1" ||
			/^127\./.test(host)
		);
	} catch {
		return false;
	}
}
