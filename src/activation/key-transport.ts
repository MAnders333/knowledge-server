/**
 * True for an http:// URL whose host is not loopback (localhost, 127.0.0.0/8, ::1).
 * Startup uses it to warn when an API key would cross the network unencrypted. It
 * never refuses a request: LAN gateways often run plain http.
 */
export function isPlainHttpRemote(url: string): boolean {
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
