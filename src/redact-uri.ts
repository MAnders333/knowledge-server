/**
 * Mask the password in a connection URI for logs, keeping scheme, user and host:
 * `postgres://user:***@host/db`.
 *
 * The userinfo ends at the last `@` before the first `?` or `#`, so a raw `@` or
 * `/` inside the password is masked too. A `password`, `passwd` or `pwd` query
 * parameter is masked as well. Input without a scheme, or without a password, is
 * returned unchanged. A raw `?` or `#` inside a password is not supported and must
 * be percent-encoded.
 */
export function redactUriCredentials(uri: string): string {
	const scheme = /^[a-z][a-z0-9+.-]*:\/\//i.exec(uri);
	if (!scheme) return uri;
	const start = scheme[0].length;
	const queryAt = uri.slice(start).search(/[?#]/);
	const limit = queryAt === -1 ? uri.length : start + queryAt;
	const at = uri.lastIndexOf("@", limit - 1);
	const colon = uri.indexOf(":", start);
	const masked =
		at >= start && colon !== -1 && colon < at
			? `${uri.slice(0, colon)}:***${uri.slice(at)}`
			: uri;
	return maskQueryPassword(masked);
}

function maskQueryPassword(uri: string): string {
	return uri.replace(/([?&;](?:password|passwd|pwd)=)[^&#;]*/gi, "$1***");
}
