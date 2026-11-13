/**
 * Mask the password in a connection URI for logs, keeping scheme, user and host:
 * `postgres://user:***@host/db`.
 *
 * The password runs from the first `:` of the userinfo to the last `@` before the
 * path, so a raw `@` inside the password is masked too. A `password`, `passwd` or
 * `pwd` query parameter is masked as well. Input without a scheme is returned
 * unchanged. A raw `/` inside a password is not supported and must be
 * percent-encoded.
 */
export function redactUriCredentials(uri: string): string {
	const scheme = /^[a-z][a-z0-9+.-]*:\/\//i.exec(uri);
	if (!scheme) return uri;
	const start = scheme[0].length;
	const pathAt = uri.indexOf("/", start);
	const end = pathAt === -1 ? uri.length : pathAt;
	const authority = uri.slice(start, end);
	const at = authority.lastIndexOf("@");
	let out = uri;
	if (at !== -1) {
		const userinfo = authority.slice(0, at);
		const colon = userinfo.indexOf(":");
		if (colon !== -1) {
			out = `${uri.slice(0, start)}${userinfo.slice(0, colon)}:***${authority.slice(at)}${uri.slice(end)}`;
		}
	}
	return out.replace(/([?&;](?:password|passwd|pwd)=)[^&#;]*/gi, "$1***");
}
