/**
 * Host and Origin allow-list for the HTTP API.
 *
 * A DNS-rebinding page is same-origin with the server only if the browser sends
 * a Host header naming its own (attacker-controlled) hostname, so every request
 * must carry a Host that names this machine. Cross-origin browser requests carry
 * an Origin header that must also name an allowed host.
 *
 * Allowed names: localhost, 127.0.0.1, [::1], the configured bind host, and the
 * entries of KNOWLEDGE_ALLOWED_HOSTS. An entry without a port matches any port
 * (so reverse proxies on default ports work); an entry with `:port` matches
 * only that port.
 */

export interface HostAllowlist {
	/** True when a raw Host header value names an allowed host. */
	matchesHost(hostHeader: string): boolean;
	/** True when a raw Origin header value is an http(s) origin on an allowed host. */
	matchesOrigin(origin: string): boolean;
	/**
	 * Exact Host header values for the MCP SDK's DNS-rebinding check, which only
	 * supports string equality. Covers each allowed name bare and with `:port`.
	 */
	sdkHostHeaders(port: number): string[];
}

interface HostEntry {
	/** Lower-case hostname without IPv6 brackets. */
	name: string;
	port: number | null;
}

const LOOPBACK_NAMES = ["localhost", "127.0.0.1", "[::1]"];

// Hostname label sequence or bracketed IPv6 literal, optional port. No userinfo,
// path, or whitespace is accepted, so the value cannot be re-interpreted.
const HOST_RE =
	/^(\[[0-9a-f:.]+\]|[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?)(?::(\d{1,5}))?$/;

function parseHostValue(raw: string): HostEntry | null {
	const m = HOST_RE.exec(raw.trim().toLowerCase());
	if (!m) return null;
	const name = m[1].replace(/^\[(.*)\]$/, "$1");
	const port = m[2] === undefined ? null : Number(m[2]);
	if (port !== null && (port < 1 || port > 65535)) return null;
	return { name, port };
}

/**
 * Build the allow-list. Throws on a malformed entry so a typo in
 * KNOWLEDGE_ALLOWED_HOSTS fails at startup rather than silently rejecting clients.
 */
export function buildHostAllowlist(
	bindHost: string,
	extraHosts: readonly string[] = [],
): HostAllowlist {
	const entries: HostEntry[] = [];
	for (const configured of [...LOOPBACK_NAMES, bindHost, ...extraHosts]) {
		// A bare IPv6 address (several colons, no brackets) has no port: bracket it.
		const raw =
			configured.split(":").length > 2 && !configured.startsWith("[")
				? `[${configured}]`
				: configured;
		const entry = parseHostValue(raw);
		if (!entry) {
			throw new Error(
				`KNOWLEDGE_ALLOWED_HOSTS contains an invalid host "${raw}" (expected name or name:port).`,
			);
		}
		entries.push(entry);
	}

	const allowlist: HostAllowlist = {
		matchesHost(hostHeader) {
			const host = parseHostValue(hostHeader);
			return (
				host !== null &&
				entries.some(
					(e) =>
						e.name === host.name && (e.port === null || e.port === host.port),
				)
			);
		},
		matchesOrigin(origin) {
			let url: URL;
			try {
				url = new URL(origin);
			} catch {
				return false;
			}
			if (url.protocol !== "http:" && url.protocol !== "https:") return false;
			return allowlist.matchesHost(url.host);
		},
		sdkHostHeaders(port) {
			const values = new Set<string>();
			for (const e of entries) {
				const h = e.name.includes(":") ? `[${e.name}]` : e.name;
				if (e.port !== null) {
					values.add(`${h}:${e.port}`);
				} else {
					values.add(h);
					values.add(`${h}:${port}`);
				}
			}
			return [...values];
		},
	};
	return allowlist;
}
