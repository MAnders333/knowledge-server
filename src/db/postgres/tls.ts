import { readFileSync } from "node:fs";
import { logger } from "../../logger.js";

/** TLS settings derived from a postgres:// URI, for postgres(uri, options). */
export interface PostgresTlsConfig {
	/**
	 * URI to pass to postgres(). Identical to the input unless `sslrootcert` is
	 * present, in which case that parameter is removed (see withoutQueryParam).
	 */
	uri: string;
	/**
	 * Explicit `ssl` option. Undefined means "pass nothing": the library reads
	 * `sslmode` from the URI itself. Spread it only when defined; an `ssl: undefined`
	 * key would override the URI and disable TLS.
	 */
	ssl?: { rejectUnauthorized: true; ca?: Buffer };
	/** True when the host is non-local and the URI does not request TLS. */
	warnNoTls: boolean;
}

/**
 * Map a postgres:// URI's TLS parameters onto postgres.js options.
 *
 * postgres.js already reads `sslmode` from the URI, so the URI is passed through
 * and `ssl` is only set when verification needs more than the library provides:
 * - verify-full / verify-ca: `rejectUnauthorized: true`, with the CA from
 *   `sslrootcert` when it is a file path (otherwise Node's default trust store).
 * - require + sslrootcert: verified against that CA, as libpq does.
 * - sslrootcert=system: verify-full, as the library does.
 * - Any other mode, including require without sslrootcert: unchanged.
 *
 * `sslrootcert` is removed from the URI because postgres.js forwards unknown
 * query parameters as server startup parameters, and the server rejects them
 * with "unrecognized configuration parameter". No other parameters are removed.
 *
 * Throws when a CA file is requested for verification but cannot be read.
 */
export function resolvePostgresTls(uri: string): PostgresTlsConfig {
	const qIdx = uri.indexOf("?");
	const params = new URLSearchParams(qIdx === -1 ? "" : uri.slice(qIdx + 1));
	const sslmode = params.getAll("sslmode").at(-1) || undefined;
	const sslrootcert = params.getAll("sslrootcert").at(-1);

	const verifies = isVerifyMode(sslmode);
	let ssl: PostgresTlsConfig["ssl"];
	if (sslrootcert === "system") {
		ssl = { rejectUnauthorized: true };
	} else if (sslrootcert && (verifies || sslmode === "require")) {
		ssl = {
			rejectUnauthorized: true,
			ca: readRootCert(sslrootcert, sslmode ?? "require"),
		};
	} else if (verifies) {
		ssl = { rejectUnauthorized: true };
	}

	return {
		uri:
			sslrootcert === undefined ? uri : withoutQueryParam(uri, "sslrootcert"),
		ssl,
		warnNoTls: !requestsTls(sslmode) && isNonLocalPostgresUri(uri),
	};
}

/**
 * True when any host in the URI is not local: not localhost, 127.0.0.0/8, ::1,
 * a `.local` name, or a Unix socket directory. A URI with no host uses PGHOST
 * (or localhost), as postgres.js does.
 */
export function isNonLocalPostgresUri(uri: string): boolean {
	const schemeEnd = uri.indexOf("://");
	const afterScheme = schemeEnd === -1 ? uri : uri.slice(schemeEnd + 3);
	const beforeQuery = afterScheme.split("?")[0] ?? "";
	// The last "@" ends the userinfo, so a password may contain "/" or ":".
	const authority = beforeQuery
		.slice(beforeQuery.lastIndexOf("@") + 1)
		.split("/")[0];
	const hostList = authority || process.env.PGHOST || "localhost";
	return hostList.split(",").some((entry) => !isLocalHost(hostnameOf(entry)));
}

let noTlsWarned = false;

/** Log the non-local/no-TLS warning once per process. Never logs the URI. */
export function warnNoTlsOnce(warnNoTls: boolean): void {
	if (!warnNoTls || noTlsWarned) return;
	noTlsWarned = true;
	logger.warn(
		"[postgres] connecting to a non-local host without TLS (sslmode not set). Use sslmode=verify-full for encrypted, verified connections.",
	);
}

function isVerifyMode(sslmode: string | undefined): boolean {
	return sslmode === "verify-ca" || sslmode === "verify-full";
}

/** Any sslmode other than absent, disable, allow or prefer requests TLS. */
function requestsTls(sslmode: string | undefined): boolean {
	return !(
		sslmode === undefined ||
		sslmode === "disable" ||
		sslmode === "allow" ||
		sslmode === "prefer"
	);
}

function readRootCert(path: string, mode: string): Buffer {
	try {
		return readFileSync(path);
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		throw new Error(
			`[postgres] sslmode=${mode} needs a readable sslrootcert file; cannot read "${path}": ${msg}`,
		);
	}
}

function withoutQueryParam(uri: string, name: string): string {
	const qIdx = uri.indexOf("?");
	const base = uri.slice(0, qIdx);
	const kept = uri
		.slice(qIdx + 1)
		.split("&")
		.filter(
			(seg) => seg !== "" && safeDecode(seg.split("=")[0] ?? "") !== name,
		);
	return kept.length > 0 ? `${base}?${kept.join("&")}` : base;
}

/** Hostname of one host[:port] entry from the URI authority. */
function hostnameOf(entry: string): string {
	const s = safeDecode(entry);
	if (s.startsWith("[")) {
		const end = s.indexOf("]");
		return (end === -1 ? s.slice(1) : s.slice(1, end)).toLowerCase();
	}
	// Unbracketed IPv6 has several colons and no port.
	return (s.split(":").length > 2 ? s : (s.split(":")[0] ?? "")).toLowerCase();
}

function isLocalHost(host: string): boolean {
	return (
		host === "" ||
		host === "localhost" ||
		host === "::1" ||
		host.startsWith("/") ||
		/^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host) ||
		host.endsWith(".local")
	);
}

function safeDecode(s: string): string {
	try {
		return decodeURIComponent(s);
	} catch {
		return s;
	}
}
