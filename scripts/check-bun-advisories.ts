/**
 * Fails when the Bun version pinned in `.bun-version` is affected by a published
 * security advisory of oven-sh/bun. Compiled release binaries embed the Bun
 * runtime, so a vulnerable pin ships the vulnerability to every user.
 *
 * A GitHub API outage, rate limit or network error only prints a warning: it says
 * nothing about the pin, and must not fail CI or block a release.
 *
 * Usage: bun scripts/check-bun-advisories.ts   (GH_TOKEN optional, raises rate limits)
 */
import { readFileSync } from "node:fs";

type Version = [number, number, number];

export function parseVersion(raw: string): Version {
	const m = raw
		.trim()
		.replace(/^v/, "")
		.match(/^(\d+)\.(\d+)\.(\d+)/);
	if (!m) throw new Error(`unparseable version: ${raw}`);
	return [Number(m[1]), Number(m[2]), Number(m[3])];
}

function compare(a: Version, b: Version): number {
	for (let i = 0; i < 3; i++) {
		const d = (a[i] as number) - (b[i] as number);
		if (d !== 0) return d;
	}
	return 0;
}

/** Evaluates GitHub advisory ranges such as ">= 1.3.14, < 1.4.4" (all comparators must hold). */
export function inRange(version: string, range: string): boolean {
	const v = parseVersion(version);
	return range.split(",").every((part) => {
		const m = part.trim().match(/^(<=|>=|<|>|=)?\s*(\S+)$/);
		if (!m) throw new Error(`unparseable range: ${range}`);
		const c = compare(v, parseVersion(m[2] as string));
		switch (m[1] ?? "=") {
			case "<":
				return c < 0;
			case "<=":
				return c <= 0;
			case ">":
				return c > 0;
			case ">=":
				return c >= 0;
			default:
				return c === 0;
		}
	});
}

interface Advisory {
	ghsa_id: string;
	cve_id?: string | null;
	severity?: string;
	summary?: string;
	withdrawn_at?: string | null;
	vulnerabilities?: { vulnerable_version_range?: string | null }[];
}

export function affectedBy(
	version: string,
	advisories: Advisory[],
): Advisory[] {
	return advisories.filter(
		(a) =>
			!a.withdrawn_at &&
			(a.vulnerabilities ?? []).some(
				(x) =>
					x.vulnerable_version_range &&
					inRange(version, x.vulnerable_version_range),
			),
	);
}

/** A GitHub API failure that says nothing about the pinned Bun: 5xx, rate limit or network error. */
export class AdvisoryApiUnavailable extends Error {}

export type AdvisoryCheck =
	| { status: "ok" }
	| { status: "affected"; hits: Advisory[] }
	| { status: "unavailable"; reason: string };

/** 5xx, 429, and the 403 GitHub sends when the rate limit is exhausted. */
function isTransient(res: Response): boolean {
	if (res.status >= 500 || res.status === 429) return true;
	return (
		res.status === 403 &&
		(res.headers.get("x-ratelimit-remaining") === "0" ||
			res.headers.has("retry-after"))
	);
}

/** Pages through the published oven-sh/bun advisories. Transient failures throw AdvisoryApiUnavailable. */
export async function fetchAdvisories(
	fetchImpl: typeof fetch = fetch,
): Promise<Advisory[]> {
	const headers: Record<string, string> = {
		accept: "application/vnd.github+json",
		"user-agent": "knowledge-server-bun-advisory-check",
	};
	if (process.env.GH_TOKEN)
		headers.authorization = `Bearer ${process.env.GH_TOKEN}`;
	const all: Advisory[] = [];
	for (let page = 1; page <= 10; page++) {
		let res: Response;
		try {
			res = await fetchImpl(
				`https://api.github.com/repos/oven-sh/bun/security-advisories?per_page=100&page=${page}`,
				{ headers, signal: AbortSignal.timeout(20_000) },
			);
		} catch (err) {
			throw new AdvisoryApiUnavailable(
				`request failed: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
		if (!res.ok) {
			if (isTransient(res))
				throw new AdvisoryApiUnavailable(`advisory API answered ${res.status}`);
			throw new Error(`advisory API answered ${res.status}`);
		}
		const batch = (await res.json()) as Advisory[];
		all.push(...batch);
		if (batch.length < 100) break;
	}
	return all;
}

/** Checks a version against the published advisories. An unavailable API is not a verdict. */
export async function checkPinnedBun(
	version: string,
	fetchImpl: typeof fetch = fetch,
): Promise<AdvisoryCheck> {
	let advisories: Advisory[];
	try {
		advisories = await fetchAdvisories(fetchImpl);
	} catch (err) {
		if (err instanceof AdvisoryApiUnavailable)
			return { status: "unavailable", reason: err.message };
		throw err;
	}
	const hits = affectedBy(version, advisories);
	return hits.length > 0 ? { status: "affected", hits } : { status: "ok" };
}

if (import.meta.main) {
	const version = readFileSync(
		new URL("../.bun-version", import.meta.url),
		"utf8",
	).trim();
	const result = await checkPinnedBun(version);
	if (result.status === "affected") {
		for (const a of result.hits) {
			console.error(
				`Bun ${version} is affected by ${a.ghsa_id}${a.cve_id ? ` (${a.cve_id})` : ""} [${a.severity ?? "unknown"}]: ${a.summary ?? ""}`,
			);
		}
		console.error(
			"Bump .bun-version to a release that fixes these advisories.",
		);
		process.exit(1);
	}
	if (result.status === "unavailable") {
		// A GitHub outage or rate limit must not fail CI or block the release gate.
		console.warn(
			`::warning::Could not check Bun ${version} against published advisories (${result.reason}). Re-run the check later.`,
		);
	} else {
		console.log(
			`Bun ${version}: no published oven-sh/bun advisory affects this version.`,
		);
	}
}
