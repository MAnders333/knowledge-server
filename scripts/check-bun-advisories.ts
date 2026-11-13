/**
 * Fails when the Bun version pinned in `.bun-version` is affected by a published
 * security advisory of oven-sh/bun. Compiled release binaries embed the Bun
 * runtime, so a vulnerable pin ships the vulnerability to every user.
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

async function fetchAdvisories(): Promise<Advisory[]> {
	const headers: Record<string, string> = {
		accept: "application/vnd.github+json",
		"user-agent": "knowledge-server-bun-advisory-check",
	};
	if (process.env.GH_TOKEN)
		headers.authorization = `Bearer ${process.env.GH_TOKEN}`;
	const all: Advisory[] = [];
	for (let page = 1; page <= 10; page++) {
		const res = await fetch(
			`https://api.github.com/repos/oven-sh/bun/security-advisories?per_page=100&page=${page}`,
			{ headers, signal: AbortSignal.timeout(20_000) },
		);
		if (!res.ok) throw new Error(`advisory API answered ${res.status}`);
		const batch = (await res.json()) as Advisory[];
		all.push(...batch);
		if (batch.length < 100) break;
	}
	return all;
}

if (import.meta.main) {
	const version = readFileSync(
		new URL("../.bun-version", import.meta.url),
		"utf8",
	).trim();
	const hits = affectedBy(version, await fetchAdvisories());
	if (hits.length > 0) {
		for (const a of hits) {
			console.error(
				`Bun ${version} is affected by ${a.ghsa_id}${a.cve_id ? ` (${a.cve_id})` : ""} [${a.severity ?? "unknown"}]: ${a.summary ?? ""}`,
			);
		}
		console.error(
			"Bump .bun-version to a release that fixes these advisories.",
		);
		process.exit(1);
	}
	console.log(
		`Bun ${version}: no published oven-sh/bun advisory affects this version.`,
	);
}
