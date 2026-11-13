/**
 * Layering rules between the directories under src/ (Arch #9). Every import,
 * type-only ones and re-exports included, is checked, so a new edge that breaks a
 * rule fails here and names the file.
 */
import { describe, expect, it } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

const SRC = resolve(import.meta.dir, "../src");

/** Every .ts file under src/, as a path relative to src/ with forward slashes. */
function sourceFiles(dir = SRC): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		const full = join(dir, entry.name);
		if (entry.isDirectory()) return sourceFiles(full);
		if (!entry.name.endsWith(".ts")) return [];
		return [relative(SRC, full).split(sep).join("/")];
	});
}

/** Module specifiers of `import`, `export … from`, `import()` and side-effect imports. */
const SPECIFIER = /(?:\bfrom\s+|\bimport\s*\(\s*|\bimport\s+)["']([^"']+)["']/g;

/**
 * Relative specifiers of `source` (the text of `file`), resolved to paths under src/
 * with forward slashes. A bare directory import ("../consolidation") resolves to the
 * directory itself, with no trailing slash.
 */
function targetsIn(file: string, source: string): string[] {
	const targets: string[] = [];
	for (const match of source.matchAll(SPECIFIER)) {
		const spec = match[1];
		if (!spec.startsWith(".")) continue;
		const target = relative(SRC, resolve(dirname(join(SRC, file)), spec));
		targets.push(
			target
				.split(sep)
				.join("/")
				.replace(/\.(ts|js)$/, ""),
		);
	}
	return targets;
}

function importsOf(file: string): string[] {
	return targetsIn(file, readFileSync(join(SRC, file), "utf8"));
}

/** True when `target` is the directory `dir` (which ends in "/") or lies under it. */
function isUnder(target: string, dir: string): boolean {
	return `${target}/`.startsWith(dir);
}

const RULES = [
	{
		name: "storage (db/) must not import consolidation/",
		from: "db/",
		to: ["consolidation/"],
	},
	{
		name: "storage (db/) must not import activation/, api/ or commands/",
		from: "db/",
		to: ["activation/", "api/", "commands/"],
	},
	{
		name: "consolidation/ must not import daemon/",
		from: "consolidation/",
		to: ["daemon/"],
	},
	{
		name: "daemon/ must not import consolidation/",
		from: "daemon/",
		to: ["consolidation/"],
	},
	{
		// domain/ may import db/interface (a type) for DomainRouter's store handles.
		name: "domain/ must not import the layers above it",
		from: "domain/",
		to: ["consolidation/", "daemon/", "activation/", "api/", "commands/"],
	},
	{
		name: "domain/ must not import mcp/, services/ or auth/",
		from: "domain/",
		to: ["mcp/", "services/", "auth/"],
	},
] as const;

/** Every breach of a rule by the imports of `file`, as "file -> target (rule)". */
function breaches(file: string, targets: readonly string[]): string[] {
	const found: string[] = [];
	for (const rule of RULES) {
		if (!file.startsWith(rule.from)) continue;
		for (const target of targets) {
			if (rule.to.some((dir) => isUnder(target, dir))) {
				found.push(`${file} -> ${target} (${rule.name})`);
			}
		}
	}
	return found;
}

function violations(): string[] {
	return sourceFiles().flatMap((file) => breaches(file, importsOf(file)));
}

describe("layering between src/ directories", () => {
	it("has no forbidden import", () => {
		expect(violations()).toEqual([]);
	});

	it("sees the imports it checks, so the rule cannot pass vacuously", () => {
		expect(importsOf("db/store-registry.ts")).toContain("domain/domain-router");
		expect(importsOf("daemon/readers/codex.ts")).toContain("domain/untrusted");
	});

	it("counts a bare directory import as an import of that directory", () => {
		expect(isUnder("consolidation", "consolidation/")).toBe(true);
		expect(isUnder("consolidation/consolidate", "consolidation/")).toBe(true);
		expect(isUnder("consolidationx/a", "consolidation/")).toBe(false);
		const source = 'import { x } from "../consolidation";';
		expect(breaches("domain/a.ts", targetsIn("domain/a.ts", source))).toEqual([
			"domain/a.ts -> consolidation (domain/ must not import the layers above it)",
		]);
	});

	it("flags each new edge: db/ into activation, api and commands; domain/ into mcp, services and auth", () => {
		const cases: Array<[file: string, source: string, target: string]> = [
			["db/x.ts", 'import "../activation/a";', "activation/a"],
			["db/x.ts", 'export * from "../api/server";', "api/server"],
			[
				"db/x.ts",
				'const c = await import("../commands/setup-tool");',
				"commands/setup-tool",
			],
			["domain/x.ts", 'import type { M } from "../mcp/index";', "mcp/index"],
			["domain/x.ts", 'import { s } from "../services/s";', "services/s"],
			["domain/x.ts", 'import { a } from "../auth/a";', "auth/a"],
		];
		for (const [file, source, target] of cases) {
			expect(breaches(file, targetsIn(file, source))).toEqual([
				expect.stringContaining(`${file} -> ${target} (`),
			]);
		}
	});
});
