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

/** Relative specifiers resolved to a path under src/, forward slashes. */
function importsOf(file: string): string[] {
	const source = readFileSync(join(SRC, file), "utf8");
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

const RULES = [
	{
		name: "storage (db/) must not import consolidation/",
		from: "db/",
		to: ["consolidation/"],
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
] as const;

/** Every import of a file that breaks a rule, as "file -> target". */
function violations(): string[] {
	const found: string[] = [];
	for (const file of sourceFiles()) {
		for (const rule of RULES) {
			if (!file.startsWith(rule.from)) continue;
			for (const target of importsOf(file)) {
				if (rule.to.some((prefix) => target.startsWith(prefix))) {
					found.push(`${file} -> ${target} (${rule.name})`);
				}
			}
		}
	}
	return found;
}

describe("layering between src/ directories", () => {
	it("has no forbidden import", () => {
		expect(violations()).toEqual([]);
	});

	it("sees the imports it checks, so the rule cannot pass vacuously", () => {
		expect(importsOf("db/store-registry.ts")).toContain("domain/domain-router");
		expect(importsOf("daemon/readers/codex.ts")).toContain("domain/untrusted");
	});
});
