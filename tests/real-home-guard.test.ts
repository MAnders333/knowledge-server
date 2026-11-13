import { describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	REAL_HOME,
	diffSnapshots,
	snapshotHome,
} from "./support/real-home-guard.js";

// Set by the preload before any test file runs. Undefined only when this file
// runs without bunfig.toml (for example `bun test` from inside tests/).
const baseline = (globalThis as { __realHomeBaseline?: Map<string, string> })
	.__realHomeBaseline;

describe("real home guard", () => {
	it("test runs leave the real home's setup targets untouched", () => {
		expect(baseline).toBeDefined();
		expect(
			diffSnapshots(baseline ?? new Map(), snapshotHome(REAL_HOME)),
		).toEqual([]);
	});

	it("reports a created watched file", () => {
		const home = mkdtempSync(join(tmpdir(), "home-guard-"));
		try {
			mkdirSync(join(home, ".cursor"));
			const before = snapshotHome(home);
			writeFileSync(join(home, ".cursor", "mcp.json"), "{}");
			expect(diffSnapshots(before, snapshotHome(home))).toEqual([
				".cursor",
				".cursor/mcp.json",
			]);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("reports changes to a watched directory's entries", () => {
		const home = mkdtempSync(join(tmpdir(), "home-guard-"));
		try {
			mkdirSync(join(home, ".codex"));
			const before = snapshotHome(home);
			writeFileSync(join(home, ".codex", "hooks.json"), "{}");
			expect(diffSnapshots(before, snapshotHome(home))).toContain(
				".codex/hooks.json",
			);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("skips watched paths that are absent", () => {
		const home = mkdtempSync(join(tmpdir(), "home-guard-"));
		try {
			expect(snapshotHome(home).size).toBe(0);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("runs with HOME in a temp sandbox, not the real home", () => {
		expect(process.env.HOME).not.toBe(REAL_HOME);
		expect(process.env.HOME?.startsWith(tmpdir())).toBe(true);
	});

	it("reports changes under the knowledge data and config directories", () => {
		const home = mkdtempSync(join(tmpdir(), "home-guard-"));
		try {
			const dataDir = join(home, ".local", "share", "knowledge-server");
			const configDir = join(home, ".config", "knowledge-server");
			mkdirSync(dataDir, { recursive: true });
			mkdirSync(configDir, { recursive: true });
			writeFileSync(join(configDir, "config.jsonc"), "{}");
			const before = snapshotHome(home);

			writeFileSync(join(dataDir, "knowledge.db"), "");
			writeFileSync(join(configDir, "config.jsonc"), '{ "port": 4321 }');

			expect(diffSnapshots(before, snapshotHome(home))).toEqual([
				".config/knowledge-server/config.jsonc",
				".local/share/knowledge-server",
			]);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});
