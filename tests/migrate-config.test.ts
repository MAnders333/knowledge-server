import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runMigrateConfig } from "../src/commands/migrate-config";
import { defaultConfigPath } from "../src/config-file";

const mode = (p: string): number => statSync(p).mode & 0o777;

describe("migrate-config", () => {
	let home: string;
	let savedHome: string | undefined;

	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "ks-migrate-config-"));
		savedHome = process.env.HOME;
		process.env.HOME = home;
		spyOn(console, "log").mockImplementation(() => {});
		spyOn(console, "warn").mockImplementation(() => {});
	});

	afterEach(() => {
		if (savedHome === undefined) Reflect.deleteProperty(process.env, "HOME");
		else process.env.HOME = savedHome;
		rmSync(home, { recursive: true, force: true });
	});

	it("writes config.jsonc owner-only, in an owner-only directory", () => {
		runMigrateConfig();
		const path = defaultConfigPath();
		expect(existsSync(path)).toBe(true);
		expect(mode(path)).toBe(0o600);
		expect(mode(join(home, ".config", "knowledge-server"))).toBe(0o700);
	});
});
