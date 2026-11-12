import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveUserId } from "../src/config-file";
import { currentUserId } from "../src/user-id";

/**
 * Daemon episodes and scoped reads must agree on one user_id. currentUserId()
 * is the single resolver the daemon and process-inbox use; the server reads the
 * same config.jsonc userId through StoreRegistry.
 */
describe("currentUserId", () => {
	let tempDir: string;
	let configPath: string;
	let savedEnv: string | undefined;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-user-id-"));
		configPath = join(tempDir, "config.jsonc");
		savedEnv = process.env.KNOWLEDGE_USER_ID;
		Reflect.deleteProperty(process.env, "KNOWLEDGE_USER_ID");
	});

	afterEach(() => {
		if (savedEnv === undefined)
			Reflect.deleteProperty(process.env, "KNOWLEDGE_USER_ID");
		else process.env.KNOWLEDGE_USER_ID = savedEnv;
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("uses the userId from config.jsonc when KNOWLEDGE_USER_ID is unset", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				userId: "laptop-7",
			}),
		);
		expect(currentUserId(configPath)).toBe("laptop-7");
	});

	it("KNOWLEDGE_USER_ID overrides config.jsonc", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				userId: "laptop-7",
			}),
		);
		process.env.KNOWLEDGE_USER_ID = "ci-runner";
		expect(currentUserId(configPath)).toBe("ci-runner");
	});

	it("falls back to the shared resolver when there is no config file", () => {
		expect(currentUserId(join(tempDir, "missing.jsonc"))).toBe(resolveUserId());
	});
});
