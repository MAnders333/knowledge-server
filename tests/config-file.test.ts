import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import {
	DEFAULT_SQLITE_PATH,
	loadConfigFile,
	resolvePostgresUri,
	resolveSqlitePath,
} from "../src/config-file";

describe("loadConfigFile", () => {
	let tempDir: string;
	let configPath: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-config-test-"));
		configPath = join(tempDir, "config.jsonc");
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── File presence ──────────────────────────────────────────────────────────

	it("returns null when config file does not exist", () => {
		const result = loadConfigFile(join(tempDir, "nonexistent.jsonc"));
		expect(result).toBeNull();
	});

	// ── Happy paths ────────────────────────────────────────────────────────────

	it("parses a minimal sqlite config", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config).not.toBeNull();
		expect(config.stores).toHaveLength(1);
		expect(config.stores[0].id).toBe("main");
		expect(config.stores[0].kind).toBe("sqlite");
		expect(config.stores[0].writable).toBe(true);
	});

	it("parses a postgres config with uri", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{
						id: "team",
						kind: "postgres",
						uri: "postgres://user:pass@host:5432/db",
						writable: true,
					},
				],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config.stores[0].uri).toBe("postgres://user:pass@host:5432/db");
	});

	it("parses a multi-store config", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "personal", kind: "sqlite", writable: true },
					{
						id: "work",
						kind: "postgres",
						uri: "postgres://host/db",
						writable: false,
					},
				],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config.stores).toHaveLength(2);
		expect(config.stores[0].writable).toBe(true);
		expect(config.stores[1].writable).toBe(false);
	});

	it("strips // line comments", () => {
		writeFileSync(
			configPath,
			`{
  // this is a comment
  "stores": [
    { "id": "main", "kind": "sqlite", "writable": true } // inline comment
  ]
}`,
		);
		const config = loadConfigFile(configPath);
		expect(config.stores[0].id).toBe("main");
	});

	it("strips /* block comments */", () => {
		writeFileSync(
			configPath,
			`{
  /* block comment */
  "stores": [{ "id": "main", "kind": "sqlite", "writable": true }]
}`,
		);
		const config = loadConfigFile(configPath);
		expect(config.stores[0].id).toBe("main");
	});

	it("does not strip // inside string values", () => {
		writeFileSync(
			configPath,
			`{
  "stores": [{
    "id": "main",
    "kind": "postgres",
    "uri": "postgres://user:pass@host/db",
    "writable": true
  }]
}`,
		);
		const config = loadConfigFile(configPath);
		expect(config.stores[0].uri).toBe("postgres://user:pass@host/db");
	});

	it("throws on unterminated block comment", () => {
		writeFileSync(configPath, "{ /* unterminated");
		expect(() => loadConfigFile(configPath)).toThrow(
			/Unterminated block comment/,
		);
	});

	// ── Validation errors ──────────────────────────────────────────────────────

	it("throws when file is not valid JSON", () => {
		writeFileSync(configPath, "{ not valid json }");
		expect(() => loadConfigFile(configPath)).toThrow(/Failed to parse/);
	});

	it("throws when stores is missing", () => {
		writeFileSync(configPath, JSON.stringify({}));
		expect(() => loadConfigFile(configPath)).toThrow(
			/missing required field "stores"/,
		);
	});

	it("throws when stores is empty", () => {
		writeFileSync(configPath, JSON.stringify({ stores: [] }));
		expect(() => loadConfigFile(configPath)).toThrow(/at least one store/);
	});

	it("throws on duplicate store ids", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "main", kind: "sqlite", writable: true },
					{ id: "main", kind: "sqlite", writable: false },
				],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(/duplicate ids:.*main/);
	});

	it("throws when no writable store", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: false }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(
			/exactly one store.*writable/,
		);
	});

	it("throws when multiple writable stores", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "a", kind: "sqlite", writable: true },
					{ id: "b", kind: "sqlite", writable: true },
				],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(/2 writable stores/);
	});

	it("throws on invalid store id characters", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "My Store!", kind: "sqlite", writable: true }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(/invalid/i);
	});

	it("throws on unknown store kind", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "mysql", writable: true }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(/"kind" must be/);
	});

	it("throws when writable is not boolean", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: "yes" }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(
			/"writable" must be a boolean/,
		);
	});
});

describe("resolveSqlitePath", () => {
	it("uses path from config if provided", () => {
		const result = resolveSqlitePath({
			id: "main",
			kind: "sqlite",
			writable: true,
			path: "/custom/path.db",
		});
		expect(result).toBe("/custom/path.db");
	});

	it("falls back to DEFAULT_SQLITE_PATH if path is omitted", () => {
		const result = resolveSqlitePath({
			id: "main",
			kind: "sqlite",
			writable: true,
		});
		expect(result).toBe(DEFAULT_SQLITE_PATH);
	});
});

describe("resolvePostgresUri", () => {
	const envKey = "STORE_WORK_URI";

	afterEach(() => {
		delete process.env[envKey];
	});

	it("uses env var when set (takes precedence over config)", () => {
		process.env[envKey] = "postgres://from-env/db";
		const result = resolvePostgresUri({
			id: "work",
			kind: "postgres",
			writable: false,
			uri: "postgres://from-config/db",
		});
		expect(result).toBe("postgres://from-env/db");
	});

	it("falls back to uri field in config when env var is absent", () => {
		const result = resolvePostgresUri({
			id: "work",
			kind: "postgres",
			writable: false,
			uri: "postgres://from-config/db",
		});
		expect(result).toBe("postgres://from-config/db");
	});

	it("throws when neither env var nor uri is set", () => {
		expect(() =>
			resolvePostgresUri({ id: "work", kind: "postgres", writable: false }),
		).toThrow(/STORE_WORK_URI/);
	});

	it("normalises hyphens in id to underscores for env key", () => {
		const hyphenEnvKey = "STORE_MY_STORE_URI";
		process.env[hyphenEnvKey] = "postgres://hyphen/db";
		const result = resolvePostgresUri({
			id: "my-store",
			kind: "postgres",
			writable: false,
		});
		expect(result).toBe("postgres://hyphen/db");
		delete process.env[hyphenEnvKey];
	});
});

describe("loadConfigFile — domain and project validation", () => {
	let tempDir: string;
	let configPath: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-domain-config-test-"));
		configPath = join(tempDir, "config.jsonc");
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("parses domains and projects", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "personal", kind: "sqlite", writable: true },
					{ id: "work", kind: "sqlite", writable: false },
				],
				domains: [
					{
						id: "personal",
						description: "Personal workflows",
						store: "personal",
					},
					{ id: "work", description: "Team knowledge", store: "work" },
				],
				projects: [{ path: "~/work/project", default_domain: "work" }],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config.domains).toHaveLength(2);
		expect(config.domains[0].id).toBe("personal");
		expect(config.domains[1].store).toBe("work");
		expect(config.projects).toHaveLength(1);
		expect(config.projects[0].default_domain).toBe("work");
	});

	it("defaults to empty domains and projects when omitted", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config.domains).toHaveLength(0);
		expect(config.projects).toHaveLength(0);
	});

	it("expands ~ in project paths", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				domains: [{ id: "personal", description: "desc", store: "main" }],
				projects: [{ path: "~/personal", default_domain: "personal" }],
			}),
		);
		const config = loadConfigFile(configPath);
		expect(config.projects[0].path).not.toContain("~");
		expect(config.projects[0].path).toContain(homedir());
	});

	it("throws when domain references unknown store", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				domains: [{ id: "work", description: "desc", store: "nonexistent" }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(
			/unknown store "nonexistent"/,
		);
	});

	it("throws on duplicate domain ids", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				domains: [
					{ id: "personal", description: "d1", store: "main" },
					{ id: "personal", description: "d2", store: "main" },
				],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(
			/duplicate ids:.*personal/,
		);
	});

	it("throws when project references unknown domain", () => {
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				domains: [{ id: "personal", description: "desc", store: "main" }],
				projects: [{ path: "~/work", default_domain: "nonexistent" }],
			}),
		);
		expect(() => loadConfigFile(configPath)).toThrow(
			/unknown domain "nonexistent"/,
		);
	});

	it("allows projects with no domains defined (domain validation skipped)", () => {
		// Projects with no domains configured — default_domain validation is skipped
		// since there are no domains to validate against.
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				projects: [{ path: "~/work", default_domain: "anything" }],
			}),
		);
		// Should not throw — no domains to validate against
		expect(() => loadConfigFile(configPath)).not.toThrow();
	});
});
