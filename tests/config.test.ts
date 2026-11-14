/**
 * Tests for the environment parse (parseEnv). parseEnv(env) is pure: each test
 * passes the exact environment it needs, so nothing depends on the process
 * environment. Parsed values are in `config`; invalid input is in `issues`,
 * which validateConfig() reports at startup.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseEnv, spawnedDaemonIntervalArgs } from "../src/config";
import { loadConfigFile } from "../src/config-file";
import { homeDir } from "../src/home";

/** A config file path that does not exist, so no data-dir override applies. */
const NO_CONFIG_FILE = join(tmpdir(), "ks-config-test-absent", "config.jsonc");

function parse(env: Record<string, string>) {
	return parseEnv(env, NO_CONFIG_FILE);
}

function issuesFor(env: Record<string, string>): string[] {
	return parse(env).issues;
}

// ── defaults and empty values ─────────────────────────────────────────────────

describe("parseEnv — defaults", () => {
	it("parses an empty environment without issues", () => {
		const { config, issues } = parse({});
		expect(issues).toEqual([]);
		expect(config.activation.rerank.remote.timeoutMs).toBe(2500);
		expect(config.activation.rerank.timeoutMs).toBe(3000);
		expect(config.activation.gate.rerankFloor).toBeNull();
		expect(config.storeInitTimeoutMs).toBe(10_000);
		expect(config.postgres.poolMax).toBe(10);
	});

	it("treats an empty value as unset", () => {
		expect(
			issuesFor({ EMBEDDING_DIMENSIONS: "", OPENAI_MAX_TOKENS_PARAM: "" }),
		).toEqual([]);
		expect(
			parse({ EMBEDDING_DIMENSIONS: "" }).config.embedding.dimensions,
		).toBe(undefined);
	});
});

// ── numeric ranges ────────────────────────────────────────────────────────────

describe("parseEnv — (0, 1] float ranges", () => {
	it("accepts 1 (inclusive upper bound) and parses it", () => {
		const { config, issues } = parse({ DECAY_ARCHIVE_THRESHOLD: "1" });
		expect(issues).toEqual([]);
		expect(config.decay.archiveThreshold).toBe(1);
	});

	it.each(["0", "1.5", "-0.2", "abc"])(
		"rejects DECAY_ARCHIVE_THRESHOLD=%s with a message naming the variable",
		(value) => {
			const issues = issuesFor({ DECAY_ARCHIVE_THRESHOLD: value });
			expect(issues).toHaveLength(1);
			expect(issues[0]).toContain(
				"DECAY_ARCHIVE_THRESHOLD must be a number in (0, 1]",
			);
		},
	);

	it("rejects ACTIVATION_SIMILARITY_THRESHOLD=0", () => {
		expect(issuesFor({ ACTIVATION_SIMILARITY_THRESHOLD: "0" })).toHaveLength(1);
	});
});

describe("parseEnv — [0, 1] float ranges (zero allowed)", () => {
	it("accepts RERANK_REL_FRAC at both ends", () => {
		expect(issuesFor({ RERANK_REL_FRAC: "0" })).toEqual([]);
		expect(issuesFor({ RERANK_REL_FRAC: "1" })).toEqual([]);
	});

	it("rejects RERANK_REL_FRAC above 1, which would empty every result", () => {
		const issues = issuesFor({ RERANK_REL_FRAC: "1.5" });
		expect(issues).toHaveLength(1);
		expect(issues[0]).toContain("RERANK_REL_FRAC must be a number in [0, 1]");
	});

	it("keeps RERANK_FLOOR=0 as a floor of 0, not as unset", () => {
		const { config, issues } = parse({ RERANK_FLOOR: "0" });
		expect(issues).toEqual([]);
		expect(config.activation.gate.rerankFloor).toBe(0);
	});

	it("rejects RERANK_FLOOR above 1 (scores are sigmoids)", () => {
		expect(issuesFor({ RERANK_FLOOR: "2" })).toHaveLength(1);
	});

	it("rejects ACTIVATION_DENSE_MARGIN above 1", () => {
		expect(issuesFor({ ACTIVATION_DENSE_MARGIN: "2" })).toHaveLength(1);
	});

	it("accepts CONTRADICTION_MIN_TOP_SIMILARITY=0 (the documented 'no floor')", () => {
		expect(issuesFor({ CONTRADICTION_MIN_TOP_SIMILARITY: "0" })).toEqual([]);
	});
});

describe("parseEnv — integer minimums", () => {
	it("rejects a rerank timeout below its 100 ms minimum instead of clamping it", () => {
		const issues = issuesFor({ RERANK_TIMEOUT_MS: "50" });
		expect(issues).toHaveLength(1);
		expect(issues[0]).toContain("RERANK_TIMEOUT_MS must be an integer ≥ 100");
	});

	it("rejects a non-numeric remote rerank timeout", () => {
		expect(issuesFor({ RERANK_REMOTE_TIMEOUT_MS: "soon" })).toHaveLength(1);
	});

	it("rejects ACTIVATION_TOKEN_BUDGET of 0", () => {
		expect(issuesFor({ ACTIVATION_TOKEN_BUDGET: "0" })).toHaveLength(1);
	});

	it("rejects STORE_INIT_TIMEOUT_MS of 0 and parses a valid value", () => {
		expect(issuesFor({ STORE_INIT_TIMEOUT_MS: "0" })).toHaveLength(1);
		expect(
			parse({ STORE_INIT_TIMEOUT_MS: "500" }).config.storeInitTimeoutMs,
		).toBe(500);
	});

	it("rejects POSTGRES_POOL_MAX of 0 and parses a valid value", () => {
		expect(issuesFor({ POSTGRES_POOL_MAX: "0" })).toHaveLength(1);
		expect(parse({ POSTGRES_POOL_MAX: "4" }).config.postgres.poolMax).toBe(4);
	});

	it("reports an invalid value once, even when a fallback chain reads it twice", () => {
		const issues = issuesFor({ LLM_TIMEOUT_MS: "abc" });
		expect(issues).toHaveLength(1);
		expect(issues[0]).toContain("LLM_TIMEOUT_MS");
	});
});

// ── cross-field: contradiction band ───────────────────────────────────────────

describe("parseEnv — contradiction scan band", () => {
	it("rejects an explicit CONTRADICTION_MIN_SIMILARITY at the default reconsolidation threshold", () => {
		const issues = issuesFor({ CONTRADICTION_MIN_SIMILARITY: "0.82" });
		expect(issues).toHaveLength(1);
		expect(issues[0]).toContain(
			"strictly below RECONSOLIDATION_SIMILARITY_THRESHOLD (0.78)",
		);
	});

	it("rejects an explicit band that a custom reconsolidation threshold empties", () => {
		const issues = issuesFor({
			RECONSOLIDATION_SIMILARITY_THRESHOLD: "0.5",
			CONTRADICTION_MIN_SIMILARITY: "0.6",
		});
		expect(issues).toHaveLength(1);
	});

	it("does not reject an unset band when the reconsolidation threshold is lowered", () => {
		expect(issuesFor({ RECONSOLIDATION_SIMILARITY_THRESHOLD: "0.3" })).toEqual(
			[],
		);
	});
});

// ── enumerations and booleans ─────────────────────────────────────────────────

describe("parseEnv — CONTRADICTION_CONFLICTS_ONLY", () => {
	it("accepts true and false, and parses them", () => {
		expect(
			parse({ CONTRADICTION_CONFLICTS_ONLY: "true" }).config.consolidation
				.contradictionConflictsOnly,
		).toBe(true);
		expect(
			parse({ CONTRADICTION_CONFLICTS_ONLY: "false" }).config.consolidation
				.contradictionConflictsOnly,
		).toBe(false);
	});

	it("rejects any other value", () => {
		const issues = issuesFor({ CONTRADICTION_CONFLICTS_ONLY: "maybe" });
		expect(issues).toHaveLength(1);
		expect(issues[0]).toContain("CONTRADICTION_CONFLICTS_ONLY");
	});
});

describe("parseEnv — OPENAI_MAX_TOKENS_PARAM", () => {
	it("accepts both names and parses them", () => {
		expect(
			parse({ OPENAI_MAX_TOKENS_PARAM: "max_tokens" }).config.llm.openai
				.maxTokensParam,
		).toBe("max_tokens");
		expect(
			parse({ OPENAI_MAX_TOKENS_PARAM: "max_completion_tokens" }).config.llm
				.openai.maxTokensParam,
		).toBe("max_completion_tokens");
	});

	it("rejects an unknown name and keeps the legacy default", () => {
		const parsed = parse({ OPENAI_MAX_TOKENS_PARAM: "max_output_tokens" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.config.llm.openai.maxTokensParam).toBe("max_tokens");
	});
});

describe("parseEnv — ACTIVATION_PRECISION_GATE and ACTIVATION_STRENGTH_DAMPENING", () => {
	it.each(["off", "false", "0", "no", "OFF", " No "])(
		"%s disables the precision gate",
		(value) => {
			const parsed = parse({ ACTIVATION_PRECISION_GATE: value });
			expect(parsed.issues).toEqual([]);
			expect(parsed.config.activation.gate.enabled).toBe(false);
		},
	);

	it.each(["on", "true", "1", "yes"])(
		"%s enables the precision gate",
		(value) => {
			expect(
				parse({ ACTIVATION_PRECISION_GATE: value }).config.activation.gate
					.enabled,
			).toBe(true);
		},
	);

	it("is on by default", () => {
		expect(parse({}).config.activation.gate.enabled).toBe(true);
	});

	it("reports an unrecognised spelling and keeps the default", () => {
		const parsed = parse({ ACTIVATION_PRECISION_GATE: "maybe" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.config.activation.gate.enabled).toBe(true);
	});

	it("reads ACTIVATION_STRENGTH_DAMPENING with the same spellings", () => {
		expect(
			parse({ ACTIVATION_STRENGTH_DAMPENING: "yes" }).config.activation.gate
				.strengthDampening,
		).toBe(true);
		expect(parse({}).config.activation.gate.strengthDampening).toBe(false);
	});
});

describe("parseEnv — EMBEDDING_DIMENSIONS", () => {
	it("accepts a positive integer", () => {
		const parsed = parse({ EMBEDDING_DIMENSIONS: "512" });
		expect(parsed.issues).toEqual([]);
		expect(parsed.config.embedding.dimensions).toBe(512);
	});

	it.each(["0", "-1", "large"])(
		"rejects %s and leaves dimensions unset",
		(value) => {
			const parsed = parse({ EMBEDDING_DIMENSIONS: value });
			expect(parsed.issues).toHaveLength(1);
			expect(parsed.issues[0]).toContain(
				"EMBEDDING_DIMENSIONS must be an integer ≥ 1",
			);
			expect(parsed.config.embedding.dimensions).toBeUndefined();
		},
	);

	it("accumulates one issue per violated constraint", () => {
		const issues = issuesFor({
			DECAY_ARCHIVE_THRESHOLD: "0",
			EMBEDDING_DIMENSIONS: "-5",
		});
		expect(issues).toHaveLength(2);
	});
});

// ── rerank provider ───────────────────────────────────────────────────────────

describe("parseEnv — RERANK_PROVIDER", () => {
	it("accepts local and remote, in any case", () => {
		for (const value of ["local", "remote", " Remote "]) {
			expect(issuesFor({ RERANK_PROVIDER: value })).toEqual([]);
		}
	});

	it("reports an unknown provider and runs as local", () => {
		const parsed = parse({ RERANK_PROVIDER: "remtoe" });
		expect(parsed.issues).toEqual([
			'RERANK_PROVIDER must be "local" or "remote" (got "remtoe").',
		]);
		expect(parsed.config.activation.rerank.provider).toBe("local");
	});
});

// ── data directory ────────────────────────────────────────────────────────────

describe("parseEnv — data directory", () => {
	let dir: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-config-data-"));
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("puts server.log, server.pid and the model cache beside the configured state DB", () => {
		const configPath = join(dir, "config.jsonc");
		const dataDir = join(dir, "data");
		mkdirSync(dataDir);
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				stateDb: { kind: "sqlite", path: join(dataDir, "state.db") },
			}),
		);

		const { config } = parseEnv({}, configPath);
		expect(config.logPath).toBe(join(dataDir, "server.log"));
		expect(config.pidPath).toBe(join(dataDir, "server.pid"));
		expect(config.activation.rerank.modelsDir).toBe(join(dataDir, "models"));
	});

	it("keeps the default location when no config file sets a state DB", () => {
		expect(parse({}).config.logPath).toBe(
			join(homeDir(), ".local", "share", "knowledge-server", "server.log"),
		);
	});
});

describe("parseEnv — per-type LLM timeouts", () => {
	it("reports a zero per-type timeout and falls back to LLM_TIMEOUT_MS", () => {
		const parsed = parse({
			LLM_TIMEOUT_EXTRACTION_MS: "0",
			LLM_TIMEOUT_MS: "45000",
		});
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain(
			"LLM_TIMEOUT_EXTRACTION_MS must be an integer ≥ 1",
		);
		expect(parsed.config.llm.timeoutsMs.extraction).toBe(45_000);
	});
});

describe("parseEnv — PID file after the data-dir move", () => {
	let dir: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-config-pid-"));
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	function customStateDbConfig(): string {
		const dataDir = join(dir, "data");
		mkdirSync(dataDir);
		const configPath = join(dir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				stateDb: { kind: "sqlite", path: join(dataDir, "state.db") },
			}),
		);
		return configPath;
	}

	it("names the PID file an earlier version wrote, when the data directory moved", () => {
		const { config } = parseEnv({}, customStateDbConfig());
		expect(config.pidPath).toBe(join(dir, "data", "server.pid"));
		expect(config.legacyPidPath).toBe(
			join(homeDir(), ".local", "share", "knowledge-server", "server.pid"),
		);
	});

	it("names no legacy PID file on the default layout or with KNOWLEDGE_PID_PATH set", () => {
		expect(parse({}).config.legacyPidPath).toBeNull();
		expect(
			parseEnv(
				{ KNOWLEDGE_PID_PATH: join(dir, "explicit.pid") },
				customStateDbConfig(),
			).config.legacyPidPath,
		).toBeNull();
	});
});

describe("daemon upload interval", () => {
	it("reads DAEMON_UPLOAD_INTERVAL_SECONDS as a positive integer", () => {
		expect(
			parse({ DAEMON_UPLOAD_INTERVAL_SECONDS: "600" }).config.daemon
				.uploadIntervalSeconds,
		).toBe(600);
		expect(parse({}).config.daemon.uploadIntervalSeconds).toBeNull();
	});

	it.each(["0", "-5", "soon"])("reports %s and treats it as unset", (value) => {
		const parsed = parse({ DAEMON_UPLOAD_INTERVAL_SECONDS: value });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain(
			"DAEMON_UPLOAD_INTERVAL_SECONDS must be an integer ≥ 1",
		);
		expect(parsed.config.daemon.uploadIntervalSeconds).toBeNull();
	});

	it("passes --interval=300 to the spawned daemon only when the variable is unset", () => {
		expect(spawnedDaemonIntervalArgs(null)).toEqual(["--interval=300"]);
		expect(spawnedDaemonIntervalArgs(600)).toEqual([]);
	});
});

describe("validateConfig — an invalid value fails startup", () => {
	it("returns the issue from the process environment, as serve does", () => {
		const configPath = new URL("../src/config.ts", import.meta.url).pathname;
		const child = Bun.spawnSync({
			cmd: [
				process.execPath,
				"-e",
				`import { validateConfig } from ${JSON.stringify(configPath)}; console.log(JSON.stringify(validateConfig()));`,
			],
			env: {
				PATH: process.env.PATH ?? "",
				HOME: process.env.HOME ?? "",
				RERANK_TIMEOUT_MS: "50",
			},
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(child.exitCode).toBe(0);
		const errors: string[] = JSON.parse(
			child.stdout.toString().trim().split("\n").pop() ?? "[]",
		);
		expect(errors).toContain(
			'RERANK_TIMEOUT_MS must be an integer ≥ 100 (got "50").',
		);
	});
});

describe("parseEnv — enable switches use the same spellings as the other flags", () => {
	it.each(["false", "0", "no", "off"])(
		"%s disables RERANK_ENABLED and a source",
		(value) => {
			const parsed = parse({ RERANK_ENABLED: value, CODEX_ENABLED: value });
			expect(parsed.issues).toEqual([]);
			expect(parsed.config.activation.rerank.enabled).toBe(false);
			expect(parsed.config.codexEnabled).toBe(false);
		},
	);

	it.each(["true", "1", "yes", "on"])("%s enables a source", (value) => {
		expect(parse({ PI_ENABLED: value }).config.piEnabled).toBe(true);
	});

	it("reports an unknown spelling instead of enabling the source", () => {
		const parsed = parse({ OMP_ENABLED: "disabled" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain("OMP_ENABLED must be one of");
		expect(parsed.config.ompEnabled).toBe(true);
	});
});

describe("parseEnv — state DB pool", () => {
	it("sizes the state DB pool from POSTGRES_POOL_MAX", () => {
		expect(parse({ POSTGRES_POOL_MAX: "4" }).config.postgres.poolMax).toBe(4);
	});
});

describe("parseEnv — REDACT_SECRETS and DAEMON_AUTO_SPAWN", () => {
	it.each(["false", "0", "no", "off"])(
		"%s disables secret redaction",
		(value) => {
			const parsed = parse({ REDACT_SECRETS: value });
			expect(parsed.issues).toEqual([]);
			expect(parsed.config.redactSecrets).toBe(false);
		},
	);

	it("redacts by default and reports an unknown spelling", () => {
		expect(parse({}).config.redactSecrets).toBe(true);
		const parsed = parse({ REDACT_SECRETS: "maybe" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain("REDACT_SECRETS must be one of");
		expect(parsed.config.redactSecrets).toBe(true);
	});

	it("reports an unknown DAEMON_AUTO_SPAWN spelling and accepts the known ones", () => {
		expect(parse({ DAEMON_AUTO_SPAWN: "off" }).issues).toEqual([]);
		expect(parse({}).issues).toEqual([]);
		const parsed = parse({ DAEMON_AUTO_SPAWN: "sometimes" });
		expect(parsed.issues[0]).toContain("DAEMON_AUTO_SPAWN must be one of");
	});
});

describe("loadConfigFile — DAEMON_AUTO_SPAWN spellings", () => {
	let dir: string;
	let saved: string | undefined;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-config-autospawn-"));
		saved = process.env.DAEMON_AUTO_SPAWN;
	});

	afterEach(() => {
		if (saved === undefined)
			Reflect.deleteProperty(process.env, "DAEMON_AUTO_SPAWN");
		else process.env.DAEMON_AUTO_SPAWN = saved;
		rmSync(dir, { recursive: true, force: true });
	});

	it("applies 0 as false over the config file, and keeps the file value for an unknown spelling", () => {
		const configPath = join(dir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				daemonAutoSpawn: true,
			}),
		);
		process.env.DAEMON_AUTO_SPAWN = "0";
		expect(loadConfigFile(configPath).daemonAutoSpawn).toBe(false);
		process.env.DAEMON_AUTO_SPAWN = "sometimes";
		expect(loadConfigFile(configPath).daemonAutoSpawn).toBe(true);
	});
});

describe("parseEnv - cluster synthesis settings", () => {
	it("defaults to running centroids, 20 calls per run and threshold 0.5", () => {
		const { config: c, issues } = parse({});
		expect(issues).toHaveLength(0);
		expect(c.consolidation.clusterCentroidMode).toBe("running");
		expect(c.consolidation.clusterSynthesisMaxCallsPerRun).toBe(20);
		expect(c.consolidation.clusterAssignmentThreshold).toBe(0.5);
	});

	it("parses CLUSTER_CENTROID_MODE=seed", () => {
		const parsed = parse({ CLUSTER_CENTROID_MODE: "seed" });
		expect(parsed.issues).toHaveLength(0);
		expect(parsed.config.consolidation.clusterCentroidMode).toBe("seed");
	});

	it("rejects an unknown CLUSTER_CENTROID_MODE and falls back to running", () => {
		const parsed = parse({ CLUSTER_CENTROID_MODE: "drift" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain("CLUSTER_CENTROID_MODE");
		expect(parsed.config.consolidation.clusterCentroidMode).toBe("running");
	});

	it("accepts CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN=0 as the dry run", () => {
		const parsed = parse({ CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN: "0" });
		expect(parsed.issues).toHaveLength(0);
		expect(parsed.config.consolidation.clusterSynthesisMaxCallsPerRun).toBe(0);
	});

	it("rejects a negative CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN", () => {
		const parsed = parse({ CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN: "-1" });
		expect(parsed.issues).toHaveLength(1);
		expect(parsed.issues[0]).toContain(
			"CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN must be an integer ≥ 0",
		);
	});
});
