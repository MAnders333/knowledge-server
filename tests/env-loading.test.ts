/**
 * Startup order and cross-process behaviour. Each case runs in a child Bun
 * process with a temp HOME and cwd, so nothing reads the real home, the repo's
 * .env, or this test process's environment.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";

const SRC = resolve(import.meta.dir, "../src");

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "ks-env-loading-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

/** Environment for a child process: nothing inherited except PATH and the temp home. */
function childEnv(extra: Record<string, string> = {}): Record<string, string> {
	return {
		PATH: process.env.PATH ?? "",
		HOME: home,
		XDG_CONFIG_HOME: join(home, ".config"),
		...extra,
	};
}

function runChild(script: string, env: Record<string, string>) {
	const child = Bun.spawnSync({
		cmd: [process.execPath, "-e", script],
		cwd: home,
		env,
		stdout: "pipe",
		stderr: "pipe",
	});
	return {
		exitCode: child.exitCode,
		stdout: child.stdout.toString(),
		stderr: child.stderr.toString(),
	};
}

function lastJsonLine(stdout: string): unknown {
	return JSON.parse(stdout.trim().split("\n").pop() ?? "null");
}

function writeEnvFile(text: string): void {
	const dir = join(home, ".config", "knowledge-server");
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, ".env"), text, { mode: 0o600 });
}

const modulePath = (name: string) => JSON.stringify(join(SRC, name));

describe(".env is applied before the config singletons are built", () => {
	it("uses the .env port, state DB and user id when there is no config.jsonc", () => {
		writeEnvFile(
			[
				"KNOWLEDGE_PORT=4999",
				"STATE_DB_URI=postgres://u:p@127.0.0.1:1/ks",
				"KNOWLEDGE_USER_ID=alice",
			].join("\n"),
		);
		const script = `
import ${modulePath("env.ts")};
import { DEFAULT_CONFIG } from ${modulePath("config-file.ts")};
console.log(JSON.stringify({ port: DEFAULT_CONFIG.port, stateDb: DEFAULT_CONFIG.stateDb, userId: DEFAULT_CONFIG.userId }));`;
		const result = runChild(script, childEnv({ KNOWLEDGE_LOAD_ENV: "1" }));
		expect(result.exitCode).toBe(0);
		expect(lastJsonLine(result.stdout)).toEqual({
			port: 4999,
			stateDb: { kind: "postgres", uri: "postgres://u:p@127.0.0.1:1/ks" },
			userId: "alice",
		});
	});

	it("still lets an explicit environment variable win over the .env", () => {
		writeEnvFile("KNOWLEDGE_PORT=4999\n");
		const script = `
import ${modulePath("env.ts")};
import { DEFAULT_CONFIG } from ${modulePath("config-file.ts")};
console.log(JSON.stringify({ port: DEFAULT_CONFIG.port }));`;
		const result = runChild(
			script,
			childEnv({ KNOWLEDGE_LOAD_ENV: "1", KNOWLEDGE_PORT: "5000" }),
		);
		expect(result.exitCode).toBe(0);
		expect(lastJsonLine(result.stdout)).toEqual({ port: 5000 });
	});
});

describe("env-file.ts stays a leaf of the import graph", () => {
	/** Relative module specifiers in one source file. */
	function localImports(file: string): string[] {
		const source = readFileSync(join(SRC, file), "utf8");
		const found: string[] = [];
		for (const match of source.matchAll(
			/(?:\bfrom\s+|\bimport\s*\(\s*|\bimport\s+)["'](\.[^"']+)["']/g,
		)) {
			const target = relative(SRC, resolve(dirname(join(SRC, file)), match[1]));
			found.push(target.split("\\").join("/").replace(/\.js$/, ".ts"));
		}
		return found;
	}

	/** Every src file reachable from `entry` through relative imports. */
	function reachableFrom(entry: string): Set<string> {
		const seen = new Set<string>();
		const queue = [entry];
		while (queue.length > 0) {
			const file = queue.pop() as string;
			if (seen.has(file)) continue;
			seen.add(file);
			queue.push(...localImports(file));
		}
		return seen;
	}

	it("does not reach config-file.ts or config.ts, which read the environment at import", () => {
		for (const entry of ["env-file.ts", "env.ts"]) {
			const reached = reachableFrom(entry);
			expect(reached.has("config-file.ts")).toBe(false);
			expect(reached.has("config.ts")).toBe(false);
		}
	});
});

describe("validateConfig and OAuth", () => {
	function writeOAuthTokens(): void {
		const dir = join(home, ".config", "knowledge-server");
		mkdirSync(dir, { recursive: true });
		writeFileSync(
			join(dir, "claude-oauth.json"),
			JSON.stringify({
				access_token: "access-not-real",
				refresh_token: "refresh-not-real",
				expires_at: Date.now() + 3_600_000,
			}),
		);
	}

	const validate = `
import { validateConfig } from ${modulePath("config.ts")};
console.log(JSON.stringify(validateConfig()));`;

	it("reports OAuth-only credentials routed to a non-official base URL at startup", () => {
		writeOAuthTokens();
		const result = runChild(
			validate,
			childEnv({ LLM_BASE_ENDPOINT: "https://gateway.example.org" }),
		);
		expect(result.exitCode).toBe(0);
		const errors = lastJsonLine(result.stdout) as string[];
		expect(
			errors.some((e) => e.includes("only sent to api.anthropic.com")),
		).toBe(true);
		expect(JSON.stringify(errors)).not.toContain("refresh-not-real");
	});

	it("accepts OAuth-only credentials on the official API", () => {
		writeOAuthTokens();
		const result = runChild(
			validate,
			childEnv({ ANTHROPIC_BASE_URL: "https://api.anthropic.com/v1" }),
		);
		expect(result.exitCode).toBe(0);
		const errors = lastJsonLine(result.stdout) as string[];
		expect(
			errors.some((e) => e.includes("only sent to api.anthropic.com")),
		).toBe(false);
	});
});

describe("an unknown DAEMON_AUTO_SPAWN spelling does not stop non-server consumers", () => {
	it("keeps the config.jsonc value for the daemon and MCP paths, and reports the spelling", () => {
		const configPath = join(home, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", writable: true }],
				daemonAutoSpawn: false,
				port: 4100,
			}),
		);
		const script = `
import { loadConfigFile, resolveServerAddress } from ${modulePath("config-file.ts")};
import { configEnvIssues } from ${modulePath("config.ts")};
console.log(JSON.stringify({
  daemonAutoSpawn: loadConfigFile(${JSON.stringify(configPath)})?.daemonAutoSpawn,
  port: resolveServerAddress(${JSON.stringify(configPath)}).port,
  issues: configEnvIssues(),
}));`;
		const result = runChild(
			script,
			childEnv({ DAEMON_AUTO_SPAWN: "sometimes" }),
		);
		expect(result.exitCode).toBe(0);
		const parsed = lastJsonLine(result.stdout) as {
			daemonAutoSpawn: boolean;
			port: number;
			issues: string[];
		};
		expect(parsed.daemonAutoSpawn).toBe(false);
		expect(parsed.port).toBe(4100);
		expect(
			parsed.issues.some((i) => i.includes("DAEMON_AUTO_SPAWN must be one of")),
		).toBe(true);
	});
});
