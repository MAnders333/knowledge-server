/**
 * Tests for stripJsoncComments(), setupCursor(), and setupCodex().
 *
 * stripJsoncComments() is a character-by-character state machine that strips
 * single-line (//) and block comments from JSONC while leaving string values
 * (including those containing // or block-comment delimiters) intact.
 *
 * setupCursor() / setupCodex() are tested via temp directories (controlled by
 * CURSOR_HOME / CODEX_HOME env vars) so that real user config is never touched.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	chmodSync,
	existsSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	readlinkSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	setupClaudeCode,
	setupCodex,
	setupCursor,
	setupOhMyPi,
	stripJsoncComments,
	writeOpenCodeMcpEntry,
} from "../src/commands/setup-tool";
import {
	type SetupIsolation,
	isolateSetupTool,
} from "./support/setup-isolation";

describe("stripJsoncComments", () => {
	// ── Basic comment stripping ───────────────────────────────────────────────

	it("strips a single-line comment", () => {
		const input = `{ "a": 1 // comment\n}`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({ a: 1 });
	});

	it("strips a block comment", () => {
		const input = `{ /* block */ "a": 1 }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({ a: 1 });
	});

	it("strips a multi-line block comment", () => {
		const input = `{\n  /* line one\n     line two */\n  "a": 1\n}`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({ a: 1 });
	});

	it("strips multiple comments in one document", () => {
		const input = `{\n  // first\n  "a": 1, /* second */ "b": 2\n}`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({ a: 1, b: 2 });
	});

	// ── String values must not be corrupted ───────────────────────────────────

	it("preserves // inside a string value (URL)", () => {
		const input = `{ "url": "https://example.com" }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({
			url: "https://example.com",
		});
	});

	it("preserves block-comment delimiter inside a string value", () => {
		const input = `{ "note": "use /* and */ freely" }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({
			note: "use /* and */ freely",
		});
	});

	it("preserves // in a string key", () => {
		const input = `{ "http://key": true }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({
			"http://key": true,
		});
	});

	// ── Escaped characters inside strings ────────────────────────────────────

	it("handles escaped quote inside a string value", () => {
		const input = `{ "a": "say \\"hello\\"" }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({
			a: 'say "hello"',
		});
	});

	it("handles escaped backslash before closing quote", () => {
		// "a": "path\\" — the \\ is an escaped backslash, not escaping the "
		const input = `{ "a": "path\\\\" }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({ a: "path\\" });
	});

	it("does not treat escaped quote as end of string", () => {
		// Value is: say \"//not a comment\"
		const input = `{ "a": "say \\"//not a comment\\"" }`;
		expect(JSON.parse(stripJsoncComments(input))).toEqual({
			a: 'say "//not a comment"',
		});
	});

	// ── Edge cases ────────────────────────────────────────────────────────────

	it("returns empty string unchanged", () => {
		expect(stripJsoncComments("")).toBe("");
	});

	it("returns plain JSON unchanged", () => {
		const input = `{"a":1,"b":"hello"}`;
		expect(stripJsoncComments(input)).toBe(input);
	});

	it("handles a file with only a block comment", () => {
		expect(stripJsoncComments("/* nothing */").trim()).toBe("");
	});

	it("handles an unterminated block comment gracefully (does not throw)", () => {
		// Unterminated block comment — strips to end of input.
		expect(() => stripJsoncComments("{ /* unterminated")).not.toThrow();
	});

	it("handles a real opencode.jsonc-style snippet", () => {
		const input = `{
  // Provider config
  "provider": {
    "anthropic": {
      "options": {
        "baseURL": "https://unified-endpoint.example.com/anthropic/v1", // custom
        "apiKey": "secret"
      }
    }
  },
  /* MCP servers */
  "mcp": {
    "knowledge": {
      "type": "local",
      "command": ["bun", "run", "/home/user/knowledge-server/src/mcp/index.ts"],
      "enabled": true,
      "environment": {
        "KNOWLEDGE_HOST": "127.0.0.1",
        "KNOWLEDGE_PORT": "3179"
      }
    }
  }
}`;
		const parsed = JSON.parse(stripJsoncComments(input));
		expect(parsed.provider.anthropic.options.baseURL).toBe(
			"https://unified-endpoint.example.com/anthropic/v1",
		);
		expect(parsed.mcp.knowledge.type).toBe("local");
	});
});

// ── setupCursor ────────────────────────────────────────────────────────────────

describe("setupCursor", () => {
	let tmpDir: string;
	let origCursorHome: string | undefined;

	beforeEach(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "setup-cursor-test-"));
		origCursorHome = process.env.CURSOR_HOME;
		process.env.CURSOR_HOME = tmpDir;
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
		if (origCursorHome === undefined) {
			Reflect.deleteProperty(process.env, "CURSOR_HOME");
		} else {
			process.env.CURSOR_HOME = origCursorHome;
		}
	});

	it("creates ~/.cursor/mcp.json with mcpServers.knowledge on first run", () => {
		const mcpPath = join(tmpDir, "mcp.json");
		expect(existsSync(mcpPath)).toBe(false);

		setupCursor();

		expect(existsSync(mcpPath)).toBe(true);
		const written = JSON.parse(readFileSync(mcpPath, "utf8")) as {
			mcpServers: {
				knowledge: { command: string; env: Record<string, string> };
			};
		};
		expect(written.mcpServers).toBeDefined();
		expect(written.mcpServers.knowledge).toBeDefined();
		expect(written.mcpServers.knowledge.command).toBeTypeOf("string");
		expect(written.mcpServers.knowledge.env).toMatchObject({
			KNOWLEDGE_HOST: expect.any(String),
			KNOWLEDGE_PORT: expect.any(String),
		});
	});

	it("is idempotent — does not duplicate entry on re-run", () => {
		setupCursor();
		const first = readFileSync(join(tmpDir, "mcp.json"), "utf8");

		setupCursor();
		const second = readFileSync(join(tmpDir, "mcp.json"), "utf8");

		// Content must be identical (no duplication, no extra keys)
		expect(second).toBe(first);
	});

	it("preserves existing mcpServers entries", () => {
		const mcpPath = join(tmpDir, "mcp.json");
		writeFileSync(
			mcpPath,
			JSON.stringify(
				{ mcpServers: { other: { command: "other-cmd" } } },
				null,
				2,
			),
		);

		setupCursor();

		const written = JSON.parse(readFileSync(mcpPath, "utf8")) as {
			mcpServers: Record<string, unknown>;
		};
		expect(written.mcpServers.other).toBeDefined();
		expect(written.mcpServers.knowledge).toBeDefined();
	});

	it("updates entry when command changes", () => {
		const mcpPath = join(tmpDir, "mcp.json");
		// Seed with a stale command path
		writeFileSync(
			mcpPath,
			JSON.stringify(
				{
					mcpServers: {
						knowledge: {
							command: "/old/path/knowledge-server-mcp",
							env: { KNOWLEDGE_HOST: "127.0.0.1", KNOWLEDGE_PORT: "3179" },
						},
					},
				},
				null,
				2,
			),
		);

		setupCursor();

		const written = JSON.parse(readFileSync(mcpPath, "utf8")) as {
			mcpServers: { knowledge: { command: string } };
		};
		// The new command should differ from the stale path
		expect(written.mcpServers.knowledge.command).not.toBe(
			"/old/path/knowledge-server-mcp",
		);
	});
});

// ── setupCodex ─────────────────────────────────────────────────────────────────

describe("setupCodex", () => {
	let tmpDir: string;
	let origCodexHome: string | undefined;

	beforeEach(() => {
		tmpDir = mkdtempSync(join(tmpdir(), "setup-codex-test-"));
		origCodexHome = process.env.CODEX_HOME;
		process.env.CODEX_HOME = tmpDir;
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
		if (origCodexHome === undefined) {
			Reflect.deleteProperty(process.env, "CODEX_HOME");
		} else {
			process.env.CODEX_HOME = origCodexHome;
		}
	});

	it("creates ~/.codex/config.toml with [mcp_servers.knowledge] on first run", () => {
		const configPath = join(tmpDir, "config.toml");
		expect(existsSync(configPath)).toBe(false);

		setupCodex();

		expect(existsSync(configPath)).toBe(true);
		const content = readFileSync(configPath, "utf8");
		expect(content).toContain("[mcp_servers.knowledge]");
		expect(content).toContain("KNOWLEDGE_HOST");
		expect(content).toContain("KNOWLEDGE_PORT");
	});

	it("is idempotent — does not duplicate section on re-run", () => {
		setupCodex();
		const first = readFileSync(join(tmpDir, "config.toml"), "utf8");
		const firstCount = first.split("[mcp_servers.knowledge]").length - 1;
		expect(firstCount).toBe(1);

		setupCodex();
		const second = readFileSync(join(tmpDir, "config.toml"), "utf8");
		const secondCount = second.split("[mcp_servers.knowledge]").length - 1;
		// Section must appear exactly once — never duplicated
		expect(secondCount).toBe(1);
	});

	it("appends to an existing config.toml without clobbering it", () => {
		const configPath = join(tmpDir, "config.toml");
		writeFileSync(configPath, '[model]\nname = "o4-mini"\n');

		setupCodex();

		const content = readFileSync(configPath, "utf8");
		// Original content preserved
		expect(content).toContain("[model]");
		expect(content).toContain('name = "o4-mini"');
		// New section appended
		expect(content).toContain("[mcp_servers.knowledge]");
	});
});

// ── Test isolation ─────────────────────────────────────────────────────────────

// Every test in this file runs with HOME and PATH isolated, independent of cwd.
let isolation: SetupIsolation;
beforeEach(() => {
	isolation = isolateSetupTool();
});
afterEach(() => {
	isolation.restore();
});

describe("setup runs never touch the real home", () => {
	it("writes Cursor and Codex config only under the temp HOME", () => {
		setupCursor();
		setupCodex();

		// Both integrations must resolve their config dirs from the temp HOME, not
		// from the account home that Bun fixed at process start.
		expect(existsSync(join(isolation.home, ".cursor", "mcp.json"))).toBe(true);
		expect(existsSync(join(isolation.home, ".codex", "hooks.json"))).toBe(true);
	});

	it("calls no launchctl/systemctl and writes no service file under HOME", () => {
		setupCursor();
		setupCodex();

		expect(existsSync(isolation.callsLog)).toBe(false);
		expect(existsSync(join(isolation.home, "Library"))).toBe(false);
		expect(existsSync(join(isolation.home, ".config", "systemd"))).toBe(false);
	});
});

// ── setupCodex UserPromptSubmit hook ─────────────────────────────────────────

describe("setupCodex UserPromptSubmit hook", () => {
	let codexHome: string;
	let origCodexHome: string | undefined;

	beforeEach(() => {
		codexHome = mkdtempSync(join(tmpdir(), "setup-codex-hook-"));
		origCodexHome = process.env.CODEX_HOME;
		process.env.CODEX_HOME = codexHome;
	});

	afterEach(() => {
		rmSync(codexHome, { recursive: true, force: true });
		if (origCodexHome === undefined) {
			Reflect.deleteProperty(process.env, "CODEX_HOME");
		} else {
			process.env.CODEX_HOME = origCodexHome;
		}
	});

	type Handler = { type?: string; command?: string; timeout?: number };
	type Group = { hooks?: Handler[] };
	const HOOKS_PATH = () => join(codexHome, "hooks.json");
	const readGroups = (): Group[] => {
		const file = JSON.parse(readFileSync(HOOKS_PATH(), "utf8")) as {
			hooks: { UserPromptSubmit: Group[] };
		};
		return file.hooks.UserPromptSubmit;
	};
	const knowledgeHandlers = (): Handler[] =>
		readGroups()
			.flatMap((g) => g.hooks ?? [])
			.filter((h) =>
				h.command?.includes("/hooks/claude-code/user-prompt?client=codex"),
			);

	it("writes a command hook that posts to the endpoint with a 5 s timeout and never fails the prompt", () => {
		setupCodex();
		const handlers = knowledgeHandlers();
		expect(handlers).toHaveLength(1);
		expect(handlers[0].type).toBe("command");
		expect(handlers[0].timeout).toBe(5);
		expect(handlers[0].command).toContain(
			"curl -s --noproxy '*' --fail --max-time 4 ${KNOWLEDGE_ADMIN_TOKEN:+-H",
		);
		expect(handlers[0].command?.endsWith("|| true")).toBe(true);
	});

	it("is idempotent: a second run leaves hooks.json byte-identical", () => {
		setupCodex();
		const first = readFileSync(HOOKS_PATH(), "utf8");
		setupCodex();
		expect(readFileSync(HOOKS_PATH(), "utf8")).toBe(first);
		expect(knowledgeHandlers()).toHaveLength(1);
	});

	it("keeps the user's own hooks", () => {
		const guard = {
			matcher: "Bash",
			hooks: [{ type: "command", command: "guard.sh", timeout: 30 }],
		};
		writeFileSync(
			HOOKS_PATH(),
			JSON.stringify({
				hooks: {
					PreToolUse: [guard],
					UserPromptSubmit: [
						{ hooks: [{ type: "command", command: "user-hook.sh" }] },
					],
				},
			}),
		);
		setupCodex();
		const file = JSON.parse(readFileSync(HOOKS_PATH(), "utf8")) as {
			hooks: { PreToolUse: unknown[] };
		};
		expect(file.hooks.PreToolUse).toEqual([guard]);
		const commands = readGroups()
			.flatMap((g) => g.hooks ?? [])
			.map((h) => h.command);
		expect(commands).toContain("user-hook.sh");
		expect(knowledgeHandlers()).toHaveLength(1);
	});

	it("replaces a handler left by an older port instead of adding a second one", () => {
		writeFileSync(
			HOOKS_PATH(),
			JSON.stringify({
				hooks: {
					UserPromptSubmit: [
						{
							hooks: [
								{
									type: "command",
									command:
										"curl -s --max-time 4 --data-binary @- 'http://127.0.0.1:1111/hooks/claude-code/user-prompt?client=codex' || true",
									timeout: 5,
								},
							],
						},
					],
				},
			}),
		);
		setupCodex();
		const handlers = knowledgeHandlers();
		expect(handlers).toHaveLength(1);
		expect(handlers[0].command).not.toContain("127.0.0.1:1111");
	});

	it("leaves a malformed hooks.json untouched instead of overwriting it", () => {
		writeFileSync(HOOKS_PATH(), "{ broken");
		setupCodex();
		expect(readFileSync(HOOKS_PATH(), "utf8")).toBe("{ broken");
	});

	it("sends the admin bearer only when KNOWLEDGE_ADMIN_TOKEN is set", async () => {
		setupCodex();
		const command = knowledgeHandlers()[0]?.command ?? "";
		const received: Array<string | null> = [];
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch(req) {
				received.push(req.headers.get("authorization"));
				return Response.json({});
			},
		});
		try {
			// Only the origin changes: the command, header expansion included, runs as written.
			// The hook runs asynchronously: a blocking spawn would stall the stub server.
			const local = command.replace(
				/'http:\/\/[^']+'/,
				`'http://127.0.0.1:${server.port}/hooks/claude-code/user-prompt?client=codex'`,
			);
			const run = async (env: Record<string, string>) => {
				const proc = Bun.spawn(["sh", "-c", local], {
					env: { PATH: process.env.PATH ?? "/usr/bin:/bin", ...env },
					stdin: Buffer.from('{"prompt":"hello"}'),
					stdout: "pipe",
					stderr: "pipe",
				});
				await proc.exited;
			};
			await run({});
			await run({ KNOWLEDGE_ADMIN_TOKEN: "stable-admin-token-0123456789" });
		} finally {
			server.stop(true);
		}
		expect(received).toEqual([null, "Bearer stable-admin-token-0123456789"]);
	});
});

// ── setupClaudeCode UserPromptSubmit hook ────────────────────────────────────

describe("setupClaudeCode UserPromptSubmit hook", () => {
	const settingsPath = () => join(isolation.home, ".claude", "settings.json");
	const readHandlers = (): Array<Record<string, unknown>> => {
		const settings = JSON.parse(readFileSync(settingsPath(), "utf8")) as {
			hooks: {
				UserPromptSubmit: Array<{ hooks?: Array<Record<string, unknown>> }>;
			};
		};
		return settings.hooks.UserPromptSubmit.flatMap(
			(group) => group.hooks ?? [],
		);
	};
	const knowledgeHandlers = () =>
		readHandlers().filter((h) =>
			String(h.url).includes("/hooks/claude-code/user-prompt"),
		);

	it("writes the hook with the admin bearer and allows that variable to be expanded", () => {
		setupClaudeCode();
		expect(knowledgeHandlers()).toEqual([
			{
				type: "http",
				url: expect.stringContaining("/hooks/claude-code/user-prompt"),
				timeout: 5,
				headers: { Authorization: "Bearer $KNOWLEDGE_ADMIN_TOKEN" },
				allowedEnvVars: ["KNOWLEDGE_ADMIN_TOKEN"],
			},
		]);
	});

	it("upgrades a hook written before the token header, and a re-run is byte-identical", () => {
		setupClaudeCode();
		const url = knowledgeHandlers()[0].url;
		writeFileSync(
			settingsPath(),
			JSON.stringify({
				hooks: {
					UserPromptSubmit: [{ hooks: [{ type: "http", url, timeout: 5 }] }],
				},
			}),
		);

		setupClaudeCode();
		expect(knowledgeHandlers()).toHaveLength(1);
		expect(knowledgeHandlers()[0].headers).toEqual({
			Authorization: "Bearer $KNOWLEDGE_ADMIN_TOKEN",
		});

		const first = readFileSync(settingsPath(), "utf8");
		setupClaudeCode();
		expect(readFileSync(settingsPath(), "utf8")).toBe(first);
	});

	it("keeps the user's own hooks", () => {
		mkdirSync(join(isolation.home, ".claude"), { recursive: true });
		writeFileSync(
			settingsPath(),
			JSON.stringify({
				hooks: {
					UserPromptSubmit: [
						{ hooks: [{ type: "command", command: "user-hook.sh" }] },
					],
				},
			}),
		);
		setupClaudeCode();
		expect(readHandlers().map((h) => h.command)).toContain("user-hook.sh");
		expect(knowledgeHandlers()).toHaveLength(1);
	});
});

// ── Config file modes ────────────────────────────────────────────────────────

describe("setup keeps config file modes", () => {
	const modeOf = (path: string) => statSync(path).mode & 0o777;

	it("creates new Codex config and hooks files 0600", () => {
		setupCodex();
		expect(modeOf(join(isolation.home, ".codex", "config.toml"))).toBe(0o600);
		expect(modeOf(join(isolation.home, ".codex", "hooks.json"))).toBe(0o600);
	});

	it("keeps the mode of an existing Codex config.toml", () => {
		mkdirSync(join(isolation.home, ".codex"), { recursive: true });
		const configPath = join(isolation.home, ".codex", "config.toml");
		writeFileSync(configPath, '[model]\nname = "o4-mini"\n');
		chmodSync(configPath, 0o640);
		setupCodex();
		expect(modeOf(configPath)).toBe(0o640);
	});

	it("keeps the mode of an existing Cursor mcp.json", () => {
		mkdirSync(join(isolation.home, ".cursor"), { recursive: true });
		const mcpPath = join(isolation.home, ".cursor", "mcp.json");
		writeFileSync(mcpPath, JSON.stringify({ mcpServers: {} }));
		chmodSync(mcpPath, 0o640);
		setupCursor();
		expect(modeOf(mcpPath)).toBe(0o640);
	});

	it("keeps the mode of an existing opencode.jsonc", () => {
		const dir = join(isolation.home, ".config", "opencode");
		mkdirSync(dir, { recursive: true });
		const configPath = join(dir, "opencode.jsonc");
		writeFileSync(configPath, "{}");
		chmodSync(configPath, 0o640);
		writeOpenCodeMcpEntry(dir);
		expect(modeOf(configPath)).toBe(0o640);
	});

	it("creates a new Claude settings.json 0600 and keeps an existing mode", () => {
		const settingsPath = join(isolation.home, ".claude", "settings.json");
		setupClaudeCode();
		expect(modeOf(settingsPath)).toBe(0o600);

		chmodSync(settingsPath, 0o640);
		setupClaudeCode();
		expect(modeOf(settingsPath)).toBe(0o640);
	});
});

// ── setupOhMyPi ──────────────────────────────────────────────────────────────

describe("setupOhMyPi", () => {
	let ompRoot: string;

	beforeEach(() => {
		ompRoot = mkdtempSync(join(tmpdir(), "setup-omp-"));
	});

	afterEach(() => {
		rmSync(ompRoot, { recursive: true, force: true });
	});

	it("places the extension in the shared agent layer and every profile layer", () => {
		const shared = join(ompRoot, "agent");
		const work = join(ompRoot, "profiles", "work", "agent");
		mkdirSync(shared, { recursive: true });
		mkdirSync(work, { recursive: true });
		// A profile without an agent dir gets no extension directory.
		mkdirSync(join(ompRoot, "profiles", "empty"), { recursive: true });

		setupOhMyPi(ompRoot);

		for (const layer of [shared, work]) {
			const dst = join(layer, "extensions", "knowledge.ts");
			// Tests run from source, so placement is a symlink to the repo file.
			expect(lstatSync(dst).isSymbolicLink()).toBe(true);
			expect(readlinkSync(dst)).toMatch(/plugin\/omp-knowledge\.ts$/);
		}
		expect(existsSync(join(ompRoot, "profiles", "empty", "extensions"))).toBe(
			false,
		);
	});

	it("writes no settings.json, because oh-my-pi auto-discovers agent extensions", () => {
		mkdirSync(join(ompRoot, "agent"), { recursive: true });
		setupOhMyPi(ompRoot);
		expect(existsSync(join(ompRoot, "agent", "settings.json"))).toBe(false);
	});
});
