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
	existsSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setupCodex, setupCursor, stripJsoncComments } from "../src/commands/setup-tool";

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
			mcpServers: { knowledge: { command: string; env: Record<string, string> } };
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
			JSON.stringify({ mcpServers: { other: { command: "other-cmd" } } }, null, 2),
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
		writeFileSync(configPath, "[model]\nname = \"o4-mini\"\n");

		setupCodex();

		const content = readFileSync(configPath, "utf8");
		// Original content preserved
		expect(content).toContain('[model]');
		expect(content).toContain('name = "o4-mini"');
		// New section appended
		expect(content).toContain("[mcp_servers.knowledge]");
	});
});
