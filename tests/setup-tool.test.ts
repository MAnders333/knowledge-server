/**
 * Tests for stripJsoncComments().
 *
 * The function is a character-by-character state machine that strips
 * single-line (//) and block comments from JSONC while leaving string values
 * (including those containing // or block-comment delimiters) intact.
 */
import { describe, expect, it } from "bun:test";
import { stripJsoncComments } from "../src/setup-tool";

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
