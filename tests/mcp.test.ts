/**
 * Tests for the MCP server's `activate` tool input schema.
 *
 * We validate the Zod schema directly — no MCP transport is started.
 * This catches regressions where schema constraints (min/max, optionality,
 * types) are accidentally changed.
 *
 * `activateInputSchema` is exported from mcp/index.ts at module scope.
 * The `main()` function is guarded by `import.meta.main` so importing the
 * module does NOT start the server or open a DB connection.
 */
import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { activateInputSchema, flagInputSchema } from "../src/mcp/index";

const activateSchema = z.object(activateInputSchema);

// ── cues ──────────────────────────────────────────────────────────────────────

describe("MCP activate schema — cues", () => {
	it("accepts a non-empty string", () => {
		const result = activateSchema.safeParse({
			cues: "churn analysis, segment X",
		});
		expect(result.success).toBe(true);
	});

	it("accepts an empty string (no min-length constraint)", () => {
		const result = activateSchema.safeParse({ cues: "" });
		expect(result.success).toBe(true);
	});

	it("rejects a missing cues field", () => {
		const result = activateSchema.safeParse({ limit: 5 });
		expect(result.success).toBe(false);
	});

	it("rejects a non-string cues value", () => {
		const result = activateSchema.safeParse({ cues: 42 });
		expect(result.success).toBe(false);
	});
});

// ── limit ─────────────────────────────────────────────────────────────────────

describe("MCP activate schema — limit", () => {
	it("accepts a valid integer in [1, 50]", () => {
		expect(activateSchema.safeParse({ cues: "test", limit: 10 }).success).toBe(
			true,
		);
		expect(activateSchema.safeParse({ cues: "test", limit: 1 }).success).toBe(
			true,
		);
		expect(activateSchema.safeParse({ cues: "test", limit: 50 }).success).toBe(
			true,
		);
	});

	it("is optional — omitting limit is valid", () => {
		expect(activateSchema.safeParse({ cues: "test" }).success).toBe(true);
	});

	it("rejects 0 (below min=1)", () => {
		expect(activateSchema.safeParse({ cues: "test", limit: 0 }).success).toBe(
			false,
		);
	});

	it("rejects 51 (above max=50)", () => {
		expect(activateSchema.safeParse({ cues: "test", limit: 51 }).success).toBe(
			false,
		);
	});

	it("rejects a float", () => {
		expect(activateSchema.safeParse({ cues: "test", limit: 5.5 }).success).toBe(
			false,
		);
	});

	it("rejects a string", () => {
		expect(
			activateSchema.safeParse({ cues: "test", limit: "10" }).success,
		).toBe(false);
	});
});

// ── threshold ─────────────────────────────────────────────────────────────────

describe("MCP activate schema — threshold", () => {
	it("accepts valid values in [0, 1]", () => {
		expect(
			activateSchema.safeParse({ cues: "test", threshold: 0 }).success,
		).toBe(true);
		expect(
			activateSchema.safeParse({ cues: "test", threshold: 0.35 }).success,
		).toBe(true);
		expect(
			activateSchema.safeParse({ cues: "test", threshold: 1 }).success,
		).toBe(true);
	});

	it("is optional — omitting threshold is valid", () => {
		expect(activateSchema.safeParse({ cues: "test" }).success).toBe(true);
	});

	it("rejects a value below 0", () => {
		expect(
			activateSchema.safeParse({ cues: "test", threshold: -0.1 }).success,
		).toBe(false);
	});

	it("rejects a value above 1", () => {
		expect(
			activateSchema.safeParse({ cues: "test", threshold: 1.01 }).success,
		).toBe(false);
	});

	it("rejects a string", () => {
		expect(
			activateSchema.safeParse({ cues: "test", threshold: "high" }).success,
		).toBe(false);
	});
});

// ── combined ──────────────────────────────────────────────────────────────────

describe("MCP activate schema — combined valid inputs", () => {
	it("accepts all three fields populated", () => {
		const result = activateSchema.safeParse({
			cues: "churn analysis, segment X, onboarding",
			limit: 8,
			threshold: 0.3,
		});
		expect(result.success).toBe(true);
		if (result.success) {
			expect(result.data.cues).toBe("churn analysis, segment X, onboarding");
			expect(result.data.limit).toBe(8);
			expect(result.data.threshold).toBeCloseTo(0.3);
		}
	});
});

// ── knowledge_flag ────────────────────────────────────────────────────────────

const flagSchema = z.object(flagInputSchema);

describe("MCP knowledge_flag schema", () => {
	it("accepts the full entry id and the short id shown in injected lines", () => {
		expect(
			flagSchema.safeParse({
				id: "0f1e2d3c-4b5a-4968-8777-665544332211",
				reason: "wrong",
			}).success,
		).toBe(true);
		expect(
			flagSchema.safeParse({ id: "0f1e2d3c", reason: "outdated" }).success,
		).toBe(true);
	});

	it("accepts each of the three reasons", () => {
		for (const reason of ["wrong", "outdated", "irrelevant"]) {
			expect(flagSchema.safeParse({ id: "0f1e2d3c", reason }).success).toBe(
				true,
			);
		}
	});

	it("rejects an unknown reason", () => {
		expect(
			flagSchema.safeParse({ id: "0f1e2d3c", reason: "delete" }).success,
		).toBe(false);
	});

	it("rejects an id with characters outside hex and dashes", () => {
		expect(
			flagSchema.safeParse({ id: "0f1e2d3c; drop", reason: "wrong" }).success,
		).toBe(false);
	});

	it("rejects an id shorter than 8 characters", () => {
		expect(flagSchema.safeParse({ id: "0f1e", reason: "wrong" }).success).toBe(
			false,
		);
	});
});
