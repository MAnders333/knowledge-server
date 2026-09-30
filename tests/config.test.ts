/**
 * Tests for validateConfig().
 *
 * validateConfig() reads from two sources:
 *   1. The module-level `config` singleton (frozen at import time) — for
 *      llm.apiKey, llm.baseEndpoint, host, opencodeDbPath.
 *   2. process.env directly — for the float-range validators
 *      (DECAY_ARCHIVE_THRESHOLD, CONTRADICTION_MIN_SIMILARITY,
 *       ACTIVATION_SIMILARITY_THRESHOLD) and EMBEDDING_DIMENSIONS.
 *
 * Only source (2) is testable with env-var manipulation in a single process,
 * because module caching freezes the singleton at import time. All tests here
 * target source (2). We filter results by error-message keyword so singleton-
 * sourced errors (LLM_API_KEY, etc.) don't cause false failures on clean CI.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { validateConfig } from "../src/config";

// ── env var helpers ───────────────────────────────────────────────────────────

const ENV_VARS = [
	"DECAY_ARCHIVE_THRESHOLD",
	"RECONSOLIDATION_SIMILARITY_THRESHOLD",
	"CONTRADICTION_MIN_SIMILARITY",
	"ACTIVATION_SIMILARITY_THRESHOLD",
	"EMBEDDING_DIMENSIONS",
	"OPENAI_MAX_TOKENS_PARAM",
] as const;

type EnvVar = (typeof ENV_VARS)[number];
const snapshot: Partial<Record<EnvVar, string>> = {};

beforeEach(() => {
	for (const v of ENV_VARS) {
		snapshot[v] = process.env[v];
		// Unset each optional validator so the baseline is clean for each test
		Reflect.deleteProperty(process.env, v);
	}
});

afterEach(() => {
	for (const v of ENV_VARS) {
		if (snapshot[v] === undefined) {
			Reflect.deleteProperty(process.env, v);
		} else {
			process.env[v] = snapshot[v];
		}
	}
});

/** Only the errors for the env vars under test — filters out the
 *  singleton-sourced errors (LLM_API_KEY, LLM_BASE_ENDPOINT, host, opencodeDbPath)
 *  that depend on how the test process was launched. */
function envErrors(): string[] {
	return validateConfig().filter((e) => ENV_VARS.some((v) => e.includes(v)));
}

// ── baseline ──────────────────────────────────────────────────────────────────

describe("validateConfig — float-range baseline (all optional vars unset)", () => {
	it("produces no float-range errors when optional vars are unset", () => {
		// All ENV_VARS are deleted in beforeEach — validateConfig should skip them.
		expect(envErrors()).toEqual([]);
	});
});

// ── DECAY_ARCHIVE_THRESHOLD ───────────────────────────────────────────────────

describe("validateConfig — DECAY_ARCHIVE_THRESHOLD", () => {
	it("accepts a valid value in (0, 1]", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "0.15";
		expect(
			envErrors().filter((e) => e.includes("DECAY_ARCHIVE_THRESHOLD")),
		).toHaveLength(0);
	});

	it("accepts 1 (upper bound is inclusive)", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "1";
		expect(
			envErrors().filter((e) => e.includes("DECAY_ARCHIVE_THRESHOLD")),
		).toHaveLength(0);
	});

	it("rejects 0 (lower bound is exclusive)", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "0";
		expect(envErrors().some((e) => e.includes("DECAY_ARCHIVE_THRESHOLD"))).toBe(
			true,
		);
	});

	it("rejects a value > 1", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "1.5";
		expect(envErrors().some((e) => e.includes("DECAY_ARCHIVE_THRESHOLD"))).toBe(
			true,
		);
	});

	it("rejects a non-numeric string", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "high";
		expect(envErrors().some((e) => e.includes("DECAY_ARCHIVE_THRESHOLD"))).toBe(
			true,
		);
	});
});

// ── RECONSOLIDATION_SIMILARITY_THRESHOLD ─────────────────────────────────────

describe("validateConfig — RECONSOLIDATION_SIMILARITY_THRESHOLD", () => {
	it("accepts a valid value in (0, 1)", () => {
		process.env.RECONSOLIDATION_SIMILARITY_THRESHOLD = "0.75";
		expect(
			envErrors().filter((e) =>
				e.includes("RECONSOLIDATION_SIMILARITY_THRESHOLD"),
			),
		).toHaveLength(0);
	});

	it("rejects 0 (lower bound is exclusive)", () => {
		process.env.RECONSOLIDATION_SIMILARITY_THRESHOLD = "0";
		expect(
			envErrors().some((e) =>
				e.includes("RECONSOLIDATION_SIMILARITY_THRESHOLD"),
			),
		).toBe(true);
	});

	it("rejects a value above 1", () => {
		process.env.RECONSOLIDATION_SIMILARITY_THRESHOLD = "1.5";
		expect(
			envErrors().some((e) =>
				e.includes("RECONSOLIDATION_SIMILARITY_THRESHOLD"),
			),
		).toBe(true);
	});

	it("rejects a non-numeric string", () => {
		process.env.RECONSOLIDATION_SIMILARITY_THRESHOLD = "high";
		expect(
			envErrors().some((e) =>
				e.includes("RECONSOLIDATION_SIMILARITY_THRESHOLD"),
			),
		).toBe(true);
	});
});

// ── CONTRADICTION_MIN_SIMILARITY ──────────────────────────────────────────────

describe("validateConfig — CONTRADICTION_MIN_SIMILARITY", () => {
	it("accepts a value strictly below 0.82 (default reconsolidation threshold)", () => {
		process.env.CONTRADICTION_MIN_SIMILARITY = "0.4";
		expect(
			envErrors().filter((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toHaveLength(0);
	});

	it("rejects 0 (lower bound is exclusive)", () => {
		process.env.CONTRADICTION_MIN_SIMILARITY = "0";
		expect(
			envErrors().some((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toBe(true);
	});

	it("rejects exactly 0.82 (upper bound is exclusive)", () => {
		process.env.CONTRADICTION_MIN_SIMILARITY = "0.82";
		expect(
			envErrors().some((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toBe(true);
	});

	it("rejects a value above 0.82", () => {
		process.env.CONTRADICTION_MIN_SIMILARITY = "0.9";
		expect(
			envErrors().some((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toBe(true);
	});

	it("rejects a non-numeric string", () => {
		process.env.CONTRADICTION_MIN_SIMILARITY = "medium";
		expect(
			envErrors().some((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toBe(true);
	});

	it("rejects CONTRADICTION_MIN_SIMILARITY >= custom RECONSOLIDATION_SIMILARITY_THRESHOLD", () => {
		// When the reconsolidation threshold is lowered (e.g. for a different embedding
		// model), CONTRADICTION_MIN_SIMILARITY must still be strictly below it.
		process.env.RECONSOLIDATION_SIMILARITY_THRESHOLD = "0.7";
		process.env.CONTRADICTION_MIN_SIMILARITY = "0.7"; // equal → must be rejected
		expect(
			envErrors().some((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toBe(true);
	});
});

// ── ACTIVATION_SIMILARITY_THRESHOLD ──────────────────────────────────────────

describe("validateConfig — ACTIVATION_SIMILARITY_THRESHOLD", () => {
	it("accepts a valid value in (0, 1]", () => {
		process.env.ACTIVATION_SIMILARITY_THRESHOLD = "0.35";
		expect(
			envErrors().filter((e) => e.includes("ACTIVATION_SIMILARITY_THRESHOLD")),
		).toHaveLength(0);
	});

	it("rejects 0 (lower bound is exclusive)", () => {
		process.env.ACTIVATION_SIMILARITY_THRESHOLD = "0";
		expect(
			envErrors().some((e) => e.includes("ACTIVATION_SIMILARITY_THRESHOLD")),
		).toBe(true);
	});

	it("rejects a value above 1", () => {
		process.env.ACTIVATION_SIMILARITY_THRESHOLD = "1.1";
		expect(
			envErrors().some((e) => e.includes("ACTIVATION_SIMILARITY_THRESHOLD")),
		).toBe(true);
	});
});

// ── EMBEDDING_DIMENSIONS ──────────────────────────────────────────────────────

describe("validateConfig — EMBEDDING_DIMENSIONS", () => {
	it("accepts a positive integer", () => {
		process.env.EMBEDDING_DIMENSIONS = "512";
		expect(
			envErrors().filter((e) => e.includes("EMBEDDING_DIMENSIONS")),
		).toHaveLength(0);
	});

	it("accepts a float string (parseInt truncates to valid integer)", () => {
		// parseInt("1.5") = 1, which is >= 1 — validateConfig accepts this.
		// TODO: consider tightening to reject non-integer strings in future.
		process.env.EMBEDDING_DIMENSIONS = "1.5";
		expect(
			envErrors().filter((e) => e.includes("EMBEDDING_DIMENSIONS")),
		).toHaveLength(0);
	});

	it("rejects 0", () => {
		process.env.EMBEDDING_DIMENSIONS = "0";
		expect(envErrors().some((e) => e.includes("EMBEDDING_DIMENSIONS"))).toBe(
			true,
		);
	});

	it("rejects a negative integer", () => {
		process.env.EMBEDDING_DIMENSIONS = "-1";
		expect(envErrors().some((e) => e.includes("EMBEDDING_DIMENSIONS"))).toBe(
			true,
		);
	});

	it("rejects a non-numeric string", () => {
		process.env.EMBEDDING_DIMENSIONS = "large";
		expect(envErrors().some((e) => e.includes("EMBEDDING_DIMENSIONS"))).toBe(
			true,
		);
	});

	it("produces no error when EMBEDDING_DIMENSIONS is unset", () => {
		// Already deleted in beforeEach
		expect(
			envErrors().filter((e) => e.includes("EMBEDDING_DIMENSIONS")),
		).toHaveLength(0);
	});
});

// ── OPENAI_MAX_TOKENS_PARAM ───────────────────────────────────────────────────

describe("validateConfig — OPENAI_MAX_TOKENS_PARAM", () => {
	it("accepts max_tokens", () => {
		process.env.OPENAI_MAX_TOKENS_PARAM = "max_tokens";
		expect(
			envErrors().filter((e) => e.includes("OPENAI_MAX_TOKENS_PARAM")),
		).toHaveLength(0);
	});

	it("accepts max_completion_tokens", () => {
		process.env.OPENAI_MAX_TOKENS_PARAM = "max_completion_tokens";
		expect(
			envErrors().filter((e) => e.includes("OPENAI_MAX_TOKENS_PARAM")),
		).toHaveLength(0);
	});

	it("rejects an unknown parameter name", () => {
		process.env.OPENAI_MAX_TOKENS_PARAM = "max_output_tokens";
		expect(envErrors().some((e) => e.includes("OPENAI_MAX_TOKENS_PARAM"))).toBe(
			true,
		);
	});

	it("produces no error when OPENAI_MAX_TOKENS_PARAM is unset", () => {
		// Already deleted in beforeEach
		expect(
			envErrors().filter((e) => e.includes("OPENAI_MAX_TOKENS_PARAM")),
		).toHaveLength(0);
	});
});

// ── multiple errors accumulated ───────────────────────────────────────────────

describe("validateConfig — multiple errors accumulated", () => {
	it("returns one error per violated constraint", () => {
		process.env.DECAY_ARCHIVE_THRESHOLD = "0";
		process.env.CONTRADICTION_MIN_SIMILARITY = "0.82";
		process.env.EMBEDDING_DIMENSIONS = "-5";
		const errors = envErrors();
		expect(
			errors.filter((e) => e.includes("DECAY_ARCHIVE_THRESHOLD")),
		).toHaveLength(1);
		expect(
			errors.filter((e) => e.includes("CONTRADICTION_MIN_SIMILARITY")),
		).toHaveLength(1);
		expect(
			errors.filter((e) => e.includes("EMBEDDING_DIMENSIONS")),
		).toHaveLength(1);
	});
});
