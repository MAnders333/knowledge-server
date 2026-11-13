import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { config, validateConfig } from "../src/config";

// config is typed readonly; these tests set the resolved values and restore them.
const mutable = config as unknown as {
	adminToken: string | null;
	protectReads: boolean;
};

// Built at runtime so no secret-shaped literal lands in the source.
const STRONG_TOKEN = [
	"stable",
	"admin",
	"token",
	"example",
	"value",
	"2026",
].join("-");

describe("KNOWLEDGE_PROTECT_READS and the stable admin token", () => {
	let saved: { adminToken: string | null; protectReads: boolean };

	beforeEach(() => {
		saved = {
			adminToken: mutable.adminToken,
			protectReads: mutable.protectReads,
		};
	});

	afterEach(() => {
		mutable.adminToken = saved.adminToken;
		mutable.protectReads = saved.protectReads;
	});

	it("is a startup error to protect reads without a stable token", () => {
		mutable.adminToken = null;
		mutable.protectReads = true;
		expect(
			validateConfig().some((e) => e.includes("KNOWLEDGE_PROTECT_READS=true")),
		).toBe(true);
	});

	it("is valid with a stable token", () => {
		mutable.adminToken = STRONG_TOKEN;
		mutable.protectReads = true;
		expect(
			validateConfig().some((e) => e.includes("KNOWLEDGE_PROTECT_READS")),
		).toBe(false);
	});

	it("is not an error when reads are open (the default)", () => {
		mutable.adminToken = null;
		mutable.protectReads = false;
		expect(
			validateConfig().some((e) => e.includes("KNOWLEDGE_PROTECT_READS")),
		).toBe(false);
	});
});

describe("weak stable admin token warning", () => {
	let savedToken: string | null;

	beforeEach(() => {
		savedToken = mutable.adminToken;
	});

	afterEach(() => {
		mutable.adminToken = savedToken;
	});

	it("warns for a value under 32 characters, without printing it", () => {
		const weak = "hand-picked-16-chr"; // 18 characters, 13 distinct
		mutable.adminToken = weak;
		const warn = spyOn(console, "warn").mockImplementation(() => {});
		try {
			validateConfig();
			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining("KNOWLEDGE_ADMIN_TOKEN is weak"),
			);
			for (const [message] of warn.mock.calls) {
				expect(String(message)).not.toContain(weak);
			}
		} finally {
			warn.mockRestore();
		}
	});

	it("warns for a value with fewer than 12 distinct characters", () => {
		mutable.adminToken = "ab".repeat(20); // 40 characters, 2 distinct
		const warn = spyOn(console, "warn").mockImplementation(() => {});
		try {
			validateConfig();
			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining("KNOWLEDGE_ADMIN_TOKEN is weak"),
			);
		} finally {
			warn.mockRestore();
		}
	});

	it("does not warn for a value of 32 or more characters with variety", () => {
		mutable.adminToken = STRONG_TOKEN;
		const warn = spyOn(console, "warn").mockImplementation(() => {});
		try {
			validateConfig();
			expect(warn).not.toHaveBeenCalledWith(
				expect.stringContaining("KNOWLEDGE_ADMIN_TOKEN is weak"),
			);
		} finally {
			warn.mockRestore();
		}
	});
});
