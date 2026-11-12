import { describe, expect, it } from "bun:test";
import { summarizeCoverage } from "../src/commands/status";

describe("summarizeCoverage", () => {
	it("lists every source seen on either side, sorted", () => {
		const { rows } = summarizeCoverage(
			{ opencode: 10, omp: 4 },
			{ opencode: 7, mcp: 2 },
		);
		expect(rows.map((r) => r.source)).toEqual(["mcp", "omp", "opencode"]);
		expect(rows.find((r) => r.source === "opencode")).toEqual({
			source: "opencode",
			captured: 10,
			injected: 7,
		});
	});

	it("warns when a source is captured but never injected", () => {
		const { warnings } = summarizeCoverage({ omp: 1528 }, {});
		expect(warnings.length).toBe(1);
		expect(warnings[0]).toContain("omp");
		expect(warnings[0]).toContain("1528");
	});

	it("does not warn for sources that were injected or never captured", () => {
		const { warnings } = summarizeCoverage(
			{ opencode: 5 },
			{ opencode: 3, mcp: 9 },
		);
		expect(warnings).toEqual([]);
	});
});
