import { describe, expect, it } from "bun:test";
import { affectedBy, inRange } from "../scripts/check-bun-advisories";

describe("Bun advisory range check", () => {
	it("evaluates bounded and open ranges", () => {
		expect(inRange("1.3.9", ">= 1.0.21, < 1.4.4")).toBe(true);
		expect(inRange("1.4.4", ">= 1.0.21, < 1.4.4")).toBe(false);
		expect(inRange("1.3.9", ">= 1.3.14, < 1.4.4")).toBe(false);
		expect(inRange("1.3.14", ">= 1.3.14, < 1.4.4")).toBe(true);
		expect(inRange("1.2.0", "< 1.4.4")).toBe(true);
		expect(inRange("1.10.0", "< 1.9.0")).toBe(false);
	});

	it("flags published advisories that cover the pin and skips withdrawn ones", () => {
		const advisories = [
			{
				ghsa_id: "A",
				vulnerabilities: [{ vulnerable_version_range: "< 1.4.4" }],
			},
			{
				ghsa_id: "B",
				vulnerabilities: [{ vulnerable_version_range: ">= 1.3.14, < 1.4.4" }],
			},
			{
				ghsa_id: "C",
				withdrawn_at: "2026-01-01T00:00:00Z",
				vulnerabilities: [{ vulnerable_version_range: "< 9.0.0" }],
			},
		];
		expect(affectedBy("1.3.9", advisories).map((a) => a.ghsa_id)).toEqual([
			"A",
		]);
		expect(affectedBy("1.4.4", advisories)).toEqual([]);
	});

	it("pins a version that parses", () => {
		expect(() => inRange("1.4.4", "< 2.0.0")).not.toThrow();
	});
});
