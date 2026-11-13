import { describe, expect, it } from "bun:test";
import {
	affectedBy,
	checkPinnedBun,
	inRange,
} from "../scripts/check-bun-advisories";

/** A fetch that answers every call with the same response (or throws the error). */
function fetchReturning(outcome: Response | Error): typeof fetch {
	return (async () => {
		if (outcome instanceof Error) throw outcome;
		return outcome;
	}) as typeof fetch;
}

/** The API's advisory list for the range "< 1.4.4". */
const ADVISORY_BODY = JSON.stringify([
	{
		ghsa_id: "GHSA-test-0000-0000",
		vulnerabilities: [{ vulnerable_version_range: "< 1.4.4" }],
	},
]);

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

describe("checkPinnedBun", () => {
	it("fails (affected) when the API lists an advisory that covers the pin", async () => {
		const result = await checkPinnedBun(
			"1.3.9",
			fetchReturning(new Response(ADVISORY_BODY, { status: 200 })),
		);
		expect(result.status).toBe("affected");
	});

	it("passes when the API lists no advisory covering the pin", async () => {
		const result = await checkPinnedBun(
			"1.4.4",
			fetchReturning(new Response(ADVISORY_BODY, { status: 200 })),
		);
		expect(result.status).toBe("ok");
	});

	it.each([
		["a 5xx", new Response("", { status: 503 })],
		["a 429", new Response("", { status: 429 })],
		[
			"an exhausted rate limit (403)",
			new Response("", {
				status: 403,
				headers: { "x-ratelimit-remaining": "0" },
			}),
		],
		["a network error", new TypeError("fetch failed")],
	])("treats %s as unavailable, not as a verdict", async (_label, outcome) => {
		const result = await checkPinnedBun("1.4.4", fetchReturning(outcome));
		expect(result.status).toBe("unavailable");
	});

	it("still rejects a non-transient API error such as a 404", async () => {
		await expect(
			checkPinnedBun(
				"1.4.4",
				fetchReturning(new Response("", { status: 404 })),
			),
		).rejects.toThrow("advisory API answered 404");
	});
});
