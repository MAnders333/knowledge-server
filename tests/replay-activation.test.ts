import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	bucketOf,
	loadPrompts,
	renderTable,
	summarize,
} from "../bench/replay-activation";

describe("replay-activation", () => {
	it("buckets prompts by character length", () => {
		expect(bucketOf(0)).toBe("<40");
		expect(bucketOf(39)).toBe("<40");
		expect(bucketOf(40)).toBe("40-199");
		expect(bucketOf(200)).toBe("200-999");
		expect(bucketOf(1000)).toBe(">=1000");
	});

	it("loads prompts from JSONL, skipping blank lines", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-replay-test-"));
		try {
			const path = join(dir, "prompts.jsonl");
			writeFileSync(path, '{"prompt":"one"}\n\n{"prompt":"two"}\n');
			expect(loadPrompts(path)).toEqual(["one", "two"]);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("rejects a line without a prompt string", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-replay-test-"));
		try {
			const path = join(dir, "bad.jsonl");
			writeFileSync(path, '{"text":"no prompt key"}\n');
			expect(() => loadPrompts(path)).toThrow(/line 1/);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("computes cap-hit, zero-hit, hub share and latency from records", () => {
		const records = [
			{
				promptChars: 10,
				nReturned: 2,
				entryIds: ["a", "b"],
				latencyMs: 10,
				gateMode: "off",
			},
			{
				promptChars: 10,
				nReturned: 2,
				entryIds: ["a", "c"],
				latencyMs: 30,
				gateMode: "off",
			},
			{
				promptChars: 300,
				nReturned: 0,
				entryIds: [],
				latencyMs: 20,
				gateMode: "off",
			},
			{
				promptChars: 300,
				nReturned: 2,
				entryIds: ["a", "b"],
				latencyMs: 40,
				gateMode: "rerank",
			},
		];
		const s = summarize(records, 2);
		expect(s.activations).toBe(4);
		expect(s.capHitPct).toBe(75);
		expect(s.zeroHitPct).toBe(25);
		// 6 slots in total; "a" fills 3 of them.
		expect(s.top1SlotPct).toBe(50);
		expect(s.top20SlotPct).toBe(100);
		// Interpolated: median of 10, 30, 20, 40 is 25.
		expect(s.latencyMsP50).toBe(25);
		expect(s.gateMode).toEqual({ off: 3, rerank: 1 });
		const small = s.buckets.find((b) => b.bucket === "<40");
		expect(small?.zeroHitPct).toBe(0);
		expect(s.buckets.find((b) => b.bucket === "200-999")?.zeroHitPct).toBe(50);
	});

	it("renders a markdown table without nulls when empty", () => {
		const table = renderTable(summarize([], 8), 8);
		expect(table).toContain("| Activations | 0 |");
		expect(table).toContain("n/a");
	});
});
