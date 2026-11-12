/**
 * Contradiction batch contract: the wrapper-object parser, the candidate
 * index guard, the per-call cap and batching, the similarity floor, and the
 * intra-chunk path. The LLM is replaced by a spy, so no network calls are made.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { config } from "../src/config";
import { ContradictionScanner } from "../src/consolidation/contradiction";
import {
	ConsolidationLLM,
	type ContradictionResult,
} from "../src/consolidation/llm";
import {
	MalformedLlmOutputError,
	parseContradictionReply,
} from "../src/consolidation/llm-output";
import type { IKnowledgeStore } from "../src/db/index";
import { logger } from "../src/logger";
import type { KnowledgeEntry } from "../src/types";

type Scanned = KnowledgeEntry & { embedding: number[] };
const consolidation = config.consolidation as {
	reconsolidationThreshold: number;
	contradictionMinSimilarity: number;
	contradictionMaxCandidates: number;
	contradictionMinTopSimilarity: number;
};
const saved = { ...consolidation };

// Unit vectors with a controlled cosine to the query [1, 0].
const vecAt = (sim: number): number[] => [sim, Math.sqrt(1 - sim * sim)];

function makeEntry(id: string, sim: number, topics = ["server"]): Scanned {
	return {
		id,
		content: `entry ${id}`,
		type: "fact",
		topics,
		confidence: 0.9,
		createdAt: 0,
		embedding: vecAt(sim),
	} as unknown as Scanned;
}

beforeEach(() => {
	consolidation.reconsolidationThreshold = 0.94;
	consolidation.contradictionMinSimilarity = 0.4;
	consolidation.contradictionMaxCandidates = Number.POSITIVE_INFINITY;
	consolidation.contradictionMinTopSimilarity = 0;
});

afterEach(() => {
	Object.assign(consolidation, saved);
	llmSpy?.mockRestore();
	llmSpy = null;
});

// ── parseContradictionReply ───────────────────────────────────────────────────

describe("parseContradictionReply: conflicts format", () => {
	const parse = (raw: unknown, n: number) =>
		parseContradictionReply(raw, n, "conflicts");

	it("rejects a checked count that differs from the batch size", () => {
		expect(() => parse({ checked: 9, conflicts: [] }, 10)).toThrow(
			MalformedLlmOutputError,
		);
		expect(() => parse({ checked: 10, conflicts: [] }, 10)).not.toThrow();
	});

	it("rejects a reply without the wrapper object", () => {
		expect(() => parse([], 1)).toThrow(MalformedLlmOutputError);
		expect(() => parse({ conflicts: [] }, 1)).toThrow(MalformedLlmOutputError);
		expect(() => parse(null, 1)).toThrow(MalformedLlmOutputError);
	});

	it("rejects the verdicts list key", () => {
		expect(() => parse({ checked: 1, results: [] }, 1)).toThrow(
			MalformedLlmOutputError,
		);
	});

	it("accepts checked as a numeric string", () => {
		expect(parse({ checked: "2", conflicts: [] }, 2)).toEqual([]);
	});

	it("drops out-of-range candidate numbers and keeps the valid siblings", () => {
		const out = parse(
			{
				checked: 2,
				conflicts: [
					{ candidate: 0, type: "supersede_old", reason: "x" },
					{ candidate: 3, type: "supersede_old", reason: "x" },
					{ candidate: -1, type: "irresolvable", reason: "x" },
					{ candidate: 2, type: "irresolvable", reason: "tie" },
				],
			},
			2,
		);
		expect(out.map((c) => [c.index, c.resolution])).toEqual([
			[2, "irresolvable"],
		]);
	});

	it("keeps the first conflict when a candidate is listed twice", () => {
		const out = parse(
			{
				checked: 1,
				conflicts: [
					{ candidate: 1, type: "supersede_old", reason: "first" },
					{ candidate: 1, type: "irresolvable", reason: "second" },
				],
			},
			1,
		);
		expect(out.map((c) => [c.index, c.reason])).toEqual([[1, "first"]]);
	});

	it("drops unknown types and no_conflict items", () => {
		const out = parse(
			{
				checked: 2,
				conflicts: [
					{ candidate: 1, type: "no_conflict", reason: "x" },
					{ candidate: 2, type: "bogus", reason: "x" },
				],
			},
			2,
		);
		expect(out).toEqual([]);
	});
});

describe("parseContradictionReply: verdicts format (default)", () => {
	const parse = (raw: unknown, n: number) =>
		parseContradictionReply(raw, n, "verdicts");

	it("rejects a checked count that differs from the batch size", () => {
		expect(() =>
			parse(
				{
					checked: 1,
					results: [{ candidate: 1, resolution: "no_conflict", reason: "r" }],
				},
				2,
			),
		).toThrow(MalformedLlmOutputError);
	});

	it("rejects the conflicts list key", () => {
		expect(() => parse({ checked: 1, conflicts: [] }, 1)).toThrow(
			MalformedLlmOutputError,
		);
	});

	it("returns conflicts only, dropping no_conflict verdicts", () => {
		const out = parse(
			{
				checked: 3,
				results: [
					{
						candidate: 1,
						resolution: "no_conflict",
						reason: "different aspect",
					},
					{ candidate: 2, resolution: "supersede_old", reason: "newer" },
					{ candidate: 3, resolution: "no_conflict", reason: "x" },
				],
			},
			3,
		);
		expect(out.map((c) => [c.index, c.resolution])).toEqual([
			[2, "supersede_old"],
		]);
	});

	it("applies the index guard to verdicts", () => {
		const out = parse(
			{
				checked: 2,
				results: [
					{ candidate: 5, resolution: "supersede_old", reason: "x" },
					{ candidate: "[2]", resolution: "irresolvable", reason: "tie" },
				],
			},
			2,
		);
		expect(out.map((c) => [c.index, c.resolution])).toEqual([
			[2, "irresolvable"],
		]);
	});
});

// ── ContradictionScanner: cap, batching, floor, intra-chunk ───────────────────

interface Recorded {
	resolution: string;
	entryId: string;
	candidateId: string;
	mergedData?: { content: string };
}

function fakeStore(pool: Scanned[], applied: Recorded[]): IKnowledgeStore {
	return {
		findContradictionCandidates: async () => pool,
		applyContradictionResolution: async (
			resolution: string,
			entryId: string,
			candidateId: string,
			mergedData?: { content: string },
		) => {
			applied.push({ resolution, entryId, candidateId, mergedData });
		},
	} as unknown as IKnowledgeStore;
}

let llmSpy: { mockRestore(): void } | null = null;

function spyLLM(
	reply: (
		entry: { id: string },
		candidates: Array<{ id: string }>,
	) => ContradictionResult[],
) {
	const calls: Array<{ entryId: string; candidateIds: string[] }> = [];
	llmSpy = spyOn(
		ConsolidationLLM.prototype,
		"detectAndResolveContradiction",
	).mockImplementation(async (newEntry, candidates) => {
		calls.push({
			entryId: newEntry.id,
			candidateIds: candidates.map((c) => c.id),
		});
		return reply(newEntry, candidates);
	});
	return { calls };
}

describe("ContradictionScanner: candidate cap and batching", () => {
	it("checks every candidate when no cap is set, in batches of 10", async () => {
		const entry = makeEntry("new", 1);
		const pool = Array.from({ length: 25 }, (_, i) =>
			makeEntry(`c${String(i).padStart(2, "0")}`, 0.9 - i * 0.01),
		);
		const { calls } = spyLLM(() => []);
		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, []),
			new Map([[entry.id, entry]]),
			new Set([entry.id]),
		);
		expect(calls.map((c) => c.candidateIds.length)).toEqual([10, 10, 5]);
	});

	it("checks only the top N by similarity, in batches of 10", async () => {
		consolidation.contradictionMaxCandidates = 40;
		const entry = makeEntry("new", 1);
		// 60 band candidates, similarity 0.9 down to ~0.49 (ids c00 is most similar).
		const pool = Array.from({ length: 60 }, (_, i) =>
			makeEntry(`c${String(i).padStart(2, "0")}`, 0.9 - i * 0.007),
		);
		const { calls } = spyLLM(() => []);
		const applied: Recorded[] = [];
		const entriesMap = new Map<string, Scanned>([[entry.id, entry]]);

		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, applied),
			entriesMap,
			new Set([entry.id]),
		);

		expect(calls.map((c) => c.candidateIds.length)).toEqual([10, 10, 10, 10]);
		const sent = calls.flatMap((c) => c.candidateIds);
		expect(sent).toEqual(pool.slice(0, 40).map((c) => c.id));
		expect(sent).not.toContain("c40");
	});

	it("honours CONTRADICTION_MAX_CANDIDATES", async () => {
		consolidation.contradictionMaxCandidates = 3;
		const entry = makeEntry("new", 1);
		const pool = [0.9, 0.8, 0.7, 0.6, 0.5].map((s, i) => makeEntry(`c${i}`, s));
		const { calls } = spyLLM(() => []);
		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, []),
			new Map([[entry.id, entry]]),
			new Set([entry.id]),
		);
		expect(calls).toHaveLength(1);
		expect(calls[0].candidateIds).toEqual(["c0", "c1", "c2"]);
	});

	it("skips an entry whose best candidate is below the floor", async () => {
		consolidation.contradictionMinTopSimilarity = 0.8;
		const entry = makeEntry("new", 1);
		const pool = [makeEntry("c0", 0.7), makeEntry("c1", 0.6)];
		const { calls } = spyLLM(() => []);
		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, []),
			new Map([[entry.id, entry]]),
			new Set([entry.id]),
		);
		expect(calls).toHaveLength(0);
	});
});

describe("ContradictionScanner: malformed reply", () => {
	const malformed = () =>
		new MalformedLlmOutputError("contradiction", "checked 2 of 3 candidates");

	it("retries a malformed batch once and applies the retry's conflict", async () => {
		const entry = makeEntry("new", 1);
		const pool = [makeEntry("c0", 0.7), makeEntry("c1", 0.6)];
		let attempts = 0;
		const { calls } = spyLLM((_entry, candidates) => {
			attempts++;
			if (attempts === 1) throw malformed();
			return [
				{
					candidateId: candidates[0].id,
					resolution: "supersede_old",
					reason: "newer",
				},
			];
		});
		const applied: Recorded[] = [];

		const result = await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, applied),
			new Map([[entry.id, entry]]),
			new Set([entry.id]),
		);

		expect(calls).toHaveLength(2);
		expect(applied).toEqual([
			{ resolution: "supersede_old", entryId: "new", candidateId: "c0" },
		]);
		expect(result).toEqual({ detected: 1, resolved: 1, malformedBatches: 0 });
	});

	it("skips a batch that is malformed twice, logs the skip, and scans the next batch", async () => {
		const entry = makeEntry("new", 1);
		// 15 candidates: batch 1 holds c00..c09 (most similar), batch 2 holds c10..c14.
		const pool = Array.from({ length: 15 }, (_, i) =>
			makeEntry(`c${String(i).padStart(2, "0")}`, 0.9 - i * 0.01),
		);
		spyLLM((_entry, candidates) => {
			if (candidates[0].id === "c00") throw malformed();
			return [
				{
					candidateId: "c10",
					resolution: "supersede_old",
					reason: "newer",
				},
			];
		});
		const applied: Recorded[] = [];
		const warn = spyOn(logger, "warn").mockImplementation(() => {});

		const result = await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore(pool, applied),
			new Map([[entry.id, entry]]),
			new Set([entry.id]),
		);

		expect(applied).toEqual([
			{ resolution: "supersede_old", entryId: "new", candidateId: "c10" },
		]);
		expect(result).toEqual({ detected: 1, resolved: 1, malformedBatches: 1 });
		expect(
			warn.mock.calls.some(([msg]) =>
				String(msg).includes(
					"batch 1/2 after one retry — batch skipped (1 skipped this scan)",
				),
			),
		).toBe(true);
		warn.mockRestore();
	});
});

describe("ContradictionScanner: intra-chunk path", () => {
	it("checks two changed entries of one chunk against each other and applies the verdict", async () => {
		// No pre-existing candidates: the only pair is the two changed entries.
		const a = makeEntry("chunk-a", 1);
		const b = makeEntry("chunk-b", 0.85);
		const { calls } = spyLLM((_entry, candidates) => [
			{
				candidateId: candidates[0].id,
				resolution: "supersede_old",
				reason: "newer",
			},
		]);
		const applied: Recorded[] = [];
		const entriesMap = new Map<string, Scanned>([
			[a.id, a],
			[b.id, b],
		]);

		const result = await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore([], applied),
			entriesMap,
			new Set([a.id, b.id]),
		);

		expect(calls).toEqual([{ entryId: "chunk-a", candidateIds: ["chunk-b"] }]);
		expect(applied).toEqual([
			{
				resolution: "supersede_old",
				entryId: "chunk-a",
				candidateId: "chunk-b",
			},
		]);
		expect(result).toEqual({ detected: 1, resolved: 1, malformedBatches: 0 });
	});

	it("ignores an intra-chunk pair outside the band", async () => {
		const a = makeEntry("chunk-a", 1);
		const b = makeEntry("chunk-b", 0.97); // above reconsolidationThreshold: decideMerge's job
		const { calls } = spyLLM(() => []);
		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore([], []),
			new Map([
				[a.id, a],
				[b.id, b],
			]),
			new Set([a.id, b.id]),
		);
		expect(calls).toHaveLength(0);
	});

	it("applies the same cap to intra-chunk candidates", async () => {
		consolidation.contradictionMaxCandidates = 2;
		const a = makeEntry("chunk-a", 1);
		const others = [0.85, 0.84, 0.83, 0.82].map((s, i) =>
			makeEntry(`chunk-${i}`, s),
		);
		const { calls } = spyLLM(() => []);
		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore([], []),
			new Map([a, ...others].map((e) => [e.id, e])),
			new Set([a.id, ...others.map((o) => o.id)]),
		);
		const first = calls.find((c) => c.entryId === "chunk-a");
		expect(first?.candidateIds).toEqual(["chunk-0", "chunk-1"]);
	});
});

describe("ContradictionScanner: stored merge content is scrubbed", () => {
	it("redacts a credential the LLM echoes into a merged entry", async () => {
		const a = makeEntry("merge-a", 1);
		const b = makeEntry("merge-b", 0.85);
		const token = ["ghp_", "b".repeat(36)].join("");
		spyLLM((_entry, candidates) => [
			{
				candidateId: candidates[0].id,
				resolution: "merge",
				reason: "same fact",
				mergedContent: `Deploy with token ${token} on Thursdays`,
				mergedType: "fact",
				mergedTopics: ["server"],
				mergedConfidence: 0.8,
			},
		]);
		const applied: Recorded[] = [];
		const entriesMap = new Map<string, Scanned>([
			[a.id, a],
			[b.id, b],
		]);

		await new ContradictionScanner(new ConsolidationLLM()).scan(
			fakeStore([], applied),
			entriesMap,
			new Set([a.id, b.id]),
		);

		expect(applied).toHaveLength(1);
		const stored = applied[0].mergedData?.content ?? "";
		expect(stored).not.toContain(token);
		expect(stored).toContain("[REDACTED:github_token]");
		expect(stored).toContain("on Thursdays");
	});
});
