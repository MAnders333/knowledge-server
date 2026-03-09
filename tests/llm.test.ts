/**
 * Tests for ConsolidationLLM and the exported prompt-formatting helpers.
 *
 * All tests that exercise the LLM methods mock `generateText` from the `ai`
 * package — no real network calls are made. This validates:
 *   - JSON parsing strategies (clean JSON, code-fenced, bracket-match, partial-array)
 *   - Response filtering (bad types, missing fields, no_conflict elision)
 *   - Safe defaults on parse failure (decideMerge → "insert", extractKnowledge → [])
 *   - formatEpisodes output shape
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as aiModule from "ai";
import {
	ConsolidationLLM,
	formatEpisodes,
} from "../src/consolidation/llm";
import type { Episode } from "../src/types";

// ── helpers ───────────────────────────────────────────────────────────────────

let generateTextSpy: ReturnType<
	typeof spyOn<typeof aiModule, "generateText">
> | null = null;

function mockGenerateText(text: string) {
	// Reuse the existing spy if present (to avoid layering spies), otherwise create one.
	if (!generateTextSpy) {
		generateTextSpy = spyOn(aiModule, "generateText");
	}
	generateTextSpy.mockResolvedValue({
		text,
		// minimal shape — only `text` is used by complete()
	} as Awaited<ReturnType<typeof aiModule.generateText>>);
	return generateTextSpy;
}

afterEach(() => {
	generateTextSpy?.mockRestore();
	generateTextSpy = null;
});

function makeEpisode(overrides: Partial<Episode> = {}): Episode {
	const now = Date.now();
	return {
		sessionId: "s1",
		startMessageId: "m1",
		endMessageId: "m2",
		sessionTitle: "Test Session",
		projectName: "test-project",
		directory: "/tmp",
		timeCreated: now,
		maxMessageTime: now,
		content: "user: hello\nassistant: world",
		contentType: "messages",
		approxTokens: 10,
		...overrides,
	};
}

// ── formatEpisodes ────────────────────────────────────────────────────────────

describe("formatEpisodes", () => {
	it("includes session title, project, and content", () => {
		const ep = makeEpisode({
			sessionTitle: "Churn Analysis",
			projectName: "analytics",
		});
		const out = formatEpisodes([ep]);
		expect(out).toContain('"Churn Analysis"');
		expect(out).toContain("analytics");
		expect(out).toContain("user: hello");
	});

	it("labels compaction summaries", () => {
		const ep = makeEpisode({ contentType: "compaction_summary" });
		const out = formatEpisodes([ep]);
		expect(out).toContain("compaction summary");
	});

	it("separates multiple episodes with a horizontal rule", () => {
		const out = formatEpisodes([
			makeEpisode(),
			makeEpisode({ sessionId: "s2" }),
		]);
		expect(out).toContain("---");
	});

	it("returns empty string for empty input", () => {
		expect(formatEpisodes([])).toBe("");
	});
});

// ── ConsolidationLLM.extractKnowledge ────────────────────────────────────────

describe("ConsolidationLLM.extractKnowledge", () => {
	let llm: ConsolidationLLM;
	beforeEach(() => {
		llm = new ConsolidationLLM();
	});

	it("parses a clean JSON array response", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "fact",
					content: "TypeScript is statically typed.",
					topics: ["ts"],
					confidence: 0.9,
					scope: "personal",
					source: "test",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
		expect(result[0].content).toBe("TypeScript is statically typed.");
	});

	it("parses a response wrapped in a JSON code fence", async () => {
		const body = JSON.stringify([
			{
				type: "principle",
				content: "Always pre-aggregate.",
				topics: ["sql"],
				confidence: 0.8,
				scope: "team",
				source: "test",
			},
		]);
		mockGenerateText(`\`\`\`json\n${body}\n\`\`\``);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
		expect(result[0].type).toBe("principle");
	});

	it("parses a response with leading text via bracket-match strategy", async () => {
		const json = JSON.stringify([
			{
				type: "fact",
				content: "Bun is fast.",
				topics: ["bun"],
				confidence: 0.85,
				scope: "personal",
				source: "test",
			},
		]);
		mockGenerateText(`Here is the result:\n${json}\nThat's all.`);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
	});

	it("recovers partial array when response is truncated after a complete object", async () => {
		// Simulate a response truncated mid-second-object
		const partial = `[{"type":"fact","content":"Entry one.","topics":["a"],"confidence":0.9,"scope":"personal","source":"t"},{"type":"fact","content":"Entry two tr`;
		mockGenerateText(partial);
		const result = await llm.extractKnowledge("episodes");
		// Strategy 4 should recover the first complete object
		expect(result.length).toBeGreaterThanOrEqual(1);
		expect(result[0].content).toBe("Entry one.");
	});

	it("returns [] when the response is unparseable", async () => {
		mockGenerateText("I'm sorry, I cannot help with that.");
		const result = await llm.extractKnowledge("episodes");
		expect(result).toEqual([]);
	});

	it("clamps entries with invalid types to 'fact' (the default fallback)", async () => {
		// Invalid type strings are clamped via clampKnowledgeType rather than dropped,
		// so no knowledge is silently lost (e.g. "fact/principle" → "fact").
		mockGenerateText(
			JSON.stringify([
				{
					type: "INVALID",
					content: "Bad type entry.",
					topics: [],
					confidence: 0.5,
					scope: "personal",
					source: "t",
				},
				{
					type: "fact",
					content: "Good entry.",
					topics: [],
					confidence: 0.9,
					scope: "personal",
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(2);
		expect(result[0].type).toBe("fact"); // clamped from "INVALID"
		expect(result[1].content).toBe("Good entry.");
	});

	it("filters out entries missing content", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "fact",
					content: "",
					topics: [],
					confidence: 0.9,
					scope: "personal",
					source: "t",
				},
				{
					type: "fact",
					content: "Valid.",
					topics: [],
					confidence: 0.9,
					scope: "personal",
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
	});

	it("preserves scope: team when LLM returns it", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "fact",
					content: "The MI Jira project key is MI.",
					topics: ["jira"],
					confidence: 0.9,
					scope: "team",
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
		expect(result[0].scope).toBe("team");
	});

	it("preserves scope: personal when LLM returns it", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "procedure",
					content: "My local bun binary is at ~/.bun/bin/bun.",
					topics: ["setup"],
					confidence: 0.8,
					scope: "personal",
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
		expect(result[0].scope).toBe("personal");
	});

	it("clamps an invalid scope value to 'personal'", async () => {
		// The extraction layer clamps unknown scope strings to the default "personal"
		// to prevent SQLite CHECK constraint violations downstream.
		mockGenerateText(
			JSON.stringify([
				{
					type: "fact",
					content: "Some fact.",
					topics: [],
					confidence: 0.9,
					scope: "unknown",
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
		expect(result[0].scope).toBe("personal");
	});
});

// ── ConsolidationLLM.decideMerge ─────────────────────────────────────────────

describe("ConsolidationLLM.decideMerge", () => {
	let llm: ConsolidationLLM;
	const existing = {
		content: "Port is 8080.",
		type: "fact",
		topics: ["server"],
		confidence: 0.9,
	};
	const extracted = {
		content: "Port is 9090.",
		type: "fact",
		topics: ["server"],
		confidence: 0.85,
	};

	beforeEach(() => {
		llm = new ConsolidationLLM();
	});

	it('returns "keep" decision when LLM says keep', async () => {
		mockGenerateText(JSON.stringify({ action: "keep" }));
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("keep");
	});

	it('returns "update" decision with merged fields', async () => {
		mockGenerateText(
			JSON.stringify({
				action: "update",
				content: "Port changed from 8080 to 9090.",
				type: "fact",
				topics: ["server"],
				confidence: 0.92,
			}),
		);
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("update");
		if (d.action === "update") {
			expect(d.content).toContain("9090");
			expect(d.confidence).toBeCloseTo(0.92);
		}
	});

	it('returns "insert" decision when LLM says insert', async () => {
		mockGenerateText(JSON.stringify({ action: "insert" }));
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("insert");
	});

	it('defaults to "insert" when response is unparseable', async () => {
		mockGenerateText("I cannot decide.");
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("insert");
	});

	it('defaults to "insert" when action is unrecognised', async () => {
		mockGenerateText(JSON.stringify({ action: "merge_all" }));
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("insert");
	});

	it("parses response wrapped in a code fence", async () => {
		mockGenerateText('```json\n{"action":"keep"}\n```');
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("keep");
	});
});

// ── ConsolidationLLM.detectAndResolveContradiction ───────────────────────────

describe("ConsolidationLLM.detectAndResolveContradiction", () => {
	let llm: ConsolidationLLM;
	const newEntry = {
		id: "new-1",
		content: "Port is 9090.",
		type: "fact",
		topics: ["server"],
		confidence: 0.9,
		createdAt: Date.now(),
	};
	const candidates = [
		{
			id: "old-1",
			content: "Port is 8080.",
			type: "fact",
			topics: ["server"],
			confidence: 0.85,
			createdAt: Date.now() - 1000,
		},
		{
			id: "old-2",
			content: "Server uses HTTPS.",
			type: "fact",
			topics: ["server"],
			confidence: 0.9,
			createdAt: Date.now() - 2000,
		},
	];

	beforeEach(() => {
		llm = new ConsolidationLLM();
	});

	it("returns [] immediately when candidates array is empty", async () => {
		const spy = mockGenerateText("should not be called");
		const result = await llm.detectAndResolveContradiction(newEntry, []);
		expect(result).toEqual([]);
		expect(spy).not.toHaveBeenCalled();
	});

	it("filters out no_conflict results", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					candidateId: "old-1",
					resolution: "supersede_old",
					reason: "newer port",
				},
				{
					candidateId: "old-2",
					resolution: "no_conflict",
					reason: "different aspect",
				},
			]),
		);
		const result = await llm.detectAndResolveContradiction(
			newEntry,
			candidates,
		);
		expect(result.length).toBe(1);
		expect(result[0].candidateId).toBe("old-1");
		expect(result[0].resolution).toBe("supersede_old");
	});

	it("returns [] on unparseable response (no crash)", async () => {
		mockGenerateText("Sorry, cannot process this.");
		const result = await llm.detectAndResolveContradiction(
			newEntry,
			candidates,
		);
		expect(result).toEqual([]);
	});

	it("includes merge fields when resolution is 'merge'", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					candidateId: "old-1",
					resolution: "merge",
					reason: "both partially correct",
					mergedContent: "Port was 8080, changed to 9090.",
					mergedType: "fact",
					mergedTopics: ["server"],
					mergedConfidence: 0.88,
				},
			]),
		);
		const result = await llm.detectAndResolveContradiction(
			newEntry,
			candidates,
		);
		expect(result.length).toBe(1);
		expect(result[0].resolution).toBe("merge");
		expect(result[0].mergedContent).toContain("9090");
	});

	it("handles irresolvable resolution", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					candidateId: "old-1",
					resolution: "irresolvable",
					reason: "equal evidence both ways",
				},
			]),
		);
		const result = await llm.detectAndResolveContradiction(
			newEntry,
			candidates,
		);
		expect(result.length).toBe(1);
		expect(result[0].resolution).toBe("irresolvable");
	});

	it("uses partial-array recovery when response is truncated", async () => {
		// One complete object, second truncated
		const partial = `[{"candidateId":"old-1","resolution":"supersede_old","reason":"newer"},{"candidateId":"old-2","resolution":"no_con`;
		mockGenerateText(partial);
		const result = await llm.detectAndResolveContradiction(
			newEntry,
			candidates,
		);
		// Should recover old-1 (supersede_old is not no_conflict so it passes the filter)
		expect(result.some((r) => r.candidateId === "old-1")).toBe(true);
	});
});

// ── synthesizePrinciple ───────────────────────────────────────────────────────

describe("ConsolidationLLM.synthesizePrinciple", () => {
	let llm: ConsolidationLLM;
	const anchor = {
		content: "Always wrap LLM-sourced content in XML tags to prevent injection.",
		type: "principle" as const,
		topics: ["security", "llm", "prompts"],
		observationCount: 3,
	};
	const neighbors = [
		{
			id: "n1",
			content: "The decideMerge prompt wraps content in XML to prevent injection.",
			type: "fact" as const,
			topics: ["security", "llm"],
		},
		{
			id: "n2",
			content: "extractKnowledge wraps episode content in XML to prevent injection.",
			type: "fact" as const,
			topics: ["security", "extraction"],
		},
	];

	beforeEach(() => {
		llm = new ConsolidationLLM();
	});

	it("returns a synthesized principle when the LLM responds with valid JSON", async () => {
		mockGenerateText(
			JSON.stringify({
				type: "principle",
				content: "All LLM prompts that include untrusted content should use XML delimiters to prevent injection.",
				topics: ["security", "llm", "prompts"],
				confidence: 0.75,
				sourceIds: ["n1", "n2"],
			}),
		);
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		if (result === null) throw new Error("Expected non-null synthesis result");
		expect(result.type).toBe("principle");
		expect(result.content).toContain("XML");
		expect(result.confidence).toBeGreaterThanOrEqual(0.5);
		expect(result.confidence).toBeLessThanOrEqual(0.85);
		expect(result.sourceIds).toEqual(["n1", "n2"]);
	});

	it("returns null when the LLM responds with empty object (bar not met)", async () => {
		mockGenerateText("{}");
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		expect(result).toBeNull();
	});

	it("returns null when the LLM returns a fact type (synthesis must be principle or pattern)", async () => {
		mockGenerateText(
			JSON.stringify({
				type: "fact",
				content: "Some specific fact.",
				topics: ["test"],
				confidence: 0.8,
				sourceIds: [],
			}),
		);
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		expect(result).toBeNull();
	});

	it("filters out hallucinated sourceIds not in the neighbor list", async () => {
		mockGenerateText(
			JSON.stringify({
				type: "pattern",
				content: "XML wrapping is a recurring security pattern.",
				topics: ["security"],
				confidence: 0.7,
				sourceIds: ["n1", "hallucinated-id-xyz", "n2"],
			}),
		);
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		if (result === null) throw new Error("Expected non-null synthesis result");
		expect(result.sourceIds).toEqual(["n1", "n2"]); // hallucinated-id-xyz filtered
	});

	it("clamps confidence to [0.5, 0.85]", async () => {
		mockGenerateText(
			JSON.stringify({
				type: "principle",
				content: "Some principle.",
				topics: ["test"],
				confidence: 0.99, // above cap
				sourceIds: [],
			}),
		);
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		if (result === null) throw new Error("Expected non-null synthesis result");
		expect(result.confidence).toBe(0.85);
	});

	it("returns null when no neighbors are provided", async () => {
		const result = await llm.synthesizePrinciple(anchor, []);
		expect(result).toBeNull();
	});

	it("returns null on parse failure and does not throw", async () => {
		mockGenerateText("not json at all");
		const result = await llm.synthesizePrinciple(anchor, neighbors);
		expect(result).toBeNull();
	});
});
