/**
 * Tests for ConsolidationLLM and the exported prompt-formatting helpers.
 *
 * All tests that exercise the LLM methods mock `generateText` from the `ai`
 * package — no real network calls are made. This validates:
 *   - JSON parsing strategies (clean JSON, code-fenced, bracket-match, partial-array)
 *   - Response filtering (bad types, missing fields, no_conflict elision)
 *   - Safe defaults on merge parse failure (decideMerge → "insert"); malformed
 *     extraction output throws a SyntaxError (deterministic failure)
 *   - formatEpisodes output shape
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { fileURLToPath } from "node:url";
import * as aiModule from "ai";
import { config } from "../src/config";
import {
	ConsolidationLLM,
	formatEpisodes,
	omitRequestParams,
	renameMaxTokensParam,
} from "../src/consolidation/llm";
import { logger } from "../src/logger";
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
		expect(out).toContain("<episode-title>Churn Analysis</episode-title>");
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

	it("formats documents with Document header and no project label", () => {
		const ep = makeEpisode({
			sessionTitle: "My Knowledge Doc",
			projectName: "should-not-appear",
			contentType: "document",
			content: "# My Knowledge Doc\n\nSome content here.",
		});
		const out = formatEpisodes([ep]);
		expect(out).toContain(
			'### Document: "<episode-title>My Knowledge Doc</episode-title>"',
		);
		expect(out).not.toContain("project:");
		expect(out).not.toContain("(document)");
		expect(out).toContain("Some content here.");
	});

	it("does not label plain message episodes", () => {
		const ep = makeEpisode({ contentType: "messages" });
		const out = formatEpisodes([ep]);
		expect(out).toContain("### Session:");
		expect(out).not.toContain("compaction summary");
		expect(out).not.toContain("(document)");
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
				source: "test",
			},
		]);
		mockGenerateText(`Here is the result:\n${json}\nThat's all.`);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
	});

	it("recovers partial array when response is truncated after a complete object", async () => {
		// Simulate a response truncated mid-second-object
		const partial = `[{"type":"fact","content":"Entry one.","topics":["a"],"confidence":0.9,"source":"t"},{"type":"fact","content":"Entry two tr`;
		mockGenerateText(partial);
		const result = await llm.extractKnowledge("episodes");
		// Strategy 4 should recover the first complete object
		expect(result.length).toBeGreaterThanOrEqual(1);
		expect(result[0].content).toBe("Entry one.");
	});

	it("throws a SyntaxError (deterministic failure) when the response is unparseable", async () => {
		mockGenerateText("I'm sorry, I cannot help with that.");
		await expect(llm.extractKnowledge("episodes")).rejects.toBeInstanceOf(
			SyntaxError,
		);
	});

	it("throws when the response is valid JSON but not an array", async () => {
		mockGenerateText(JSON.stringify({ entries: [] }));
		await expect(llm.extractKnowledge("episodes")).rejects.toBeInstanceOf(
			SyntaxError,
		);
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
					source: "t",
				},
				{
					type: "fact",
					content: "Good entry.",
					topics: [],
					confidence: 0.9,
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
					source: "t",
				},
				{
					type: "fact",
					content: "Valid.",
					topics: [],
					confidence: 0.9,
					source: "t",
				},
			]),
		);
		const result = await llm.extractKnowledge("episodes");
		expect(result.length).toBe(1);
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

	it("throws a SyntaxError when the response is unparseable", async () => {
		mockGenerateText("Sorry, cannot process this.");
		await expect(
			llm.detectAndResolveContradiction(newEntry, candidates),
		).rejects.toBeInstanceOf(SyntaxError);
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

// ── synthesizePrinciple (cluster-first peer API) ──────────────────────────────

describe("ConsolidationLLM.synthesizePrinciple", () => {
	let llm: ConsolidationLLM;
	const peers = [
		{
			id: "p1",
			content:
				"Always wrap LLM-sourced content in XML tags to prevent injection.",
			type: "principle" as const,
			topics: ["security", "llm", "prompts"],
		},
		{
			id: "p2",
			content:
				"The decideMerge prompt wraps content in XML to prevent injection.",
			type: "fact" as const,
			topics: ["security", "llm"],
		},
		{
			id: "p3",
			content:
				"extractKnowledge wraps episode content in XML to prevent injection.",
			type: "fact" as const,
			topics: ["security", "extraction"],
		},
	];

	beforeEach(() => {
		llm = new ConsolidationLLM();
	});

	it("returns synthesized principles when the LLM responds with a valid JSON array", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "principle",
					content:
						"All LLM prompts that include untrusted content should use XML delimiters to prevent injection.",
					topics: ["security", "llm", "prompts"],
					confidence: 0.75,
					sourceIds: ["p2", "p3"],
				},
			]),
		);
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toHaveLength(1);
		expect(results[0].type).toBe("principle");
		expect(results[0].content).toContain("XML");
		expect(results[0].confidence).toBeGreaterThanOrEqual(0.5);
		expect(results[0].confidence).toBeLessThanOrEqual(0.85);
		expect(results[0].sourceIds).toEqual(["p2", "p3"]);
	});

	it("returns empty array when the LLM responds with empty array (bar not met)", async () => {
		mockGenerateText("[]");
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toEqual([]);
	});

	it("filters out results with fact type (synthesis must be principle or pattern)", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "fact",
					content: "Some specific fact.",
					topics: ["test"],
					confidence: 0.8,
					sourceIds: [],
				},
			]),
		);
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toEqual([]);
	});

	it("filters out hallucinated sourceIds not in the peer list", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "pattern",
					content: "XML wrapping is a recurring security pattern.",
					topics: ["security"],
					confidence: 0.7,
					sourceIds: ["p1", "hallucinated-id-xyz", "p2"],
				},
			]),
		);
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toHaveLength(1);
		expect(results[0].sourceIds).toEqual(["p1", "p2"]); // hallucinated-id-xyz filtered
	});

	it("clamps confidence to [0.5, 0.85]", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "principle",
					content: "Some principle.",
					topics: ["test"],
					confidence: 0.99, // above cap
					sourceIds: [],
				},
			]),
		);
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toHaveLength(1);
		expect(results[0].confidence).toBe(0.85);
	});

	it("returns empty array when no peers are provided", async () => {
		const results = await llm.synthesizePrinciple([]);
		expect(results).toEqual([]);
	});

	it("returns empty array on parse failure and does not throw", async () => {
		mockGenerateText("not json at all");
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toEqual([]);
	});

	it("returns multiple principles when the LLM returns multiple valid results", async () => {
		mockGenerateText(
			JSON.stringify([
				{
					type: "principle",
					content: "XML wrapping prevents prompt injection.",
					topics: ["security"],
					confidence: 0.75,
					sourceIds: ["p1", "p2"],
				},
				{
					type: "pattern",
					content: "Defense-in-depth is applied at every LLM call boundary.",
					topics: ["security", "llm"],
					confidence: 0.65,
					sourceIds: ["p1", "p3"],
				},
			]),
		);
		const results = await llm.synthesizePrinciple(peers);
		expect(results).toHaveLength(2);
		expect(results[0].type).toBe("principle");
		expect(results[1].type).toBe("pattern");
	});
});

// ── renameMaxTokensParam ───────────────────────────────────────────────────────

describe("renameMaxTokensParam", () => {
	it("renames max_tokens to max_completion_tokens, preserving the value", () => {
		expect(
			renameMaxTokensParam({
				model: "gpt-5.6-luna",
				max_tokens: 8192,
				temperature: 0.2,
			}),
		).toEqual({
			model: "gpt-5.6-luna",
			max_completion_tokens: 8192,
			temperature: 0.2,
		});
	});

	it("leaves bodies without max_tokens untouched", () => {
		const body = { model: "gpt-5.6-luna", messages: [] };
		expect(renameMaxTokensParam(body)).toEqual(body);
	});

	it("passes through an absent max_tokens instead of emitting an undefined value", () => {
		expect(renameMaxTokensParam({ max_tokens: undefined })).toEqual({});
	});
});

// ── omitRequestParams ─────────────────────────────────────────────────────────

describe("omitRequestParams", () => {
	it("removes the listed parameters, preserving the rest", () => {
		expect(
			omitRequestParams(
				{
					model: "gpt-5.6-luna",
					temperature: 0.2,
					top_p: 0.9,
					max_completion_tokens: 8192,
				},
				["temperature", "top_p"],
			),
		).toEqual({ model: "gpt-5.6-luna", max_completion_tokens: 8192 });
	});

	it("ignores parameters that are not present in the body", () => {
		expect(omitRequestParams({ model: "m" }, ["temperature"])).toEqual({
			model: "m",
		});
	});

	it("returns the body unchanged when the list is empty", () => {
		const body = { model: "m", temperature: 0.2 };
		expect(omitRequestParams(body, [])).toEqual(body);
	});
});

// ── per-call-type timeouts and retry policy ───────────────────────────────────

const CONFIG_MODULE = fileURLToPath(
	new URL("../src/config.ts", import.meta.url),
);

/** Read config.llm.timeoutsMs in a fresh process, so env is read at import time. */
async function timeoutsFromEnv(env: Record<string, string>) {
	const inherited = Object.fromEntries(
		Object.entries(process.env).filter(
			(entry): entry is [string, string] =>
				entry[1] !== undefined && !entry[0].startsWith("LLM_TIMEOUT_"),
		),
	);
	const script = `import(${JSON.stringify(CONFIG_MODULE)}).then(({ config }) => console.log(JSON.stringify(config.llm.timeoutsMs)));`;
	const proc = Bun.spawn([process.execPath, "-e", script], {
		env: { ...inherited, ...env },
		stdout: "pipe",
		stderr: "pipe",
	});
	const [stdout, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		proc.exited,
	]);
	expect(exitCode).toBe(0);
	return JSON.parse(stdout.trim().split("\n").at(-1) ?? "");
}

describe("per-call-type LLM timeout defaults and env resolution", () => {
	it("defaults to 60s contradiction, 90s merge, 300s extraction and synthesis", async () => {
		expect(await timeoutsFromEnv({})).toEqual({
			contradiction: 60_000,
			merge: 90_000,
			extraction: 300_000,
			synthesis: 300_000,
		});
	});

	it("applies each LLM_TIMEOUT_<TYPE>_MS override to its own call type only", async () => {
		expect(
			await timeoutsFromEnv({
				LLM_TIMEOUT_CONTRADICTION_MS: "20000",
				LLM_TIMEOUT_MERGE_MS: "30000",
				LLM_TIMEOUT_EXTRACTION_MS: "400000",
				LLM_TIMEOUT_SYNTHESIS_MS: "500000",
			}),
		).toEqual({
			contradiction: 20_000,
			merge: 30_000,
			extraction: 400_000,
			synthesis: 500_000,
		});
	});

	it("uses LLM_TIMEOUT_MS as the fallback for every type without its own override", async () => {
		expect(await timeoutsFromEnv({ LLM_TIMEOUT_MS: "45000" })).toEqual({
			contradiction: 45_000,
			merge: 45_000,
			extraction: 45_000,
			synthesis: 45_000,
		});
	});

	it("gives a per-type override precedence over LLM_TIMEOUT_MS", async () => {
		expect(
			await timeoutsFromEnv({
				LLM_TIMEOUT_MS: "45000",
				LLM_TIMEOUT_SYNTHESIS_MS: "7000",
			}),
		).toEqual({
			contradiction: 45_000,
			merge: 45_000,
			extraction: 45_000,
			synthesis: 7_000,
		});
	});

	it("ignores non-numeric per-type values and clamps to a minimum of 1 ms", async () => {
		expect(
			await timeoutsFromEnv({
				LLM_TIMEOUT_MERGE_MS: "soon",
				LLM_TIMEOUT_EXTRACTION_MS: "0",
				LLM_TIMEOUT_MS: "45000",
			}),
		).toEqual({
			contradiction: 45_000,
			merge: 45_000,
			extraction: 1,
			synthesis: 45_000,
		});
	});
});

describe("ConsolidationLLM timeout and retry policy", () => {
	let llm: ConsolidationLLM;
	const savedTimeouts = { ...config.llm.timeoutsMs };
	const savedRetry = {
		maxRetries: config.llm.maxRetries,
		retryBaseDelayMs: config.llm.retryBaseDelayMs,
	};
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
	];

	beforeEach(() => {
		llm = new ConsolidationLLM();
		config.llm.maxRetries = 2;
		config.llm.retryBaseDelayMs = 0;
	});

	afterEach(() => {
		Object.assign(config.llm.timeoutsMs, savedTimeouts);
		Object.assign(config.llm, savedRetry);
	});

	/** Spy on generateText, creating the spy if this test has not yet. */
	function generateTextMock() {
		generateTextSpy ??= spyOn(aiModule, "generateText");
		return generateTextSpy;
	}

	/** Stand-in model call that never answers and rejects only when aborted. */
	function hangUntilAborted(opts: { abortSignal?: AbortSignal }) {
		const { promise, reject } = Promise.withResolvers<never>();
		opts.abortSignal?.addEventListener("abort", () =>
			reject(new Error("This operation was aborted")),
		);
		return promise;
	}

	it("aborts a hung contradiction call at its budget, logs the type, and does not retry", async () => {
		config.llm.timeoutsMs.contradiction = 50;
		const spy = generateTextMock();
		spy.mockImplementation(
			hangUntilAborted as unknown as typeof aiModule.generateText,
		);
		const errorSpy = spyOn(logger, "error").mockImplementation(() => {});
		const started = Date.now();
		try {
			await expect(
				llm.detectAndResolveContradiction(newEntry, candidates),
			).rejects.toThrow("aborted");
			expect(Date.now() - started).toBeLessThan(5_000);
			expect(spy).toHaveBeenCalledTimes(1);
			const logged = errorSpy.mock.calls.map((c) => String(c[0])).join("\n");
			expect(logged).toContain(
				"contradiction call to anthropic/claude-haiku-4-5 exceeded 50ms timeout (attempt 1/3) — not retrying",
			);
		} finally {
			errorSpy.mockRestore();
		}
	});

	it("uses the merge budget for merge decisions", async () => {
		config.llm.timeoutsMs.merge = 50;
		const spy = generateTextMock();
		spy.mockImplementation(
			hangUntilAborted as unknown as typeof aiModule.generateText,
		);
		const errorSpy = spyOn(logger, "error").mockImplementation(() => {});
		try {
			await expect(llm.decideMerge(existing, extracted)).rejects.toThrow(
				"aborted",
			);
			expect(spy).toHaveBeenCalledTimes(1);
			expect(String(errorSpy.mock.calls[0]?.[0])).toContain(
				"merge call to anthropic/claude-haiku-4-5 exceeded 50ms timeout",
			);
		} finally {
			errorSpy.mockRestore();
		}
	});

	it("retries a transient non-timeout error up to maxRetries, then succeeds", async () => {
		const spy = generateTextMock();
		spy
			.mockRejectedValueOnce(new Error("HTTP 503 upstream unavailable"))
			.mockRejectedValueOnce(new Error("HTTP 503 upstream unavailable"))
			.mockResolvedValueOnce({
				text: JSON.stringify({ action: "keep" }),
			} as Awaited<ReturnType<typeof aiModule.generateText>>);
		const d = await llm.decideMerge(existing, extracted);
		expect(d.action).toBe("keep");
		expect(spy).toHaveBeenCalledTimes(3);
	});

	it("gives up after 1 + maxRetries attempts when a non-timeout error persists", async () => {
		const spy = generateTextMock();
		spy.mockRejectedValue(new Error("HTTP 503 upstream unavailable"));
		await expect(llm.decideMerge(existing, extracted)).rejects.toThrow(
			"HTTP 503",
		);
		expect(spy).toHaveBeenCalledTimes(3);
	});

	it("passes maxRetries: 0 to generateText so the SDK never retries inside an attempt", async () => {
		const spy = mockGenerateText(JSON.stringify({ action: "keep" }));
		await llm.decideMerge(existing, extracted);
		expect(spy.mock.calls[0]?.[0]).toMatchObject({ maxRetries: 0 });
	});
});
