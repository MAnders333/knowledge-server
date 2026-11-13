import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import { formatActivatedKnowledge } from "../src/activation/format";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import type { KnowledgeEntry } from "../src/types";
import { fakeEmbedding, makeEntry } from "./fixtures";

const QUERY = "alpha topic for the commit test";
const ids = (r: { entries: Array<{ entry: { id: string } }> }) =>
	r.entries.map((e) => e.entry.id);

const gate = config.activation.gate as {
	sessionTtlMs: number;
	tokenBudget: number;
};

let saved: { sessionTtlMs: number; tokenBudget: number };
beforeEach(() => {
	saved = { sessionTtlMs: gate.sessionTtlMs, tokenBudget: gate.tokenBudget };
});
afterEach(() => {
	Object.assign(gate, saved);
});

function engineFor(entries: KnowledgeEntry[]) {
	const recordInjections = mock(() => Promise.resolve());
	const store = {
		id: "test-store",
		getActiveEntriesWithEmbeddings: mock(() => Promise.resolve(entries)),
		scanEmbeddings: mock(() =>
			Promise.resolve(
				entries.map((e) => ({
					id: e.id,
					embedding: new Float32Array(e.embedding ?? []),
				})),
			),
		),
		getEntriesByIds: mock((want: string[]) =>
			Promise.resolve(entries.filter((e) => want.includes(e.id))),
		),
		getSupportSourcesForIds: mock(() => Promise.resolve(new Map())),
		getContradictPairsForIds: mock(() => Promise.resolve(new Map())),
		recordInjections,
	} as unknown as IKnowledgeStore;
	const engine = new ActivationEngine(store);
	engine.embeddings.embedBatch = mock(async (qs: string[]) =>
		qs.map((q) => fakeEmbedding(q)),
	) as typeof engine.embeddings.embedBatch;
	return { engine, recordInjections };
}

describe("session dedupe is recorded only when the response is committed", () => {
	const entries = ["e1", "e2", "e3"].map((id, i) =>
		makeEntry({
			id,
			content: `alpha topic ${i}`,
			embedding: fakeEmbedding(QUERY),
		}),
	);

	it("an abandoned response does not suppress entries for the next prompt", async () => {
		gate.sessionTtlMs = 60_000;
		const { engine } = engineFor(entries);
		const first = await engine.activate(QUERY, { session: "claude-code\0s1" });
		expect(ids(first)).toEqual(["e1", "e2", "e3"]);
		// The client gave up: the server skips commitInjections for this response.
		const second = await engine.activate(QUERY, {
			session: "claude-code\0s1",
		});
		expect(ids(second)).toEqual(["e1", "e2", "e3"]);
		expect(second.telemetry?.deduped).toBe(0);
	});

	it("a committed response suppresses its entries for the next prompt", async () => {
		gate.sessionTtlMs = 60_000;
		const { engine } = engineFor(entries);
		const first = await engine.activate(QUERY, { session: "claude-code\0s1" });
		engine.commitInjections(first.injection);
		const second = await engine.activate(QUERY, {
			session: "claude-code\0s1",
		});
		expect(second.entries).toEqual([]);
		expect(second.telemetry?.deduped).toBe(3);
	});

	it("dry runs write nothing and remember nothing until the plan is committed", async () => {
		gate.sessionTtlMs = 60_000;
		const { engine, recordInjections } = engineFor(entries);
		const first = await engine.activate(QUERY, {
			session: "k",
			dryRun: true,
		});
		expect(first.injection?.writes).toEqual([]);
		const second = await engine.activate(QUERY, {
			session: "k",
			dryRun: true,
		});
		expect(ids(second)).toEqual(["e1", "e2", "e3"]);
		// The replay harness commits explicitly: this records dedupe state only.
		engine.commitInjections(first.injection);
		const third = await engine.activate(QUERY, {
			session: "k",
			dryRun: true,
		});
		expect(third.entries).toEqual([]);
		expect(recordInjections).not.toHaveBeenCalled();
	});
});

describe("one cap for the token budget and the formatter", () => {
	// Each line is 23 + 760 = 783 characters (784 with the newline): 196 tokens.
	// 7 lines = 1372 tokens fit the 1500-token render cap; 8 lines do not.
	const long = "x".repeat(760);
	const entries = Array.from({ length: 10 }, (_, i) =>
		makeEntry({
			id: `long${i}`,
			content: `${long}${i}`.slice(0, 760),
			embedding: fakeEmbedding(QUERY),
		}),
	);

	it("returns exactly the entries the formatter renders, and only those are recorded", async () => {
		gate.sessionTtlMs = 60_000;
		gate.tokenBudget = 2000;
		const { engine, recordInjections } = engineFor(entries);
		const result = await engine.activate(QUERY, {
			limit: 10,
			session: "claude-code\0s1",
		});
		const rendered = formatActivatedKnowledge(result.entries)
			.split("\n")
			.filter((line) => line.startsWith("- ["));
		expect(result.entries.length).toBe(7);
		expect(rendered.length).toBe(result.entries.length);
		expect(result.telemetry?.budgetDropped).toBe(3);
		expect(result.telemetry?.injectedTokens).toBeLessThanOrEqual(1500);

		engine.commitInjections(result.injection);
		expect(recordInjections).toHaveBeenCalledTimes(1);
		expect(recordInjections.mock.calls[0][0]).toEqual(ids(result));
	});

	it("entries cut by the cap are not remembered, so the next prompt gets them", async () => {
		gate.sessionTtlMs = 60_000;
		gate.tokenBudget = 2000;
		const { engine } = engineFor(entries);
		const first = await engine.activate(QUERY, {
			limit: 10,
			session: "claude-code\0s1",
		});
		engine.commitInjections(first.injection);
		const second = await engine.activate(QUERY, {
			limit: 10,
			session: "claude-code\0s1",
		});
		expect(second.telemetry?.deduped).toBe(7);
		expect(ids(second)).toEqual(["long7", "long8", "long9"]);
	});
});
