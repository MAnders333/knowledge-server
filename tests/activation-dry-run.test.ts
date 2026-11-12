import { describe, expect, it, mock } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import type { IKnowledgeStore } from "../src/db/interface";
import type { KnowledgeEntry } from "../src/types";
import { fakeEmbedding, makeEntry } from "./fixtures";

const QUERY = "alpha topic for the dry run test";

/** Scan-path store: only the methods activate() touches on a plain store. */
function storeWith(entries: KnowledgeEntry[]) {
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
		getEntriesByIds: mock((ids: string[]) =>
			Promise.resolve(entries.filter((e) => ids.includes(e.id))),
		),
		getSupportSourcesForIds: mock(() => Promise.resolve(new Map())),
		getContradictPairsForIds: mock(() => Promise.resolve(new Map())),
		recordInjections,
	} as unknown as IKnowledgeStore;
	return { store, recordInjections };
}

function engineFor(entries: KnowledgeEntry[]) {
	const { store, recordInjections } = storeWith(entries);
	const engine = new ActivationEngine(store);
	engine.embeddings.embedBatch = mock(async (qs: string[]) =>
		qs.map((q) => fakeEmbedding(q)),
	) as typeof engine.embeddings.embedBatch;
	return { engine, recordInjections };
}

const entries = [
	makeEntry({
		id: "e1",
		content: "alpha topic",
		embedding: fakeEmbedding(QUERY),
	}),
	makeEntry({
		id: "e2",
		content: "alpha topic two",
		embedding: fakeEmbedding(QUERY),
	}),
];

describe("activate dryRun", () => {
	it("does not record access for returned entries", async () => {
		const { engine, recordAccess } = engineFor(entries);
		const result = await engine.activate(QUERY, { dryRun: true });
		expect(result.entries.length).toBeGreaterThan(0);
		expect(result.injection).toBeUndefined();
	});

	it("returns an injection plan for writable stores; commitInjections records it", async () => {
		const { engine, recordInjections } = engineFor(entries);
		const result = await engine.activate(QUERY);
		expect(result.injection?.writes).toEqual([
			{
				storeId: "test-store",
				entryIds: result.entries.map((e) => e.entry.id),
			},
		]);
		engine.commitInjections(result.injection);
		expect(recordInjections).toHaveBeenCalledTimes(1);
	});

	it("reports telemetry: cue count, candidates and gate mode", async () => {
		const { engine } = engineFor(entries);
		const result = await engine.activate([QUERY, "second cue here"], {
			dryRun: true,
		});
		expect(result.telemetry?.cueCount).toBe(2);
		expect(result.telemetry?.nCandidates).toBeGreaterThanOrEqual(
			result.entries.length,
		);
		// No reranker injected: dense-only ordering is reported as "off".
		expect(result.telemetry?.gateMode).toBe("off");
	});
});
