/**
 * Tests for the second-stage rerank wiring in ActivationEngine
 * (src/activation/rerank.ts + activate.ts).
 *
 * The LocalReranker itself (ONNX model download + WASM inference) is NOT
 * exercised here — that would need network + ~23MB in CI. Instead a fake
 * RerankScorer is injected, which is exactly why the engine takes the scorer
 * as a constructor parameter.
 *
 * Covered:
 *   - Final order follows rerankScore × strength, not dense similarity
 *   - Dense overfetch: ANN fetch limit becomes rerank.candidates (Path A)
 *   - Dense-only fallback: scorer throws → dense order, no error
 *   - rerankScore exposed on response entries
 *   - Candidate pool is capped at rerank.candidates before scoring
 *   - Single-candidate shortcut: scorer not called
 *   - Null reranker: unchanged dense behavior (limit = maxResults)
 */
import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import type { RerankScorer } from "../src/activation/rerank";
import { cosineSimilarity } from "../src/activation/embeddings";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/types";
import type { KnowledgeEntry } from "../src/types";
import { makeEntry } from "./fixtures";

// ── Store stub (same shape as vector-search.test.ts) ──────────────────────────

function makeStoreStub(
	entries: Array<KnowledgeEntry & { embedding: number[] }>,
	opts: { supportAnn?: boolean } = {},
): IKnowledgeStore {
	const base: IKnowledgeStore = {
		insertEntry: mock(() => Promise.resolve()),
		updateEntry: mock(() => Promise.resolve()),
		getEntry: mock(() => Promise.resolve(null)),
		getActiveEntries: mock(() => Promise.resolve([])),
		getActiveEntriesWithEmbeddings: mock(() => Promise.resolve(entries)),
		getOneEntryWithEmbedding: mock(() => Promise.resolve(null)),
		getActiveAndConflictedEntries: mock(() => Promise.resolve([])),
		getEntriesMissingEmbeddings: mock(() => Promise.resolve([])),
		getEntriesByStatus: mock(() => Promise.resolve([])),
		getEntries: mock(() => Promise.resolve([])),
		recordAccess: mock(() => Promise.resolve()),
		reinforceObservation: mock(() => Promise.resolve()),
		updateStrength: mock(() => Promise.resolve()),
		getStats: mock(() => Promise.resolve({})),
		getEntriesWithOverlappingTopics: mock(() => Promise.resolve([])),
		applyContradictionResolution: mock(() => Promise.resolve()),
		deleteEntry: mock(() => Promise.resolve(false)),
		insertRelation: mock(() => Promise.resolve()),
		getRelationsFor: mock(() => Promise.resolve([])),
		getSupportSourcesForIds: mock(() => Promise.resolve(new Map())),
		getContradictPairsForIds: mock(() => Promise.resolve(new Map())),
		mergeEntry: mock(() => Promise.resolve()),
		reinitialize: mock(() => Promise.resolve()),
		getEmbeddingMetadata: mock(() => Promise.resolve(null)),
		setEmbeddingMetadata: mock(() => Promise.resolve()),
		getClustersWithMembers: mock(() => Promise.resolve([])),
		persistClusters: mock(() => Promise.resolve()),
		markClusterSynthesized: mock(() => Promise.resolve()),
		clearAllEmbeddings: mock(() => Promise.resolve(0)),
		tryAcquireConsolidationLock: mock(() => Promise.resolve(true)),
		releaseConsolidationLock: mock(() => Promise.resolve()),
		close: mock(() => Promise.resolve()),
	};

	if (opts.supportAnn) {
		base.findSimilarEntries = mock(
			(queryVector: number[], limit: number, threshold: number) => {
				const results = entries
					.map((e) => ({
						entry: e,
						similarity: cosineSimilarity(queryVector, e.embedding),
					}))
					.filter((r) => r.similarity >= threshold)
					.sort((a, b) => b.similarity - a.similarity)
					.slice(0, limit);
				return Promise.resolve(results);
			},
		);
	}

	return base;
}

// ── Deterministic vectors: known cosine similarities to the query ─────────────

const QUERY_EMB = [1, 0, 0, 0, 0, 0, 0, 0];
// cosine ≈ 1.0
const HIGH_EMB = [1, 0, 0, 0, 0, 0, 0, 0];
// cosine = 0.8
const MID_EMB = [0.8, 0.6, 0, 0, 0, 0, 0, 0];
// cosine = 0.5
const LOW_EMB = [0.5, Math.sqrt(0.75), 0, 0, 0, 0, 0, 0];

function makeEntries() {
	return [
		makeEntry({ id: "high", content: "dense high", embedding: HIGH_EMB }),
		makeEntry({ id: "mid", content: "dense mid", embedding: MID_EMB }),
		makeEntry({ id: "low", content: "dense low", embedding: LOW_EMB }),
	] as Array<KnowledgeEntry & { embedding: number[] }>;
}

/** Fake scorer that INVERTS the dense order: low > mid > high. */
function makeInvertingScorer(): RerankScorer & { calls: string[][] } {
	const calls: string[][] = [];
	return {
		calls,
		scoreBatch: mock((_query: string, documents: string[]) => {
			calls.push(documents);
			// Documents arrive as formatEmbeddingText: "[fact] <content> (topics: ...)"
			return Promise.resolve(
				documents.map((d) =>
					d.includes("dense low")
						? 0.99
						: d.includes("dense mid")
							? 0.5
							: 0.1,
				),
			);
		}),
	};
}

function makeFailingScorer(): RerankScorer {
	return {
		scoreBatch: mock(() => Promise.reject(new Error("model exploded"))),
	};
}

beforeEach(() => {
	// Ensure a clean threshold (other test files mutate config fields).
	config.activation.similarityThreshold = 0.3;
});

afterEach(() => {
	mock.restore();
});

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("ActivationEngine rerank stage", () => {
	it("orders by rerankScore × strength, not dense similarity (Path A)", async () => {
		const store = makeStoreStub(makeEntries(), { supportAnn: true });
		const scorer = makeInvertingScorer();
		const activation = new ActivationEngine(store, undefined, undefined, scorer);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		const result = await activation.activate("test query");

		expect(result.entries.map((e) => e.entry.id)).toEqual([
			"low",
			"mid",
			"high",
		]);
		// rerankScore is exposed and the blended similarity is score × strength
		const low = result.entries[0];
		expect(low.rerankScore).toBeCloseTo(0.99, 5);
		expect(low.similarity).toBeCloseTo(0.99 * low.staleness.strength, 5);
	});

	it("overfetches: ANN limit becomes rerank.candidates, not maxResults", async () => {
		const store = makeStoreStub(makeEntries(), { supportAnn: true });
		const scorer = makeInvertingScorer();
		const activation = new ActivationEngine(store, undefined, undefined, scorer);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		await activation.activate("test query");

		expect(store.findSimilarEntries).toHaveBeenCalled();
		const limitArg = (store.findSimilarEntries as ReturnType<typeof mock>).mock
			.calls[0][1] as number;
		expect(limitArg).toBe(config.activation.rerank.candidates);
	});

	it("falls back to dense ordering when the scorer throws", async () => {
		const store = makeStoreStub(makeEntries(), { supportAnn: true });
		const activation = new ActivationEngine(
			store,
			undefined,
			undefined,
			makeFailingScorer(),
		);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		const result = await activation.activate("test query");

		// Dense order preserved, no rerankScore, no error surfaced
		expect(result.entries.map((e) => e.entry.id)).toEqual([
			"high",
			"mid",
			"low",
		]);
		expect(result.entries[0].rerankScore).toBeUndefined();
	});

	it("caps the candidate pool at rerank.candidates before scoring", async () => {
		const original = config.activation.rerank.candidates;
		config.activation.rerank.candidates = 2;
		try {
			const store = makeStoreStub(makeEntries(), { supportAnn: true });
			const scorer = makeInvertingScorer();
			const activation = new ActivationEngine(
				store,
				undefined,
				undefined,
				scorer,
			);
			spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
				QUERY_EMB,
			]);

			const result = await activation.activate("test query");

			// Only the dense-best 2 candidates are scored...
			expect(scorer.calls).toHaveLength(1);
			expect(scorer.calls[0]).toHaveLength(2);
			expect(scorer.calls[0].join(" ")).toContain("dense high");
			expect(scorer.calls[0].join(" ")).toContain("dense mid");
			// ...and the capped-out entry is gone from the result entirely
			expect(result.entries.map((e) => e.entry.id)).not.toContain("low");
		} finally {
			config.activation.rerank.candidates = original;
		}
	});

	it("skips the scorer when only one candidate survives retrieval", async () => {
		const store = makeStoreStub(
			[makeEntries()[0]], // only "high"
			{ supportAnn: true },
		);
		const scorer = makeInvertingScorer();
		const activation = new ActivationEngine(store, undefined, undefined, scorer);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		const result = await activation.activate("test query");

		expect(scorer.calls).toHaveLength(0);
		expect(result.entries).toHaveLength(1);
		expect(result.entries[0].rerankScore).toBeUndefined();
	});

	it("works on the in-process full-scan path (Path B)", async () => {
		const store = makeStoreStub(makeEntries()); // no findSimilarEntries
		const scorer = makeInvertingScorer();
		const activation = new ActivationEngine(store, undefined, undefined, scorer);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		const result = await activation.activate("test query");

		expect(result.entries.map((e) => e.entry.id)).toEqual([
			"low",
			"mid",
			"high",
		]);
	});

	it("null reranker keeps dense-only behavior (fetch limit = maxResults)", async () => {
		const store = makeStoreStub(makeEntries(), { supportAnn: true });
		const activation = new ActivationEngine(store); // no reranker
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([QUERY_EMB]);

		const result = await activation.activate("test query");

		const limitArg = (store.findSimilarEntries as ReturnType<typeof mock>).mock
			.calls[0][1] as number;
		expect(limitArg).toBe(config.activation.maxResults);
		expect(result.entries.map((e) => e.entry.id)).toEqual([
			"high",
			"mid",
			"low",
		]);
		expect(result.entries[0].rerankScore).toBeUndefined();
	});
});
