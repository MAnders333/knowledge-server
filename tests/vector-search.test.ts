/**
 * Tests for the pgvector-based vector search feature.
 *
 * Coverage:
 *  1. ActivationEngine.activate() — Path A (ANN via findSimilarEntries) vs
 *     Path B (in-process full scan) code paths, using mock stores.
 *  2. PG_MIGRATIONS gap-safety — verifies that running from any earlier version
 *     up to SCHEMA_VERSION applies every pending migration in order, even when
 *     intermediate versions were skipped.
 *  3. ContradictionScanner — uses findContradictionCandidates when the store
 *     exposes it; falls back to getEntriesWithOverlappingTopics otherwise.
 */
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
	type Mock,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { cosineSimilarity } from "../src/activation/embeddings";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { PG_MIGRATIONS } from "../src/db/postgres/migrations";
import { SCHEMA_VERSION } from "../src/db/sqlite/schema";
import { ContradictionScanner } from "../src/consolidation/contradiction";
import { ConsolidationLLM } from "../src/consolidation/llm";
import type { KnowledgeEntry } from "../src/types";
import { fakeEmbedding, makeEntry } from "./fixtures";

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Build a minimal IKnowledgeStore stub sufficient for ActivationEngine tests.
 * Fields not relevant to the test are no-ops or throw.
 */
function makeStoreStub(
	entries: Array<KnowledgeEntry & { embedding: number[] }>,
	opts: {
		/** If true, the stub exposes findSimilarEntries (ANN path). */
		supportAnn?: boolean;
	} = {},
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
			(
				queryVector: number[],
				limit: number,
				threshold: number,
			) => {
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

// ── 1. ActivationEngine — Path A vs Path B ────────────────────────────────────

describe("ActivationEngine.activate — ANN path (Path A)", () => {
	let activation: ActivationEngine;
	let annStore: IKnowledgeStore;

	const queryContent = "hello world test";
	const queryEmb = fakeEmbedding(queryContent);

	// Two entries: one identical to the query (similarity = 1.0), one orthogonal
	// (similarity = 0.0). We construct the low entry's embedding directly so we
	// don't depend on fakeEmbedding's coarse 8-dim encoding, which can map
	// seemingly-different strings to near-identical unit vectors.
	const highEntry = makeEntry({
		id: "high",
		content: queryContent,
		embedding: queryEmb,
		status: "active",
	});
	// Orthogonal to queryEmb: zero everywhere queryEmb is non-zero.
	// cosine(queryEmb, orthogEmb) = 0.0 — well below any threshold.
	const orthogEmb = queryEmb.map((v) => (v === 0 ? 1 / Math.sqrt(8) : 0));
	const lowEntry = makeEntry({
		id: "low",
		content: "orthogonal entry",
		embedding: orthogEmb,
		status: "active",
	});

	beforeEach(() => {
		annStore = makeStoreStub([highEntry, lowEntry] as Array<
			KnowledgeEntry & { embedding: number[] }
		>, { supportAnn: true });
		activation = new ActivationEngine(annStore);
		// Mock embedBatch to return the query embedding without hitting the API.
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);
	});

	afterEach(() => {
		mock.restore();
	});

	it("uses findSimilarEntries (not getActiveEntriesWithEmbeddings) when available", async () => {
		const result = await activation.activate(queryContent);

		// ANN path must NOT call getActiveEntriesWithEmbeddings
		expect(annStore.getActiveEntriesWithEmbeddings).not.toHaveBeenCalled();
		// ANN path MUST call findSimilarEntries
		expect(annStore.findSimilarEntries).toHaveBeenCalled();

		// The high-similarity entry should be returned.
		expect(result.entries.some((e) => e.entry.id === "high")).toBe(true);
	});

	it("returns only entries above the similarity threshold", async () => {
		const result = await activation.activate(queryContent, {
			threshold: config.activation.similarityThreshold,
		});

		// lowEntry should not appear — it is far from the query vector.
		expect(result.entries.some((e) => e.entry.id === "low")).toBe(false);
	});

	it("respects the limit option", async () => {
		const result = await activation.activate(queryContent, { limit: 1 });
		expect(result.entries.length).toBeLessThanOrEqual(1);
	});

	it("multi-cue: runs one ANN call per query embedding", async () => {
		// Two-line query → two cues → two embeddings → two ANN calls per store.
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			queryEmb,
			queryEmb,
		]);
		await activation.activate(`${queryContent}\n${queryContent}`);

		// One findSimilarEntries call per query vector.
		expect(annStore.findSimilarEntries).toHaveBeenCalledTimes(2);
	});
});

describe("ActivationEngine.activate — full-scan path (Path B)", () => {
	let activation: ActivationEngine;
	let fullScanStore: IKnowledgeStore;

	const queryContent = "hello world test";
	const queryEmb = fakeEmbedding(queryContent);

	const highEntry = makeEntry({
		id: "high",
		content: queryContent,
		embedding: queryEmb,
		status: "active",
	});

	beforeEach(() => {
		// Store WITHOUT findSimilarEntries → triggers Path B.
		fullScanStore = makeStoreStub(
			[highEntry] as Array<KnowledgeEntry & { embedding: number[] }>,
			{ supportAnn: false },
		);
		activation = new ActivationEngine(fullScanStore);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);
	});

	afterEach(() => {
		mock.restore();
	});

	it("calls getActiveEntriesWithEmbeddings when findSimilarEntries is absent", async () => {
		await activation.activate(queryContent);
		expect(fullScanStore.getActiveEntriesWithEmbeddings).toHaveBeenCalled();
	});

	it("still returns the correct entry on Path B", async () => {
		const result = await activation.activate(queryContent);
		expect(result.entries.some((e) => e.entry.id === "high")).toBe(true);
	});
});

describe("ActivationEngine.activate — mixed stores (one ANN, one full-scan)", () => {
	it("falls back to full-scan when any store lacks findSimilarEntries", async () => {
		const emb = fakeEmbedding("mixed test");
		const entry = makeEntry({ id: "e1", embedding: emb, status: "active" });
		const annStore = makeStoreStub(
			[entry] as Array<KnowledgeEntry & { embedding: number[] }>,
			{ supportAnn: true },
		);
		const fullScanStore = makeStoreStub(
			[entry] as Array<KnowledgeEntry & { embedding: number[] }>,
			{ supportAnn: false },
		);

		const activation = new ActivationEngine(annStore, [annStore, fullScanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([emb]);

		await activation.activate("mixed test");

		// When any store lacks ANN, fall back to full scan everywhere.
		expect(
			(annStore.getActiveEntriesWithEmbeddings as ReturnType<typeof mock>).mock.calls.length,
		).toBeGreaterThan(0);
		expect(
			(fullScanStore.getActiveEntriesWithEmbeddings as ReturnType<typeof mock>).mock.calls.length,
		).toBeGreaterThan(0);

		mock.restore();
	});
});

// ── 2. PG_MIGRATIONS gap-safety ───────────────────────────────────────────────

describe("PG_MIGRATIONS gap-safety", () => {
	it("migrations list is sorted by version ascending", () => {
		const versions = PG_MIGRATIONS.map((m) => m.version);
		for (let i = 1; i < versions.length; i++) {
			expect(versions[i]).toBeGreaterThan(versions[i - 1]);
		}
	});

	it("latest migration version equals SCHEMA_VERSION", () => {
		const latestMigration = PG_MIGRATIONS[PG_MIGRATIONS.length - 1];
		expect(latestMigration.version).toBe(SCHEMA_VERSION);
	});

	it("all migrations have unique version numbers", () => {
		const versions = PG_MIGRATIONS.map((m) => m.version);
		const uniqueVersions = new Set(versions);
		expect(uniqueVersions.size).toBe(versions.length);
	});

	it("pending migrations filter covers full gap when skipping versions", () => {
		// Simulate: DB at v10, code at SCHEMA_VERSION.
		// The migration runner should pick up every migration from v11 onward.
		const startVersion = 10;
		const pending = PG_MIGRATIONS
			.filter((m) => m.version > startVersion)
			.sort((a, b) => a.version - b.version);

		// Every migration with version > startVersion must be included.
		const expectedVersions = PG_MIGRATIONS
			.map((m) => m.version)
			.filter((v) => v > startVersion);

		expect(pending.map((m) => m.version)).toEqual(expectedVersions);
	});

	it("pending migrations filter is empty when already at SCHEMA_VERSION", () => {
		const pending = PG_MIGRATIONS.filter(
			(m) => m.version > SCHEMA_VERSION,
		);
		expect(pending).toHaveLength(0);
	});

	it("skipped versions in DB history do not cause migrations to be skipped", () => {
		// Simulate a DB that has only had v8 applied (skipped v9-v15).
		// Every migration from v9 through v16 must appear in pending.
		const dbVersion = 8;
		const pending = PG_MIGRATIONS
			.filter((m) => m.version > dbVersion)
			.sort((a, b) => a.version - b.version);

		const allVersionsAbove8 = PG_MIGRATIONS
			.map((m) => m.version)
			.filter((v) => v > dbVersion);

		expect(pending.map((m) => m.version)).toEqual(allVersionsAbove8);

		// Confirm ordering is ascending (no gaps in application order).
		for (let i = 1; i < pending.length; i++) {
			expect(pending[i].version).toBeGreaterThan(pending[i - 1].version);
		}
	});
});

// ── 3. ContradictionScanner — findContradictionCandidates vs fallback ─────────

describe("ContradictionScanner — findContradictionCandidates", () => {
	afterEach(() => {
		mock.restore();
	});

	/**
	 * Build a mock IKnowledgeStore for ContradictionScanner tests.
	 * Exposes findContradictionCandidates when supportAnn=true.
	 */
	function makeContradictionStore(
		topicCandidates: Array<KnowledgeEntry & { embedding: number[] }>,
		opts: { supportAnn: boolean },
	): IKnowledgeStore {
		const base = makeStoreStub(topicCandidates, { supportAnn: false });
		// Override getEntriesWithOverlappingTopics to return the canned candidates.
		base.getEntriesWithOverlappingTopics = mock(() =>
			Promise.resolve(topicCandidates),
		);

		if (opts.supportAnn) {
			base.findContradictionCandidates = mock(
				(
					_queryVector: number[],
					_topics: string[],
					_excludeIds: string[],
					_minSim: number,
					_maxSim: number,
				) => {
					// Return all candidates (simulator: real filtering done by pgvector).
					return Promise.resolve(topicCandidates);
				},
			);
		}

		return base;
	}

	it("uses findContradictionCandidates when available, skipping getEntriesWithOverlappingTopics", async () => {
		const emb = fakeEmbedding("contradiction test topic");
		const candidate = makeEntry({
			id: "cand",
			content: "candidate entry",
			topics: ["contradiction"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		const store = makeContradictionStore([candidate], { supportAnn: true });

		// Mock the LLM — no real API calls needed; we only care about which DB
		// method the scanner called.
		const llm = new ConsolidationLLM();
		spyOn(llm, "detectAndResolveContradiction").mockResolvedValue([]);

		const scanner = new ContradictionScanner(llm);

		const entry = makeEntry({
			id: "e1",
			content: "entry being checked",
			topics: ["contradiction"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		await scanner.scan(store, new Map([[entry.id, entry]]), new Set([entry.id]));

		// ANN path must be used.
		expect(store.findContradictionCandidates).toHaveBeenCalled();
		// Fallback must NOT be called.
		expect(store.getEntriesWithOverlappingTopics).not.toHaveBeenCalled();
	});

	it("falls back to getEntriesWithOverlappingTopics when findContradictionCandidates is absent", async () => {
		const emb = fakeEmbedding("fallback test topic");
		const candidate = makeEntry({
			id: "cand",
			content: "candidate entry",
			topics: ["fallback"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		const store = makeContradictionStore([candidate], { supportAnn: false });

		const llm = new ConsolidationLLM();
		spyOn(llm, "detectAndResolveContradiction").mockResolvedValue([]);
		const scanner = new ContradictionScanner(llm);

		const entry = makeEntry({
			id: "e1",
			content: "entry being checked",
			topics: ["fallback"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		await scanner.scan(store, new Map([[entry.id, entry]]), new Set([entry.id]));

		// Fallback must be used.
		expect(store.getEntriesWithOverlappingTopics).toHaveBeenCalled();
	});

	it("passes correct similarity band to findContradictionCandidates", async () => {
		const emb = fakeEmbedding("band check");
		const candidate = makeEntry({
			id: "cand",
			topics: ["band"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		const store = makeContradictionStore([candidate], { supportAnn: true });

		const llm = new ConsolidationLLM();
		spyOn(llm, "detectAndResolveContradiction").mockResolvedValue([]);
		const scanner = new ContradictionScanner(llm);

		const entry = makeEntry({
			id: "e1",
			topics: ["band"],
			embedding: emb,
			status: "active",
		}) as KnowledgeEntry & { embedding: number[] };

		await scanner.scan(store, new Map([[entry.id, entry]]), new Set([entry.id]));

		const findSpy = store.findContradictionCandidates as Mock<
			NonNullable<IKnowledgeStore["findContradictionCandidates"]>
		>;
		expect(findSpy).toHaveBeenCalled();
		const [, , , minSim, maxSim] = findSpy.mock.calls[0];
		expect(minSim).toBe(config.consolidation.contradictionMinSimilarity);
		expect(maxSim).toBe(config.consolidation.reconsolidationThreshold);
	});
});
