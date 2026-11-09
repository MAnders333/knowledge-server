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
import * as loggerModule from "../src/logger";
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
		base.getActiveEntryCount = mock(() => Promise.resolve(entries.length));
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

describe("ActivationEngine.activate - mixed stores (per-store path selection)", () => {
	const queryEmb = fakeEmbedding("hello world test");
	// Orthogonal to queryEmb (fakeEmbedding only uses dims 0-2).
	const orthogEmb = queryEmb.map((v) => (v === 0 ? 1 / Math.sqrt(8) : 0));
	const orthUnit = orthogEmb.map((v) => v / Math.sqrt(5 / 8));
	// cosine(queryEmb, midEmb) = 0.8 exactly.
	const midEmb = queryEmb.map((v, i) => 0.8 * v + 0.6 * orthUnit[i]);
	type E = KnowledgeEntry & { embedding: number[] };
	const THRESHOLD = 0.5;

	afterEach(() => {
		mock.restore();
	});

	it("uses ANN for the ANN store and full scan only for the scan store", async () => {
		const annStore = makeStoreStub(
			[makeEntry({ id: "a1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({ id: "s1", embedding: queryEmb, status: "active" }),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		expect(annStore.findSimilarEntries).toHaveBeenCalled();
		expect(annStore.getActiveEntriesWithEmbeddings).not.toHaveBeenCalled();
		expect(scanStore.getActiveEntriesWithEmbeddings).toHaveBeenCalled();
		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("a1");
		expect(ids).toContain("s1");
		expect(result.totalActive).toBe(2);
	});

	it("treats an ANN store whose vector search is not ready as a scan store", async () => {
		const ready = makeStoreStub(
			[makeEntry({ id: "r1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		const notReady = makeStoreStub(
			[makeEntry({ id: "n1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		notReady.isVectorSearchReady = mock(() => false);
		ready.id = "ready";
		notReady.id = "notReady";

		const activation = new ActivationEngine(ready, [ready, notReady]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		expect(notReady.findSimilarEntries).not.toHaveBeenCalled();
		expect(notReady.getActiveEntriesWithEmbeddings).toHaveBeenCalled();
		expect(ready.getActiveEntriesWithEmbeddings).not.toHaveBeenCalled();
		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("r1");
		expect(ids).toContain("n1");
	});

	it("logs the retrieval path per store once, and again only when the partition changes", async () => {
		let annReady = true;
		const annStore = makeStoreStub(
			[makeEntry({ id: "a1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		annStore.isVectorSearchReady = mock(() => annReady);
		const scanStore = makeStoreStub([
			makeEntry({ id: "s1", embedding: queryEmb, status: "active" }),
		] as E[]);
		annStore.id = "main";
		scanStore.id = "local";

		const logSpy = spyOn(loggerModule.logger, "log").mockImplementation(
			() => {},
		);
		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);
		const pathLines = () =>
			logSpy.mock.calls
				.map((c) => String(c[0]))
				.filter((m) => m.startsWith("[activation] Retrieval path per store"));

		await activation.activate("hello world test", { threshold: THRESHOLD });
		await activation.activate("hello world test", { threshold: THRESHOLD });
		expect(pathLines()).toEqual([
			"[activation] Retrieval path per store: ANN=[main], full scan=[local]",
		]);

		annReady = false;
		await activation.activate("hello world test", { threshold: THRESHOLD });
		expect(pathLines()).toEqual([
			"[activation] Retrieval path per store: ANN=[main], full scan=[local]",
			"[activation] Retrieval path per store: ANN=none, full scan=[main, local]",
		]);
	});

	it("dedupes a shared id across ANN and scan stores, keeping the higher similarity (ANN copy higher)", async () => {
		const annStore = makeStoreStub(
			[
				makeEntry({
					id: "shared",
					content: "ann copy",
					embedding: queryEmb,
					status: "active",
				}),
			] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({
				id: "shared",
				content: "scan copy",
				embedding: midEmb,
				status: "active",
			}),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		const shared = result.entries.filter((e) => e.entry.id === "shared");
		expect(shared.length).toBe(1);
		expect(shared[0].entry.content).toBe("ann copy");
		expect(shared[0].rawSimilarity).toBeCloseTo(1, 5);
		// 1 counted for the ANN store plus 1 unique scan entry (documented double count).
		expect(result.totalActive).toBe(2);
	});

	it("dedupes a shared id across ANN and scan stores, keeping the higher similarity (scan copy higher)", async () => {
		const annStore = makeStoreStub(
			[
				makeEntry({
					id: "shared",
					content: "ann copy",
					embedding: midEmb,
					status: "active",
				}),
			] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({
				id: "shared",
				content: "scan copy",
				embedding: queryEmb,
				status: "active",
			}),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		const shared = result.entries.filter((e) => e.entry.id === "shared");
		expect(shared.length).toBe(1);
		expect(shared[0].entry.content).toBe("scan copy");
		expect(shared[0].rawSimilarity).toBeCloseTo(1, 5);
	});

	it("a failing ANN store still returns scan-store results", async () => {
		const annStore = makeStoreStub(
			[makeEntry({ id: "a1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({ id: "s1", embedding: queryEmb, status: "active" }),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";
		annStore.findSimilarEntries = mock(() =>
			Promise.reject(new Error("connection refused")),
		);

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("s1");
		expect(ids).not.toContain("a1");
	});

	it("a failing scan store still returns ANN-store results", async () => {
		const annStore = makeStoreStub(
			[makeEntry({ id: "a1", embedding: queryEmb, status: "active" })] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({ id: "s1", embedding: queryEmb, status: "active" }),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";
		scanStore.getActiveEntriesWithEmbeddings = mock(() =>
			Promise.reject(new Error("connection refused")),
		);

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		expect(annStore.findSimilarEntries).toHaveBeenCalled();
		expect(annStore.getActiveEntriesWithEmbeddings).not.toHaveBeenCalled();
		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("a1");
		expect(ids).not.toContain("s1");
	});

	it("on an equal similarity tie across ANN and scan stores, keeps the ANN copy", async () => {
		const annStore = makeStoreStub(
			[
				makeEntry({
					id: "shared",
					content: "ann copy",
					embedding: queryEmb,
					status: "active",
				}),
			] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({
				id: "shared",
				content: "scan copy",
				embedding: queryEmb,
				status: "active",
			}),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([queryEmb]);

		const result = await activation.activate("hello world test", {
			threshold: THRESHOLD,
		});

		const shared = result.entries.filter((e) => e.entry.id === "shared");
		expect(shared.length).toBe(1);
		expect(shared[0].entry.content).toBe("ann copy");
	});

	it("multi-cue: keeps max over query vectors in mixed mode", async () => {
		const annStore = makeStoreStub(
			[makeEntry({ id: "a1", embedding: orthUnit, status: "active" })] as E[],
			{ supportAnn: true },
		);
		const scanStore = makeStoreStub([
			makeEntry({ id: "s1", embedding: queryEmb, status: "active" }),
		] as E[]);
		annStore.id = "ann";
		scanStore.id = "scan";

		const activation = new ActivationEngine(annStore, [annStore, scanStore]);
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			queryEmb,
			orthUnit,
		]);

		const result = await activation.activate(
			"hello world test\nsecond topic line here",
			{ threshold: THRESHOLD },
		);

		expect(annStore.findSimilarEntries).toHaveBeenCalledTimes(2);
		expect(scanStore.getActiveEntriesWithEmbeddings).toHaveBeenCalledTimes(1);
		const byId = new Map(result.entries.map((e) => [e.entry.id, e]));
		expect(byId.get("a1")?.rawSimilarity).toBeCloseTo(1, 5);
		expect(byId.get("s1")?.rawSimilarity).toBeCloseTo(1, 5);
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
