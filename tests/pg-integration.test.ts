/**
 * PostgreSQL integration tests.
 *
 * These tests run against a real PostgreSQL instance and exercise every
 * IKnowledgeDB method on PostgresKnowledgeDB. They are skipped when
 * PG_TEST_URI is not set — so CI without a PostgreSQL service just skips them.
 *
 * Usage:
 *   docker run -d --name ks-pg-test -p 5433:5432 \
 *     -e POSTGRES_USER=ks -e POSTGRES_PASSWORD=ks -e POSTGRES_DB=knowledge --rm pgvector/pgvector:pg16
 *   PG_TEST_URI=postgres://ks:ks@localhost:5433/knowledge bun test tests/pg-integration.test.ts
 */

import {
	afterAll,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import postgres from "postgres";
import type { IKnowledgeStore } from "../src/db/interface";
import { PostgresKnowledgeDB } from "../src/db/postgres/index";
import { ServerStateDB } from "../src/db/state/index";

const PG_URI = process.env.PG_TEST_URI;

// Helper: create a standard test entry object
function makeEntry(id: string, overrides: Record<string, unknown> = {}) {
	const now = Date.now();
	return {
		id,
		type: "fact" as const,
		content: `Entry ${id}`,
		topics: ["test"],
		confidence: 0.8,
		source: "test-source",
		status: "active" as const,
		strength: 1.0,
		createdAt: now,
		updatedAt: now,
		lastAccessedAt: now,
		accessCount: 0,
		observationCount: 1,
		supersededBy: null,
		derivedFrom: [] as string[],
		isSynthesized: false,
		...overrides,
	};
}

describe.skipIf(!PG_URI)("PostgresKnowledgeDB integration", () => {
	let db: IKnowledgeStore;
	let serverStateDb: ServerStateDB;
	let pgTestSeq = 0;
	const uri = PG_URI as string;
	// Shared truncation client — created once to avoid pool churn on every test.
	let truncSql: ReturnType<typeof postgres>;
	let tempDir: string;

	beforeAll(async () => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-pg-test-"));
		truncSql = postgres(uri, { max: 1 });
		db = new PostgresKnowledgeDB(uri);
		await (db as PostgresKnowledgeDB).initialize();
	});

	beforeEach(async () => {
		// Wipe knowledge data between tests for isolation.
		// knowledge_cluster must be listed explicitly — it has no FK back to
		// knowledge_entry so it is not reached by CASCADE.
		// consolidated_episode and consolidation_state are NOT truncated here —
		// they now live in state.db (ServerStateDB), not in Postgres.
		await truncSql`TRUNCATE knowledge_entry, knowledge_cluster, embedding_metadata, schema_version CASCADE`;
		// Drop embedding_vec column and HNSW index so each test starts with a
		// clean schema state. ensureVectorColumn() creates them lazily when
		// setEmbeddingMetadata() is called; dropping here prevents a test that
		// calls setEmbeddingMetadata() from leaving behind a vector(N) column
		// that interferes with subsequent tests.
		// We close and reopen `db` after the DDL so the connection pool is fresh
		// and no cached prepared statements reference the dropped column.
		await truncSql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
		await truncSql`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
		await truncSql.unsafe(
			"ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS embedding_vec",
		);
		// Close the existing db pool (flushes any cached plans referencing embedding_vec)
		// and create a fresh instance for the next test.
		await db.close();
		db = new PostgresKnowledgeDB(uri);
		await (db as PostgresKnowledgeDB).initialize();
		// Fresh ServerStateDB for each test — staging methods now live here.
		// Use a counter rather than Date.now() to avoid filename collisions on fast machines.
		await serverStateDb
			?.close()
			.catch((e: unknown) =>
				console.error("[test] serverStateDb close error:", e),
			);
		serverStateDb = new ServerStateDB(
			join(tempDir, `server-${++pgTestSeq}.db`),
		);
	});

	afterAll(async () => {
		// Run in parallel — independent connections, no ordering dependency.
		await Promise.all([db.close(), serverStateDb?.close(), truncSql.end()]);
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── Schema & Init ──

	it("initializes schema and returns empty stats", async () => {
		const stats = await db.getStats();
		expect(stats.total).toBe(0);
	});

	// ── Entry CRUD ──

	it("inserts and retrieves an entry", async () => {
		const entry = makeEntry("test-1", {
			content: "Churn rate is 4.2%",
			topics: ["churn", "metrics"],
			derivedFrom: ["session-123"],
		});
		await db.insertEntry(entry);

		const retrieved = await db.getEntry("test-1");
		expect(retrieved).not.toBeNull();
		expect(retrieved?.content).toBe("Churn rate is 4.2%");
		expect(retrieved?.topics).toEqual(["churn", "metrics"]);
		expect(retrieved?.derivedFrom).toEqual(["session-123"]);
		expect(retrieved?.isSynthesized).toBe(false);
	});

	it("returns null for non-existent entry", async () => {
		const entry = await db.getEntry("does-not-exist");
		expect(entry).toBeNull();
	});

	it("updates entry fields", async () => {
		await db.insertEntry(
			makeEntry("test-2", { content: "Old content", confidence: 0.5 }),
		);

		await db.updateEntry("test-2", {
			content: "New content",
			confidence: 0.8,
			status: "superseded",
			supersededBy: "test-3",
		});

		const entry = await db.getEntry("test-2");
		expect(entry?.content).toBe("New content");
		expect(entry?.confidence).toBe(0.8);
		expect(entry?.status).toBe("superseded");
		expect(entry?.supersededBy).toBe("test-3");
	});

	it("records access and increments count", async () => {
		const now = Date.now();
		await db.insertEntry(makeEntry("test-3"));

		await db.recordAccess("test-3");
		await db.recordAccess("test-3");
		await db.recordAccess("test-3");

		const entry = await db.getEntry("test-3");
		expect(entry?.accessCount).toBe(3);
		expect(entry?.lastAccessedAt).toBeGreaterThanOrEqual(now);
	});

	it("reinforces observation", async () => {
		const now = Date.now();
		await db.insertEntry(makeEntry("test-reinforce", { observationCount: 1 }));

		await db.reinforceObservation("test-reinforce");

		const entry = await db.getEntry("test-reinforce");
		expect(entry?.observationCount).toBe(2);
		expect(entry?.lastAccessedAt).toBeGreaterThanOrEqual(now);
	});

	it("updates strength", async () => {
		await db.insertEntry(makeEntry("test-strength", { strength: 1.0 }));

		await db.updateStrength("test-strength", 0.42);

		const entry = await db.getEntry("test-strength");
		expect(entry?.strength).toBeCloseTo(0.42);
	});

	it("filters entries by status", async () => {
		await db.insertEntry(makeEntry("a1", { status: "active" }));
		await db.insertEntry(makeEntry("a2", { status: "active" }));
		await db.insertEntry(makeEntry("a3", { status: "archived" }));

		const active = await db.getActiveEntries();
		expect(active.length).toBe(2);

		const archived = await db.getEntriesByStatus("archived");
		expect(archived.length).toBe(1);
		expect(archived[0].id).toBe("a3");
	});

	it("getActiveAndConflictedEntries returns active and conflicted only", async () => {
		await db.insertEntry(makeEntry("ac1", { status: "active" }));
		await db.insertEntry(makeEntry("ac2", { status: "conflicted" }));
		await db.insertEntry(makeEntry("ac3", { status: "archived" }));
		await db.insertEntry(makeEntry("ac4", { status: "superseded" }));

		const entries = await db.getActiveAndConflictedEntries();
		expect(entries.length).toBe(2);
		const ids = entries.map((e) => e.id).sort();
		expect(ids).toEqual(["ac1", "ac2"]);
	});

	it("getEntries with filters", async () => {
		await db.insertEntry(
			makeEntry("f1", { status: "active", type: "fact" }),
		);
		await db.insertEntry(
			makeEntry("f2", {
				status: "active",
				type: "principle",
			}),
		);
		await db.insertEntry(
			makeEntry("f3", { status: "archived", type: "fact" }),
		);

		const byStatus = await db.getEntries({ status: "active" });
		expect(byStatus.length).toBe(2);

		const byType = await db.getEntries({ type: "fact" });
		expect(byType.length).toBe(2);

		const combined = await db.getEntries({
			status: "active",
			type: "fact",
		});
		expect(combined.length).toBe(1);
		expect(combined[0].id).toBe("f1");
	});

	it("deleteEntry removes entry and returns true", async () => {
		await db.insertEntry(makeEntry("del-1"));
		const deleted = await db.deleteEntry("del-1");
		expect(deleted).toBe(true);
		expect(await db.getEntry("del-1")).toBeNull();
	});

	it("deleteEntry returns false for non-existent entry", async () => {
		const deleted = await db.deleteEntry("no-such-entry");
		expect(deleted).toBe(false);
	});

	// ── Embeddings ──

	it("stores and retrieves embeddings", async () => {
		const embedding = [0.1, 0.2, 0.3, 0.4, 0.5];
		await db.insertEntry(makeEntry("emb-1", { embedding }));

		const entry = await db.getEntry("emb-1");
		expect(entry?.embedding).toBeDefined();
		expect(entry?.embedding?.length).toBe(5);
		for (let i = 0; i < embedding.length; i++) {
			expect(
				Math.abs((entry?.embedding?.[i] ?? 0) - embedding[i]),
			).toBeLessThan(0.0001);
		}
	});

	it("getActiveEntriesWithEmbeddings returns only entries with embeddings", async () => {
		await db.insertEntry(makeEntry("emb-yes", { embedding: [0.1, 0.2] }));
		await db.insertEntry(makeEntry("emb-no"));

		const entries = await db.getActiveEntriesWithEmbeddings();
		expect(entries.length).toBe(1);
		expect(entries[0].id).toBe("emb-yes");
		expect(entries[0].embedding.length).toBe(2);
	});

	it("getOneEntryWithEmbedding returns a single entry", async () => {
		expect(await db.getOneEntryWithEmbedding()).toBeNull();

		await db.insertEntry(makeEntry("probe-1", { embedding: [0.5, 0.6] }));
		const probe = await db.getOneEntryWithEmbedding();
		expect(probe).not.toBeNull();
		expect(probe?.embedding.length).toBe(2);
	});

	it("getEntriesMissingEmbeddings returns active/conflicted entries without embeddings", async () => {
		await db.insertEntry(makeEntry("has-emb", { embedding: [0.1, 0.2] }));
		await db.insertEntry(makeEntry("no-emb"));
		await db.insertEntry(
			makeEntry("superseded-no-emb", {
				status: "superseded",
				supersededBy: "has-emb",
			}),
		);

		const missing = await db.getEntriesMissingEmbeddings();
		expect(missing.length).toBe(1);
		expect(missing[0].id).toBe("no-emb");
	});

	it("clearAllEmbeddings NULLs embeddings on active/conflicted entries only", async () => {
		const emb = [0.1, 0.2, 0.3];
		await db.insertEntry(makeEntry("e1", { status: "active", embedding: emb }));
		await db.insertEntry(
			makeEntry("e2", { status: "conflicted", embedding: emb }),
		);
		await db.insertEntry(
			makeEntry("e3", {
				status: "superseded",
				supersededBy: "e1",
				embedding: emb,
			}),
		);

		const cleared = await db.clearAllEmbeddings();
		expect(cleared).toBe(2);

		expect((await db.getEntry("e1"))?.embedding).toBeUndefined();
		expect((await db.getEntry("e2"))?.embedding).toBeUndefined();
		expect((await db.getEntry("e3"))?.embedding).toBeDefined();
	});

	// ── Stats ──

	it("returns correct stats", async () => {
		await db.insertEntry(makeEntry("s1", { status: "active" }));
		await db.insertEntry(makeEntry("s2", { status: "active" }));
		await db.insertEntry(makeEntry("s3", { status: "active" }));
		await db.insertEntry(makeEntry("s4", { status: "archived" }));
		await db.insertEntry(makeEntry("s5", { status: "superseded" }));

		const stats = await db.getStats();
		expect(stats.total).toBe(5);
		expect(stats.active).toBe(3);
		expect(stats.archived).toBe(1);
		expect(stats.superseded).toBe(1);
	});

	// ── Consolidation State ──
	// These methods now live on IServerStateDB (ServerStateDB), not IKnowledgeStore.

	it("manages consolidation state", async () => {
		const state = await serverStateDb.getConsolidationState();
		expect(state.lastConsolidatedAt).toBe(0);
		expect(state.totalSessionsProcessed).toBe(0);

		await serverStateDb.updateConsolidationState({
			lastConsolidatedAt: 1000000,
			totalSessionsProcessed: 50,
			totalEntriesCreated: 25,
		});

		const updated = await serverStateDb.getConsolidationState();
		expect(updated.lastConsolidatedAt).toBe(1000000);
		expect(updated.totalSessionsProcessed).toBe(50);
		expect(updated.totalEntriesCreated).toBe(25);
	});

	// ── Episode Tracking ──
	// These methods now live on IServerStateDB (ServerStateDB), not IKnowledgeStore.

	it("records and retrieves episode ranges", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-start-1",
			"msg-end-1",
			"messages",
			3,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-start-2",
			"msg-end-2",
			"compaction_summary",
			1,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-2",
			"msg-start-3",
			"msg-end-3",
			"messages",
			0,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges([
			"session-1",
			"session-2",
		]);

		expect(ranges.size).toBe(2);
		const s1 = ranges.get("session-1") ?? [];
		expect(s1).toHaveLength(2);
		expect(
			s1.some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-1" &&
					r.endMessageId === "msg-end-1",
			),
		).toBe(true);
		expect(
			s1.some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-2" &&
					r.endMessageId === "msg-end-2",
			),
		).toBe(true);

		const s2 = ranges.get("session-2") ?? [];
		expect(s2).toHaveLength(1);
		expect(s2[0].startMessageId).toBe("msg-start-3");
	});

	it("recordEpisode is idempotent", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges(["session-1"]);
		expect(ranges.get("session-1")).toHaveLength(1);
	});

	it("episodes from different sources for the same session are both returned", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);
		await serverStateDb.recordEpisode(
			"claude-code",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges(["session-1"]);
		expect(ranges.get("session-1")).toHaveLength(2);
	});

	it("getProcessedEpisodeRanges returns empty map for unknown session", async () => {
		const ranges = await serverStateDb.getProcessedEpisodeRanges([
			"no-such-session",
		]);
		expect(ranges.size).toBe(0);
	});

	// ── Relations ──

	it("inserts and retrieves relations", async () => {
		const now = Date.now();
		await db.insertEntry(makeEntry("r1"));
		await db.insertEntry(makeEntry("r2"));

		await db.insertRelation({
			id: "rel-1",
			sourceId: "r1",
			targetId: "r2",
			type: "supports",
			createdAt: now,
		});

		const relations = await db.getRelationsFor("r1");
		expect(relations.length).toBe(1);
		expect(relations[0].type).toBe("supports");
		expect(relations[0].targetId).toBe("r2");
	});

	it("getSupportSourcesForIds returns source entries for synthesized entries", async () => {
		await db.insertEntry(makeEntry("source-1"));
		await db.insertEntry(makeEntry("synth-1", { isSynthesized: true }));

		// `supports` relations: source_id = synthesized entry, target_id = source entry
		await db.insertRelation({
			id: "rel-support",
			sourceId: "synth-1",
			targetId: "source-1",
			type: "supports",
			createdAt: Date.now(),
		});

		const sources = await db.getSupportSourcesForIds(["synth-1"]);
		expect(sources.has("synth-1")).toBe(true);
		const synthSources = sources.get("synth-1") ?? [];
		expect(synthSources.length).toBe(1);
		expect(synthSources[0].id).toBe("source-1");
	});

	it("getContradictPairsForIds returns contradiction pairs", async () => {
		await db.insertEntry(makeEntry("c1"));
		await db.insertEntry(makeEntry("c2"));

		await db.insertRelation({
			id: "rel-contra",
			sourceId: "c1",
			targetId: "c2",
			type: "contradicts",
			createdAt: Date.now(),
		});

		const pairs = await db.getContradictPairsForIds(["c1", "c2"]);
		expect(pairs.size).toBeGreaterThan(0);
	});

	// ── Contradiction Resolution ──

	it("applyContradictionResolution supersede_old", async () => {
		await db.insertEntry(makeEntry("new-entry"));
		await db.insertEntry(makeEntry("old-entry"));

		await db.applyContradictionResolution(
			"supersede_old",
			"new-entry",
			"old-entry",
		);

		const oldEntry = await db.getEntry("old-entry");
		expect(oldEntry?.status).toBe("superseded");
		expect(oldEntry?.supersededBy).toBe("new-entry");

		const relations = await db.getRelationsFor("new-entry");
		expect(relations.some((r) => r.type === "supersedes")).toBe(true);
	});

	it("applyContradictionResolution supersede_new", async () => {
		await db.insertEntry(makeEntry("new-entry"));
		await db.insertEntry(makeEntry("old-entry"));

		await db.applyContradictionResolution(
			"supersede_new",
			"new-entry",
			"old-entry",
		);

		const newEntry = await db.getEntry("new-entry");
		expect(newEntry?.status).toBe("superseded");
		expect(newEntry?.supersededBy).toBe("old-entry");
	});

	it("applyContradictionResolution merge", async () => {
		await db.insertEntry(makeEntry("new-entry"));
		await db.insertEntry(makeEntry("old-entry"));

		await db.applyContradictionResolution("merge", "new-entry", "old-entry", {
			content: "Merged content",
			type: "fact",
			topics: ["merged"],
			confidence: 0.95,
		});

		const newEntry = await db.getEntry("new-entry");
		expect(newEntry?.content).toBe("Merged content");
		expect(newEntry?.topics).toEqual(["merged"]);

		const oldEntry = await db.getEntry("old-entry");
		expect(oldEntry?.status).toBe("superseded");
	});

	it("applyContradictionResolution irresolvable marks both conflicted", async () => {
		await db.insertEntry(makeEntry("new-entry"));
		await db.insertEntry(makeEntry("old-entry"));

		await db.applyContradictionResolution(
			"irresolvable",
			"new-entry",
			"old-entry",
		);

		expect((await db.getEntry("new-entry"))?.status).toBe("conflicted");
		expect((await db.getEntry("old-entry"))?.status).toBe("conflicted");
	});

	// ── Merge Entry ──

	it("merges new content into an existing entry", async () => {
		await db.insertEntry(
			makeEntry("merge-target", {
				content: "Original",
				topics: ["old"],
				derivedFrom: ["session-1"],
			}),
		);

		await db.mergeEntry("merge-target", {
			content: "Merged content",
			type: "principle",
			topics: ["new"],
			confidence: 0.95,
			additionalSources: ["session-2"],
		});

		const entry = await db.getEntry("merge-target");
		expect(entry?.content).toBe("Merged content");
		expect(entry?.type).toBe("principle");
		expect(entry?.topics).toEqual(["new"]);
		expect(entry?.confidence).toBe(0.95);
		expect(entry?.derivedFrom).toContain("session-2");
	});

	it("merges with an embedding", async () => {
		await db.insertEntry(makeEntry("merge-emb"));

		const freshEmb = [0.9, 0.8, 0.7];
		await db.mergeEntry(
			"merge-emb",
			{
				content: "Updated with embedding",
				type: "fact",
				topics: ["emb"],
				confidence: 0.9,
				additionalSources: ["s2"],
			},
			freshEmb,
		);

		const entry = await db.getEntry("merge-emb");
		expect(entry?.content).toBe("Updated with embedding");
		expect(entry?.embedding).toBeDefined();
		expect(entry?.embedding?.length).toBe(3);
		expect(entry?.embedding?.[0]).toBeCloseTo(0.9);
	});

	// ── Overlapping Topics ──

	it("getEntriesWithOverlappingTopics finds entries sharing topics", async () => {
		await db.insertEntry(
			makeEntry("ot1", { topics: ["alpha", "beta"], embedding: [0.1, 0.2] }),
		);
		await db.insertEntry(
			makeEntry("ot2", { topics: ["beta", "gamma"], embedding: [0.3, 0.4] }),
		);
		await db.insertEntry(
			makeEntry("ot3", { topics: ["delta"], embedding: [0.5, 0.6] }),
		);

		const overlapping = await db.getEntriesWithOverlappingTopics(
			["beta"],
			["ot1"],
		);
		expect(overlapping.length).toBe(1);
		expect(overlapping[0].id).toBe("ot2");
		expect(overlapping[0].embedding).toBeDefined();
	});

	// ── Embedding Metadata ──

	it("getEmbeddingMetadata returns null when no metadata exists", async () => {
		expect(await db.getEmbeddingMetadata()).toBeNull();
	});

	it("setEmbeddingMetadata creates and updates metadata", async () => {
		await db.setEmbeddingMetadata("text-embedding-3-small", 1536);
		const meta = await db.getEmbeddingMetadata();
		expect(meta).not.toBeNull();
		expect(meta?.model).toBe("text-embedding-3-small");
		expect(meta?.dimensions).toBe(1536);
		expect(meta?.recordedAt).toBeGreaterThan(0);

		// Upsert
		await db.setEmbeddingMetadata("text-embedding-3-large", 3072);
		const updated = await db.getEmbeddingMetadata();
		expect(updated?.model).toBe("text-embedding-3-large");
		expect(updated?.dimensions).toBe(3072);
	});

	it("enables ANN with halfvec index for >2000 dimensions", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);

		const idx = await truncSql`
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND indexname = 'idx_entry_embedding_halfvec_hnsw'
		`;
		expect(idx.length).toBe(1);
		expect(await pg.isVectorSearchReady()).toBe(true);

		const q = Array.from({ length: 3072 }, (_, i) => (i === 0 ? 1 : 0));
		const orth = Array.from({ length: 3072 }, (_, i) => (i === 1 ? 1 : 0));

		await pg.insertEntry(makeEntry("ann3072-high", { embedding: q }));
		await pg.insertEntry(makeEntry("ann3072-low", { embedding: orth }));

		const result = await pg.findSimilarEntries(q, 5, 0.5);
		expect(result.some((r) => r.entry.id === "ann3072-high")).toBe(true);
		expect(result.some((r) => r.entry.id === "ann3072-low")).toBe(false);
	});

	it("findSimilarEntries does not starve active results behind many filtered statuses", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);

		const q = Array.from({ length: 3072 }, (_, i) => (i === 0 ? 1 : 0));
		const near = Array.from({ length: 3072 }, (_, i) => {
			if (i === 0) return 0.99;
			if (i === 1) return 0.01;
			return 0;
		});

		for (let i = 0; i < 180; i++) {
			await pg.insertEntry(
				makeEntry(`ann3072-sup-${i}`, {
					status: "superseded",
					supersededBy: "ann3072-active",
					embedding: q,
				}),
			);
		}

		await pg.insertEntry(
			makeEntry("ann3072-active", {
				status: "active",
				embedding: near,
			}),
		);

		const result = await pg.findSimilarEntries(q, 5, 0.5, ["active"]);
		expect(result.some((r) => r.entry.id === "ann3072-active")).toBe(true);
		expect(result.every((r) => r.entry.status === "active")).toBe(true);
	});

	it("degrades gracefully when dimensions exceed halfvec HNSW limit", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("oversized-model", 4096);
		expect(await pg.isVectorSearchReady()).toBe(false);
	});

	it("self-heals missing embedding_vec rows when ensureVectorColumn reruns", async () => {
		let pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);

		const q = Array.from({ length: 3072 }, (_, i) => (i === 0 ? 1 : 0));
		await pg.insertEntry(makeEntry("heal1", { embedding: q }));

		// Simulate partial/legacy state: BYTEA embedding present but embedding_vec missing.
		await truncSql`UPDATE knowledge_entry SET embedding_vec = NULL WHERE id = 'heal1'`;

		// Recreate db instance, then trigger ensureVectorColumn() again via
		// setEmbeddingMetadata (same model/dims) so legacy partial states are repaired.
		await db.close();
		db = new PostgresKnowledgeDB(uri);
		pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);

		const counts = await truncSql`
			SELECT COUNT(*)::int AS missing
			FROM knowledge_entry
			WHERE embedding IS NOT NULL
			  AND embedding_vec IS NULL
		`;
		expect(Number((counts[0] as { missing: number }).missing)).toBe(0);
		expect(await pg.isVectorSearchReady()).toBe(true);
	});

	// ── Clusters ──

	it("getClustersWithMembers returns empty when no clusters", async () => {
		expect(await db.getClustersWithMembers()).toEqual([]);
	});

	it("persistClusters inserts clusters with members", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await db.insertEntry(makeEntry("ce1", { embedding: centroid }));
		await db.insertEntry(makeEntry("ce2", { embedding: centroid }));

		await db.persistClusters([
			{
				id: "cluster-1",
				centroid,
				memberIds: ["ce1", "ce2"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		expect(clusters[0].id).toBe("cluster-1");
		expect(clusters[0].memberIds.sort()).toEqual(["ce1", "ce2"]);
		expect(clusters[0].memberCount).toBe(2);
		expect(clusters[0].lastSynthesizedAt).toBeNull();
		expect(clusters[0].centroid[0]).toBeCloseTo(1.0);
	});

	it("persistClusters updates centroid without bumping lastMembershipChangedAt when stable", async () => {
		const centroid = [0.5, 0.5, 0.0, 0.0];
		await db.insertEntry(makeEntry("ce3", { embedding: centroid }));
		await db.insertEntry(makeEntry("ce4", { embedding: centroid }));

		await db.persistClusters([
			{
				id: "cluster-2",
				centroid,
				memberIds: ["ce3", "ce4"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		const before = (await db.getClustersWithMembers())[0]
			.lastMembershipChangedAt;

		const newCentroid = [0.6, 0.4, 0.0, 0.0];
		await db.persistClusters([
			{
				id: "cluster-2",
				centroid: newCentroid,
				memberIds: ["ce3", "ce4"],
				isNew: false,
				membershipChanged: false,
			},
		]);

		const after = (await db.getClustersWithMembers())[0];
		expect(after.lastMembershipChangedAt).toBe(before);
		expect(after.centroid[0]).toBeCloseTo(0.6);
	});

	it("persistClusters deletes stale clusters", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0];
		await db.insertEntry(makeEntry("ce5", { embedding: centroid }));

		await db.persistClusters([
			{
				id: "cluster-old",
				centroid,
				memberIds: ["ce5"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		expect(await db.getClustersWithMembers()).toHaveLength(1);

		await db.persistClusters([
			{
				id: "cluster-new",
				centroid,
				memberIds: ["ce5"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		expect(clusters[0].id).toBe("cluster-new");
	});

	it("markClusterSynthesized stamps last_synthesized_at", async () => {
		const centroid = [1.0, 0.0];
		await db.insertEntry(makeEntry("ce6", { embedding: centroid }));

		await db.persistClusters([
			{
				id: "cluster-synth",
				centroid,
				memberIds: ["ce6"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		expect((await db.getClustersWithMembers())[0].lastSynthesizedAt).toBeNull();

		const before = Date.now();
		await db.markClusterSynthesized("cluster-synth");
		const after = Date.now();

		const stamped = (await db.getClustersWithMembers())[0].lastSynthesizedAt;
		expect(stamped).not.toBeNull();
		expect(stamped).toBeGreaterThanOrEqual(before);
		expect(stamped).toBeLessThanOrEqual(after);
	});

	// ── Reinitialize ──

	it("reinitialize clears all data", async () => {
		await db.insertEntry(makeEntry("ri-1"));
		await db.setEmbeddingMetadata("test-model", 128);

		// reinitialize() clears knowledge tables only.
		// Staging lives in state.db (ServerStateDB), not in the knowledge store.
		await db.reinitialize();

		expect((await db.getStats()).total).toBe(0);
		expect(await db.getEmbeddingMetadata()).toBeNull();
	});

	it("reinitialize clears cluster tables", async () => {
		const centroid = [1.0, 0.0];
		await db.insertEntry(makeEntry("ri-cluster-e1", { embedding: centroid }));
		await db.persistClusters([
			{
				id: "ri-cluster",
				centroid,
				memberIds: ["ri-cluster-e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		expect(await db.getClustersWithMembers()).toHaveLength(1);

		await db.reinitialize();

		expect(await db.getClustersWithMembers()).toHaveLength(0);
	});
});
