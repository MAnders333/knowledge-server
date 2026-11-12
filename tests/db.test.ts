import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { CREATE_TABLES } from "../src/db/sqlite/schema";
import { ServerStateDB } from "../src/db/state/index";

describe("KnowledgeDB", () => {
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("should create tables on initialization", async () => {
		const stats = await db.getStats();
		expect(stats.total).toBe(0);
	});

	it("should insert and retrieve an entry", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "test-1",
			type: "fact",
			content: "Churn rate is 4.2%",
			topics: ["churn", "metrics"],
			confidence: 0.9,
			source: "session: Churn Analysis",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: ["session-123"],
		});

		const entry = await db.getEntry("test-1");
		expect(entry).not.toBeNull();
		expect(entry?.content).toBe("Churn rate is 4.2%");
		expect(entry?.topics).toEqual(["churn", "metrics"]);
		expect(entry?.derivedFrom).toEqual(["session-123"]);
	});

	it("should update entry fields", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "test-2",
			type: "fact",
			content: "Old content",
			topics: ["test"],
			confidence: 0.5,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

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

	it("records injections: injection_count grows, access_count is frozen, last_accessed_at moves at most daily", async () => {
		const day = 24 * 60 * 60 * 1000;
		const t0 = Date.now() - 3 * day;
		await db.insertEntry({
			id: "test-3",
			type: "principle",
			content: "Test principle",
			topics: [],
			confidence: 0.7,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: t0,
			updatedAt: t0,
			lastAccessedAt: t0,
			accessCount: 4,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const t1 = t0 + 2 * day;
		await db.recordInjections(["test-3"], t1);
		// Within 24 h of the last bump: counted, but freshness does not move.
		await db.recordInjections(["test-3", "test-3"], t1 + 60_000);
		// A day later the freshness moves again.
		await db.recordInjections(["test-3"], t1 + day + 1);

		const entry = await db.getEntry("test-3");
		expect(entry?.accessCount).toBe(4);
		expect(entry?.lastAccessedAt).toBe(t1 + day + 1);

		const raw = new Database(join(tempDir, "test.db"), { readonly: true });
		const row = raw
			.prepare(
				"SELECT injection_count FROM knowledge_entry WHERE id = 'test-3'",
			)
			.get() as { injection_count: number };
		raw.close();
		// Each id counts once per batch: 1 + 1 (duplicate id in one batch) + 1.
		expect(row.injection_count).toBe(3);
	});

	it("recordInjections is a no-op for an empty batch", async () => {
		await db.recordInjections([], Date.now());
	});

	it("should filter entries by status", async () => {
		const now = Date.now();
		const makeEntry = (id: string, status: string) => ({
			id,
			type: "fact" as const,
			content: `Entry ${id}`,
			topics: [],
			confidence: 0.5,
			source: "test",
			status: status as "active" | "archived",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		await db.insertEntry(makeEntry("a1", "active"));
		await db.insertEntry(makeEntry("a2", "active"));
		await db.insertEntry(makeEntry("a3", "archived"));

		const active = await db.getActiveEntries();
		expect(active.length).toBe(2);

		const archived = await db.getEntriesByStatus("archived");
		expect(archived.length).toBe(1);
	});

	it("should store and retrieve embeddings", async () => {
		const now = Date.now();
		const embedding = [0.1, 0.2, 0.3, 0.4, 0.5];

		await db.insertEntry({
			id: "test-emb",
			type: "fact",
			content: "Entry with embedding",
			topics: [],
			confidence: 0.5,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding,
		});

		// Plain reads never carry the embedding BLOB; scoring reads do.
		expect((await db.getEntry("test-emb"))?.embedding).toBeUndefined();
		const [scored] = (await db.getActiveEntriesWithEmbeddings()).filter(
			(e) => e.id === "test-emb",
		);
		expect(scored?.embedding.length).toBe(5);
		// Float32 precision — check approximate equality
		for (let i = 0; i < embedding.length; i++) {
			expect(Math.abs(scored.embedding[i] - embedding[i])).toBeLessThan(0.0001);
		}
	});

	it("reports strength computed on read; the stored column is not rewritten", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "live-strength",
			type: "fact",
			content: "Strength is live",
			topics: [],
			confidence: 0.9,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now - 400 * 86_400_000,
			updatedAt: now,
			lastAccessedAt: now - 400 * 86_400_000,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});
		const live = (await db.getEntry("live-strength"))?.strength ?? 1;
		expect(live).toBeLessThan(0.05);
		const raw = new Database(join(tempDir, "test.db"), { readonly: true });
		try {
			const row = raw
				.prepare("SELECT strength FROM knowledge_entry WHERE id = ?")
				.get("live-strength") as { strength: number };
			expect(row.strength).toBe(1.0);
		} finally {
			raw.close();
		}
		// Ordering and the review stale filter use the live value.
		const stale = (await db.getActiveEntries()).filter(
			(e) => e.id === "live-strength",
		);
		expect(stale[0]?.strength).toBeLessThan(0.3);
	});

	it("scanEmbeddings returns id and vector only, for active and conflicted entries", async () => {
		const now = Date.now();
		const base = {
			type: "fact" as const,
			topics: ["t"],
			confidence: 0.5,
			source: "test",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		};
		await db.insertEntry({
			...base,
			id: "scan-a",
			content: "a",
			status: "active",
			embedding: [0.5, -1, 2],
		});
		await db.insertEntry({
			...base,
			id: "scan-c",
			content: "c",
			status: "conflicted",
			embedding: [1, 1, 1],
		});
		await db.insertEntry({
			...base,
			id: "scan-old",
			content: "o",
			status: "superseded",
			embedding: [9, 9, 9],
		});
		await db.insertEntry({
			...base,
			id: "scan-none",
			content: "n",
			status: "active",
		});

		const rows = await db.scanEmbeddings();
		expect(rows.map((r) => r.id).sort()).toEqual(["scan-a", "scan-c"]);
		const a = rows.find((r) => r.id === "scan-a");
		expect(a).toBeDefined();
		expect(a?.embedding).toBeInstanceOf(Float32Array);
		expect(Array.from(a?.embedding ?? [])).toEqual([0.5, -1, 2]);

		const hydrated = await db.getEntriesByIds([
			"scan-a",
			"scan-old",
			"scan-none",
		]);
		// Hydration returns active/conflicted rows whatever their embedding state.
		expect(hydrated.map((e) => e.id).sort()).toEqual(["scan-a", "scan-none"]);
		expect(hydrated[0].embedding).toBeUndefined();
		expect(hydrated[0].content).toBe("a");
		expect(await db.getEntriesByIds([])).toEqual([]);
	});

	it("plain entry reads never carry the embedding", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "plain-1",
			type: "fact",
			content: "Plain read",
			topics: ["t"],
			confidence: 0.5,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding: [0.1, 0.2, 0.3],
		});
		const reads = [
			await db.getEntry("plain-1"),
			...(await db.getActiveEntries()),
			...(await db.getActiveAndConflictedEntries()),
			...(await db.getEntriesByStatus("active")),
			...(await db.getEntries({ status: "active" })),
		];
		expect(reads.length).toBeGreaterThanOrEqual(5);
		for (const entry of reads) {
			expect(entry?.embedding).toBeUndefined();
		}
		expect(
			(await db.getActiveEntriesWithEmbeddings()).map((e) => e.id),
		).toEqual(["plain-1"]);
	});

	it("should manage consolidation state", async () => {
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

	it("should record and retrieve episode ranges", async () => {
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
		const s1 = ranges.get("session-1");
		expect(s1).toBeDefined();
		expect(s1).toHaveLength(2);
		expect(
			(s1 ?? []).some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-1" &&
					r.endMessageId === "msg-end-1",
			),
		).toBe(true);
		expect(
			(s1 ?? []).some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-2" &&
					r.endMessageId === "msg-end-2",
			),
		).toBe(true);

		const s2 = ranges.get("session-2");
		expect(s2).toBeDefined();
		expect(s2).toHaveLength(1);
		expect((s2 ?? [{}])[0].startMessageId).toBe("msg-start-3");
		expect((s2 ?? [{}])[0].endMessageId).toBe("msg-end-3");
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
		// Both source entries are returned — idempotency is per (source, session, start, end)
		expect(ranges.get("session-1")).toHaveLength(2);
	});

	it("recordEpisode is idempotent — duplicate inserts are ignored", async () => {
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

	it("getProcessedEpisodeRanges returns empty map for unknown session", async () => {
		const ranges = await serverStateDb.getProcessedEpisodeRanges([
			"no-such-session",
		]);
		expect(ranges.size).toBe(0);
	});

	it("should handle relations between entries", async () => {
		const now = Date.now();
		const makeEntry = (id: string) => ({
			id,
			type: "fact" as const,
			content: `Entry ${id}`,
			topics: [],
			confidence: 0.5,
			source: "test",
			status: "active" as const,
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		await db.insertEntry(makeEntry("e1"));
		await db.insertEntry(makeEntry("e2"));

		await db.insertRelation({
			id: "rel-1",
			sourceId: "e1",
			targetId: "e2",
			type: "supports",
			createdAt: now,
		});

		const relations = await db.getRelationsFor("e1");
		expect(relations.length).toBe(1);
		expect(relations[0].type).toBe("supports");
		expect(relations[0].targetId).toBe("e2");
	});

	// ── Embedding Metadata ──

	it("getEmbeddingMetadata returns null when no metadata exists", async () => {
		const meta = await db.getEmbeddingMetadata();
		expect(meta).toBeNull();
	});

	it("setEmbeddingMetadata creates singleton row and getEmbeddingMetadata reads it", async () => {
		await db.setEmbeddingMetadata("text-embedding-3-small", 1536);
		const meta = await db.getEmbeddingMetadata();
		expect(meta).not.toBeNull();
		expect(meta?.model).toBe("text-embedding-3-small");
		expect(meta?.dimensions).toBe(1536);
		expect(meta?.recordedAt).toBeGreaterThan(0);
	});

	it("setEmbeddingMetadata overwrites previous metadata (upsert)", async () => {
		await db.setEmbeddingMetadata("text-embedding-3-small", 1536);
		await db.setEmbeddingMetadata("text-embedding-3-large", 3072);
		const meta = await db.getEmbeddingMetadata();
		expect(meta?.model).toBe("text-embedding-3-large");
		expect(meta?.dimensions).toBe(3072);
	});

	// ── getEntriesMissingEmbeddings ──

	it("getEntriesMissingEmbeddings returns only active/conflicted entries without embeddings", async () => {
		const now = Date.now();
		// Active with embedding — should NOT be returned
		await db.insertEntry({
			id: "has-emb",
			type: "fact",
			content: "Has embedding",
			topics: ["t"],
			confidence: 0.8,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding: [0.1, 0.2],
		});
		// Active without embedding — should be returned
		await db.insertEntry({
			id: "no-emb",
			type: "fact",
			content: "No embedding",
			topics: ["t"],
			confidence: 0.8,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});
		// Superseded without embedding — should NOT be returned
		await db.insertEntry({
			id: "superseded-no-emb",
			type: "fact",
			content: "Superseded",
			topics: ["t"],
			confidence: 0.8,
			source: "test",
			status: "superseded",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: "has-emb",
			derivedFrom: [],
		});

		const missing = await db.getEntriesMissingEmbeddings();
		expect(missing.length).toBe(1);
		expect(missing[0].id).toBe("no-emb");
	});

	// ── reinitialize clears embedding_metadata ──

	it("reinitialize clears embedding_metadata", async () => {
		await db.setEmbeddingMetadata("test-model", 128);
		expect(await db.getEmbeddingMetadata()).not.toBeNull();

		await db.reinitialize();

		expect(await db.getEmbeddingMetadata()).toBeNull();
	});

	it("should return correct stats", async () => {
		const now = Date.now();
		const makeEntry = (id: string, status: string) => ({
			id,
			type: "fact" as const,
			content: `Entry ${id}`,
			topics: [],
			confidence: 0.5,
			source: "test",
			status: status as "active" | "archived" | "superseded",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		await db.insertEntry(makeEntry("s1", "active"));
		await db.insertEntry(makeEntry("s2", "active"));
		await db.insertEntry(makeEntry("s3", "active"));
		await db.insertEntry(makeEntry("s4", "archived"));
		await db.insertEntry(makeEntry("s5", "superseded"));

		const stats = await db.getStats();
		expect(stats.total).toBe(5);
		expect(stats.active).toBe(3);
		expect(stats.archived).toBe(1);
		expect(stats.superseded).toBe(1);
	});
});

describe("KnowledgeDB — cluster CRUD", () => {
	let db: KnowledgeDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-cluster-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	async function insertEntry(id: string, embedding: number[]) {
		const now = Date.now();
		await db.insertEntry({
			id,
			type: "fact",
			content: `Entry ${id}`,
			topics: ["test"],
			confidence: 0.8,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding,
		});
	}

	it("getClustersWithMembers returns empty array when no clusters exist", async () => {
		expect(await db.getClustersWithMembers()).toEqual([]);
	});

	it("persistClusters inserts a new cluster with members", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);
		await insertEntry("e2", centroid);

		await db.persistClusters([
			{
				id: "cluster-1",
				centroid,
				memberIds: ["e1", "e2"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		expect(clusters[0].id).toBe("cluster-1");
		expect(clusters[0].memberIds.sort()).toEqual(["e1", "e2"]);
		expect(clusters[0].memberCount).toBe(2);
		expect(clusters[0].lastSynthesizedAt).toBeNull();
		// centroid round-trips through float32 — values should be close
		expect(clusters[0].centroid[0]).toBeCloseTo(1.0);
	});

	it("persistClusters updates centroid and member count on re-run, does not bump last_membership_changed_at when membership is stable", async () => {
		const centroid = [0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);
		await insertEntry("e2", centroid);

		// First persist — new cluster
		await db.persistClusters([
			{
				id: "cluster-1",
				centroid,
				memberIds: ["e1", "e2"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		const before = (await db.getClustersWithMembers())[0]
			.lastMembershipChangedAt;

		// Small delay to ensure timestamps differ if the code is wrong
		const newCentroid = [0.6, 0.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

		// Re-persist same cluster, same members, membershipChanged=false
		await db.persistClusters([
			{
				id: "cluster-1",
				centroid: newCentroid,
				memberIds: ["e1", "e2"],
				isNew: false,
				membershipChanged: false,
			},
		]);

		const after = (await db.getClustersWithMembers())[0];
		expect(after.lastMembershipChangedAt).toBe(before); // unchanged
		expect(after.centroid[0]).toBeCloseTo(0.6); // centroid updated
	});

	it("persistClusters keeps joined_at for members that stay and stamps new members", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);
		await insertEntry("e2", centroid);

		await db.persistClusters([
			{
				id: "c1",
				centroid,
				memberIds: ["e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		const first = (await db.getClustersWithMembers())[0].memberJoinedAt.get(
			"e1",
		);
		expect(first).toBeNumber();

		await Bun.sleep(10);
		await db.persistClusters([
			{
				id: "c1",
				centroid,
				memberIds: ["e1", "e2"],
				isNew: false,
				membershipChanged: true,
			},
		]);
		const joined = (await db.getClustersWithMembers())[0].memberJoinedAt;
		expect(joined.get("e1")).toBe(first as number);
		expect(joined.get("e2") as number).toBeGreaterThan(first as number);

		// Members that left are removed; the remaining member keeps its timestamp.
		await db.persistClusters([
			{
				id: "c1",
				centroid,
				memberIds: ["e2"],
				isNew: false,
				membershipChanged: true,
			},
		]);
		const after = (await db.getClustersWithMembers())[0];
		expect(after.memberIds).toEqual(["e2"]);
		expect(after.memberJoinedAt.has("e1")).toBe(false);
		expect(after.memberJoinedAt.get("e2")).toBe(joined.get("e2") as number);
	});

	it("persistClusters deletes stale clusters not in the new set", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);

		await db.persistClusters([
			{
				id: "cluster-old",
				centroid,
				memberIds: ["e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		expect(await db.getClustersWithMembers()).toHaveLength(1);

		// New pass produces a different cluster (cluster-old disappears)
		await db.persistClusters([
			{
				id: "cluster-new",
				centroid,
				memberIds: ["e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		expect(clusters[0].id).toBe("cluster-new");
	});

	it("markClusterSynthesized stamps last_synthesized_at", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);

		await db.persistClusters([
			{
				id: "cluster-1",
				centroid,
				memberIds: ["e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);

		expect((await db.getClustersWithMembers())[0].lastSynthesizedAt).toBeNull();

		const before = Date.now();
		await db.markClusterSynthesized("cluster-1");
		const after = Date.now();

		const stamped = (await db.getClustersWithMembers())[0].lastSynthesizedAt;
		expect(stamped).not.toBeNull();
		expect(stamped).toBeGreaterThanOrEqual(before);
		expect(stamped).toBeLessThanOrEqual(after);
	});

	it("reinitialize clears cluster tables", async () => {
		const centroid = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
		await insertEntry("e1", centroid);
		await db.persistClusters([
			{
				id: "cluster-1",
				centroid,
				memberIds: ["e1"],
				isNew: true,
				membershipChanged: true,
			},
		]);
		expect(await db.getClustersWithMembers()).toHaveLength(1);

		await db.reinitialize();

		expect(await db.getClustersWithMembers()).toHaveLength(0);
	});
});
