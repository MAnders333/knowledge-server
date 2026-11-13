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
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import postgres from "postgres";
import { ActivationEngine } from "../src/activation/activate";
import type { EmbeddingClient } from "../src/activation/embeddings";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import { PENDING_EPISODE_MAX_ATTEMPTS } from "../src/db/interface";
import {
	PostgresKnowledgeDB,
	buildAnnSimilarEntriesSql,
} from "../src/db/postgres/index";
import { PG_MIGRATIONS } from "../src/db/postgres/migrations";
import { SCHEMA_VERSION } from "../src/db/postgres/schema";
import { ServerStateDB } from "../src/db/state/index";
import { PostgresServerStateDB } from "../src/db/state/postgres";
import { logger } from "../src/logger";
import { KnowledgeService } from "../src/services/knowledge-service";
import type { KnowledgeEntry } from "../src/types";

const PG_URI = process.env.PG_TEST_URI;
// Non-owner role password, built at runtime so no credential literal sits in source.
const NONOWNER_PW = `pw${crypto.randomUUID().replaceAll("-", "")}`;

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

/**
 * Replace a store's postgres.js handle so begin() fails the way postgres.js
 * does when set_config or the ANN query errors: the transaction aborts (the
 * query then reports 25P02) and the first error is rethrown. `unsafe()` and
 * `end()` go to the real database, so the fallback path still runs for real.
 */
function stubAnnTransaction(
	store: PostgresKnowledgeDB,
	faults: { setConfig?: unknown; query?: unknown },
) {
	const realSql = (store as unknown as { sql: postgres.Sql }).sql;
	const aborted = Object.assign(new Error("current transaction is aborted"), {
		code: "25P02",
	});
	const calls = { begin: 0 };
	(store as unknown as { sql: unknown }).sql = {
		unsafe: (query: string, params: postgres.ParameterOrJSON<never>[]) =>
			realSql.unsafe(query, params),
		end: () => realSql.end(),
		begin: async (cb: (tx: unknown) => unknown[]) => {
			calls.begin++;
			const tx = Object.assign(
				() =>
					faults.setConfig === undefined
						? Promise.resolve([])
						: Promise.reject(faults.setConfig),
				{
					unsafe: () => Promise.reject(faults.query ?? aborted),
				},
			);
			return Promise.all(cb(tx));
		},
	};
	return calls;
}

/**
 * EXPLAIN the production ANN SQL under the same transaction-local ef_search the
 * search uses, and assert the planner picks the HNSW index. Guards the ANN
 * tests against passing on a seq scan.
 */
async function expectHnswPlan(planSql: postgres.Sql) {
	const plan = await planSql.begin(async (tx) => {
		await tx`SELECT set_config('hnsw.ef_search', '640', true)`;
		return tx.unsafe(`EXPLAIN (COSTS OFF) ${buildAnnSimilarEntriesSql(8)}`, [
			"[1,0,0,0,0,0,0,0]",
			640,
			["active"],
			"[1,0,0,0,0,0,0,0]",
			0.5,
			40,
		]);
	});
	expect(plan.map((r) => String(r["QUERY PLAN"])).join("\n")).toContain(
		"Index Scan using idx_entry_embedding_halfvec_hnsw",
	);
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

	it("reports strength computed on read, not the stored column", async () => {
		const now = Date.now();
		await db.insertEntry(
			makeEntry("test-strength", {
				confidence: 0.9,
				strength: 1.0,
				lastAccessedAt: now - 400 * 86_400_000,
			}),
		);

		const entry = await db.getEntry("test-strength");
		expect(entry?.strength).toBeLessThan(0.05);
		const [stored] = await truncSql`
			SELECT strength FROM knowledge_entry WHERE id = 'test-strength'
		`;
		expect(Number(stored.strength)).toBe(1.0);
	});

	it("migration v18 drops the strength and last_accessed_at indexes", async () => {
		const names = await truncSql`
			SELECT indexname FROM pg_indexes
			WHERE tablename = 'knowledge_entry'
			  AND indexname IN ('idx_entry_strength', 'idx_entry_accessed')
		`;
		expect(names).toHaveLength(0);
		const v18 = PG_MIGRATIONS.find((m) => m.version === 18);
		expect(v18).toBeDefined();
		// Recreate the legacy indexes, then run the migration step on them.
		await truncSql`CREATE INDEX IF NOT EXISTS idx_entry_strength ON knowledge_entry(strength)`;
		await truncSql`CREATE INDEX IF NOT EXISTS idx_entry_accessed ON knowledge_entry(last_accessed_at)`;
		await truncSql.begin(async (tx) => {
			await (v18 as (typeof PG_MIGRATIONS)[number]).up(tx as never);
		});
		const after = await truncSql`
			SELECT indexname FROM pg_indexes
			WHERE tablename = 'knowledge_entry'
			  AND indexname IN ('idx_entry_strength', 'idx_entry_accessed')
		`;
		expect(after).toHaveLength(0);
	});

	it("migration v20 clamps member join times that older builds rewrote", async () => {
		await db.insertEntry(makeEntry("v19-member"));
		await truncSql`DELETE FROM knowledge_cluster_member`;
		await truncSql`DELETE FROM knowledge_cluster`;
		await truncSql`
			INSERT INTO knowledge_cluster (id, centroid, member_count, last_synthesized_at, last_membership_changed_at, created_at)
			VALUES ('v19-synth', '\\x00'::bytea, 1, 1000, 1000, 0),
			       ('v19-never', '\\x00'::bytea, 1, NULL, 3000, 0)
		`;
		await truncSql`
			INSERT INTO knowledge_cluster_member (cluster_id, entry_id, joined_at) VALUES
			  ('v19-synth', 'v19-member', 2000),
			  ('v19-never', 'v19-member', 2000)
		`;
		await truncSql.begin(async (tx) => {
			await (
				PG_MIGRATIONS.find(
					(m) => m.version === 20,
				) as (typeof PG_MIGRATIONS)[number]
			).up(tx as never);
		});
		const rows = await truncSql<
			{ cluster_id: string; joined_at: string | number }[]
		>`
			SELECT cluster_id, joined_at FROM knowledge_cluster_member ORDER BY cluster_id
		`;
		expect(rows.map((r) => [r.cluster_id, Number(r.joined_at)])).toEqual([
			["v19-never", 2000],
			["v19-synth", 1000],
		]);
		await truncSql`DELETE FROM knowledge_cluster_member`;
		await truncSql`DELETE FROM knowledge_cluster`;
	});

	// ── Embeddings ──

	// ── Stats ──

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

	// ── Contradiction Resolution ──

	// ── Merge Entry ──

	// ── Overlapping Topics ──

	// ── Embedding Metadata ──

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

	it("findSimilarEntries returns active rows beyond the hnsw.ef_search default when HNSW is used", async () => {
		// enable_seqscan=off is a session startup parameter on this instance only,
		// so the planner must use the halfvec HNSW index for every ANN query here.
		const forcedUri = new URL(uri);
		forcedUri.searchParams.set("enable_seqscan", "off");
		const forced = new PostgresKnowledgeDB(forcedUri.toString(), 1);
		const planSql = postgres(forcedUri.toString(), {
			max: 1,
			onnotice: () => {},
		});
		try {
			await forced.initialize();
			await forced.setEmbeddingMetadata("ann-ceiling-test", 8);

			const vec = (a: number, b: number) => [1, a, b, 0, 0, 0, 0, 0];
			const q = vec(0, 0);

			// 100 superseded neighbours sit closest to the query. With limit=40,
			// annCandidates = 640; the old code let HNSW stop at ef_search=40
			// superseded rows, so no active row was ever reached.
			for (let i = 0; i < 100; i++) {
				await forced.insertEntry(
					makeEntry(`ceil-sup-${i}`, {
						status: "superseded",
						supersededBy: "ceil-act-0",
						embedding: vec(0.001 * (i + 1), 0),
					}),
				);
			}
			for (let i = 0; i < 3; i++) {
				await forced.insertEntry(
					makeEntry(`ceil-act-${i}`, {
						status: "active",
						embedding: vec(0.2, 0.01 * (i + 1)),
					}),
				);
			}

			await expectHnswPlan(planSql);

			const result = await forced.findSimilarEntries(q, 40, 0.5, ["active"]);
			expect(result.map((r) => r.entry.id).sort()).toEqual([
				"ceil-act-0",
				"ceil-act-1",
				"ceil-act-2",
			]);
		} finally {
			await Promise.all([forced.close(), planSql.end()]);
		}
	});

	it("findSimilarEntries returns full ANN results when two calls race on a fresh pool connection", async () => {
		// Pool of 2: initialize() leaves one idle connection. The two concurrent calls
		// below reserve that one for the first call and open a new connection for the
		// second. This assumes postgres.js connects lazily and reserves synchronously,
		// so the second call runs on a backend where vector.so is not yet loaded.
		const forcedUri = new URL(uri);
		forcedUri.searchParams.set("enable_seqscan", "off");
		const forced = new PostgresKnowledgeDB(forcedUri.toString(), 2);
		const planSql = postgres(forcedUri.toString(), {
			max: 1,
			onnotice: () => {},
		});
		try {
			await forced.initialize();
			await forced.setEmbeddingMetadata("ann-fresh-conn-test", 8);

			const vec = (a: number, b: number) => [1, a, b, 0, 0, 0, 0, 0];
			const q = vec(0, 0);

			// 50 superseded neighbours fill the default ef_search of 40 on their own,
			// so only a raised ef_search can return the 40 active rows below.
			for (let i = 0; i < 50; i++) {
				await forced.insertEntry(
					makeEntry(`fresh-sup-${i}`, {
						status: "superseded",
						supersededBy: "fresh-act-0",
						embedding: vec(0.001 * (i + 1), 0),
					}),
				);
			}
			for (let i = 0; i < 60; i++) {
				await forced.insertEntry(
					makeEntry(`fresh-act-${i}`, {
						status: "active",
						embedding: vec(0.2, 0.01 * (i + 1)),
					}),
				);
			}

			// Nearest 40 active rows, in distance order (fresh-act-0 .. fresh-act-39).
			const expected = Array.from(
				{ length: 40 },
				(_, i) => `fresh-act-${i}`,
			).sort();

			// Guard: the race must run on the HNSW path, or it proves nothing.
			await expectHnswPlan(planSql);

			const [first, second] = await Promise.all([
				forced.findSimilarEntries(q, 40, 0.5, ["active"]),
				forced.findSimilarEntries(q, 40, 0.5, ["active"]),
			]);
			expect(first.map((r) => r.entry.id).sort()).toEqual(expected);
			expect(second.map((r) => r.entry.id).sort()).toEqual(expected);
		} finally {
			await Promise.all([forced.close(), planSql.end()]);
		}
	});

	it("findSimilarEntries returns every entry field including the embedding", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("parity-test", 8);
		await pg.insertEntry(
			makeEntry("parity-1", {
				type: "principle",
				topics: ["alpha", "beta"],
				confidence: 0.7,
				source: "parity-source",
				strength: 0.9,
				accessCount: 3,
				observationCount: 2,
				derivedFrom: ["src-a", "src-b"],
				isSynthesized: true,
				embedding: [1, 0.1, 0, 0, 0, 0, 0, 0],
			}),
		);

		const result = await pg.findSimilarEntries(
			[1, 0, 0, 0, 0, 0, 0, 0],
			5,
			0.5,
		);
		expect(result).toHaveLength(1);
		const expected = await pg.getEntry("parity-1");
		expect(expected).not.toBeNull();
		// ANN rows carry the same plain fields as getEntry, and no embedding.
		// Strength is computed on read against the clock, so compare it separately.
		const { strength: annStrength, ...annPlain } = result[0].entry;
		const { strength: plainStrength, ...plain } = expected as KnowledgeEntry;
		expect(annPlain).toEqual(plain);
		expect(annStrength).toBeCloseTo(plainStrength, 3);
		expect(result[0].entry.embedding).toBeUndefined();
	});

	it("findContradictionCandidates returns every entry field including the embedding", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("parity-test", 8);
		await pg.insertEntry(
			makeEntry("contra-parity", {
				topics: ["shared-topic"],
				derivedFrom: ["src-a"],
				isSynthesized: true,
				embedding: [1, 0.5, 0, 0, 0, 0, 0, 0],
			}),
		);

		// Similarity is about 0.894, inside the [0.5, 0.99) band.
		const res = await pg.findContradictionCandidates(
			[1, 0, 0, 0, 0, 0, 0, 0],
			["shared-topic"],
			[],
			0.5,
			0.99,
		);
		expect(res).toHaveLength(1);
		const expected = await pg.getEntry("contra-parity");
		expect(expected).not.toBeNull();
		const { embedding, strength, ...plain } = res[0];
		const { strength: plainStrength, ...expectedPlain } =
			expected as KnowledgeEntry;
		expect(plain).toEqual(expectedPlain);
		expect(strength).toBeCloseTo(plainStrength, 3);
		expect(embedding).toHaveLength(8);
	});

	it("findSimilarEntries latches the ef_search fallback only for unsupported-setting errors", async () => {
		const store = new PostgresKnowledgeDB(uri, 1);
		store.id = "fallback-store";
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		try {
			await store.initialize();
			await store.setEmbeddingMetadata("ef-fallback-test", 8);
			await store.insertEntry(
				makeEntry("fallback-1", { embedding: [1, 0, 0, 0, 0, 0, 0, 0] }),
			);
			const unsupported = Object.assign(
				new Error("unrecognized configuration parameter"),
				{ code: "42704" },
			);
			const calls = stubAnnTransaction(store, { setConfig: unsupported });

			const q = [1, 0, 0, 0, 0, 0, 0, 0];
			const first = await store.findSimilarEntries(q, 5, 0.5, ["active"]);
			const second = await store.findSimilarEntries(q, 5, 0.5, ["active"]);
			expect(first.map((r) => r.entry.id)).toEqual(["fallback-1"]);
			expect(second.map((r) => r.entry.id)).toEqual(["fallback-1"]);
			// Latched: the second call goes straight to the plain query.
			expect(calls.begin).toBe(1);
			expect(warn).toHaveBeenCalledTimes(1);
			const latchedWarning = String(warn.mock.calls[0][0]);
			expect(latchedWarning).toContain('Store "fallback-store"');
			expect(latchedWarning).toContain(
				"ANN recall for this store is capped at the pgvector default (40 candidates) until the process restarts",
			);
			expect(
				(store as unknown as { efSearchUnsupported: boolean })
					.efSearchUnsupported,
			).toBe(true);
		} finally {
			warn.mockRestore();
			await store.close();
		}
	});

	it("findSimilarEntries falls back per call without latching on transient set_config errors", async () => {
		const store = new PostgresKnowledgeDB(uri, 1);
		store.id = "fallback-store";
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		try {
			await store.initialize();
			await store.setEmbeddingMetadata("ef-fallback-test", 8);
			await store.insertEntry(
				makeEntry("fallback-1", { embedding: [1, 0, 0, 0, 0, 0, 0, 0] }),
			);
			const transient = Object.assign(
				new Error("canceling statement due to statement timeout"),
				{ code: "57014" },
			);
			const calls = stubAnnTransaction(store, { setConfig: transient });

			const q = [1, 0, 0, 0, 0, 0, 0, 0];
			for (let i = 0; i < 2; i++) {
				const res = await store.findSimilarEntries(q, 5, 0.5, ["active"]);
				expect(res.map((r) => r.entry.id)).toEqual(["fallback-1"]);
			}
			// Not latched: every call still tries set_config first.
			expect(calls.begin).toBe(2);
			expect(warn).toHaveBeenCalledTimes(2);
			for (const call of warn.mock.calls) {
				expect(String(call[0])).toContain('Store "fallback-store"');
			}
			expect(
				(store as unknown as { efSearchUnsupported: boolean })
					.efSearchUnsupported,
			).toBe(false);
		} finally {
			warn.mockRestore();
			await store.close();
		}
	});

	it("findSimilarEntries rethrows errors from the ANN query itself", async () => {
		const store = new PostgresKnowledgeDB(uri, 1);
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		try {
			await store.initialize();
			await store.setEmbeddingMetadata("ef-fallback-test", 8);
			const boom = Object.assign(new Error("boom"), { code: "XX000" });
			const calls = stubAnnTransaction(store, { query: boom });

			await expect(
				store.findSimilarEntries([1, 0, 0, 0, 0, 0, 0, 0], 5, 0.5, ["active"]),
			).rejects.toBe(boom);
			expect(calls.begin).toBe(1);
			expect(warn).not.toHaveBeenCalled();
			expect(
				(store as unknown as { efSearchUnsupported: boolean })
					.efSearchUnsupported,
			).toBe(false);
		} finally {
			warn.mockRestore();
			await store.close();
		}
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

	// ── Reinitialize ──

	// ── Embedding model change (issue #103 and H2) ──

	/** Build a vector-length-n test vector with a 1 at `hot`. */
	const unitVec = (n: number, hot: number) =>
		Array.from({ length: n }, (_, i) => (i === hot ? 1 : 0));

	const vecColumnType = async () =>
		String(
			(
				await truncSql`
					SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
					WHERE attrelid = 'knowledge_entry'::regclass AND attname = 'embedding_vec'
				`
			)[0].t,
		);

	it("issue #103: after a model change every stored vector matches the new model", async () => {
		const pg = db as PostgresKnowledgeDB;
		const OLD = unitVec(4, 0);
		const NEW = unitVec(6, 2); // different dimension on purpose
		const statuses = [
			"active",
			"conflicted",
			"superseded",
			"archived",
			"tombstoned",
		] as const;
		await pg.setEmbeddingMetadata("old-model", OLD.length);
		for (const status of statuses) {
			await pg.insertEntry(
				makeEntry(status, {
					status,
					supersededBy: status === "superseded" ? "active" : null,
					embedding: OLD,
				}),
			);
		}

		const savedModel = config.embedding.model;
		config.embedding.model = "new-model";
		try {
			const activation = new ActivationEngine(pg);
			spyOn(activation.embeddings, "embedBatch").mockImplementation(
				async (texts: string[]) => texts.map(() => NEW),
			);
			expect(await activation.checkAndReEmbed()).toBe(true);
		} finally {
			config.embedding.model = savedModel;
		}

		for (const status of statuses) {
			const entry = await pg.getEntryWithEmbedding(status);
			if (status === "tombstoned") {
				expect(entry?.embedding).toBeUndefined();
			} else {
				expect(entry?.embedding).toEqual(NEW);
			}
		}

		// Every non-tombstoned row has a vector_column entry of the new size and
		// the same value as its BYTEA, and no row is left on the old model.
		const stale = await truncSql`
			SELECT COUNT(*)::int AS n FROM knowledge_entry
			WHERE status <> 'tombstoned'
			  AND (embedding_vec IS NULL OR octet_length(embedding) <> ${NEW.length * 4})
		`;
		expect(Number((stale[0] as { n: number }).n)).toBe(0);
		const tomb = await truncSql`
			SELECT embedding IS NULL AND embedding_vec IS NULL AS cleared
			FROM knowledge_entry WHERE id = 'tombstoned'
		`;
		expect((tomb[0] as { cleared: boolean }).cleared).toBe(true);

		expect(await pg.getEmbeddingMetadata()).toMatchObject({
			model: "new-model",
			dimensions: NEW.length,
		});
		expect(await pg.isVectorSearchReady()).toBe(true);
		expect(await vecColumnType()).toBe(`vector(${NEW.length})`);
		const hits = await pg.findSimilarEntries(NEW, 5, 0.5);
		expect(hits.map((h) => h.entry.id).sort()).toEqual([
			"active",
			"conflicted",
		]);
	});

	it("dimension change 3072 -> 1536: ANN is ready without restart and survives two restarts", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);
		await pg.insertEntry(
			makeEntry("big-active", { embedding: unitVec(3072, 0) }),
		);
		await pg.insertEntry(
			makeEntry("big-archived", {
				status: "archived",
				embedding: unitVec(3072, 0),
			}),
		);

		const savedModel = config.embedding.model;
		config.embedding.model = "text-embedding-3-small";
		try {
			const activation = new ActivationEngine(pg);
			spyOn(activation.embeddings, "embedBatch").mockImplementation(
				async (texts: string[]) => texts.map(() => unitVec(1536, 5)),
			);
			expect(await activation.checkAndReEmbed()).toBe(true);
		} finally {
			config.embedding.model = savedModel;
		}

		expect(await vecColumnType()).toBe("vector(1536)");
		expect(await pg.isVectorSearchReady()).toBe(true);
		const q = unitVec(1536, 5);
		expect(
			(await pg.findSimilarEntries(q, 5, 0.5)).map((h) => h.entry.id),
		).toEqual(["big-active"]);

		// Two consecutive restarts: each must initialise without error and keep ANN.
		for (let i = 0; i < 2; i++) {
			await db.close();
			db = new PostgresKnowledgeDB(uri);
			await (db as PostgresKnowledgeDB).initialize();
			expect(await (db as PostgresKnowledgeDB).isVectorSearchReady()).toBe(
				true,
			);
		}
		expect(await vecColumnType()).toBe("vector(1536)");
	});

	it("a new-size vector can be written before the metadata is stamped (H2)", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);
		await pg.insertEntry(makeEntry("h2-old", { embedding: unitVec(3072, 0) }));

		await pg.beginEmbeddingModelChange(1536);
		// Metadata still says 3072 here; this write used to fail against vector(3072).
		await pg.updateEntry("h2-old", { embedding: unitVec(1536, 1) });
		await pg.insertEntry(makeEntry("h2-new", { embedding: unitVec(1536, 2) }));

		expect(await pg.isVectorSearchReady()).toBe(false);
		await pg.setEmbeddingMetadata("text-embedding-3-small", 1536);
		expect(await pg.isVectorSearchReady()).toBe(true);
		expect((await pg.findSimilarEntries(unitVec(1536, 1), 5, 0.5)).length).toBe(
			1,
		);
	});

	it("interrupted re-embed keeps the old metadata, leaves ANN off, and resumes on the next run", async () => {
		const pg = db as PostgresKnowledgeDB;
		const OLD = unitVec(4, 0);
		const NEW = unitVec(6, 3);
		await pg.setEmbeddingMetadata("old-model", OLD.length);
		for (let i = 0; i < 150; i++) {
			await pg.insertEntry(
				makeEntry(`resume-${String(i).padStart(3, "0")}`, { embedding: OLD }),
			);
		}

		const savedModel = config.embedding.model;
		config.embedding.model = "new-model";
		try {
			const activation = new ActivationEngine(pg);
			let calls = 0;
			const spy = spyOn(activation.embeddings, "embedBatch").mockImplementation(
				async (texts: string[]) => {
					calls++;
					if (calls === 2) throw new Error("rate limit exceeded");
					return texts.map(() => NEW);
				},
			);

			await expect(activation.checkAndReEmbed()).rejects.toThrow(
				"rate limit exceeded",
			);
			expect((await pg.getEmbeddingMetadata())?.model).toBe("old-model");
			expect(await pg.isVectorSearchReady()).toBe(false);

			spy.mockImplementation(async (texts: string[]) => texts.map(() => NEW));
			expect(await activation.checkAndReEmbed()).toBe(true);
		} finally {
			config.embedding.model = savedModel;
		}

		expect((await pg.getEmbeddingMetadata())?.model).toBe("new-model");
		expect(await pg.isVectorSearchReady()).toBe(true);
		const stale = await truncSql`
			SELECT COUNT(*)::int AS n FROM knowledge_entry
			WHERE embedding_vec IS NULL OR octet_length(embedding) <> ${NEW.length * 4}
		`;
		expect(Number((stale[0] as { n: number }).n)).toBe(0);
		expect((await pg.findSimilarEntries(NEW, 500, 0.5)).length).toBe(150);
	});

	it("reinitialize followed by a new dimension restores ANN without restart (H2)", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);
		await pg.reinitialize();
		await pg.setEmbeddingMetadata("text-embedding-3-small", 1536);

		expect(await pg.isVectorSearchReady()).toBe(true);
		expect(await vecColumnType()).toBe("vector(1536)");
		await pg.insertEntry(
			makeEntry("after-reinit", { embedding: unitVec(1536, 4) }),
		);
		expect(
			(await pg.findSimilarEntries(unitVec(1536, 4), 5, 0.5)).map(
				(h) => h.entry.id,
			),
		).toEqual(["after-reinit"]);
	});

	it("startup does not cast leftover old-size BYTEA into the vector column", async () => {
		let pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("text-embedding-3-large", 3072);
		await pg.insertEntry(makeEntry("ok-row", { embedding: unitVec(3072, 0) }));
		await pg.insertEntry(makeEntry("leftover", { status: "superseded" }));
		// A 1536-float BYTEA left behind by an interrupted re-embed.
		const leftover = Buffer.from(new Float32Array(unitVec(1536, 0)).buffer);
		await truncSql`UPDATE knowledge_entry SET embedding = ${leftover} WHERE id = 'leftover'`;

		await db.close();
		db = new PostgresKnowledgeDB(uri);
		pg = db as PostgresKnowledgeDB;
		await expect(pg.initialize()).resolves.toBeUndefined();

		const left = await truncSql`
			SELECT embedding IS NULL AS cleared, embedding_vec IS NULL AS no_vec
			FROM knowledge_entry WHERE id = 'leftover'
		`;
		expect(left[0]).toEqual({ cleared: true, no_vec: true });
		expect(await pg.isVectorSearchReady()).toBe(true);
		expect(
			(await pg.findSimilarEntries(unitVec(3072, 0), 5, 0.5)).map(
				(h) => h.entry.id,
			),
		).toEqual(["ok-row"]);
	});

	it("reviving an archived entry with a stale vector re-embeds it so ANN finds it", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("previous-model", 4);
		await pg.insertEntry(
			makeEntry("revive-me", { status: "archived", embedding: unitVec(4, 0) }),
		);

		const NEW = unitVec(4, 3);
		const embedder = {
			embed: async () => NEW,
		} as unknown as EmbeddingClient;
		const service = new KnowledgeService(pg, embedder);
		const savedModel = config.embedding.model;
		config.embedding.model = "current-model";
		try {
			await service.updateEntry("revive-me", { status: "active" });
		} finally {
			config.embedding.model = savedModel;
		}

		expect((await pg.getEntryWithEmbedding("revive-me"))?.embedding).toEqual(
			NEW,
		);
		const hits = await pg.findSimilarEntries(NEW, 5, 0.5);
		expect(hits.map((h) => h.entry.id)).toEqual(["revive-me"]);
	});

	it("re-embedding keeps updated_at of archived entries", async () => {
		const pg = db as PostgresKnowledgeDB;
		const OLD = unitVec(4, 0);
		const NEW = unitVec(4, 1);
		const ARCHIVED_AT = 1_000;
		await pg.setEmbeddingMetadata("old-model", OLD.length);
		await pg.insertEntry(
			makeEntry("archived-old", {
				status: "archived",
				updatedAt: ARCHIVED_AT,
				embedding: OLD,
			}),
		);

		const savedModel = config.embedding.model;
		config.embedding.model = "new-model";
		try {
			const activation = new ActivationEngine(pg);
			spyOn(activation.embeddings, "embedBatch").mockImplementation(
				async (texts: string[]) => texts.map(() => NEW),
			);
			expect(await activation.checkAndReEmbed()).toBe(true);
		} finally {
			config.embedding.model = savedModel;
		}

		const entry = await pg.getEntryWithEmbedding("archived-old");
		expect(entry?.embedding).toEqual(NEW);
		expect(entry?.updatedAt).toBe(ARCHIVED_AT);
		const [row] = await truncSql`
			SELECT embedding_vec IS NOT NULL AS has_vec FROM knowledge_entry
			WHERE id = 'archived-old'
		`;
		expect(row.has_vec).toBe(true);
	});

	it("merge resolution clears embedding_vec together with the BYTEA embedding", async () => {
		const pg = db as PostgresKnowledgeDB;
		await pg.setEmbeddingMetadata("m", 4);
		await pg.insertEntry(makeEntry("merge-new", { embedding: unitVec(4, 0) }));
		await pg.insertEntry(makeEntry("merge-old", { embedding: unitVec(4, 1) }));
		const [before] = await truncSql`
			SELECT embedding_vec IS NOT NULL AS has_vec FROM knowledge_entry
			WHERE id = 'merge-new'
		`;
		expect(before.has_vec).toBe(true);

		await pg.applyContradictionResolution("merge", "merge-new", "merge-old", {
			content: "Merged content",
			type: "fact",
			topics: ["merged"],
			confidence: 0.9,
		});

		const [after] = await truncSql`
			SELECT embedding IS NULL AND embedding_vec IS NULL AS cleared
			FROM knowledge_entry WHERE id = 'merge-new'
		`;
		expect(after.cleared).toBe(true);
	});
});

// ── Migration v14: staging tables leave Postgres ─────────────────────────────

describe.skipIf(!PG_URI)("PostgreSQL migration v14 (staging tables)", () => {
	const uri = PG_URI as string;
	// A private schema per run: the migration works on current_schema(), so these
	// tables never touch the schema the other suites use.
	const schema = `ks_v14_${crypto.randomUUID().replaceAll("-", "")}`;
	let admin: postgres.Sql;
	let sql: postgres.Sql;

	beforeAll(async () => {
		admin = postgres(uri, { max: 1 });
		await admin.unsafe(`CREATE SCHEMA ${schema}`);
		sql = postgres(uri, { max: 1, connection: { search_path: schema } });
	});

	beforeEach(async () => {
		await admin.unsafe(`DROP SCHEMA ${schema} CASCADE`);
		await admin.unsafe(`CREATE SCHEMA ${schema}`);
	});

	afterAll(async () => {
		await sql.end();
		await admin.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		await admin.end();
	});

	const createStagingTable = async (name: string, rows: number) => {
		await sql.unsafe(`CREATE TABLE ${name} (id TEXT PRIMARY KEY)`);
		await sql.unsafe(
			`INSERT INTO ${name} (id) SELECT 'row-' || g FROM generate_series(1, $1::int) g`,
			[rows],
		);
	};
	const rowCount = async (name: string) => {
		const [row] = await sql.unsafe<{ n: number }[]>(
			`SELECT COUNT(*)::int AS n FROM ${name}`,
		);
		return row.n;
	};
	const tableNames = async () => {
		const rows = await admin<{ table_name: string }[]>`
			SELECT table_name FROM information_schema.tables
			WHERE table_schema = ${schema}
			ORDER BY table_name
		`;
		return rows.map((r) => r.table_name);
	};
	const runV14 = () =>
		sql.begin(async (tx) => {
			await (
				PG_MIGRATIONS.find(
					(m) => m.version === 14,
				) as (typeof PG_MIGRATIONS)[number]
			).up(tx as never);
		});

	it("drops empty staging tables", async () => {
		await createStagingTable("pending_episodes", 0);
		await createStagingTable("consolidated_episode", 0);
		await createStagingTable("consolidation_state", 0);

		await runV14();

		expect(await tableNames()).toEqual([]);
	});

	it("renames staging tables that hold rows to legacy_* and keeps the rows", async () => {
		await createStagingTable("pending_episodes", 2);
		await createStagingTable("consolidated_episode", 0);
		await createStagingTable("consolidation_state", 1);

		await runV14();

		expect(await tableNames()).toEqual([
			"legacy_consolidation_state",
			"legacy_pending_episodes",
		]);
		expect(await rowCount("legacy_pending_episodes")).toBe(2);
		expect(await rowCount("legacy_consolidation_state")).toBe(1);
	});

	it("leaves the tables alone when a Postgres state DB in the same schema owns them", async () => {
		await sql.unsafe(
			"CREATE TABLE state_schema_version (version INTEGER NOT NULL)",
		);
		await createStagingTable("pending_episodes", 1);

		await runV14();

		expect(await tableNames()).toEqual([
			"pending_episodes",
			"state_schema_version",
		]);
		expect(await rowCount("pending_episodes")).toBe(1);
	});
});

// ── Migrations v16/v17: pgvector setup from a v15 store ──────────────────────

describe.skipIf(!PG_URI)("PostgreSQL migrations v16/v17", () => {
	const uri = PG_URI as string;
	let admin: ReturnType<typeof postgres>;

	/**
	 * Turn the test DB into a v15 store with an embedding_metadata row (dims 8)
	 * and no embedding_vec column, so v16 and v17 are the pending migrations.
	 */
	async function rewindToV15(): Promise<void> {
		// Earlier tests may have dropped pgvector or left injected objects behind;
		// clear them before seeding so initialize() starts from a clean v15 shape.
		await admin`CREATE EXTENSION IF NOT EXISTS vector`;
		await admin`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
		await admin`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
		await admin.unsafe(
			"ALTER TABLE IF EXISTS knowledge_entry DROP COLUMN IF EXISTS embedding_vec",
		);
		// Create the current schema if the test DB is fresh.
		const seed = new PostgresKnowledgeDB(uri);
		await seed.initialize();
		await seed.close();
		await admin`TRUNCATE knowledge_entry, knowledge_cluster, embedding_metadata CASCADE`;
		await admin.unsafe(
			"ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS embedding_vec",
		);
		await admin`INSERT INTO embedding_metadata (id, model, dimensions, recorded_at) VALUES (1, 'test-model', 8, ${Date.now()}) ON CONFLICT (id) DO UPDATE SET dimensions = 8`;
		await admin`DELETE FROM schema_version`;
		await admin`INSERT INTO schema_version (version, applied_at) VALUES (15, ${Date.now()})`;
	}

	async function storeVersion(): Promise<number> {
		const rows = await admin`SELECT MAX(version) AS v FROM schema_version`;
		return Number((rows[0] as { v: number | string }).v);
	}

	beforeAll(() => {
		admin = postgres(uri, { max: 2 });
	});

	afterAll(async () => {
		// Leave the shared test DB at the current schema with pgvector available.
		await admin`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
		await admin.unsafe(
			"ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS embedding_vec",
		);
		await admin`CREATE EXTENSION IF NOT EXISTS vector`;
		await admin`TRUNCATE knowledge_entry, knowledge_cluster, embedding_metadata CASCADE`;
		await admin`DELETE FROM schema_version`;
		await admin`INSERT INTO schema_version (version, applied_at) VALUES (${SCHEMA_VERSION}, ${Date.now()})`;
		await admin.end();
	});

	it("a failing optional ANN index build does not abort the migration transaction", async () => {
		await rewindToV15();
		// Make the optional ANN DDL fail: DROP INDEX IF EXISTS errors on a relation
		// that is not an index. The column is recreated by the dimension check, so an
		// integer column no longer reaches the index build.
		await admin`CREATE VIEW idx_entry_embedding_vec_hnsw AS SELECT 1 AS x`;

		const store = new PostgresKnowledgeDB(uri);
		try {
			// Must resolve: the optional index is skipped with a warning, not fatal.
			await store.initialize();
			expect(store.isVectorSearchReady()).toBe(false);
		} finally {
			await store.close();
			await admin`DROP VIEW IF EXISTS idx_entry_embedding_vec_hnsw`;
		}
		expect(await storeVersion()).toBe(SCHEMA_VERSION);
	});

	it("creates the pgvector extension before adding embedding_vec", async () => {
		await rewindToV15();
		// Simulate a server where the extension is not installed yet.
		await admin`DROP EXTENSION IF EXISTS vector CASCADE`;

		const store = new PostgresKnowledgeDB(uri);
		try {
			await store.initialize();
			expect(store.isVectorSearchReady()).toBe(true);
		} finally {
			await store.close();
		}
		expect(await storeVersion()).toBe(SCHEMA_VERSION);
	});

	it("serialises migrations when several processes start against the same v15 store", async () => {
		await rewindToV15();
		const stores = [new PostgresKnowledgeDB(uri), new PostgresKnowledgeDB(uri)];
		try {
			// Without a cross-process lock both processes see no embedding_vec column,
			// and the second ADD COLUMN fails with "column already exists".
			await Promise.all(stores.map((s) => s.initialize()));
		} finally {
			await Promise.all(stores.map((s) => s.close()));
		}
		expect(await storeVersion()).toBe(SCHEMA_VERSION);
	});
});

describe.skipIf(!PG_URI)(
	"PostgreSQL migration v18 for a non-owner role",
	() => {
		const uri = PG_URI as string;
		const role = "ks_store_nonowner";
		let admin: ReturnType<typeof postgres>;

		beforeAll(() => {
			admin = postgres(uri, { max: 1 });
		});

		/** Create the non-owner login role (once) and grant it the app's DML. */
		async function grantNonOwner(): Promise<void> {
			await admin.unsafe(`
			DO $$ BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN
					CREATE ROLE ${role} LOGIN PASSWORD '${NONOWNER_PW}';
				END IF;
			END $$;
			GRANT USAGE, CREATE ON SCHEMA public TO ${role};
			GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${role};
		`);
		}

		function nonOwnerUrl(): string {
			const url = new URL(uri);
			url.username = role;
			url.password = NONOWNER_PW;
			return url.toString();
		}

		/** Put the store at `version` with knowledge_entry.injection_count absent. */
		async function rewindWithoutInjectionCount(version: number): Promise<void> {
			await admin.unsafe(`
			ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS injection_count;
			DELETE FROM schema_version;
			INSERT INTO schema_version (version, applied_at) VALUES (${version}, ${Date.now()});
		`);
		}

		/** Owner-side: put the column back so later tests see the current schema. */
		async function restoreInjectionCount(): Promise<void> {
			await admin.unsafe(
				"ALTER TABLE knowledge_entry ADD COLUMN IF NOT EXISTS injection_count INTEGER NOT NULL DEFAULT 0",
			);
		}

		afterAll(async () => {
			await restoreInjectionCount();
			await admin`DELETE FROM schema_version`;
			await admin`INSERT INTO schema_version (version, applied_at) VALUES (${SCHEMA_VERSION}, ${Date.now()})`;
			await admin.unsafe(`
			DROP INDEX IF EXISTS idx_entry_strength;
			DROP INDEX IF EXISTS idx_entry_accessed;
		`);
			await admin.unsafe(`
			DROP OWNED BY ${role};
			DROP ROLE IF EXISTS ${role};
		`);
			await admin.end();
		});

		it("a v17 store owned by another role upgrades to v18 with a warning", async () => {
			// Owner-side setup: a v17 schema that still has the two indexes v18 drops.
			const seed = new PostgresKnowledgeDB(uri);
			await seed.initialize();
			await seed.close();
			await admin.unsafe(`
			CREATE INDEX IF NOT EXISTS idx_entry_strength ON knowledge_entry(strength);
			CREATE INDEX IF NOT EXISTS idx_entry_accessed ON knowledge_entry(last_accessed_at);
			DELETE FROM schema_version;
			INSERT INTO schema_version (version, applied_at) VALUES (17, ${Date.now()});
		`);
			await grantNonOwner();
			const url = nonOwnerUrl();
			const warn = spyOn(logger, "warn");
			const store = new PostgresKnowledgeDB(url);
			try {
				// v18 must not abort the store: the owner-only DDL is skipped with a warning.
				await store.initialize();
				expect(warn.mock.calls.some((c) => String(c[0]).includes("v18"))).toBe(
					true,
				);
			} finally {
				warn.mockRestore();
				await store.close();
			}
			const rows = await admin`SELECT MAX(version) AS v FROM schema_version`;
			expect(Number((rows[0] as { v: number | string }).v)).toBe(
				SCHEMA_VERSION,
			);
		});

		it("a v18 store without injection_count stops with the owner-only fix named", async () => {
			await grantNonOwner();
			await rewindWithoutInjectionCount(18);
			const store = new PostgresKnowledgeDB(nonOwnerUrl());
			let error: Error | undefined;
			try {
				await store.initialize().catch((err: unknown) => {
					error = err as Error;
				});
			} finally {
				await store.close();
				await restoreInjectionCount();
			}
			// v19 cannot be skipped: the write path increments injection_count.
			expect(error?.message).toContain("Owner-only fix");
			expect(error?.message).toContain(
				"ALTER TABLE knowledge_entry ADD COLUMN injection_count",
			);
			const rows = await admin`SELECT MAX(version) AS v FROM schema_version`;
			expect(Number((rows[0] as { v: number | string }).v)).toBe(18);
		});

		it("a non-owner opens the store once the owner runs the named fix", async () => {
			await grantNonOwner();
			await rewindWithoutInjectionCount(18);
			// The owner-only fix from the error message, run as the table owner.
			await admin.unsafe(
				"ALTER TABLE knowledge_entry ADD COLUMN injection_count INTEGER NOT NULL DEFAULT 0",
			);
			const store = new PostgresKnowledgeDB(nonOwnerUrl());
			try {
				await store.initialize();
			} finally {
				await store.close();
			}
			const rows = await admin`SELECT MAX(version) AS v FROM schema_version`;
			expect(Number((rows[0] as { v: number | string }).v)).toBe(
				SCHEMA_VERSION,
			);
		});
	},
);

// ── PostgresServerStateDB: schema upgrade, dead-lettering, user-scoped reset ──

describe.skipIf(!PG_URI)("PostgresServerStateDB", () => {
	const uri = PG_URI as string;
	let admin: ReturnType<typeof postgres>;

	/** Make the state tables look like schema v1 (no attempt columns), with one row. */
	async function rewindStateToV1(rows: Array<{ id: string; userId: string }>) {
		await admin`CREATE TABLE IF NOT EXISTS state_schema_version (version INTEGER NOT NULL UNIQUE, applied_at BIGINT NOT NULL)`;
		await admin`DELETE FROM state_schema_version`;
		await admin`INSERT INTO state_schema_version (version, applied_at) VALUES (1, 0)`;
		await admin.unsafe("DROP TABLE IF EXISTS pending_episode_attempts");
		await admin`DELETE FROM pending_episodes`;
		for (const row of rows) {
			await admin`
				INSERT INTO pending_episodes
				(id, user_id, source, session_id, start_message_id, end_message_id,
				 content, content_type, max_message_time, uploaded_at)
				VALUES (${row.id}, ${row.userId}, 'opencode', ${`s-${row.id}`}, 'a', 'b',
				        'content', 'messages', 1, 1)
			`;
		}
	}

	beforeAll(async () => {
		admin = postgres(uri, { max: 1 });
		// Create the state schema once so the tables exist before rewinding them.
		const seed = new PostgresServerStateDB(uri);
		await seed.initialize();
		await seed.close();
	});

	afterAll(async () => {
		await admin`DELETE FROM pending_episodes`;
		await admin`DELETE FROM consolidated_episode`;
		await admin.end();
	});

	it("upgrades a v1 state schema and dead-letters rows after the attempt limit", async () => {
		await rewindStateToV1([{ id: "legacy-1", userId: "alice" }]);

		const state = new PostgresServerStateDB(uri);
		try {
			await state.initialize();
			const [row] = await state.getPendingEpisodes(0);
			expect(row.id).toBe("legacy-1");
			expect(row.failedAttempts).toBe(0);

			for (let i = 0; i < PENDING_EPISODE_MAX_ATTEMPTS; i++) {
				await state.recordPendingEpisodeFailures(["legacy-1"], "boom");
			}
			expect(await state.getPendingEpisodes(0)).toEqual([]);
			expect(await state.countPendingSessions()).toBe(0);
		} finally {
			await state.close();
		}
	});

	it("reinitialize(userId) deletes only that user's pending episodes", async () => {
		await rewindStateToV1([
			{ id: "alice-1", userId: "alice" },
			{ id: "bob-1", userId: "bob" },
		]);
		const state = new PostgresServerStateDB(uri);
		try {
			await state.initialize();
			await state.reinitialize("alice");
			const remaining = await state.getPendingEpisodes(0);
			expect(remaining.map((r) => r.id)).toEqual(["bob-1"]);
		} finally {
			await state.close();
		}
	});
	it("upgrades a v1 state DB owned by another role without ALTER TABLE", async () => {
		await rewindStateToV1([{ id: "owned-1", userId: "alice" }]);
		const role = "ks_state_nonowner";
		await admin.unsafe(`
			DO $$ BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN
					CREATE ROLE ${role} LOGIN PASSWORD '${NONOWNER_PW}';
				END IF;
			END $$;
			GRANT USAGE, CREATE ON SCHEMA public TO ${role};
			GRANT SELECT, INSERT, UPDATE, DELETE ON pending_episodes, consolidated_episode, consolidation_state, state_schema_version TO ${role};
		`);
		const url = new URL(uri);
		url.username = role;
		url.password = NONOWNER_PW;
		const state = new PostgresServerStateDB(url.toString());
		try {
			// Must not run ALTER TABLE on pending_episodes (owned by another role).
			await state.initialize();
			const [row] = await state.getPendingEpisodes(0);
			expect(row.id).toBe("owned-1");
			await state.recordPendingEpisodeFailures(["owned-1"], "boom");
			expect((await state.getPendingEpisodes(0))[0].failedAttempts).toBe(1);
		} finally {
			await state.close();
			await admin.unsafe(`
				DROP TABLE IF EXISTS pending_episode_attempts;
				DROP OWNED BY ${role};
				DROP ROLE IF EXISTS ${role};
			`);
		}
	});
});

// ── Schema version guard: refuse newer stores, never drop, name missing versions ──

describe.skipIf(!PG_URI)("schema version guard", () => {
	const uri = PG_URI as string;
	let admin: ReturnType<typeof postgres>;

	async function setVersion(version: number): Promise<void> {
		await admin`DELETE FROM schema_version`;
		await admin`INSERT INTO schema_version (version, applied_at) VALUES (${version}, ${Date.now()})`;
	}

	async function tableExists(name: string): Promise<boolean> {
		const rows = await admin`SELECT to_regclass(${name}) AS t`;
		return rows[0].t !== null;
	}

	beforeAll(async () => {
		admin = postgres(uri, { max: 1 });
		const seed = new PostgresKnowledgeDB(uri);
		await seed.initialize();
		await seed.close();
	});

	afterAll(async () => {
		await setVersion(SCHEMA_VERSION);
		await admin.end();
	});

	it("refuses a knowledge store newer than this build and keeps its tables", async () => {
		await setVersion(SCHEMA_VERSION + 1);
		const store = new PostgresKnowledgeDB(uri);
		try {
			await expect(store.initialize()).rejects.toThrow(/newer than this build/);
		} finally {
			await store.close();
		}
		expect(await tableExists("knowledge_entry")).toBe(true);
		await setVersion(SCHEMA_VERSION);
	});

	it("refuses a knowledge store with a missing migration, naming the version", async () => {
		const missing = PG_MIGRATIONS[0].version - 1;
		await setVersion(missing - 1);
		const store = new PostgresKnowledgeDB(uri);
		try {
			await expect(store.initialize()).rejects.toThrow(
				new RegExp(`no migration for v${missing}`),
			);
		} finally {
			await store.close();
		}
		expect(await tableExists("knowledge_entry")).toBe(true);
		await setVersion(SCHEMA_VERSION);
	});

	it("refuses a Postgres state DB newer than this build and keeps its tables", async () => {
		await admin`DELETE FROM state_schema_version`;
		await admin`INSERT INTO state_schema_version (version, applied_at) VALUES (99, 0)`;
		const state = new PostgresServerStateDB(uri);
		try {
			await expect(state.initialize()).rejects.toThrow(/newer than this build/);
		} finally {
			await state.close();
			await admin`DELETE FROM state_schema_version WHERE version = 99`;
		}
		expect(await tableExists("pending_episodes")).toBe(true);
	});

	it("insertPendingEpisode stores the same episode range once (Postgres)", async () => {
		const state = new PostgresServerStateDB(uri);
		const base = {
			userId: "alice",
			source: "opencode",
			sessionId: "dup-session",
			startMessageId: "m0",
			endMessageId: "m9",
			sessionTitle: "t",
			projectName: "p",
			directory: "/tmp",
			content: "c",
			contentType: "messages" as const,
			timeCreated: 1,
			maxMessageTime: 2,
			approxTokens: 3,
			uploadedAt: 4,
		};
		try {
			await admin`DELETE FROM pending_episodes WHERE session_id = 'dup-session'`;
			await state.insertPendingEpisode({ ...base, id: "dup-a" });
			await state.insertPendingEpisode({ ...base, id: "dup-b" });
			const rows = (await state.getPendingEpisodes(0)).filter(
				(r) => r.sessionId === "dup-session",
			);
			expect(rows.map((r) => r.id)).toEqual(["dup-a"]);
		} finally {
			await admin`DELETE FROM pending_episodes WHERE session_id = 'dup-session'`;
			await state.close();
		}
	});
});
