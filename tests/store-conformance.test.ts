/**
 * Conformance suite for IKnowledgeStore: one statement of the store contract,
 * run against every backend. SQLite always runs; Postgres runs when PG_TEST_URI
 * is set (see tests/pg-integration.test.ts for the container recipe).
 *
 * A backend-specific test belongs in its own file. A case here is a behaviour
 * a caller can observe through the interface, so it must hold on both stores.
 */
import {
	afterAll,
	afterEach,
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
import { KnowledgeDB } from "../src/db/sqlite/index";
import { type KnowledgeEntry, clampKnowledgeType } from "../src/types";
import { makeEntry as fixtureEntry } from "./fixtures";

const PG_URI = process.env.PG_TEST_URI;
const DAY_MS = 86_400_000;
const DIMS = 3;

/** A test database: its first store, a second store on the same database, and cleanup. */
interface Opened {
	store: IKnowledgeStore;
	second(): Promise<IKnowledgeStore>;
	close(): Promise<void>;
}

interface Backend {
	name: string;
	open(): Promise<Opened>;
}

// ── SQLite ────────────────────────────────────────────────────────────────

const sqliteBackend: Backend = {
	name: "sqlite",
	async open() {
		const dir = mkdtempSync(join(tmpdir(), "ks-conformance-"));
		const path = join(dir, "knowledge.db");
		const stores: IKnowledgeStore[] = [];
		const make = async (): Promise<IKnowledgeStore> => {
			const store = new KnowledgeDB(path);
			stores.push(store);
			return store;
		};
		return {
			store: await make(),
			second: make,
			async close() {
				for (const s of stores) await s.close();
				rmSync(dir, { recursive: true, force: true });
			},
		};
	},
};

// ── Postgres ──────────────────────────────────────────────────────────────

let pgAdmin: postgres.Sql | null = null;

/** Empty the knowledge tables and drop the vector column, so each test starts from a fresh schema. */
async function resetPostgres(uri: string): Promise<void> {
	pgAdmin ??= postgres(uri, { max: 1 });
	await pgAdmin`TRUNCATE knowledge_entry, knowledge_relation, knowledge_cluster, embedding_metadata, schema_version CASCADE`;
	await pgAdmin`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
	await pgAdmin`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
	await pgAdmin`ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS embedding_vec`;
}

const postgresBackend: Backend | null = PG_URI
	? {
			name: "postgres",
			async open() {
				const uri = PG_URI;
				// Create the schema first (a fresh database has no tables to truncate), then reset it.
				const bootstrap = new PostgresKnowledgeDB(uri);
				await bootstrap.initialize();
				await bootstrap.close();
				await resetPostgres(uri);
				const stores: PostgresKnowledgeDB[] = [];
				const make = async (): Promise<IKnowledgeStore> => {
					const store = new PostgresKnowledgeDB(uri);
					await store.initialize();
					stores.push(store);
					return store;
				};
				return {
					store: await make(),
					second: make,
					async close() {
						for (const s of stores) await s.close();
					},
				};
			},
		}
	: null;

const BACKENDS = [sqliteBackend, ...(postgresBackend ? [postgresBackend] : [])];

afterAll(async () => {
	await pgAdmin?.end();
});

// ── Helpers ───────────────────────────────────────────────────────────────

/** A fixture entry; every field a test does not set takes the fixture default. */
function entry(overrides: Partial<KnowledgeEntry> & { id: string }) {
	return fixtureEntry(overrides);
}

function cosine(a: readonly number[], b: ArrayLike<number>): number {
	let dot = 0;
	let na = 0;
	let nb = 0;
	for (let i = 0; i < a.length; i++) {
		dot += a[i] * b[i];
		na += a[i] * a[i];
		nb += b[i] * b[i];
	}
	return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

/**
 * Assert the ids of the nearest neighbours of `query`. The brute-force path
 * (scanEmbeddings + cosine) is the contract every store must match. A store
 * that also offers ANN search is checked against the same expectation.
 */
async function expectNearest(
	store: IKnowledgeStore,
	query: number[],
	limit: number,
	threshold: number,
	expected: string[],
): Promise<void> {
	const brute = (await store.scanEmbeddings())
		.map(({ id, embedding }) => ({ id, sim: cosine(query, embedding) }))
		.filter((r) => r.sim >= threshold)
		.sort((a, b) => b.sim - a.sim || (a.id < b.id ? -1 : 1))
		.slice(0, limit)
		.map((r) => r.id);
	expect(brute).toEqual(expected);

	if (store.findSimilarEntries && store.isVectorSearchReady?.()) {
		const hits = await store.findSimilarEntries(query, limit, threshold);
		expect(hits.map((h) => h.entry.id)).toEqual(expected);
	}
}

async function ids(entries: Array<{ id: string }>): Promise<string[]> {
	return entries.map((e) => e.id).sort();
}

// ── Suite ─────────────────────────────────────────────────────────────────

for (const backend of BACKENDS) {
	describe(`IKnowledgeStore conformance: ${backend.name}`, () => {
		let opened: Opened;
		let store: IKnowledgeStore;

		beforeEach(async () => {
			opened = await backend.open();
			store = opened.store;
		});

		afterEach(async () => {
			await opened.close();
		});

		// ── Entry round-trip and CRUD ──

		it("round-trips every field through insert and read", async () => {
			await store.insertEntry(
				entry({
					id: "rt-1",
					type: "principle",
					content: "Prefer small commits",
					topics: ["git", "review"],
					confidence: 0.73,
					source: "opencode",
					status: "active",
					createdAt: 1_000,
					updatedAt: 2_000,
					lastAccessedAt: 3_000,
					accessCount: 4,
					injectionCount: 5,
					observationCount: 6,
					supersededBy: null,
					derivedFrom: ["s1", "s2"],
					isSynthesized: true,
					embedding: [0.5, -0.25, 0.125, 1],
				}),
			);

			const read = await store.getEntry("rt-1");
			expect(read).toMatchObject({
				id: "rt-1",
				type: "principle",
				content: "Prefer small commits",
				topics: ["git", "review"],
				confidence: 0.73,
				source: "opencode",
				status: "active",
				createdAt: 1_000,
				updatedAt: 2_000,
				lastAccessedAt: 3_000,
				accessCount: 4,
				injectionCount: 5,
				observationCount: 6,
				supersededBy: null,
				derivedFrom: ["s1", "s2"],
				isSynthesized: true,
			});
			expect(read?.embedding).toBeUndefined();

			const withVector = await store.getEntryWithEmbedding("rt-1");
			expect(withVector?.embedding).toEqual([0.5, -0.25, 0.125, 1]);
		});

		it("reads no vector through getEntry and undefined when none was stored", async () => {
			await store.insertEntry(entry({ id: "no-vec" }));
			expect((await store.getEntry("no-vec"))?.embedding).toBeUndefined();
			expect(
				(await store.getEntryWithEmbedding("no-vec"))?.embedding,
			).toBeUndefined();
			expect(await store.getEntry("missing")).toBeNull();
			expect(await store.getEntryWithEmbedding("missing")).toBeNull();
		});

		it("updateEntry changes only the given fields and bumps updated_at", async () => {
			await store.insertEntry(
				entry({ id: "upd", updatedAt: 1_000, confidence: 0.9 }),
			);
			const before = Date.now();
			await store.updateEntry("upd", {
				status: "archived",
				confidence: 0.3,
				isSynthesized: true,
			});
			const read = await store.getEntry("upd");
			expect(read?.status).toBe("archived");
			expect(read?.confidence).toBe(0.3);
			expect(read?.isSynthesized).toBe(true);
			expect(read?.content).toBe("Test content");
			expect(read?.updatedAt).toBeGreaterThanOrEqual(before);
		});

		it("deleteEntry removes the entry and its relations, and reports whether it existed", async () => {
			await store.insertEntry(entry({ id: "d-a" }));
			await store.insertEntry(entry({ id: "d-b" }));
			await store.insertRelation({
				id: "rel-d",
				sourceId: "d-a",
				targetId: "d-b",
				type: "supports",
				createdAt: 1,
			});

			expect(await store.deleteEntry("d-a")).toBe(true);
			expect(await store.getEntry("d-a")).toBeNull();
			expect(await store.getRelationsFor("d-b")).toEqual([]);
			expect(await store.deleteEntry("d-a")).toBe(false);
		});

		it("getEntries filters by status and type and returns newest-created first", async () => {
			await store.insertEntry(
				entry({ id: "g-a", createdAt: 1_000, status: "active", type: "fact" }),
			);
			await store.insertEntry(
				entry({
					id: "g-b",
					createdAt: 3_000,
					status: "active",
					type: "decision",
				}),
			);
			await store.insertEntry(
				entry({
					id: "g-c",
					createdAt: 2_000,
					status: "archived",
					type: "fact",
				}),
			);

			expect((await store.getEntries({})).map((e) => e.id)).toEqual([
				"g-b",
				"g-c",
				"g-a",
			]);
			expect(
				(await store.getEntries({ status: "active" })).map((e) => e.id),
			).toEqual(["g-b", "g-a"]);
			expect(
				(await store.getEntries({ type: "fact" })).map((e) => e.id),
			).toEqual(["g-c", "g-a"]);
		});

		it("getEntriesByStatus returns that status, most recently updated first", async () => {
			await store.insertEntry(
				entry({ id: "s-a", status: "conflicted", updatedAt: 1_000 }),
			);
			await store.insertEntry(
				entry({ id: "s-b", status: "conflicted", updatedAt: 3_000 }),
			);
			await store.insertEntry(entry({ id: "s-c", status: "active" }));
			expect(
				(await store.getEntriesByStatus("conflicted")).map((e) => e.id),
			).toEqual(["s-b", "s-a"]);
		});

		it("getActiveEntries returns only active entries", async () => {
			await store.insertEntry(entry({ id: "ac-1", status: "active" }));
			await store.insertEntry(entry({ id: "ac-2", status: "conflicted" }));
			await store.insertEntry(entry({ id: "ac-3", status: "archived" }));
			expect(await ids(await store.getActiveEntries())).toEqual(["ac-1"]);
		});

		it("getActiveAndConflictedEntries returns active and conflicted entries", async () => {
			await store.insertEntry(entry({ id: "ac-1", status: "active" }));
			await store.insertEntry(entry({ id: "ac-2", status: "conflicted" }));
			await store.insertEntry(entry({ id: "ac-3", status: "superseded" }));
			expect(await ids(await store.getActiveAndConflictedEntries())).toEqual([
				"ac-1",
				"ac-2",
			]);
		});

		it("getStats counts entries per status and in total", async () => {
			await store.insertEntry(entry({ id: "st-1", status: "active" }));
			await store.insertEntry(entry({ id: "st-2", status: "active" }));
			await store.insertEntry(entry({ id: "st-3", status: "archived" }));
			expect(await store.getStats()).toMatchObject({
				total: 3,
				active: 2,
				archived: 1,
			});
		});

		it("getEntriesByIds returns active and conflicted entries among the given ids", async () => {
			await store.insertEntry(entry({ id: "bi-1", status: "active" }));
			await store.insertEntry(entry({ id: "bi-2", status: "conflicted" }));
			await store.insertEntry(entry({ id: "bi-3", status: "archived" }));
			expect(
				await ids(await store.getEntriesByIds(["bi-1", "bi-2", "bi-3"])),
			).toEqual(["bi-1", "bi-2"]);
			expect(await store.getEntriesByIds([])).toEqual([]);
		});

		it("getEntriesMissingEmbeddings lists active and conflicted entries without a vector", async () => {
			await store.insertEntry(entry({ id: "mi-1", status: "active" }));
			await store.insertEntry(
				entry({ id: "mi-2", status: "active", embedding: [1, 0, 0, 0] }),
			);
			await store.insertEntry(entry({ id: "mi-3", status: "archived" }));
			expect(await ids(await store.getEntriesMissingEmbeddings())).toEqual([
				"mi-1",
			]);
		});

		it("getOneEntryWithEmbedding returns null with no vectors and an entry with one", async () => {
			await store.insertEntry(entry({ id: "one-a", status: "active" }));
			expect(await store.getOneEntryWithEmbedding()).toBeNull();
			await store.insertEntry(
				entry({ id: "one-b", status: "active", embedding: [0, 1, 0, 0] }),
			);
			expect((await store.getOneEntryWithEmbedding())?.id).toBe("one-b");
		});

		it("getActiveEntriesWithEmbeddings returns active and conflicted entries that have vectors", async () => {
			await store.insertEntry(
				entry({ id: "wv-1", status: "active", embedding: [1, 0, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "wv-2", status: "conflicted", embedding: [0, 1, 0, 0] }),
			);
			await store.insertEntry(entry({ id: "wv-3", status: "active" }));
			await store.insertEntry(
				entry({ id: "wv-4", status: "archived", embedding: [0, 0, 1, 0] }),
			);
			const withVectors = await store.getActiveEntriesWithEmbeddings();
			expect(await ids(withVectors)).toEqual(["wv-1", "wv-2"]);
			expect(withVectors.every((e) => e.embedding.length === 4)).toBe(true);
		});

		it("scanEmbeddings returns id and vector for active and conflicted entries only", async () => {
			await store.insertEntry(
				entry({ id: "sc-1", status: "active", embedding: [0.5, 0.25, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "sc-2", status: "archived", embedding: [1, 0, 0, 0] }),
			);
			const scanned = await store.scanEmbeddings();
			expect(scanned.map((s) => s.id)).toEqual(["sc-1"]);
			expect(Array.from(scanned[0].embedding)).toEqual([0.5, 0.25, 0, 0]);
		});

		it("getEntriesForReEmbed pages by id over non-tombstoned entries that have a vector", async () => {
			await store.insertEntry(
				entry({ id: "re-a", status: "active", embedding: [1, 0, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "re-b", status: "archived", embedding: [0, 1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "re-c", status: "tombstoned", embedding: [0, 0, 1, 0] }),
			);
			await store.insertEntry(entry({ id: "re-d", status: "active" }));
			await store.insertEntry(
				entry({ id: "re-e", status: "superseded", embedding: [0, 0, 0, 1] }),
			);

			const first = await store.getEntriesForReEmbed(null, 2);
			expect(first.map((e) => e.id)).toEqual(["re-a", "re-b"]);
			expect(first[0]).toMatchObject({
				type: "fact",
				content: "Test content",
				topics: ["test"],
			});
			const second = await store.getEntriesForReEmbed("re-b", 2);
			expect(second.map((e) => e.id)).toEqual(["re-e"]);
		});

		it("recordInjections counts each id once per call and moves last_accessed_at at most daily", async () => {
			const now = Date.now();
			await store.insertEntry(
				entry({
					id: "inj-x",
					accessCount: 7,
					lastAccessedAt: now - 2 * DAY_MS,
				}),
			);
			await store.insertEntry(entry({ id: "inj-y", lastAccessedAt: now }));

			await store.recordInjections(["inj-x", "inj-x", "inj-y"], now);
			const first = await store.getEntry("inj-x");
			expect(first?.injectionCount).toBe(1);
			expect(first?.lastAccessedAt).toBe(now);

			await store.recordInjections(["inj-x"], now + 60_000);
			const second = await store.getEntry("inj-x");
			expect(second?.injectionCount).toBe(2);
			expect(second?.lastAccessedAt).toBe(now);
			expect(second?.accessCount).toBe(7);
			expect((await store.getEntry("inj-y"))?.injectionCount).toBe(1);
		});

		it("reinforceObservation bumps observation_count and the timestamps", async () => {
			await store.insertEntry(entry({ id: "rf", observationCount: 2 }));
			const before = Date.now();
			await store.reinforceObservation("rf");
			const read = await store.getEntry("rf");
			expect(read?.observationCount).toBe(3);
			expect(read?.lastAccessedAt).toBeGreaterThanOrEqual(before);
			expect(read?.updatedAt).toBeGreaterThanOrEqual(before);
		});

		it("getEntriesWithOverlappingTopics returns active or conflicted entries with a vector that share a topic, minus excluded ids", async () => {
			await store.insertEntry(
				entry({
					id: "ov-a",
					topics: ["shared"],
					status: "active",
					embedding: [1, 0, 0, 0],
				}),
			);
			await store.insertEntry(
				entry({
					id: "ov-b",
					topics: ["shared", "other"],
					status: "conflicted",
					embedding: [0, 1, 0, 0],
				}),
			);
			await store.insertEntry(
				entry({ id: "ov-c", topics: ["shared"], status: "active" }),
			);
			await store.insertEntry(
				entry({
					id: "ov-d",
					topics: ["other"],
					status: "active",
					embedding: [0, 0, 1, 0],
				}),
			);
			await store.insertEntry(
				entry({
					id: "ov-ex",
					topics: ["shared"],
					status: "active",
					embedding: [0, 0, 0, 1],
				}),
			);
			await store.insertEntry(
				entry({
					id: "ov-s",
					topics: ["shared"],
					status: "superseded",
					embedding: [1, 1, 0, 0],
				}),
			);

			const overlapping = await store.getEntriesWithOverlappingTopics(
				["shared"],
				["ov-ex"],
			);
			expect(await ids(overlapping)).toEqual(["ov-a", "ov-b"]);
			expect(await store.getEntriesWithOverlappingTopics([], [])).toEqual([]);
		});

		// ── Vectors, prefixes, and model metadata ──

		it("setEmbedding replaces the vector and leaves updated_at alone, for any status", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({
					id: "se",
					status: "archived",
					updatedAt: 1_000,
					embedding: [1, 0, 0],
				}),
			);
			await store.setEmbedding("se", [0, 1, 0]);
			const read = await store.getEntryWithEmbedding("se");
			expect(read?.embedding).toEqual([0, 1, 0]);
			expect(read?.updatedAt).toBe(1_000);
		});

		it("findIdsByPrefix returns ids of any status with the prefix, ascending, up to the limit", async () => {
			const A1 = "ab12cd34-0000-4000-8000-000000000001";
			const A2 = "ab12cd34-0000-4000-8000-000000000002";
			const B1 = "ffff0000-0000-4000-8000-000000000003";
			await store.insertEntry(entry({ id: A1, status: "archived" }));
			await store.insertEntry(entry({ id: A2, status: "active" }));
			await store.insertEntry(entry({ id: B1 }));

			expect(await store.findIdsByPrefix("ab12cd34", 5)).toEqual([A1, A2]);
			expect(await store.findIdsByPrefix("ab12cd34", 1)).toEqual([A1]);
			expect(await store.findIdsByPrefix("ffff", 5)).toEqual([B1]);
			expect(await store.findIdsByPrefix("ab12cd35", 5)).toEqual([]);
		});

		it("findIdsByPrefix matches nothing for wildcards, upper case, or an empty prefix", async () => {
			await store.insertEntry(
				entry({ id: "ab12cd34-0000-4000-8000-000000000001" }),
			);
			for (const prefix of ["", "ab%", "ab_", "AB12"]) {
				expect(await store.findIdsByPrefix(prefix, 5)).toEqual([]);
			}
		});

		it("the vector search returns the same neighbours on every store, ordered and thresholded", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({ id: "vs-a", status: "active", embedding: [1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "vs-b", status: "active", embedding: [0.9, 0.1, 0] }),
			);
			await store.insertEntry(
				entry({ id: "vs-c", status: "active", embedding: [0, 1, 0] }),
			);
			await store.insertEntry(
				entry({ id: "vs-d", status: "active", embedding: [-1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "vs-e", status: "archived", embedding: [1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "vs-f", status: "conflicted", embedding: [0.5, 0.5, 0] }),
			);

			const query = [1, 0, 0];
			await expectNearest(store, query, 10, 0.5, ["vs-a", "vs-b", "vs-f"]);
			await expectNearest(store, query, 2, 0.5, ["vs-a", "vs-b"]);
			await expectNearest(store, query, 10, 0.999, ["vs-a"]);
		});

		it("updateEntry with a new vector is found by vector search", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({ id: "ue", status: "active", embedding: [1, 0, 0] }),
			);
			await store.updateEntry("ue", { embedding: [0, 0, 1] });
			await expectNearest(store, [0, 0, 1], 5, 0.5, ["ue"]);
			await expectNearest(store, [1, 0, 0], 5, 0.5, []);
		});

		it("clearTombstonedEmbeddings clears vectors of tombstoned entries only and counts them", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({ id: "tb-1", status: "tombstoned", embedding: [1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "tb-2", status: "tombstoned", embedding: [0, 1, 0] }),
			);
			await store.insertEntry(entry({ id: "tb-3", status: "tombstoned" }));
			await store.insertEntry(
				entry({ id: "tb-live", status: "active", embedding: [0, 0, 1] }),
			);

			expect(await store.clearTombstonedEmbeddings()).toBe(2);
			expect(
				(await store.getEntryWithEmbedding("tb-1"))?.embedding,
			).toBeUndefined();
			expect(
				(await store.getEntryWithEmbedding("tb-2"))?.embedding,
			).toBeUndefined();
			expect((await store.getEntryWithEmbedding("tb-live"))?.embedding).toEqual(
				[0, 0, 1],
			);
			await expectNearest(store, [1, 0, 0], 5, 0.5, []);
		});

		it("a model change reshapes the vectors and records the new metadata", async () => {
			await store.setEmbeddingMetadata("old-model", DIMS);
			await store.insertEntry(
				entry({ id: "mc", status: "active", embedding: [1, 0, 0] }),
			);

			await store.beginEmbeddingModelChange(2);
			await store.setEmbedding("mc", [0, 1]);
			await store.setEmbeddingMetadata("new-model", 2);

			await expectNearest(store, [0, 1], 5, 0.5, ["mc"]);
			expect(await store.getEmbeddingMetadata()).toMatchObject({
				model: "new-model",
				dimensions: 2,
			});
		});

		it("getEmbeddingMetadata is null until set, is overwritten by a second set, and reinitialize clears it", async () => {
			expect(await store.getEmbeddingMetadata()).toBeNull();
			await store.setEmbeddingMetadata("model-x", DIMS);
			expect(await store.getEmbeddingMetadata()).toMatchObject({
				model: "model-x",
				dimensions: DIMS,
			});
			await store.setEmbeddingMetadata("model-y", 2);
			expect(await store.getEmbeddingMetadata()).toMatchObject({
				model: "model-y",
				dimensions: 2,
			});
			await store.reinitialize();
			expect(await store.getEmbeddingMetadata()).toBeNull();
		});

		// ── Merge ──

		it("mergeEntry rewrites the content, unions the sources, and clears the vector when none is supplied", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({
					id: "mg",
					type: "fact",
					content: "old",
					topics: ["x"],
					confidence: 0.5,
					derivedFrom: ["s1"],
					observationCount: 1,
					embedding: [1, 0, 0],
				}),
			);
			const before = Date.now();

			const merged = await store.mergeEntry("mg", {
				content: "new",
				type: "principle",
				topics: ["y"],
				confidence: 0.9,
				additionalSources: ["s2", "s1"],
			});

			expect(merged).toBe(true);
			const read = await store.getEntry("mg");
			expect(read).toMatchObject({
				content: "new",
				type: "principle",
				topics: ["y"],
				confidence: 0.9,
				derivedFrom: ["s1", "s2"],
				observationCount: 2,
			});
			expect(read?.updatedAt).toBeGreaterThanOrEqual(before);
			expect(read?.lastAccessedAt).toBeGreaterThanOrEqual(before);
			expect(
				(await store.getEntryWithEmbedding("mg"))?.embedding,
			).toBeUndefined();
			await expectNearest(store, [1, 0, 0], 5, 0.5, []);
		});

		it("mergeEntry stores a supplied vector and it is found by vector search", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({ id: "mv", status: "active", embedding: [1, 0, 0] }),
			);
			await store.mergeEntry(
				"mv",
				{
					content: "merged",
					type: "fact",
					topics: ["t"],
					confidence: 0.6,
					additionalSources: [],
				},
				[0, 1, 0],
			);
			expect((await store.getEntryWithEmbedding("mv"))?.embedding).toEqual([
				0, 1, 0,
			]);
			await expectNearest(store, [0, 1, 0], 5, 0.5, ["mv"]);
		});

		it("mergeEntry clamps an unknown type the way clampKnowledgeType does", async () => {
			await store.insertEntry(entry({ id: "mt", type: "fact" }));
			await store.mergeEntry("mt", {
				content: "c",
				type: "Fact/Principle",
				topics: [],
				confidence: 0.5,
				additionalSources: [],
			});
			expect((await store.getEntry("mt"))?.type).toBe(
				clampKnowledgeType("Fact/Principle"),
			);
		});

		it("mergeEntry on a deleted target returns false and writes nothing", async () => {
			const before = await store.getStats();
			const merged = await store.mergeEntry("gone", {
				content: "c",
				type: "fact",
				topics: [],
				confidence: 0.5,
				additionalSources: [],
			});
			expect(merged).toBe(false);
			expect(await store.getEntry("gone")).toBeNull();
			expect(await store.getStats()).toEqual(before);
		});

		// ── Contradiction resolution ──

		/** Two active entries: `n` (new) and `e` (existing), both with vectors. */
		async function seedPair(): Promise<void> {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(
				entry({ id: "n", content: "new", embedding: [1, 0, 0] }),
			);
			await store.insertEntry(
				entry({ id: "e", content: "existing", embedding: [0, 1, 0] }),
			);
		}

		it("supersede_old marks the existing entry superseded and records the supersedes link", async () => {
			await seedPair();
			await store.applyContradictionResolution("supersede_old", "n", "e");

			expect((await store.getEntry("e"))?.status).toBe("superseded");
			expect((await store.getEntry("e"))?.supersededBy).toBe("n");
			expect((await store.getEntry("n"))?.status).toBe("active");
			const links = await store.getRelationsFor("n");
			expect(links).toEqual([
				expect.objectContaining({
					sourceId: "n",
					targetId: "e",
					type: "supersedes",
				}),
			]);
		});

		it("supersede_new marks the new entry superseded by the existing one", async () => {
			await seedPair();
			await store.applyContradictionResolution("supersede_new", "n", "e");

			expect((await store.getEntry("n"))?.status).toBe("superseded");
			expect((await store.getEntry("n"))?.supersededBy).toBe("e");
			expect((await store.getEntry("e"))?.status).toBe("active");
			expect(await store.getRelationsFor("e")).toEqual([
				expect.objectContaining({
					sourceId: "e",
					targetId: "n",
					type: "supersedes",
				}),
			]);
		});

		it("irresolvable marks both entries conflicted and links them as contradicting", async () => {
			await seedPair();
			await store.applyContradictionResolution("irresolvable", "n", "e");

			expect((await store.getEntry("n"))?.status).toBe("conflicted");
			expect((await store.getEntry("e"))?.status).toBe("conflicted");
			expect((await store.getContradictPairsForIds(["n"])).get("n")).toBe("e");
			expect((await store.getContradictPairsForIds(["e"])).get("e")).toBe("n");
		});

		it("merge with merged data rewrites the new entry, clears its vector, and supersedes the existing one", async () => {
			await seedPair();
			await store.applyContradictionResolution("merge", "n", "e", {
				content: "merged",
				type: "principle",
				topics: ["m"],
				confidence: 0.95,
			});

			expect(await store.getEntry("n")).toMatchObject({
				status: "active",
				content: "merged",
				type: "principle",
				topics: ["m"],
				confidence: 0.95,
			});
			expect(
				(await store.getEntryWithEmbedding("n"))?.embedding,
			).toBeUndefined();
			expect((await store.getEntry("e"))?.status).toBe("superseded");
			expect((await store.getEntry("e"))?.supersededBy).toBe("n");
			await expectNearest(store, [1, 0, 0], 5, 0.5, []);
		});

		it("merge without merged data keeps the new entry as it was and still supersedes the existing one", async () => {
			await seedPair();
			await store.applyContradictionResolution("merge", "n", "e");

			expect(await store.getEntry("n")).toMatchObject({
				status: "active",
				content: "new",
			});
			expect((await store.getEntryWithEmbedding("n"))?.embedding).toEqual([
				1, 0, 0,
			]);
			expect((await store.getEntry("e"))?.status).toBe("superseded");
		});

		it("supersede_old restores the existing entry's conflict partner and drops their contradicts link", async () => {
			await seedPair();
			await store.insertEntry(entry({ id: "p", content: "partner" }));
			await store.applyContradictionResolution("irresolvable", "e", "p");
			expect((await store.getEntry("p"))?.status).toBe("conflicted");

			await store.applyContradictionResolution("supersede_old", "n", "e");

			expect((await store.getEntry("e"))?.status).toBe("superseded");
			expect((await store.getEntry("p"))?.status).toBe("active");
			expect(await store.getContradictPairsForIds(["p"])).toEqual(new Map());
		});

		it("supersede_old on a conflicted new entry settles it: the new entry and its partner return to active", async () => {
			await seedPair();
			await store.insertEntry(entry({ id: "m", content: "rival" }));
			await store.applyContradictionResolution("irresolvable", "n", "m");
			expect((await store.getEntry("n"))?.status).toBe("conflicted");

			await store.applyContradictionResolution("supersede_old", "n", "e");

			expect((await store.getEntry("n"))?.status).toBe("active");
			expect((await store.getEntry("m"))?.status).toBe("active");
			expect((await store.getEntry("e"))?.status).toBe("superseded");
			expect(await store.getContradictPairsForIds(["n"])).toEqual(new Map());
		});

		it("merge with merged data settles a conflicted new entry; without it the entry stays conflicted", async () => {
			await seedPair();
			await store.insertEntry(entry({ id: "m", content: "rival" }));
			await store.applyContradictionResolution("irresolvable", "n", "m");
			await store.applyContradictionResolution("merge", "n", "e");
			expect((await store.getEntry("n"))?.status).toBe("conflicted");
			expect((await store.getEntry("m"))?.status).toBe("conflicted");

			await store.applyContradictionResolution("merge", "n", "e", {
				content: "merged",
				type: "fact",
				topics: [],
				confidence: 0.5,
			});
			expect((await store.getEntry("n"))?.status).toBe("active");
			expect((await store.getEntry("m"))?.status).toBe("active");
		});

		// ── Relations and supports ──

		it("getSupportSourcesForIds maps each synthesized entry to its active or conflicted sources", async () => {
			await store.insertEntry(entry({ id: "syn", type: "principle" }));
			await store.insertEntry(entry({ id: "src-a", status: "active" }));
			await store.insertEntry(entry({ id: "src-b", status: "conflicted" }));
			await store.insertEntry(entry({ id: "src-c", status: "superseded" }));
			await store.insertEntry(entry({ id: "plain" }));
			for (const [i, target] of ["src-a", "src-b", "src-c"].entries()) {
				await store.insertRelation({
					id: `sup-${i}`,
					sourceId: "syn",
					targetId: target,
					type: "supports",
					createdAt: 1,
				});
			}

			const map = await store.getSupportSourcesForIds(["syn", "plain"]);
			expect(await ids(map.get("syn") ?? [])).toEqual(["src-a", "src-b"]);
			expect(map.has("plain")).toBe(false);
			expect(await store.getSupportSourcesForIds([])).toEqual(new Map());
		});

		// ── Clusters ──

		it("persistClusters stores a new cluster with its members and centroid", async () => {
			await store.insertEntry(entry({ id: "k-a" }));
			await store.insertEntry(entry({ id: "k-b" }));
			await store.persistClusters([
				{
					id: "c1",
					centroid: [0.5, 0.5, 0],
					memberIds: ["k-a", "k-b"],
					isNew: true,
					membershipChanged: true,
				},
			]);

			const [cluster] = await store.getClustersWithMembers();
			expect(cluster).toMatchObject({
				id: "c1",
				centroid: [0.5, 0.5, 0],
				memberCount: 2,
				lastSynthesizedAt: null,
			});
			expect([...cluster.memberIds].sort()).toEqual(["k-a", "k-b"]);
			expect([...cluster.memberJoinedAt.keys()].sort()).toEqual(["k-a", "k-b"]);
		});

		it("persistClusters keeps joined_at for members that stay, adds new members, and drops departed ones", async () => {
			for (const id of ["k-a", "k-b", "k-c"]) {
				await store.insertEntry(entry({ id }));
			}
			await store.persistClusters([
				{
					id: "c1",
					centroid: [1, 0, 0],
					memberIds: ["k-a", "k-b"],
					isNew: true,
					membershipChanged: true,
				},
			]);
			const [first] = await store.getClustersWithMembers();
			const joinedB = first.memberJoinedAt.get("k-b");

			await store.persistClusters([
				{
					id: "c1",
					centroid: [0, 1, 0],
					memberIds: ["k-b", "k-c"],
					isNew: false,
					membershipChanged: true,
				},
			]);

			const [second] = await store.getClustersWithMembers();
			expect([...second.memberIds].sort()).toEqual(["k-b", "k-c"]);
			expect(second.memberCount).toBe(2);
			expect(second.centroid).toEqual([0, 1, 0]);
			expect(second.memberJoinedAt.get("k-b")).toBe(joinedB);
			expect(second.lastMembershipChangedAt).toBeGreaterThanOrEqual(
				first.lastMembershipChangedAt,
			);
		});

		it("persistClusters removes clusters that are no longer in the pass", async () => {
			await store.insertEntry(entry({ id: "k-a" }));
			await store.insertEntry(entry({ id: "k-b" }));
			await store.persistClusters([
				{
					id: "keep",
					centroid: [1, 0, 0],
					memberIds: ["k-a"],
					isNew: true,
					membershipChanged: true,
				},
				{
					id: "drop",
					centroid: [0, 1, 0],
					memberIds: ["k-b"],
					isNew: true,
					membershipChanged: true,
				},
			]);
			await store.persistClusters([
				{
					id: "keep",
					centroid: [1, 0, 0],
					memberIds: ["k-a"],
					isNew: false,
					membershipChanged: false,
				},
			]);
			const clusters = await store.getClustersWithMembers();
			expect(clusters.map((c) => c.id)).toEqual(["keep"]);
			expect(clusters[0].memberIds).toEqual(["k-a"]);
		});

		it("markClusterSynthesized stamps the synthesis time", async () => {
			await store.persistClusters([
				{
					id: "syn-c",
					centroid: [1, 0, 0],
					memberIds: [],
					isNew: true,
					membershipChanged: true,
				},
			]);
			const before = Date.now();
			await store.markClusterSynthesized("syn-c");
			const [cluster] = await store.getClustersWithMembers();
			expect(cluster.lastSynthesizedAt).toBeGreaterThanOrEqual(before);
			expect(cluster.memberCount).toBe(0);
		});

		it("reinitialize clears entries, relations, clusters, and metadata", async () => {
			await store.setEmbeddingMetadata("conformance", DIMS);
			await store.insertEntry(entry({ id: "r-a" }));
			await store.insertEntry(entry({ id: "r-b" }));
			await store.insertRelation({
				id: "r-rel",
				sourceId: "r-a",
				targetId: "r-b",
				type: "supports",
				createdAt: 1,
			});
			await store.persistClusters([
				{
					id: "r-c",
					centroid: [1, 0, 0],
					memberIds: ["r-a"],
					isNew: true,
					membershipChanged: true,
				},
			]);

			await store.reinitialize();

			expect(await store.getStats()).toMatchObject({ total: 0 });
			expect(await store.getRelationsFor("r-a")).toEqual([]);
			expect(await store.getClustersWithMembers()).toEqual([]);
			expect(await store.getEmbeddingMetadata()).toBeNull();
		});

		// ── Consolidation lock ──

		it("a second acquire on the same store throws until the lock is released", async () => {
			expect(await store.tryAcquireConsolidationLock()).toBe(true);
			await expect(store.tryAcquireConsolidationLock()).rejects.toThrow(
				"already held",
			);
			await store.releaseConsolidationLock();
			expect(await store.tryAcquireConsolidationLock()).toBe(true);
			await store.releaseConsolidationLock();
		});

		it("returns false while another store object on the same database holds the lock", async () => {
			expect(await store.tryAcquireConsolidationLock()).toBe(true);
			const other = await opened.second();
			expect(await other.tryAcquireConsolidationLock()).toBe(false);
			await store.releaseConsolidationLock();
			expect(await other.tryAcquireConsolidationLock()).toBe(true);
			await other.releaseConsolidationLock();
		});

		it("a relation is read back from both of its entries with its type and endpoints", async () => {
			await store.insertEntry(entry({ id: "rl-a" }));
			await store.insertEntry(entry({ id: "rl-b" }));
			await store.insertRelation({
				id: "rl-1",
				sourceId: "rl-a",
				targetId: "rl-b",
				type: "contradicts",
				createdAt: 42,
			});
			const expected = {
				id: "rl-1",
				sourceId: "rl-a",
				targetId: "rl-b",
				type: "contradicts",
				createdAt: 42,
			};
			expect(await store.getRelationsFor("rl-a")).toEqual([expected]);
			expect(await store.getRelationsFor("rl-b")).toEqual([expected]);
			expect(await store.getRelationsFor("rl-none")).toEqual([]);
		});

		it("releasing a lock that is not held is a no-op", async () => {
			await expect(store.releaseConsolidationLock()).resolves.toBeUndefined();
		});
	});
}
