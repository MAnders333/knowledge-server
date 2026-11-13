/**
 * mergeEntry on a deleted target. The merge is skipped (false), and the
 * extracted knowledge is kept as a NEW entry: the chunk is counted as a create,
 * not an update, and the deleted id is not resurrected.
 *
 * SQLite always runs. PostgreSQL runs when PG_TEST_URI is set:
 *   PG_TEST_URI=postgres://<user>:<password>@localhost:<port>/<db> bun test tests/merge-entry-deleted-target.test.ts
 */
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import postgres from "postgres";
import { ActivationEngine } from "../src/activation/activate";
import type { EmbeddingClient } from "../src/activation/embeddings";
import { config } from "../src/config";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { ExtractedKnowledge } from "../src/consolidation/llm";
import { ConsolidationLLM } from "../src/consolidation/llm";
import { PendingEpisodesReader } from "../src/consolidation/readers/pending";
import { Reconsolidator } from "../src/consolidation/reconsolidate";
import type { IKnowledgeStore } from "../src/db/interface";
import { PostgresKnowledgeDB } from "../src/db/postgres/index";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import { logger } from "../src/logger";
import type { KnowledgeEntry, PendingEpisode } from "../src/types";

const PG_URI = process.env.PG_TEST_URI;
const TARGET_ID = "merge-target";
const EXTRACTED = "The new fact is recorded.";

/** Every text embeds to the same vector, so the extraction is a merge candidate. */
const VEC = [1, 0, 0, 0, 0, 0, 0, 0];

type Store = IKnowledgeStore & { close(): Promise<void> };

const mergeUpdates = {
	content: "Merged fact.",
	type: "fact",
	topics: ["test"],
	confidence: 0.9,
	additionalSources: ["session-x"],
};

function storedEntry(id: string, content: string) {
	const now = Date.now();
	return {
		id,
		type: "fact" as const,
		content,
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
		embedding: VEC,
	};
}

function extracted(content: string): ExtractedKnowledge {
	return {
		type: "fact",
		content,
		topics: ["test"],
		confidence: 0.9,
		source: "test",
	};
}

function pendingRow(): PendingEpisode {
	const now = Date.now();
	return {
		id: "row-1",
		userId: "alice",
		source: "opencode",
		sessionId: "session-1",
		startMessageId: "msg-0",
		endMessageId: "msg-9",
		sessionTitle: "Session",
		projectName: "project",
		directory: "/tmp/merge-deleted-test",
		content: "User: the new fact is recorded.",
		contentType: "messages",
		timeCreated: now - 2000,
		maxMessageTime: now - 1000,
		approxTokens: 10,
		uploadedAt: now,
	};
}

/**
 * Shared cases. `open` returns a fresh store for each test; `reset` wipes the
 * knowledge tables before each test (PostgreSQL shares one database).
 */
function defineMergeTargetDeletedSuite(
	label: string,
	open: (dir: string) => Promise<Store>,
	reset: () => Promise<void>,
) {
	describe(label, () => {
		let tempDir: string;
		let store: Store;
		let state: ServerStateDB;
		let savedChunkSize: number;

		beforeEach(async () => {
			await reset();
			tempDir = mkdtempSync(join(tmpdir(), "ks-merge-deleted-"));
			store = await open(tempDir);
			state = new ServerStateDB(join(tempDir, "state.db"));
			savedChunkSize = config.consolidation.chunkSize;
			config.consolidation.chunkSize = 1;
		});

		afterEach(async () => {
			config.consolidation.chunkSize = savedChunkSize;
			mock.restore();
			await store.close();
			await state.close();
			rmSync(tempDir, { recursive: true, force: true });
		});

		it("mergeEntry returns false for a missing id and true for an existing one", async () => {
			await store.insertEntry(storedEntry(TARGET_ID, "Old fact."));

			expect(await store.mergeEntry("missing-id", mergeUpdates)).toBe(false);
			expect(await store.getEntry("missing-id")).toBeNull();

			expect(await store.mergeEntry(TARGET_ID, mergeUpdates)).toBe(true);
			expect((await store.getEntry(TARGET_ID))?.content).toBe("Merged fact.");
		});

		it("consolidation keeps the extracted knowledge as a new entry when the merge target was deleted", async () => {
			await store.insertEntry(storedEntry(TARGET_ID, "Old fact."));
			await state.insertPendingEpisode(pendingRow());

			spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
				extracted(EXTRACTED),
			]);
			spyOn(ConsolidationLLM.prototype, "batchDecideMerge").mockResolvedValue([
				{ action: "update", ...mergeUpdates },
			]);
			// The target is deleted after candidate selection, right before the merge.
			const realMerge = store.mergeEntry.bind(store);
			const merge = spyOn(store, "mergeEntry").mockImplementation(
				async (id, updates, embedding) => {
					await store.deleteEntry(id);
					return realMerge(id, updates, embedding);
				},
			);
			const log = spyOn(logger, "log");

			const activation = new ActivationEngine(store);
			spyOn(activation.embeddings, "embed").mockResolvedValue(VEC);
			const engine = new ConsolidationEngine(
				store,
				state,
				activation,
				[new PendingEpisodesReader(state)],
				null,
			);
			await engine.consolidate();

			expect(merge).toHaveBeenCalledTimes(1);
			expect(await store.getEntry(TARGET_ID)).toBeNull();
			const active = await store.getActiveEntries();
			expect(active.map((e) => e.content)).toEqual([EXTRACTED]);
			expect(active[0].id).not.toBe(TARGET_ID);
			expect(await state.getPendingEpisodes(0)).toEqual([]);

			const messages = log.mock.calls.map((c) => String(c[0]));
			expect(messages.some((m) => m.includes("+ insert"))).toBe(true);
			expect(messages.some((m) => m.includes("Updated:"))).toBe(false);
		});

		it("reconsolidate keeps the extracted knowledge as a new entry and never runs onUpdate", async () => {
			await store.insertEntry(storedEntry(TARGET_ID, "Old fact."));
			const loaded = await store.getActiveEntriesWithEmbeddings();
			const entriesMap = new Map<
				string,
				KnowledgeEntry & { embedding: number[] }
			>(loaded.map((e) => [e.id, e]));
			expect(entriesMap.has(TARGET_ID)).toBe(true);
			// The target is deleted after it was loaded into entriesMap.
			expect(await store.deleteEntry(TARGET_ID)).toBe(true);

			const llm = {
				decideMerge: mock(async () => ({ action: "update", ...mergeUpdates })),
			} as unknown as ConsolidationLLM;
			const embeddings = {
				embed: mock(async () => VEC),
			} as unknown as EmbeddingClient;
			const reconsolidator = new Reconsolidator(store, embeddings, llm);
			const callbacks = {
				onInsert: mock(() => {}),
				onUpdate: mock(() => {}),
				onKeep: mock(() => {}),
			};

			await reconsolidator.reconsolidate(
				extracted(EXTRACTED),
				["session-x"],
				entriesMap,
				callbacks,
				undefined,
				undefined,
				"consolidation",
				store,
				store,
			);

			expect(callbacks.onUpdate).not.toHaveBeenCalled();
			expect(callbacks.onInsert).toHaveBeenCalledTimes(1);
			expect(callbacks.onInsert.mock.calls[0][0].content).toBe(EXTRACTED);
			expect(entriesMap.has(TARGET_ID)).toBe(false);
			expect(await store.getEntry(TARGET_ID)).toBeNull();
			expect((await store.getActiveEntries()).map((e) => e.content)).toEqual([
				EXTRACTED,
			]);
		});
	});
}

defineMergeTargetDeletedSuite(
	"SQLite store",
	async (dir) => new KnowledgeDB(join(dir, "knowledge.db")),
	async () => {},
);

describe.skipIf(!PG_URI)("PostgreSQL store", () => {
	let truncSql: postgres.Sql;

	beforeAll(() => {
		truncSql = postgres(PG_URI as string, { max: 1 });
	});

	afterAll(async () => {
		await truncSql.end();
	});

	defineMergeTargetDeletedSuite(
		"PostgresKnowledgeDB",
		async () => {
			const pg = new PostgresKnowledgeDB(PG_URI as string);
			await pg.initialize();
			return pg;
		},
		async () => {
			// Create the schema on a first connection so the truncate has tables to hit.
			const init = new PostgresKnowledgeDB(PG_URI as string);
			await init.initialize();
			await init.close();
			await truncSql`TRUNCATE knowledge_entry, knowledge_cluster, embedding_metadata, schema_version CASCADE`;
			await truncSql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
			await truncSql`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
			await truncSql.unsafe(
				"ALTER TABLE knowledge_entry DROP COLUMN IF EXISTS embedding_vec",
			);
		},
	);
});
