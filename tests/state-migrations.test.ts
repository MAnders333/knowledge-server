import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ServerStateDB } from "../src/db/state/index";
import type { PendingEpisode } from "../src/types";

/**
 * ServerStateDB runs the v3 staging-table migration against a legacy
 * knowledge.db. The only file it may read or modify is the one it is given.
 */
describe("ServerStateDB legacy staging migration", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-state-migration-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	/** A legacy knowledge.db that still holds one pending staging row. */
	async function makeLegacyDb(path: string): Promise<void> {
		const legacy = new ServerStateDB(path);
		await legacy.insertPendingEpisode({
			id: "ep-legacy",
			userId: "u1",
			source: "opencode",
			sessionId: "s1",
			startMessageId: "m1",
			endMessageId: "m2",
			sessionTitle: "t",
			projectName: "p",
			directory: "/d",
			content: "content",
			contentType: "messages",
			timeCreated: 1,
			maxMessageTime: 1,
			approxTokens: 1,
			uploadedAt: 1,
		} as PendingEpisode);
		await legacy.close();
	}

	function legacyTables(path: string): string[] {
		const db = new Database(path, { readonly: true });
		try {
			return db
				.query<{ name: string }, []>(
					"SELECT name FROM sqlite_master WHERE type = 'table'",
				)
				.all()
				.map((r) => r.name);
		} finally {
			db.close();
		}
	}

	it("copies staging rows from the legacy file it is given, then drops them there", async () => {
		const legacyPath = join(tempDir, "knowledge.db");
		await makeLegacyDb(legacyPath);

		const stateDb = new ServerStateDB(join(tempDir, "state.db"), legacyPath);
		const pending = await stateDb.getPendingEpisodes(0);
		await stateDb.close();

		expect(pending.map((e) => e.id)).toEqual(["ep-legacy"]);
		expect(legacyTables(legacyPath)).not.toContain("pending_episodes");
	});

	it("never reads or modifies a legacy file it was not given", async () => {
		const legacyPath = join(tempDir, "knowledge.db");
		await makeLegacyDb(legacyPath);

		const stateDb = new ServerStateDB(join(tempDir, "state.db"));
		expect(await stateDb.getPendingEpisodes(0)).toEqual([]);
		await stateDb.close();

		expect(legacyTables(legacyPath)).toContain("pending_episodes");
		const legacy = new Database(legacyPath, { readonly: true });
		const count = legacy
			.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM pending_episodes")
			.get();
		legacy.close();
		expect(count?.n).toBe(1);
	});

	it("creates the state DB file with mode 0600", async () => {
		const statePath = join(tempDir, "state.db");
		const stateDb = new ServerStateDB(statePath);
		await stateDb.close();

		expect(statSync(statePath).mode & 0o777).toBe(0o600);
	});
});

/*
 * Fixtures for the upgrade tests below.
 *
 * These are reconstructed historical shapes, not shipped files. No release ever
 * stamped state.db as v3 or v4; the DDL is rebuilt from the git history of the v3
 * (d0f468d) and v4 (8096c1a) commits so that the upgrade path has a realistic
 * starting point. Do not edit them to follow the current schema: the upgrade
 * under test is what brings them there.
 */

/** schema_version table with the given versions stamped. */
function stampDdl(versions: number[]): string {
	return `
  CREATE TABLE schema_version (version INTEGER NOT NULL, applied_at INTEGER NOT NULL);
  INSERT INTO schema_version (version, applied_at) VALUES ${versions.map((v) => `(${v}, 0)`).join(", ")};
`;
}

/** The v1 tables, with one staged episode. */
const V1_TABLES = `
  CREATE TABLE pending_episodes (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT 'default',
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    start_message_id TEXT NOT NULL,
    end_message_id TEXT NOT NULL,
    session_title TEXT NOT NULL DEFAULT '',
    project_name TEXT NOT NULL DEFAULT '',
    directory TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK(content_type IN ('messages', 'compaction_summary', 'document')),
    session_timestamp INTEGER NOT NULL DEFAULT 0,
    max_message_time INTEGER NOT NULL DEFAULT 0,
    approx_tokens INTEGER NOT NULL DEFAULT 0,
    uploaded_at INTEGER NOT NULL
  );
  CREATE INDEX idx_pending_source_time ON pending_episodes(source, max_message_time);

  CREATE TABLE consolidated_episode (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    start_message_id TEXT NOT NULL,
    end_message_id TEXT NOT NULL,
    content_type TEXT NOT NULL,
    processed_at INTEGER NOT NULL,
    entries_created INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id, start_message_id, end_message_id)
  );
  CREATE INDEX idx_episode_source_session ON consolidated_episode(source, session_id);

  CREATE TABLE consolidation_state (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
    last_consolidated_at INTEGER NOT NULL DEFAULT 0,
    total_sessions_processed INTEGER NOT NULL DEFAULT 0,
    total_entries_created INTEGER NOT NULL DEFAULT 0,
    total_entries_updated INTEGER NOT NULL DEFAULT 0
  );
  INSERT INTO consolidation_state (id) VALUES (1);

  INSERT INTO pending_episodes (id, source, session_id, start_message_id, end_message_id, content, content_type, max_message_time, uploaded_at)
    VALUES ('ep-legacy', 'opencode', 's1', 'a', 'b', 'staged text', 'messages', 1, 1);
`;

/** What the v2 and v3 steps added: applied_migrations, attempts, activation_event (v3 shape). */
const V3_TABLES = `
  CREATE TABLE applied_migrations (name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL);

  CREATE TABLE pending_episode_attempts (
    id TEXT PRIMARY KEY,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
  );

  CREATE TABLE activation_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    client TEXT NOT NULL,
    session_hash TEXT,
    prompt_chars INTEGER NOT NULL,
    cue_count INTEGER NOT NULL,
    n_candidates INTEGER NOT NULL,
    n_returned INTEGER NOT NULL,
    entry_ids TEXT NOT NULL,
    raw_sims TEXT NOT NULL,
    rerank_scores TEXT,
    gate_mode TEXT NOT NULL CHECK(gate_mode IN ('rerank', 'dense', 'off')),
    latency_ms INTEGER NOT NULL,
    client_deadline_exceeded INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX idx_activation_event_ts ON activation_event(ts);
  CREATE INDEX idx_activation_event_client_ts ON activation_event(client, ts);

  INSERT INTO activation_event (ts, client, prompt_chars, cue_count, n_candidates, n_returned, entry_ids, raw_sims, gate_mode, latency_ms)
    VALUES (1000, 'opencode', 42, 2, 5, 1, '["entry-1"]', '[0.5]', 'off', 12);
`;

/** What the v4 step added: four activation_event columns, stamped v4. */
const V4_STEP = `
  ALTER TABLE activation_event ADD COLUMN gate_dropped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN budget_dropped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN deduped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN injected_tokens INTEGER NOT NULL DEFAULT 0;
  INSERT INTO schema_version (version, applied_at) VALUES (4, 0);
`;

const V1_FILE = stampDdl([1]) + V1_TABLES;
const V3_FILE = stampDdl([1, 2, 3]) + V1_TABLES + V3_TABLES;
const V4_FILE = V3_FILE + V4_STEP;

describe("ServerStateDB schema upgrades", () => {
	let dir: string;
	let statePath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-state-upgrade-"));
		statePath = join(dir, "state.db");
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	function writeFixture(ddl: string): void {
		writeFileSync(statePath, "");
		const raw = new Database(statePath);
		raw.exec(ddl);
		raw.close();
	}

	function readRaw<T>(fn: (db: Database) => T): T {
		const raw = new Database(statePath, { readonly: true });
		try {
			return fn(raw);
		} finally {
			raw.close();
		}
	}

	function storedVersions(): number[] {
		return readRaw((db) =>
			(
				db
					.prepare("SELECT version FROM schema_version ORDER BY version")
					.all() as Array<{ version: number }>
			).map((r) => r.version),
		);
	}

	function activationColumns(): string[] {
		return readRaw((db) =>
			(
				db.prepare("PRAGMA table_info(activation_event)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name),
		);
	}

	function indexNames(): string[] {
		return readRaw((db) =>
			(
				db
					.prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
					.all() as Array<{ name: string }>
			).map((r) => r.name),
		);
	}

	const V4_COLUMNS = [
		"gate_dropped",
		"budget_dropped",
		"deduped",
		"injected_tokens",
	];

	it("a new file is stamped with every registered version and has every column", async () => {
		const state = new ServerStateDB(statePath);
		await state.close();

		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
		expect(activationColumns()).toEqual(
			expect.arrayContaining([...V4_COLUMNS, "rerank_model"]),
		);
		expect(indexNames()).toContain("idx_pending_session");
	});

	it("a reconstructed v3 file is upgraded through v6, keeps its rows and history", async () => {
		writeFixture(V3_FILE);

		const state = new ServerStateDB(statePath);
		try {
			expect((await state.getPendingEpisodes(0)).map((e) => e.id)).toEqual([
				"ep-legacy",
			]);
			await state.insertActivationEvents([
				{
					ts: 2000,
					client: "claude-code",
					sessionHash: "abc123",
					promptChars: 10,
					cueCount: 1,
					nCandidates: 3,
					nReturned: 2,
					entryIds: ["e1", "e2"],
					rawSims: [0.8, 0.6],
					rerankScores: [0.9, 0.4],
					rerankModel: "remote/jev-1.14.0",
					gateMode: "rerank",
					latencyMs: 30,
					clientDeadlineExceeded: false,
					gateDropped: 1,
					budgetDropped: 0,
					deduped: 0,
					injectedTokens: 40,
				},
			]);
			expect(await state.countActivationsByClient(0)).toEqual({
				opencode: 1,
				"claude-code": 1,
			});
		} finally {
			await state.close();
		}

		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
		expect(activationColumns()).toEqual(
			expect.arrayContaining([...V4_COLUMNS, "rerank_model"]),
		);
		expect(indexNames()).toContain("idx_pending_session");
		const kept = readRaw((db) =>
			db
				.prepare(
					"SELECT ts, gate_dropped, budget_dropped, deduped, injected_tokens, rerank_model FROM activation_event ORDER BY ts",
				)
				.all(),
		);
		expect(kept).toEqual([
			{
				ts: 1000,
				gate_dropped: 0,
				budget_dropped: 0,
				deduped: 0,
				injected_tokens: 0,
				rerank_model: null,
			},
			{
				ts: 2000,
				gate_dropped: 1,
				budget_dropped: 0,
				deduped: 0,
				injected_tokens: 40,
				rerank_model: "remote/jev-1.14.0",
			},
		]);
	});

	it("a reconstructed v4 file gains rerank_model and keeps its rows", async () => {
		writeFixture(V4_FILE);
		expect(activationColumns()).not.toContain("rerank_model");

		const state = new ServerStateDB(statePath);
		await state.close();

		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
		expect(activationColumns()).toContain("rerank_model");
		const rows = readRaw((db) =>
			db.prepare("SELECT ts, rerank_model FROM activation_event").all(),
		);
		expect(rows).toEqual([{ ts: 1000, rerank_model: null }]);
	});

	it("a v1-stamped file without applied_migrations gets the tables and bookkeeping it lacks", async () => {
		writeFixture(V1_FILE);
		expect(
			readRaw((db) =>
				db
					.prepare(
						"SELECT name FROM sqlite_master WHERE name = 'applied_migrations'",
					)
					.all(),
			),
		).toEqual([]);

		const state = new ServerStateDB(statePath);
		try {
			expect((await state.getPendingEpisodes(0)).map((e) => e.id)).toEqual([
				"ep-legacy",
			]);
			await state.insertActivationEvents([
				{
					ts: 3000,
					client: "opencode",
					sessionHash: null,
					promptChars: 1,
					cueCount: 1,
					nCandidates: 1,
					nReturned: 1,
					entryIds: ["e"],
					rawSims: [0.5],
					rerankScores: null,
					rerankModel: null,
					gateMode: "off",
					latencyMs: 1,
					clientDeadlineExceeded: false,
					gateDropped: 0,
					budgetDropped: 0,
					deduped: 0,
					injectedTokens: 0,
				},
			]);
		} finally {
			await state.close();
		}

		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
		expect(
			readRaw((db) =>
				db
					.prepare(
						"SELECT name FROM sqlite_master WHERE name = 'applied_migrations'",
					)
					.all(),
			),
		).toHaveLength(1);
		expect(indexNames()).toContain("idx_pending_session");
		expect(activationColumns()).toEqual(
			expect.arrayContaining([...V4_COLUMNS, "rerank_model"]),
		);
	});

	it("an unstamped legacy file with the v3 tables is stamped and brought to v6 without losing rows", async () => {
		// Tables from the v3 shape, but no schema_version table at all.
		writeFixture(V1_TABLES + V3_TABLES);
		expect(activationColumns()).not.toContain("gate_dropped");

		const state = new ServerStateDB(statePath);
		try {
			expect((await state.getPendingEpisodes(0)).map((e) => e.id)).toEqual([
				"ep-legacy",
			]);
			expect(await state.countActivationsByClient(0)).toEqual({ opencode: 1 });
		} finally {
			await state.close();
		}

		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
		expect(activationColumns()).toEqual(
			expect.arrayContaining([...V4_COLUMNS, "rerank_model"]),
		);
		expect(
			readRaw((db) =>
				db.prepare("SELECT ts, rerank_model FROM activation_event").all(),
			),
		).toEqual([{ ts: 1000, rerank_model: null }]);
	});

	it("reopening an upgraded file applies nothing again", async () => {
		writeFixture(V3_FILE);
		const first = new ServerStateDB(statePath);
		await first.close();
		const state = new ServerStateDB(statePath);
		try {
			expect(await state.getPendingEpisodes(0)).toHaveLength(1);
		} finally {
			await state.close();
		}
		expect(storedVersions()).toEqual([1, 2, 3, 4, 5, 6]);
	});
});
