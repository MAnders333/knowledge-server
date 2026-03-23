/**
 * Tests for the SQLite migration chain from v10 → v13.
 *
 * Creates a v10-schema database (source_cursor and consolidated_episode
 * without user_id), opens it with KnowledgeDB (which triggers migrations
 * v11 → v12 → v13), and verifies the resulting schema is correct.
 *
 * v11 added user_id to both tables; v13 removed source_cursor entirely and
 * removed user_id from consolidated_episode. This test verifies the full
 * migration chain produces the correct final schema.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerLocalDB } from "../src/db/server-local/index";

// ── v10 schema DDL ────────────────────────────────────────────────────────────

/** Minimal v10 DDL: just the two tables that change in v11/v13. */
const V10_TABLES = `
  CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at BIGINT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS source_cursor (
    source TEXT PRIMARY KEY,
    last_message_time_created INTEGER NOT NULL DEFAULT 0,
    last_consolidated_at      INTEGER NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS consolidated_episode (
    source           TEXT    NOT NULL,
    session_id       TEXT    NOT NULL,
    start_message_id TEXT    NOT NULL,
    end_message_id   TEXT    NOT NULL,
    content_type     TEXT    NOT NULL,
    processed_at     INTEGER NOT NULL,
    entries_created  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id, start_message_id, end_message_id)
  );

  CREATE INDEX IF NOT EXISTS idx_episode_source_session
    ON consolidated_episode(source, session_id);
`;

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Create a v10-schema SQLite database at `dbPath` with seed data.
 * Stamps schema_version = 10 so KnowledgeDB picks up from v10.
 */
function createV10Fixture(dbPath: string): void {
	const raw = new Database(dbPath);
	raw.exec(V10_TABLES);
	raw
		.prepare("INSERT INTO schema_version (version, applied_at) VALUES (?, ?)")
		.run(10, Date.now());
	// Seed one cursor and one episode using the v10 schema (no user_id)
	raw
		.prepare(
			"INSERT INTO source_cursor (source, last_message_time_created, last_consolidated_at) VALUES (?, ?, ?)",
		)
		.run("opencode", 12345, 67890);
	raw
		.prepare(
			`INSERT INTO consolidated_episode
       (source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
		)
		.run(
			"opencode",
			"session-abc",
			"msg-start",
			"msg-end",
			"messages",
			Date.now(),
			2,
		);
	raw.close();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("v11 SQLite migration (v10 → v11)", () => {
	let tempDir: string;
	let dbPath: string;
	let db: KnowledgeDB;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-migration-v11-test-"));
		dbPath = join(tempDir, "test.db");
		createV10Fixture(dbPath);
	});

	afterEach(async () => {
		await db?.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("source_cursor is dropped by v13 migration", async () => {
		db = new KnowledgeDB(dbPath);

		const raw = new Database(dbPath, { readonly: true });
		const tables = (
			raw
				.prepare("SELECT name FROM sqlite_master WHERE type='table'")
				.all() as Array<{ name: string }>
		).map((r) => r.name);
		raw.close();

		expect(tables).not.toContain("source_cursor");
	});

	it("consolidated_episode no longer has user_id after v13", async () => {
		db = new KnowledgeDB(dbPath);

		const raw = new Database(dbPath, { readonly: true });
		const cols = (
			raw.prepare("PRAGMA table_info(consolidated_episode)").all() as Array<{
				name: string;
			}>
		).map((c) => c.name);
		raw.close();

		expect(cols).not.toContain("user_id");
		expect(cols).toContain("source");
		expect(cols).toContain("session_id");
	});

	it("ServerLocalDB independently manages episode tracking", async () => {
		// After the split, episode tracking (consolidated_episode) lives in server.db,
		// not in knowledge.db. Verify ServerLocalDB works for episode writes/reads.
		db = new KnowledgeDB(dbPath);
		const serverLocalDb = new ServerLocalDB(join(tempDir, "server.db"));

		// ServerLocalDB has its own fresh tables — episode from knowledge.db fixture
		// is not automatically migrated here (migrateFromKnowledgeDb requires StoreRegistry).
		// Just verify the API works correctly.
		await serverLocalDb.recordEpisode(
			"opencode",
			"session-check",
			"s",
			"e",
			"messages",
			1,
		);
		const ranges = await serverLocalDb.getProcessedEpisodeRanges([
			"session-check",
		]);
		expect(ranges.size).toBe(1);
		await serverLocalDb.close();
	});

	it("stamps schema version 13 after full migration chain", async () => {
		db = new KnowledgeDB(dbPath);

		const raw = new Database(dbPath, { readonly: true });
		const row = raw
			.prepare("SELECT MAX(version) as v FROM schema_version")
			.get() as { v: number };
		raw.close();

		expect(row.v).toBe(13);
	});

	it("migrateFromKnowledgeDb copies legacy consolidated_episode rows to server.db", async () => {
		// Open KnowledgeDB to trigger the v10 → v13 migration chain.
		db = new KnowledgeDB(dbPath);

		// Simulate what StoreRegistry does: create ServerLocalDB then migrate from knowledge.db.
		const serverLocalDb2 = new ServerLocalDB(join(tempDir, "server2.db"));
		serverLocalDb2.migrateFromKnowledgeDb(dbPath);

		// The consolidated_episode row seeded in the v10 fixture should now be in server.db.
		const ranges = await serverLocalDb2.getProcessedEpisodeRanges([
			"session-abc",
		]);
		expect(ranges.size).toBe(1);
		expect(ranges.get("session-abc")?.[0].source).toBe("opencode");
		expect(ranges.get("session-abc")?.[0].startMessageId).toBe("msg-start");
		await serverLocalDb2.close();
	});

	it("episode writes and reads work via ServerLocalDB", async () => {
		db = new KnowledgeDB(dbPath);
		const serverLocalDb = new ServerLocalDB(join(tempDir, "server.db"));

		await serverLocalDb.recordEpisode(
			"claude-code",
			"session-new",
			"s",
			"e",
			"messages",
			1,
		);
		const ranges = await serverLocalDb.getProcessedEpisodeRanges([
			"session-new",
		]);
		expect(ranges.get("session-new")).toHaveLength(1);
		expect(ranges.get("session-new")?.[0].source).toBe("claude-code");
		await serverLocalDb.close();
	});
});
