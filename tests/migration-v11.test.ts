/**
 * Tests for the v11 SQLite migration.
 *
 * Creates a v10-schema database (source_cursor and consolidated_episode
 * without user_id), opens it with KnowledgeDB (which triggers the migration),
 * and verifies the resulting schema is correct.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { KnowledgeDB } from "../src/db/database";

// ── v10 schema DDL ────────────────────────────────────────────────────────────

/** Minimal v10 DDL: just the two tables that change in v11. */
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
 * Stamps schema_version = 10 so KnowledgeDB picks up from v10 → v11.
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
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("adds user_id column to source_cursor", async () => {
		db = new KnowledgeDB(dbPath);
		// KnowledgeDB.constructor runs migrations synchronously

		const raw = new Database(dbPath, { readonly: true });
		const cols = (
			raw.prepare("PRAGMA table_info(source_cursor)").all() as Array<{
				name: string;
			}>
		).map((c) => c.name);
		raw.close();

		expect(cols).toContain("user_id");
		expect(cols).toContain("source");
		expect(cols).toContain("last_message_time_created");
		expect(cols).toContain("last_consolidated_at");
	});

	it("adds user_id column to consolidated_episode", async () => {
		db = new KnowledgeDB(dbPath);

		const raw = new Database(dbPath, { readonly: true });
		const cols = (
			raw.prepare("PRAGMA table_info(consolidated_episode)").all() as Array<{
				name: string;
			}>
		).map((c) => c.name);
		raw.close();

		expect(cols).toContain("user_id");
		expect(cols).toContain("source");
		expect(cols).toContain("session_id");
	});

	it("preserves existing cursor row with user_id = 'default'", async () => {
		db = new KnowledgeDB(dbPath);

		const cursor = await db.getSourceCursor("opencode", "default");
		expect(cursor.source).toBe("opencode");
		expect(cursor.userId).toBe("default");
		expect(cursor.lastMessageTimeCreated).toBe(12345);
		expect(cursor.lastConsolidatedAt).toBe(67890);
	});

	it("preserves existing episode row with user_id = 'default'", async () => {
		db = new KnowledgeDB(dbPath);

		const ranges = await db.getProcessedEpisodeRanges("opencode", "default", [
			"session-abc",
		]);
		expect(ranges.size).toBe(1);
		expect(ranges.get("session-abc")).toHaveLength(1);
		const sessionRanges = ranges.get("session-abc");
		expect(sessionRanges).toBeDefined();
		expect(sessionRanges?.[0].startMessageId).toBe("msg-start");
	});

	it("stamps schema version 11 after migration", async () => {
		db = new KnowledgeDB(dbPath);

		const raw = new Database(dbPath, { readonly: true });
		const row = raw
			.prepare("SELECT MAX(version) as v FROM schema_version")
			.get() as { v: number };
		raw.close();

		expect(row.v).toBe(11);
	});

	it("new cursor writes and reads work after migration", async () => {
		db = new KnowledgeDB(dbPath);

		await db.updateSourceCursor("claude-code", "alice", {
			lastMessageTimeCreated: 99999,
		});
		const cursor = await db.getSourceCursor("claude-code", "alice");
		expect(cursor.userId).toBe("alice");
		expect(cursor.lastMessageTimeCreated).toBe(99999);

		// Original cursor is unaffected
		const original = await db.getSourceCursor("opencode", "default");
		expect(original.lastMessageTimeCreated).toBe(12345);
	});
});
