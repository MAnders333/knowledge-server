import type { Database } from "bun:sqlite";

/**
 * Incremental SQLite migration registry.
 *
 * Each entry runs when the DB version is below the target version.
 * Migrations are applied sequentially, each in its own transaction so a crash
 * mid-migration leaves the DB at the last successfully committed version.
 *
 * Rules for adding a new migration:
 *  1. Append a new { version, label, up } entry — never reorder or modify existing ones.
 *  2. `up` must be idempotent (use IF NOT EXISTS, PRAGMA checks, etc.).
 *  3. Only additive changes (new tables, new nullable columns) are safe here.
 *     Destructive changes that cannot be expressed idempotently should fall
 *     through to the full schema reset path instead.
 */
export const MIGRATIONS: Array<{
	version: number;
	label: string;
	up: (db: Database) => void;
}> = [
	{
		version: 8,
		label: "add embedding_metadata table",
		up: (db) => {
			db.exec(`
				CREATE TABLE IF NOT EXISTS embedding_metadata (
					id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
					model TEXT NOT NULL,
					dimensions INTEGER NOT NULL,
					recorded_at INTEGER NOT NULL
				);
			`);
		},
	},
	{
		version: 9,
		label: "add cluster tables, drop per-entry synthesis column",
		up: (db) => {
			db.exec(`
				CREATE TABLE IF NOT EXISTS knowledge_cluster (
					id                         TEXT    PRIMARY KEY,
					centroid                   BLOB    NOT NULL,
					member_count               INTEGER NOT NULL DEFAULT 0,
					last_synthesized_at        INTEGER,
					last_membership_changed_at INTEGER NOT NULL,
					created_at                 INTEGER NOT NULL
				);
				CREATE TABLE IF NOT EXISTS knowledge_cluster_member (
					cluster_id  TEXT    NOT NULL REFERENCES knowledge_cluster(id) ON DELETE CASCADE,
					entry_id    TEXT    NOT NULL REFERENCES knowledge_entry(id)   ON DELETE CASCADE,
					joined_at   INTEGER NOT NULL,
					PRIMARY KEY (cluster_id, entry_id)
				);
				CREATE INDEX IF NOT EXISTS idx_cluster_membership_changed
					ON knowledge_cluster(last_membership_changed_at);
				CREATE INDEX IF NOT EXISTS idx_cluster_member_entry
					ON knowledge_cluster_member(entry_id);
			`);
			// DROP COLUMN requires SQLite ≥ 3.35; Bun ships 3.46+.
			const hasSynthCol = (
				db.prepare("PRAGMA table_info(knowledge_entry)").all() as Array<{
					name: string;
				}>
			).some((c) => c.name === "last_synthesized_observation_count");
			if (hasSynthCol) {
				db.exec(
					"ALTER TABLE knowledge_entry DROP COLUMN last_synthesized_observation_count",
				);
			}
		},
	},
	{
		version: 10,
		label: "add is_synthesized column to knowledge_entry",
		up: (db) => {
			const cols = db
				.prepare("PRAGMA table_info(knowledge_entry)")
				.all() as Array<{ name: string }>;
			// cols is empty when the table doesn't exist yet (fresh DB) — CREATE_TABLES
			// already includes the column, so nothing to do in that case.
			if (cols.length === 0) return;
			const hasCol = cols.some((c) => c.name === "is_synthesized");
			if (!hasCol) {
				db.exec(
					"ALTER TABLE knowledge_entry ADD COLUMN is_synthesized INTEGER NOT NULL DEFAULT 0",
				);
				// Backfill: flag all existing entries whose source marks them as synthesis outputs
				db.exec(
					"UPDATE knowledge_entry SET is_synthesized = 1 WHERE source LIKE 'synthesis:%'",
				);
			}
		},
	},
	{
		version: 11,
		label:
			"add user_id to source_cursor and consolidated_episode for multi-user support",
		up: (db) => {
			// source_cursor: add user_id column and rebuild with composite PK.
			// SQLite does not support ALTER PRIMARY KEY, so we drop+recreate.
			// If the table doesn't exist yet (fresh DB running migrations before
			// CREATE_TABLES), skip — CREATE_TABLES will create it correctly.
			const cursorCols = (
				db.prepare("PRAGMA table_info(source_cursor)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name);
			if (cursorCols.length > 0 && !cursorCols.includes("user_id")) {
				// Each DDL statement in a separate exec() call for clarity and safety.
				// In better-sqlite3, exec() participates in the enclosing transaction()
				// wrapper regardless of statement count — splitting here avoids any
				// ambiguity about multi-statement string handling across binding versions.
				db.exec(
					"ALTER TABLE source_cursor ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'",
				);
				db.exec(`CREATE TABLE source_cursor_v11 (
					source                    TEXT    NOT NULL,
					user_id                   TEXT    NOT NULL DEFAULT 'default',
					last_message_time_created INTEGER NOT NULL DEFAULT 0,
					last_consolidated_at      INTEGER NOT NULL DEFAULT 0,
					PRIMARY KEY (source, user_id)
				)`);
				db.exec(
					"INSERT OR IGNORE INTO source_cursor_v11 SELECT source, user_id, last_message_time_created, last_consolidated_at FROM source_cursor",
				);
				db.exec("DROP TABLE source_cursor");
				db.exec("ALTER TABLE source_cursor_v11 RENAME TO source_cursor");
			}

			// consolidated_episode: add user_id column and rebuild with new PK.
			// Same guard — skip if table doesn't exist yet.
			const episodeCols = (
				db.prepare("PRAGMA table_info(consolidated_episode)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name);
			if (episodeCols.length > 0 && !episodeCols.includes("user_id")) {
				db.exec(
					"ALTER TABLE consolidated_episode ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'",
				);
				db.exec(`CREATE TABLE consolidated_episode_v11 (
					source           TEXT    NOT NULL,
					user_id          TEXT    NOT NULL DEFAULT 'default',
					session_id       TEXT    NOT NULL,
					start_message_id TEXT    NOT NULL,
					end_message_id   TEXT    NOT NULL,
					content_type     TEXT    NOT NULL,
					processed_at     INTEGER NOT NULL,
					entries_created  INTEGER NOT NULL DEFAULT 0,
					PRIMARY KEY (source, user_id, session_id, start_message_id, end_message_id)
				)`);
				db.exec(
					"INSERT OR IGNORE INTO consolidated_episode_v11 SELECT source, user_id, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created FROM consolidated_episode",
				);
				db.exec("DROP TABLE consolidated_episode");
				db.exec(
					"ALTER TABLE consolidated_episode_v11 RENAME TO consolidated_episode",
				);
				db.exec(
					"CREATE INDEX IF NOT EXISTS idx_episode_source_user_session ON consolidated_episode(source, user_id, session_id)",
				);
				db.exec(
					"CREATE INDEX IF NOT EXISTS idx_episode_processed ON consolidated_episode(processed_at)",
				);
			}
		},
	},
	{
		version: 12,
		label:
			"add pending_episodes and daemon_cursor tables for episode uploader daemon",
		up: (db) => {
			// Both tables are additive — CREATE TABLE IF NOT EXISTS is safe on any DB.
			// pending_episodes: only needed on the shared Postgres (server side), but
			// creating it on SQLite is harmless and simplifies the single-machine setup.
			// daemon_cursor: only needed on local SQLite (daemon side), same logic.
			const existingTables = (
				db
					.prepare("SELECT name FROM sqlite_master WHERE type='table'")
					.all() as Array<{ name: string }>
			).map((r) => r.name);

			if (!existingTables.includes("pending_episodes")) {
				db.exec(`
					CREATE TABLE pending_episodes (
						id               TEXT    PRIMARY KEY,
						user_id          TEXT    NOT NULL DEFAULT 'default',
						source           TEXT    NOT NULL,
						session_id       TEXT    NOT NULL,
						start_message_id TEXT    NOT NULL,
						end_message_id   TEXT    NOT NULL,
						session_title    TEXT    NOT NULL DEFAULT '',
						project_name     TEXT    NOT NULL DEFAULT '',
						directory        TEXT    NOT NULL DEFAULT '',
						content          TEXT    NOT NULL,
						content_type     TEXT    NOT NULL CHECK(content_type IN ('messages', 'compaction_summary', 'document')),
						session_timestamp INTEGER NOT NULL DEFAULT 0,
						max_message_time  INTEGER NOT NULL DEFAULT 0,
						approx_tokens    INTEGER NOT NULL DEFAULT 0,
						uploaded_at      INTEGER NOT NULL
					)
				`);
				db.exec(
					"CREATE INDEX IF NOT EXISTS idx_pending_source_user_time ON pending_episodes(source, user_id, max_message_time)",
				);
			}

			if (!existingTables.includes("daemon_cursor")) {
				db.exec(`
				CREATE TABLE daemon_cursor (
					source                    TEXT    PRIMARY KEY,
					last_message_time_created INTEGER NOT NULL DEFAULT 0,
					last_uploaded_at          INTEGER NOT NULL DEFAULT 0
				)
			`);
			}
		},
	},
	{
		version: 13,
		label:
			"daemon-only consolidation: drop source_cursor, remove user_id from consolidated_episode",
		up: (db) => {
			// Drop source_cursor — no longer needed with daemon-only consolidation.
			// pending_episodes is self-draining; consolidated_episode handles idempotency.
			db.exec("DROP TABLE IF EXISTS source_cursor");

			// NOTE: After v13, consolidated_episode belongs in state.db (ServerStateDB),
			// not in knowledge.db. This migration preserves the existing table to allow
			// ServerStateDB (runStateMigrations) to copy the rows on first startup.
			// After migration, the table in knowledge.db becomes orphaned (never read or
			// written again) but is intentionally NOT dropped here — dropping before
			// runStateMigrations() copies rows would silently destroy the idempotency
			// history, causing all previously-consolidated episodes to be re-processed.

			// Rebuild consolidated_episode without user_id.
			// Guard: skip if table doesn't exist yet (fresh DB gets correct schema from CREATE_TABLES).
			const episodeCols = (
				db.prepare("PRAGMA table_info(consolidated_episode)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name);
			if (episodeCols.length > 0 && episodeCols.includes("user_id")) {
				db.exec(`CREATE TABLE consolidated_episode_v13 (
					source           TEXT    NOT NULL,
					session_id       TEXT    NOT NULL,
					start_message_id TEXT    NOT NULL,
					end_message_id   TEXT    NOT NULL,
					content_type     TEXT    NOT NULL,
					processed_at     INTEGER NOT NULL,
					entries_created  INTEGER NOT NULL DEFAULT 0,
					PRIMARY KEY (source, session_id, start_message_id, end_message_id)
				)`);
				// Copy data; INSERT OR IGNORE deduplicates on the new PK in case the same
				// (source, session_id, start, end) was recorded for multiple user_ids.
				db.exec(`
					INSERT OR IGNORE INTO consolidated_episode_v13
					SELECT source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created
					FROM consolidated_episode
				`);
				db.exec("DROP TABLE consolidated_episode");
				db.exec(
					"ALTER TABLE consolidated_episode_v13 RENAME TO consolidated_episode",
				);
				db.exec(
					"CREATE INDEX IF NOT EXISTS idx_episode_source_session ON consolidated_episode(source, session_id)",
				);
				db.exec(
					"CREATE INDEX IF NOT EXISTS idx_episode_processed ON consolidated_episode(processed_at)",
				);
			}
		},
	},
	{
		version: 14,
		label: "staging tables moved to state.db — no-op for knowledge.db (SQLite)",
		up: (_db) => {
			// SQLite knowledge.db: staging tables (consolidated_episode, consolidation_state,
			// pending_episodes) were already handled in v13 and remain as orphaned legacy
			// tables until runStateMigrations() copies them to state.db.
			// No DDL changes needed for SQLite knowledge stores.
		},
	},
	{
		version: 15,
		label:
			"drop scope column — domain routing replaces personal/team scope distinction",
		up: (db) => {
			// scope was a 'personal|team' classification assigned by the LLM at extraction
			// time. With multi-store domain routing, the domain field carries this signal
			// more precisely. scope was never used in routing, activation weighting, or
			// decay logic — it was purely metadata. Dropping it removes a confusing field
			// that the LLM was conflating with domain assignment.
			//
			// SQLite ALTER TABLE DROP COLUMN requires SQLite 3.35+ (Bun's bundled SQLite
			// supports it). The column is dropped and the index is removed.
			const cols = db
				.prepare("PRAGMA table_info(knowledge_entry)")
				.all() as Array<{ name: string }>;
			if (cols.length === 0) return; // table doesn't exist — fresh DB initialises without scope
			if (cols.some((c) => c.name === "scope")) {
				db.exec("DROP INDEX IF EXISTS idx_entry_scope");
				db.exec("ALTER TABLE knowledge_entry DROP COLUMN scope");
			}
		},
	},
	{
		version: 16,
		label: "pgvector ANN search — no-op for SQLite (Postgres-only feature)",
		up: (_db) => {
			// v16 adds the pgvector `embedding_vec vector(N)` column and HNSW index
			// to the Postgres schema (see src/db/postgres/migrations.ts).
			// SQLite stores continue to use the BLOB `embedding` column with in-process
			// cosine similarity; no schema changes are needed here.
		},
	},
];
