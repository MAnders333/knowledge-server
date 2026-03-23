import { logger } from "../../logger.js";

// biome-ignore lint: TS limitation with Omit stripping call signatures from postgres Sql
type TxSql = any;

/**
 * Incremental PostgreSQL migration registry.
 *
 * Rules for adding a new migration:
 *  1. Append a new { version, label, up } entry — never reorder or modify existing ones.
 *  2. `up` must be idempotent (IF NOT EXISTS, information_schema checks, etc.).
 *  3. Only additive changes (new tables, new nullable columns) are safe.
 *     Destructive changes that can't be expressed idempotently should fall
 *     through to the drop+recreate path.
 */
export const PG_MIGRATIONS: Array<{
	version: number;
	label: string;
	up: (sql: TxSql) => Promise<void>;
}> = [
	{
		version: 8,
		label: "add embedding_metadata table",
		up: async (sql: TxSql) => {
			await sql`
				CREATE TABLE IF NOT EXISTS embedding_metadata (
					id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
					model TEXT NOT NULL,
					dimensions INTEGER NOT NULL,
					recorded_at BIGINT NOT NULL
				)
			`;
		},
	},
	{
		version: 9,
		label: "add cluster tables, drop per-entry synthesis column",
		up: async (sql: TxSql) => {
			await sql`
				CREATE TABLE IF NOT EXISTS knowledge_cluster (
					id TEXT PRIMARY KEY,
					centroid BYTEA NOT NULL,
					member_count INTEGER NOT NULL DEFAULT 0,
					last_synthesized_at BIGINT,
					last_membership_changed_at BIGINT NOT NULL,
					created_at BIGINT NOT NULL
				)
			`;
			await sql`
				CREATE TABLE IF NOT EXISTS knowledge_cluster_member (
					cluster_id TEXT NOT NULL REFERENCES knowledge_cluster(id) ON DELETE CASCADE,
					entry_id TEXT NOT NULL REFERENCES knowledge_entry(id) ON DELETE CASCADE,
					joined_at BIGINT NOT NULL,
					PRIMARY KEY (cluster_id, entry_id)
				)
			`;
			await sql`
				CREATE INDEX IF NOT EXISTS idx_cluster_membership_changed
					ON knowledge_cluster(last_membership_changed_at)
			`;
			await sql`
				CREATE INDEX IF NOT EXISTS idx_cluster_member_entry
					ON knowledge_cluster_member(entry_id)
			`;
			// Drop the old per-entry synthesis column if it exists
			const cols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_name = 'knowledge_entry'
				  AND column_name = 'last_synthesized_observation_count'
			`;
			if (cols.length > 0) {
				await sql`
					ALTER TABLE knowledge_entry
					DROP COLUMN last_synthesized_observation_count
				`;
			}
		},
	},
	{
		version: 10,
		label: "add is_synthesized column to knowledge_entry",
		up: async (sql: TxSql) => {
			const cols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_name = 'knowledge_entry'
				  AND column_name = 'is_synthesized'
			`;
			if (cols.length === 0) {
				await sql`
					ALTER TABLE knowledge_entry
					ADD COLUMN is_synthesized INTEGER NOT NULL DEFAULT 0
				`;
				// Backfill: flag existing synthesis entries
				await sql`
					UPDATE knowledge_entry
					SET is_synthesized = 1
					WHERE source LIKE 'synthesis:%'
				`;
			}
		},
	},
	{
		version: 11,
		label:
			"add user_id to source_cursor and consolidated_episode for multi-user support",
		up: async (sql: TxSql) => {
			// source_cursor: add user_id column, rebuild PK to (source, user_id).
			// Add table_schema filter so we don't match a same-named table in a
			// different schema (e.g. when multiple apps share one PG instance).
			const cursorCols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'source_cursor'
				  AND column_name = 'user_id'
			`;
			if (cursorCols.length === 0) {
				await sql`
					ALTER TABLE source_cursor
					ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'
				`;
				// Look up the actual PK constraint name rather than assuming
				// the auto-generated name — avoids failure on restored dumps
				// or manually named constraints.
				const cursorPkRows = await sql`
					SELECT constraint_name FROM information_schema.table_constraints
					WHERE table_schema = current_schema()
					  AND table_name = 'source_cursor'
					  AND constraint_type = 'PRIMARY KEY'
				`;
				const cursorPkName =
					cursorPkRows.length > 0
						? String(cursorPkRows[0].constraint_name ?? "").trim()
						: "";
				if (cursorPkName) {
					await sql`ALTER TABLE source_cursor DROP CONSTRAINT ${sql(cursorPkName)}`;
				} else {
					// No PK found via information_schema — log a warning so operators
					// can investigate, then attempt ADD PRIMARY KEY anyway. If a PK
					// actually exists under an unexpected name, the ADD will fail with
					// a clear Postgres error rather than silently corrupting state.
					logger.warn(
						"[pg-db] v11 migration: could not find source_cursor primary key via information_schema. Proceeding with ADD PRIMARY KEY — will fail if an unnamed PK already exists.",
					);
				}
				await sql`ALTER TABLE source_cursor ADD PRIMARY KEY (source, user_id)`;
			}

			// consolidated_episode: add user_id column, rebuild PK.
			const episodeCols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'consolidated_episode'
				  AND column_name = 'user_id'
			`;
			if (episodeCols.length === 0) {
				await sql`
					ALTER TABLE consolidated_episode
					ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'
				`;
				const episodePkRows = await sql`
					SELECT constraint_name FROM information_schema.table_constraints
					WHERE table_schema = current_schema()
					  AND table_name = 'consolidated_episode'
					  AND constraint_type = 'PRIMARY KEY'
				`;
				const episodePkName =
					episodePkRows.length > 0
						? String(episodePkRows[0].constraint_name ?? "").trim()
						: "";
				if (episodePkName) {
					await sql`ALTER TABLE consolidated_episode DROP CONSTRAINT ${sql(episodePkName)}`;
				} else {
					logger.warn(
						"[pg-db] v11 migration: could not find consolidated_episode primary key via information_schema. Proceeding with ADD PRIMARY KEY — will fail if an unnamed PK already exists.",
					);
				}
				await sql`
					ALTER TABLE consolidated_episode
					ADD PRIMARY KEY (source, user_id, session_id, start_message_id, end_message_id)
				`;
				// Update index to include user_id
				await sql`DROP INDEX IF EXISTS idx_episode_source_session`;
				await sql`
					CREATE INDEX IF NOT EXISTS idx_episode_source_user_session
					ON consolidated_episode(source, user_id, session_id)
				`;
			}
		},
	},
	{
		version: 12,
		label: "add pending_episodes table for episode uploader daemon",
		up: async (sql: TxSql) => {
			// pending_episodes: staging table for daemon-uploaded episodes.
			// daemon_cursor lives in local SQLite only — not created in Postgres.
			const tableExists = await sql`
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = current_schema()
				  AND table_name = 'pending_episodes'
			`;
			if (tableExists.length === 0) {
				await sql`
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
						session_timestamp BIGINT  NOT NULL DEFAULT 0,
						max_message_time  BIGINT  NOT NULL DEFAULT 0,
						approx_tokens    INTEGER NOT NULL DEFAULT 0,
						uploaded_at      BIGINT  NOT NULL
					)
				`;
				await sql`
				CREATE INDEX IF NOT EXISTS idx_pending_source_user_time
				ON pending_episodes(source, user_id, max_message_time)
			`;
			}
		},
	},
	{
		version: 13,
		label:
			"daemon-only consolidation: drop source_cursor, remove user_id from consolidated_episode",
		up: async (sql: TxSql) => {
			// Drop source_cursor — no longer needed with daemon-only consolidation.
			await sql`DROP TABLE IF EXISTS source_cursor`;

			// Remove user_id from consolidated_episode PK.
			// Guard: only run if user_id column still exists.
			const userIdCol = await sql`
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'consolidated_episode'
				  AND column_name = 'user_id'
			`;
			if (userIdCol.length > 0) {
				// Rename old table, create new without user_id, copy data, drop old.
				await sql`ALTER TABLE consolidated_episode RENAME TO consolidated_episode_old`;
				await sql`
					CREATE TABLE consolidated_episode (
						source           TEXT    NOT NULL,
						session_id       TEXT    NOT NULL,
						start_message_id TEXT    NOT NULL,
						end_message_id   TEXT    NOT NULL,
						content_type     TEXT    NOT NULL,
						processed_at     BIGINT  NOT NULL,
						entries_created  INTEGER NOT NULL DEFAULT 0,
						PRIMARY KEY (source, session_id, start_message_id, end_message_id)
					)
				`;
				// INSERT IGNORE equivalent in Postgres: ON CONFLICT DO NOTHING deduplicates
				// in case the same (source, session_id, start, end) existed for multiple user_ids.
				await sql`
					INSERT INTO consolidated_episode
					SELECT source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created
					FROM consolidated_episode_old
					ON CONFLICT (source, session_id, start_message_id, end_message_id) DO NOTHING
				`;
				await sql`DROP TABLE consolidated_episode_old`;
				await sql`DROP INDEX IF EXISTS idx_episode_source_user_session`;
				await sql`
					CREATE INDEX IF NOT EXISTS idx_episode_source_session
					ON consolidated_episode(source, session_id)
				`;
			}
		},
	},
	{
		version: 14,
		label:
			"drop staging tables from Postgres — they now live in state.db (ServerStateDB)",
		up: async (sql: TxSql) => {
			// consolidated_episode, consolidation_state, pending_episodes all moved to
			// state.db (local SQLite). Drop them from Postgres so the schema is clean.
			// These tables may not exist on fresh installs — DROP IF EXISTS is safe.
			//
			// IMPORTANT: any data in consolidated_episode here should have already been
			// migrated to state.db by ServerStateDB (runStateMigrations) on startup.
			// The idempotency history from these rows is preserved in state.db.
			await sql`DROP TABLE IF EXISTS pending_episodes CASCADE`;
			await sql`DROP TABLE IF EXISTS consolidated_episode CASCADE`;
			await sql`DROP TABLE IF EXISTS consolidation_state CASCADE`;
		},
	},
];
