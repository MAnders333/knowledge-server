/**
 * SQLite schema for state.db — the server staging and bookkeeping database.
 *
 * Can be local SQLite (default) or remote Postgres (see PostgresServerStateDB).
 * This file defines the SQLite schema only.
 *
 * Holds:
 *   - pending_episodes: daemon writes, server drains
 *   - consolidated_episode: idempotency log for consolidation
 *   - consolidation_state: global server counters
 *
 * Does NOT hold daemon_cursor — that lives in daemon.db (DaemonDB), always
 * local SQLite per-machine. See src/db/daemon/index.ts.
 *
 * Migration from knowledge.db:
 *   On first startup with state.db, if knowledge.db exists and state.db does not,
 *   the staging tables are copied from knowledge.db to state.db automatically.
 */

import type { Database } from "bun:sqlite";
import {
	type VersionedMigration,
	latestVersion,
} from "../migration-registry.js";
import {
	ACTIVATION_EVENT_COLUMNS,
	activationEventColumnDefs,
} from "./activation-columns.js";

/**
 * Ordered schema migrations for state.db. Each `up` runs once, in the same
 * transaction that stamps schema_version. A new file runs every step from v1, so
 * each `up` must be idempotent (IF NOT EXISTS, or a column check before ALTER).
 */
export const SERVER_LOCAL_MIGRATIONS: ReadonlyArray<
	VersionedMigration & { readonly up: (db: Database) => void }
> = [
	{
		version: 1,
		label: "baseline state schema",
		up: (db) => {
			// Rows are deleted after successful consolidation. Failure counts live in
			// pending_episode_attempts (v2), so schema upgrades never ALTER this table.
			db.exec(`
				CREATE TABLE IF NOT EXISTS pending_episodes (
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
				);

				CREATE INDEX IF NOT EXISTS idx_pending_source_time
					ON pending_episodes(source, max_message_time);

				-- Per-episode idempotency log: prevents re-processing episodes on crash/restart.
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

				-- Consolidation state: global counters and last-run timestamp.
				CREATE TABLE IF NOT EXISTS consolidation_state (
					id                       INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
					last_consolidated_at     INTEGER NOT NULL DEFAULT 0,
					total_sessions_processed INTEGER NOT NULL DEFAULT 0,
					total_entries_created    INTEGER NOT NULL DEFAULT 0,
					total_entries_updated    INTEGER NOT NULL DEFAULT 0
				);

				INSERT OR IGNORE INTO consolidation_state
					(id, last_consolidated_at, total_sessions_processed, total_entries_created, total_entries_updated)
				VALUES (1, 0, 0, 0, 0);
			`);
		},
	},
	{
		version: 2,
		label: "pending_episode_attempts table",
		up: (db) => {
			// Failed-attempt bookkeeping, kept apart from pending_episodes. A separate table
			// means a schema upgrade never needs ALTER TABLE on pending_episodes, which a
			// non-owner role cannot run on a shared Postgres state DB.
			db.exec(`
				CREATE TABLE IF NOT EXISTS pending_episode_attempts (
					id              TEXT    PRIMARY KEY,
					failed_attempts INTEGER NOT NULL DEFAULT 0,
					last_error      TEXT
				);
			`);
		},
	},
	{
		version: 3,
		label: "activation_event table (content-free measurement)",
		up: (db) => {
			// Content-free telemetry, one row per /activate result. Never holds prompt or
			// entry text. Pruned after 90 days (see src/activation/events.ts).
			db.exec(`
				CREATE TABLE IF NOT EXISTS activation_event (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					${activationEventColumnDefs("sqlite", 3)}
				);

				CREATE INDEX IF NOT EXISTS idx_activation_event_ts
					ON activation_event(ts);

				CREATE INDEX IF NOT EXISTS idx_activation_event_client_ts
					ON activation_event(client, ts);
			`);
		},
	},
	{
		version: 4,
		label: "activation_event gate and budget counts (precision gate)",
		up: (db) => addActivationEventColumns(db, 4),
	},
	{
		version: 5,
		label: "activation_event rerank model (scorer of rerank_scores)",
		up: (db) => addActivationEventColumns(db, 5),
	},
	{
		version: 6,
		label: "pending_episodes session_id index (duplicate check, queryRanges)",
		up: (db) => {
			db.exec(`
				CREATE INDEX IF NOT EXISTS idx_pending_session
					ON pending_episodes(session_id);
			`);
		},
	},
];

export const SERVER_LOCAL_SCHEMA_VERSION = latestVersion(
	SERVER_LOCAL_MIGRATIONS,
);

/** Add the activation_event columns introduced in `version` that the table lacks. */
function addActivationEventColumns(db: Database, version: number): void {
	const present = new Set(
		(
			db.prepare("PRAGMA table_info(activation_event)").all() as Array<{
				name: string;
			}>
		).map((c) => c.name),
	);
	for (const column of ACTIVATION_EVENT_COLUMNS) {
		if (column.since === version && !present.has(column.name)) {
			db.exec(
				`ALTER TABLE activation_event ADD COLUMN ${column.name} ${column.sqlite}`,
			);
		}
	}
}
