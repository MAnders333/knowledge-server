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

export const SERVER_LOCAL_SCHEMA_VERSION = 1;

// Schema is always additive (CREATE TABLE IF NOT EXISTS), so column-manifest
// drift detection is not used for state.db. See KnowledgeDB (knowledge.db)
// for the EXPECTED_TABLE_COLUMNS pattern used when the schema has a more
// complex migration history requiring column-presence assertions.

export const SERVER_LOCAL_CREATE_TABLES = `
  CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER NOT NULL,
    applied_at INTEGER NOT NULL
  );

  -- One-time migration tracking — records which legacy DBs have been migrated
  -- into this state.db. Prevents re-running migrations on every startup.
  CREATE TABLE IF NOT EXISTS applied_migrations (
    name       TEXT    PRIMARY KEY,
    applied_at INTEGER NOT NULL
  );

  -- Pending episodes — staging table written by the daemon, drained by the server.
  -- user_id is set by the daemon from KNOWLEDGE_USER_ID / hostname (provenance only).
  -- Rows are deleted after successful consolidation.
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
`;
