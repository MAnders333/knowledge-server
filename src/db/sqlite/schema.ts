/**
 * SQLite schema for the knowledge graph.
 *
 * Design principles:
 * - Embeddings stored as BLOB (raw float32 arrays) for efficient cosine similarity
 * - Timestamps in unix milliseconds (consistent with OpenCode)
 * - Topics stored as JSON array (queryable via json_each)
 * - Derived-from stored as JSON array of session/entry IDs (provenance chain)
 *
 * v5: Added observation_count — evidence signal (how many episodes produced this
 * knowledge), kept separate from access_count (retrieval signal). Removed all
 * migration code — single clean schema, DB is reinitialized on upgrade.
 *
 * v6: Multi-source support.
 * - consolidated_episode gains a `source` column (e.g. "opencode", "claude-code")
 *   and a new composite PK (source, session_id, start_message_id, end_message_id).
 * - New source_cursor table replaces the per-source cursor previously embedded in
 *   consolidation_state. Keyed by source; stores the per-source high-water mark
 *   (last_message_time_created) and the last time that source was consolidated.
 * - consolidation_state retains the global counters and lastConsolidatedAt but
 *   last_message_time_created is removed (superseded by source_cursor).
 *
 * v7: Cross-session synthesis (per-entry trigger).
 * - knowledge_entry gains `last_synthesized_observation_count` (INTEGER, NULL).
 *   MIGRATION: v6 → v7 is incremental (ALTER TABLE, no data loss).
 *
 * v8: Embedding metadata.
 * - New embedding_metadata singleton table stores the model name and dimension
 *   count used to produce the current embeddings. On startup, the server compares
 *   the stored model against the configured EMBEDDING_MODEL — if they differ, all
 *   entry embeddings are regenerated automatically (re-embed) so cosine similarity
 *   remains valid across model changes.
 *   MIGRATION: v7 → v8 is incremental (CREATE TABLE, no data loss).
 *
 * v9: Cluster-first synthesis.
 * - Replaces per-entry synthesis trigger (observation_count threshold) with
 *   persistent cluster tables. Each consolidation run greedily clusters active
 *   entries by embedding similarity, matches clusters to persisted rows by centroid
 *   similarity, and synthesizes any cluster whose membership changed since last
 *   synthesis (last_membership_changed_at > last_synthesized_at).
 * - New tables: knowledge_cluster, knowledge_cluster_member.
 * - knowledge_entry loses `last_synthesized_observation_count` (no longer needed).
 * - MIGRATION: v8 → v9 is incremental: DROP COLUMN on knowledge_entry, CREATE TABLE
 *   for both cluster tables. No knowledge data is lost.
 *
 * v10: is_synthesized column.
 * - knowledge_entry gains `is_synthesized INTEGER NOT NULL DEFAULT 0`.
 *   Replaces the fragile source.startsWith('synthesis:') convention with an
 *   authoritative persistent flag. Existing synthesis entries are backfilled.
 * - MIGRATION: v9 → v10 is incremental: ALTER TABLE + UPDATE backfill. No data loss.
 *
 * v11: Multi-user cursor support.
 * - source_cursor gains a `user_id` column and a new composite PK (source, user_id).
 *   Each user running against a shared DB advances their own cursor independently.
 * - consolidated_episode gains a `user_id` column and is added to the PK.
 *   Prevents one user's processed episodes from blocking another user's consolidation.
 * - user_id defaults to 'default' for backwards-compatible single-user mode.
 *   In multi-user setups, set user_id via the KNOWLEDGE_USER_ID env var or config.jsonc.
 * - MIGRATION: v10 → v11 is incremental: ALTER TABLE + PK rebuild via drop+recreate.
 *
 * v12: Episode uploader daemon support.
 * - pending_episodes: staging table where the daemon writes raw episodes before
 *   the server consolidates them. Decouples episode reading (done on the user's
 *   machine by the daemon) from consolidation (done on the server). Enables
 *   cross-device and multi-user setups where the server can't read local files.
 * - daemon_cursor: per-source high-water mark for the daemon.
 * - MIGRATION: v11 → v12 is additive: CREATE TABLE IF NOT EXISTS — no data loss.
 *
 * v13: Daemon-only consolidation — remove direct file reader infrastructure.
 * - source_cursor table removed: was the high-water mark for direct file readers.
 *   With daemon-only consolidation, pending_episodes is self-draining (rows deleted
 *   after consolidation) and consolidated_episode provides idempotency — no cursor needed.
 * - user_id removed from consolidated_episode: consolidation drains all pending episodes
 *   regardless of origin; user_id on pending_episodes and consolidated_episode is now
 *   provenance metadata only, not a routing or filtering key.
 * - MIGRATION: v12 → v13 drops source_cursor and rebuilds consolidated_episode without
 *   user_id. Knowledge data is not affected.
 */

/**
 * v14: Drop staging tables from Postgres knowledge stores.
 * - consolidated_episode, consolidation_state, pending_episodes now live
 *   exclusively in state.db (ServerStateDB local SQLite).
 * - These tables are dropped from Postgres on migration.
 * - SQLite knowledge.db: no change — these tables were already handled in v13.
 */
/**
 * v17: pgvector ANN repair migration (Postgres-only) + startup self-heal.
 * - Existing Postgres stores that were already at v16 can have `embedding_vec`
 *   present but unpopulated and/or missing ANN indexes (legacy rollout edge case).
 * - v17 applies the repair migration on Postgres; SQLite remains a no-op.
 */
export const SCHEMA_VERSION = 17;

/**
 * Expected columns for each table, derived from the DDL below.
 * Used by initialize() to detect schema drift (missing columns) independently
 * of the schema_version number — catches partial startups where the version was
 * written before the DROP+recreate completed.
 *
 * Keep in sync with CREATE_TABLES. Adding a column to the DDL requires adding
 * it here too — the drift check will then catch any DB that's missing it.
 */
export const EXPECTED_TABLE_COLUMNS: Readonly<
	Record<string, readonly string[]>
> = {
	knowledge_entry: [
		"id",
		"type",
		"content",
		"topics",
		"confidence",
		"source",
		"status",
		"strength",
		"created_at",
		"updated_at",
		"last_accessed_at",
		"access_count",
		"observation_count",
		"superseded_by",
		"derived_from",
		"is_synthesized",
		"embedding",
	],
	knowledge_relation: ["id", "source_id", "target_id", "type", "created_at"],
	knowledge_cluster: [
		"id",
		"centroid",
		"member_count",
		"last_synthesized_at",
		"last_membership_changed_at",
		"created_at",
	],
	knowledge_cluster_member: ["cluster_id", "entry_id", "joined_at"],
	embedding_metadata: ["id", "model", "dimensions", "recorded_at"],
	// Note: consolidation_state, consolidated_episode, pending_episodes, daemon_cursor
	// have moved to state.db (ServerStateDB) in v13+. They are no longer created
	// in knowledge.db — omitting them here prevents false-positive drift warnings
	// on new-architecture installs where these tables are absent from knowledge.db.
};

export const CREATE_TABLES = `
  -- Schema version tracking
  CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at INTEGER NOT NULL
  );

  -- Core knowledge entries
  CREATE TABLE IF NOT EXISTS knowledge_entry (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('fact', 'principle', 'pattern', 'decision', 'procedure')),
    content TEXT NOT NULL,
    topics TEXT NOT NULL DEFAULT '[]',  -- JSON array of strings
    confidence REAL NOT NULL DEFAULT 0.5 CHECK(confidence >= 0 AND confidence <= 1),
    source TEXT NOT NULL DEFAULT '',
    -- Lifecycle
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived', 'superseded', 'conflicted', 'tombstoned')),
    strength REAL NOT NULL DEFAULT 1.0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_accessed_at INTEGER NOT NULL,
    access_count INTEGER NOT NULL DEFAULT 0,
    observation_count INTEGER NOT NULL DEFAULT 1,
    
    -- Provenance
    superseded_by TEXT,
    derived_from TEXT NOT NULL DEFAULT '[]',  -- JSON array of session/entry IDs
    is_synthesized INTEGER NOT NULL DEFAULT 0, -- 1 when produced by the synthesis pass

    -- Embedding (float32 array stored as blob)
    embedding BLOB
  );

  -- Indices for common queries
  CREATE INDEX IF NOT EXISTS idx_entry_status ON knowledge_entry(status);
  CREATE INDEX IF NOT EXISTS idx_entry_type ON knowledge_entry(type);

  CREATE INDEX IF NOT EXISTS idx_entry_strength ON knowledge_entry(strength);
  CREATE INDEX IF NOT EXISTS idx_entry_created ON knowledge_entry(created_at);
  CREATE INDEX IF NOT EXISTS idx_entry_accessed ON knowledge_entry(last_accessed_at);

  -- Relationships between entries
  CREATE TABLE IF NOT EXISTS knowledge_relation (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('supports', 'contradicts', 'supersedes')),
    created_at INTEGER NOT NULL,
    FOREIGN KEY (source_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_relation_source ON knowledge_relation(source_id);
  CREATE INDEX IF NOT EXISTS idx_relation_target ON knowledge_relation(target_id);
  CREATE INDEX IF NOT EXISTS idx_relation_type ON knowledge_relation(type);

  -- Synthesis clusters — persistent embedding-similarity groups of knowledge entries.
  -- Rebuilt each consolidation run (greedy agglomerative clustering). Matched to
  -- existing rows by centroid similarity so stable clusters keep their ID across runs.
  -- A cluster is ripe for synthesis when last_membership_changed_at > last_synthesized_at
  -- (or last_synthesized_at IS NULL). Synthesized entries are regular knowledge_entry
  -- rows and participate in future cluster formation just like extracted entries.
  CREATE TABLE IF NOT EXISTS knowledge_cluster (
    id                         TEXT    PRIMARY KEY,
    centroid                   BLOB    NOT NULL,   -- running-average embedding of members (float32)
    member_count               INTEGER NOT NULL DEFAULT 0,
    last_synthesized_at        INTEGER,            -- NULL = never synthesized
    last_membership_changed_at INTEGER NOT NULL,   -- updated on any member add/remove
    created_at                 INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS knowledge_cluster_member (
    cluster_id  TEXT    NOT NULL REFERENCES knowledge_cluster(id) ON DELETE CASCADE,
    entry_id    TEXT    NOT NULL REFERENCES knowledge_entry(id)   ON DELETE CASCADE,
    joined_at   INTEGER NOT NULL,
    PRIMARY KEY (cluster_id, entry_id)
  );

  CREATE INDEX IF NOT EXISTS idx_cluster_membership_changed ON knowledge_cluster(last_membership_changed_at);
  CREATE INDEX IF NOT EXISTS idx_cluster_member_entry ON knowledge_cluster_member(entry_id);

  -- Embedding metadata — singleton row tracking the model and dimensions used
  -- to produce the current embeddings. Compared against the configured model at
  -- startup; a mismatch triggers a full re-embed of all entries.
  CREATE TABLE IF NOT EXISTS embedding_metadata (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),  -- singleton row
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    recorded_at INTEGER NOT NULL
  );

  -- Note: pending_episodes, daemon_cursor, consolidated_episode, and
  -- consolidation_state are NOT created here. As of v13, these staging and
  -- bookkeeping tables live in state.db (ServerStateDB), not in knowledge.db.
`;
