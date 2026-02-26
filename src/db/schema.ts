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
 */

export const SCHEMA_VERSION = 5;

/**
 * Expected columns for each table, derived from the DDL below.
 * Used by initialize() to detect schema drift (missing columns) independently
 * of the schema_version number — catches partial startups where the version was
 * written before the DROP+recreate completed.
 *
 * Keep in sync with CREATE_TABLES. Adding a column to the DDL requires adding
 * it here too — the drift check will then catch any DB that's missing it.
 */
export const EXPECTED_TABLE_COLUMNS: Readonly<Record<string, readonly string[]>> = {
  knowledge_entry: [
    "id", "type", "content", "topics", "confidence", "source", "scope",
    "status", "strength", "created_at", "updated_at", "last_accessed_at",
    "access_count", "observation_count", "superseded_by", "derived_from", "embedding",
  ],
  knowledge_relation: [
    "id", "source_id", "target_id", "type", "created_at",
  ],
  consolidation_state: [
    "id", "last_consolidated_at", "last_message_time_created",
    "total_sessions_processed", "total_entries_created", "total_entries_updated",
  ],
  consolidated_episode: [
    "session_id", "start_message_id", "end_message_id",
    "content_type", "processed_at", "entries_created",
  ],
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
    scope TEXT NOT NULL DEFAULT 'personal' CHECK(scope IN ('personal', 'team')),
    
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
    
    -- Embedding (float32 array stored as blob)
    embedding BLOB
  );

  -- Indices for common queries
  CREATE INDEX IF NOT EXISTS idx_entry_status ON knowledge_entry(status);
  CREATE INDEX IF NOT EXISTS idx_entry_type ON knowledge_entry(type);
  CREATE INDEX IF NOT EXISTS idx_entry_scope ON knowledge_entry(scope);
  CREATE INDEX IF NOT EXISTS idx_entry_strength ON knowledge_entry(strength);
  CREATE INDEX IF NOT EXISTS idx_entry_created ON knowledge_entry(created_at);
  CREATE INDEX IF NOT EXISTS idx_entry_accessed ON knowledge_entry(last_accessed_at);

  -- Relationships between entries
  CREATE TABLE IF NOT EXISTS knowledge_relation (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('supports', 'contradicts', 'refines', 'depends_on', 'supersedes')),
    created_at INTEGER NOT NULL,
    FOREIGN KEY (source_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_relation_source ON knowledge_relation(source_id);
  CREATE INDEX IF NOT EXISTS idx_relation_target ON knowledge_relation(target_id);
  CREATE INDEX IF NOT EXISTS idx_relation_type ON knowledge_relation(type);

  -- Consolidation state (message-time cursor + summary counters)
  CREATE TABLE IF NOT EXISTS consolidation_state (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),  -- singleton row
    last_consolidated_at INTEGER NOT NULL DEFAULT 0,
    last_message_time_created INTEGER NOT NULL DEFAULT 0,
    total_sessions_processed INTEGER NOT NULL DEFAULT 0,
    total_entries_created INTEGER NOT NULL DEFAULT 0,
    total_entries_updated INTEGER NOT NULL DEFAULT 0
  );

  INSERT OR IGNORE INTO consolidation_state (id, last_consolidated_at, last_message_time_created, total_sessions_processed, total_entries_created, total_entries_updated)
  VALUES (1, 0, 0, 0, 0, 0);

  -- Per-episode processing log — enables incremental within-session consolidation.
  CREATE TABLE IF NOT EXISTS consolidated_episode (
    session_id       TEXT    NOT NULL,
    start_message_id TEXT    NOT NULL,
    end_message_id   TEXT    NOT NULL,
    content_type     TEXT    NOT NULL,
    processed_at     INTEGER NOT NULL,
    entries_created  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, start_message_id, end_message_id)
  );

  CREATE INDEX IF NOT EXISTS idx_episode_session ON consolidated_episode(session_id);
  CREATE INDEX IF NOT EXISTS idx_episode_processed ON consolidated_episode(processed_at);
`;
