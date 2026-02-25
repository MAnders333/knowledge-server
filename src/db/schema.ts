/**
 * SQLite schema for the knowledge graph.
 *
 * Design principles:
 * - Embeddings stored as BLOB (raw float32 arrays) for efficient cosine similarity
 * - Timestamps in unix milliseconds (consistent with OpenCode)
 * - Topics stored as JSON array (queryable via json_each)
 * - Derived-from stored as JSON array of session/entry IDs (provenance chain)
 */

export const SCHEMA_VERSION = 4;

/**
 * DDL for the consolidated_episode table (v3 schema).
 * Exported as a standalone constant so the runtime migration in database.ts
 * can reference the same definition — preventing the two copies from drifting.
 *
 * An episode is uniquely identified by (session_id, start_message_id, end_message_id).
 * Message IDs are stable UUIDs from the OpenCode DB — they never shift even as a
 * session grows with new messages, unlike the old segment_index approach where
 * token-chunk boundaries could move when new messages were appended.
 *
 * This allows running consolidate() twice in the same session:
 *   1st run: records episodes up to the last message at that point
 *   2nd run: only new messages (after the last processed end_message_id) are picked up
 */
export const CONSOLIDATED_EPISODE_DDL = `
  CREATE TABLE IF NOT EXISTS consolidated_episode (
    session_id       TEXT    NOT NULL,
    start_message_id TEXT    NOT NULL,       -- first message ID in this episode (inclusive)
    end_message_id   TEXT    NOT NULL,       -- last message ID in this episode (inclusive)
    content_type     TEXT    NOT NULL,       -- 'compaction_summary' | 'messages'
    processed_at     INTEGER NOT NULL,       -- unix ms when this episode was consolidated
    entries_created  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, start_message_id, end_message_id)
  );

  CREATE INDEX IF NOT EXISTS idx_episode_session ON consolidated_episode(session_id);
  CREATE INDEX IF NOT EXISTS idx_episode_processed ON consolidated_episode(processed_at);
`;

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
    last_message_time_created INTEGER NOT NULL DEFAULT 0,  -- max message.time_created processed (v4 cursor)
    last_message_cursor_seeded INTEGER NOT NULL DEFAULT 0,  -- v4 migration sentinel; 1 = seed complete
    total_sessions_processed INTEGER NOT NULL DEFAULT 0,
    total_entries_created INTEGER NOT NULL DEFAULT 0,
    total_entries_updated INTEGER NOT NULL DEFAULT 0
  );

  -- Initialize consolidation state if not present
  INSERT OR IGNORE INTO consolidation_state (id, last_consolidated_at, last_message_time_created, last_message_cursor_seeded, total_sessions_processed, total_entries_created, total_entries_updated)
  VALUES (1, 0, 0, 1, 0, 0, 0);

  -- Per-episode processing log — enables incremental within-session consolidation.
  -- See CONSOLIDATED_EPISODE_DDL for the table definition (shared with the v3 migration).
  ${CONSOLIDATED_EPISODE_DDL}
`;
