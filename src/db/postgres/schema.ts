/**
 * PostgreSQL schema for the knowledge graph.
 *
 * Mirror of schema.ts (SQLite) adapted for PostgreSQL syntax.
 * Key differences:
 * - BLOB → BYTEA
 * - REAL → DOUBLE PRECISION
 * - json_each() → jsonb_array_elements_text()
 * - INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
 * - INSERT OR REPLACE → INSERT ... ON CONFLICT ... DO UPDATE
 * - No PRAGMA support (WAL, foreign_keys are handled differently)
 *
 * Schema version is kept in sync with the SQLite schema.
 *
 * v16 (pgvector):
 * `embedding_vec vector(N)` is NOT included in PG_CREATE_TABLES because the
 * dimension N is only known after the first setEmbeddingMetadata() call.
 * PostgresKnowledgeDB.ensureVectorColumn() creates it lazily and is called:
 *   - from setEmbeddingMetadata() (first call / model change)
 *   - from _initialize() when embedding_metadata already exists (upgrade path)
 * The HNSW index is created by ensureVectorColumn() as well.
 */

export { SCHEMA_VERSION } from "../sqlite/schema.js";

export const PG_CREATE_TABLES = `
  -- Schema version tracking
  CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at BIGINT NOT NULL
  );

  -- Core knowledge entries
  CREATE TABLE IF NOT EXISTS knowledge_entry (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('fact', 'principle', 'pattern', 'decision', 'procedure')),
    content TEXT NOT NULL,
    topics JSONB NOT NULL DEFAULT '[]',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK(confidence >= 0 AND confidence <= 1),
    source TEXT NOT NULL DEFAULT '',
    -- Lifecycle
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'archived', 'superseded', 'conflicted', 'tombstoned')),
    strength DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    last_accessed_at BIGINT NOT NULL,
    access_count INTEGER NOT NULL DEFAULT 0,
    observation_count INTEGER NOT NULL DEFAULT 1,

    -- Provenance
    superseded_by TEXT,
    derived_from JSONB NOT NULL DEFAULT '[]',
    is_synthesized INTEGER NOT NULL DEFAULT 0,

    -- Embedding (float32 array stored as bytea)
    embedding BYTEA
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
    created_at BIGINT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES knowledge_entry(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_relation_source ON knowledge_relation(source_id);
  CREATE INDEX IF NOT EXISTS idx_relation_target ON knowledge_relation(target_id);
  CREATE INDEX IF NOT EXISTS idx_relation_type ON knowledge_relation(type);

  -- Synthesis clusters — persistent embedding-similarity groups of knowledge entries.
  -- Note: consolidation_state, consolidated_episode, pending_episodes, and daemon_cursor
  -- are NOT created here — they live in state.db (ServerStateDB, local SQLite).
  -- See src/db/state/schema.ts for those tables.
  CREATE TABLE IF NOT EXISTS knowledge_cluster (
    id TEXT PRIMARY KEY,
    centroid BYTEA NOT NULL,
    member_count INTEGER NOT NULL DEFAULT 0,
    last_synthesized_at BIGINT,
    last_membership_changed_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS knowledge_cluster_member (
    cluster_id TEXT NOT NULL REFERENCES knowledge_cluster(id) ON DELETE CASCADE,
    entry_id TEXT NOT NULL REFERENCES knowledge_entry(id) ON DELETE CASCADE,
    joined_at BIGINT NOT NULL,
    PRIMARY KEY (cluster_id, entry_id)
  );

  CREATE INDEX IF NOT EXISTS idx_cluster_membership_changed ON knowledge_cluster(last_membership_changed_at);
  CREATE INDEX IF NOT EXISTS idx_cluster_member_entry ON knowledge_cluster_member(entry_id);

  -- Embedding metadata — singleton row tracking the model and dimensions.
  CREATE TABLE IF NOT EXISTS embedding_metadata (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK(id = 1),
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    recorded_at BIGINT NOT NULL
  );

`;
