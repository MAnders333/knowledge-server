import { Database } from "bun:sqlite";
import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import { config } from "../config.js";
import { CREATE_TABLES, SCHEMA_VERSION } from "./schema.js";
import type {
  KnowledgeEntry,
  KnowledgeRelation,
  KnowledgeStatus,
  ConsolidationState,
} from "../types.js";

/**
 * Database layer for the knowledge graph.
 *
 * Uses bun:sqlite (Bun's native SQLite binding) for all operations:
 * CRUD for entries/relations, embedding storage/retrieval,
 * and consolidation state management.
 */
export class KnowledgeDB {
  private db: Database;

  constructor(dbPath?: string) {
    const path = dbPath || config.dbPath;
    const dir = dirname(path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(path);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.initialize();
  }

  private initialize(): void {
    this.db.exec(CREATE_TABLES);

    // Apply incremental migrations for existing databases.
    // ALTER TABLE is idempotent via try/catch — SQLite throws if the column
    // already exists, which we safely ignore.
    this.runMigrations();

    // Record schema version if not present
    const row = this.db
      .prepare("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
      .get() as { version: number } | null;

    if (!row) {
      this.db
        .prepare("INSERT INTO schema_version (version, applied_at) VALUES (?, ?)")
        .run(SCHEMA_VERSION, Date.now());
    } else if (row.version < SCHEMA_VERSION) {
      this.db
        .prepare("INSERT INTO schema_version (version, applied_at) VALUES (?, ?)")
        .run(SCHEMA_VERSION, Date.now());
    }
  }

  /**
   * Incremental migrations for existing databases.
   * Each migration is wrapped in try/catch so it's safe to run on every startup —
   * if the column/table already exists, SQLite throws and we ignore it.
   */
  private runMigrations(): void {
    // v2: add total_entries_updated to consolidation_state
    try {
      this.db.exec(
        "ALTER TABLE consolidation_state ADD COLUMN total_entries_updated INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // Column already exists — no-op
    }

    // v2: add consolidated_episode table (reserved for future fine-grained idempotency)
    try {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS consolidated_episode (
          session_id TEXT NOT NULL,
          segment_index INTEGER NOT NULL DEFAULT 0,
          content_type TEXT NOT NULL,
          processed_at INTEGER NOT NULL,
          entries_created INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (session_id, segment_index, content_type)
        )
      `);
      this.db.exec(
        "CREATE INDEX IF NOT EXISTS idx_episode_session ON consolidated_episode(session_id)"
      );
      this.db.exec(
        "CREATE INDEX IF NOT EXISTS idx_episode_processed ON consolidated_episode(processed_at)"
      );
    } catch {
      // Table already exists — no-op
    }
  }

  // ── Entry CRUD ──

  insertEntry(entry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] }): void {
    const embeddingBlob = entry.embedding
      ? new Uint8Array(new Float32Array(entry.embedding).buffer)
      : null;

    this.db
      .prepare(
        `INSERT INTO knowledge_entry 
         (id, type, content, topics, confidence, source, scope, status, strength,
          created_at, updated_at, last_accessed_at, access_count,
          superseded_by, derived_from, embedding)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        entry.id,
        entry.type,
        entry.content,
        JSON.stringify(entry.topics),
        entry.confidence,
        entry.source,
        entry.scope,
        entry.status,
        entry.strength,
        entry.createdAt,
        entry.updatedAt,
        entry.lastAccessedAt,
        entry.accessCount,
        entry.supersededBy,
        JSON.stringify(entry.derivedFrom),
        embeddingBlob
      );
  }

  updateEntry(id: string, updates: Partial<KnowledgeEntry>): void {
    const fields: string[] = [];
    const values: unknown[] = [];

    if (updates.content !== undefined) {
      fields.push("content = ?");
      values.push(updates.content);
    }
    if (updates.topics !== undefined) {
      fields.push("topics = ?");
      values.push(JSON.stringify(updates.topics));
    }
    if (updates.confidence !== undefined) {
      fields.push("confidence = ?");
      values.push(updates.confidence);
    }
    if (updates.status !== undefined) {
      fields.push("status = ?");
      values.push(updates.status);
    }
    if (updates.strength !== undefined) {
      fields.push("strength = ?");
      values.push(updates.strength);
    }
    if (updates.supersededBy !== undefined) {
      fields.push("superseded_by = ?");
      values.push(updates.supersededBy);
    }
    if (updates.scope !== undefined) {
      fields.push("scope = ?");
      values.push(updates.scope);
    }
    if (updates.embedding !== undefined) {
      fields.push("embedding = ?");
      values.push(new Uint8Array(new Float32Array(updates.embedding).buffer));
    }

    // Always update timestamp
    fields.push("updated_at = ?");
    values.push(Date.now());

    values.push(id);

    this.db
      .prepare(`UPDATE knowledge_entry SET ${fields.join(", ")} WHERE id = ?`)
      .run(...values);
  }

  getEntry(id: string): KnowledgeEntry | null {
    const row = this.db
      .prepare("SELECT * FROM knowledge_entry WHERE id = ?")
      .get(id) as RawEntryRow | null;

    return row ? this.rowToEntry(row) : null;
  }

  /**
   * Get all active entries (for consolidation context and activation).
   */
  getActiveEntries(): KnowledgeEntry[] {
    const rows = this.db
      .prepare(
        "SELECT * FROM knowledge_entry WHERE status = 'active' ORDER BY strength DESC"
      )
      .all() as RawEntryRow[];

    return rows.map((r) => this.rowToEntry(r));
  }

  /**
   * Get all active entries that have embeddings (for similarity search).
   */
  getActiveEntriesWithEmbeddings(): Array<KnowledgeEntry & { embedding: number[] }> {
    const rows = this.db
      .prepare(
        "SELECT * FROM knowledge_entry WHERE status = 'active' AND embedding IS NOT NULL ORDER BY strength DESC"
      )
      .all() as RawEntryRow[];

    return rows
      .map((r) => this.rowToEntry(r))
      .filter((e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding);
  }

  /**
   * Get entries by status (for review, decay processing, etc.)
   */
  getEntriesByStatus(status: KnowledgeStatus): KnowledgeEntry[] {
    const rows = this.db
      .prepare("SELECT * FROM knowledge_entry WHERE status = ? ORDER BY updated_at DESC")
      .all(status) as RawEntryRow[];

    return rows.map((r) => this.rowToEntry(r));
  }

  /**
   * Get all entries (including non-active) for stats/review.
   */
  getAllEntries(): KnowledgeEntry[] {
    const rows = this.db
      .prepare("SELECT * FROM knowledge_entry ORDER BY created_at DESC")
      .all() as RawEntryRow[];

    return rows.map((r) => this.rowToEntry(r));
  }

  /**
   * Record an access (bump access count and last_accessed_at).
   */
  recordAccess(id: string): void {
    this.db
      .prepare(
        `UPDATE knowledge_entry 
         SET access_count = access_count + 1, last_accessed_at = ?, updated_at = ?
         WHERE id = ?`
      )
      .run(Date.now(), Date.now(), id);
  }

  /**
   * Batch update strength scores (used during decay).
   */
  updateStrength(id: string, strength: number): void {
    this.db
      .prepare("UPDATE knowledge_entry SET strength = ?, updated_at = ? WHERE id = ?")
      .run(strength, Date.now(), id);
  }

  /**
   * Count entries by status.
   */
  getStats(): Record<string, number> {
    const rows = this.db
      .prepare("SELECT status, COUNT(*) as count FROM knowledge_entry GROUP BY status")
      .all() as Array<{ status: string; count: number }>;

    const stats: Record<string, number> = { total: 0 };
    for (const row of rows) {
      stats[row.status] = row.count;
      stats.total += row.count;
    }
    return stats;
  }

  // ── Relations ──

  insertRelation(relation: KnowledgeRelation): void {
    this.db
      .prepare(
        "INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at) VALUES (?, ?, ?, ?, ?)"
      )
      .run(relation.id, relation.sourceId, relation.targetId, relation.type, relation.createdAt);
  }

  getRelationsFor(entryId: string): KnowledgeRelation[] {
    const rows = this.db
      .prepare(
        "SELECT * FROM knowledge_relation WHERE source_id = ? OR target_id = ?"
      )
      .all(entryId, entryId) as Array<{
      id: string;
      source_id: string;
      target_id: string;
      type: string;
      created_at: number;
    }>;

    return rows.map((r) => ({
      id: r.id,
      sourceId: r.source_id,
      targetId: r.target_id,
      type: r.type as KnowledgeRelation["type"],
      createdAt: r.created_at,
    }));
  }

  // ── Consolidation State ──

  getConsolidationState(): ConsolidationState {
    const row = this.db
      .prepare("SELECT * FROM consolidation_state WHERE id = 1")
      .get() as {
      last_consolidated_at: number;
      last_session_time_created: number;
      total_sessions_processed: number;
      total_entries_created: number;
      total_entries_updated: number;
    };

    return {
      lastConsolidatedAt: row.last_consolidated_at,
      lastSessionTimeCreated: row.last_session_time_created,
      totalSessionsProcessed: row.total_sessions_processed,
      totalEntriesCreated: row.total_entries_created,
      // Column added in schema v2 — default to 0 for existing DBs without migration
      totalEntriesUpdated: row.total_entries_updated ?? 0,
    };
  }

  updateConsolidationState(state: Partial<ConsolidationState>): void {
    const fields: string[] = [];
    const values: unknown[] = [];

    if (state.lastConsolidatedAt !== undefined) {
      fields.push("last_consolidated_at = ?");
      values.push(state.lastConsolidatedAt);
    }
    if (state.lastSessionTimeCreated !== undefined) {
      fields.push("last_session_time_created = ?");
      values.push(state.lastSessionTimeCreated);
    }
    if (state.totalSessionsProcessed !== undefined) {
      fields.push("total_sessions_processed = ?");
      values.push(state.totalSessionsProcessed);
    }
    if (state.totalEntriesCreated !== undefined) {
      fields.push("total_entries_created = ?");
      values.push(state.totalEntriesCreated);
    }
    if (state.totalEntriesUpdated !== undefined) {
      fields.push("total_entries_updated = ?");
      values.push(state.totalEntriesUpdated);
    }

    if (fields.length === 0) return;

    this.db
      .prepare(`UPDATE consolidation_state SET ${fields.join(", ")} WHERE id = 1`)
      .run(...values);
  }

  // ── Helpers ──

  private rowToEntry(row: RawEntryRow): KnowledgeEntry {
    let embedding: number[] | undefined;
    if (row.embedding) {
      const buf = row.embedding as Uint8Array;
      const float32 = new Float32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4);
      embedding = Array.from(float32);
    }

    return {
      id: row.id,
      type: row.type as KnowledgeEntry["type"],
      content: row.content,
      topics: JSON.parse(row.topics),
      confidence: row.confidence,
      source: row.source,
      scope: row.scope as KnowledgeEntry["scope"],
      status: row.status as KnowledgeEntry["status"],
      strength: row.strength,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      lastAccessedAt: row.last_accessed_at,
      accessCount: row.access_count,
      supersededBy: row.superseded_by,
      derivedFrom: JSON.parse(row.derived_from),
      embedding,
    };
  }

  /**
   * Merge new content into an existing entry (reconsolidation).
   * Updates content, type, topics, confidence, and timestamps in place.
   * The entry's derivedFrom is expanded to include the new session IDs.
   */
  mergeEntry(
    id: string,
    updates: {
      content: string;
      type: string;
      topics: string[];
      confidence: number;
      additionalSources: string[]; // session IDs from the new episode
    }
  ): void {
    const existing = this.getEntry(id);
    if (!existing) return;

    const mergedSources = [
      ...new Set([...existing.derivedFrom, ...updates.additionalSources]),
    ];

    const now = Date.now();
    this.db
      .prepare(
        `UPDATE knowledge_entry
         SET content = ?, type = ?, topics = ?, confidence = ?,
             derived_from = ?, updated_at = ?, last_accessed_at = ?,
             embedding = NULL
         WHERE id = ?`
      )
      .run(
        updates.content,
        updates.type,
        JSON.stringify(updates.topics),
        updates.confidence,
        JSON.stringify(mergedSources),
        now,
        now,
        id
      );
  }

  /**
   * Wipe all knowledge entries, relations, and reset the consolidation cursor.
   * Used during development/iteration to start fresh with improved extraction.
   */
  reinitialize(): void {
    this.db.exec("DELETE FROM knowledge_relation");
    this.db.exec("DELETE FROM knowledge_entry");
    this.db.exec(
      `UPDATE consolidation_state SET 
        last_consolidated_at = 0,
        last_session_time_created = 0,
        total_sessions_processed = 0,
        total_entries_created = 0,
        total_entries_updated = 0
       WHERE id = 1`
    );
  }

  close(): void {
    this.db.close();
  }
}

/**
 * Raw row shape from SQLite (snake_case columns).
 */
interface RawEntryRow {
  id: string;
  type: string;
  content: string;
  topics: string;
  confidence: number;
  source: string;
  scope: string;
  status: string;
  strength: number;
  created_at: number;
  updated_at: number;
  last_accessed_at: number;
  access_count: number;
  superseded_by: string | null;
  derived_from: string;
  embedding: Uint8Array | null;
}
