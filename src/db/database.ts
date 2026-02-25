import { Database } from "bun:sqlite";
import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import { CREATE_TABLES, SCHEMA_VERSION, CONSOLIDATED_EPISODE_DDL } from "./schema.js";
import { KNOWLEDGE_TYPES } from "../types.js";
import type {
  KnowledgeEntry,
  KnowledgeRelation,
  KnowledgeStatus,
  KnowledgeType,
  ConsolidationState,
  ProcessedRange,
} from "../types.js";

/**
 * Clamp an LLM-returned type string to a valid KnowledgeType.
 * LLMs occasionally return values outside the schema CHECK constraint
 * (e.g. "fact/principle"), which would abort the SQLite transaction.
 * Falls back to "fact" — the broadest, least-assertive type.
 */
function clampKnowledgeType(type: string): KnowledgeType {
  if ((KNOWLEDGE_TYPES as readonly string[]).includes(type)) {
    return type as KnowledgeType;
  }
  console.warn(`[db] invalid knowledge type "${type}" — falling back to "fact"`);
  return "fact";
}

/**
 * Database layer for the knowledge graph.
 *
 * Uses bun:sqlite (Bun's native SQLite binding) for all operations:
 * CRUD for entries/relations, embedding storage/retrieval,
 * and consolidation state management.
 */
export class KnowledgeDB {
  private db: Database;

  constructor(dbPath?: string, opencodeDbPath?: string) {
    const path = dbPath || config.dbPath;
    const dir = dirname(path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(path);
    this.db.exec("PRAGMA journal_mode = WAL");
    // With WAL mode, NORMAL synchronous is safe and ~3x faster than FULL.
    // FULL is only needed for non-WAL journals.
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.initialize(opencodeDbPath || config.opencodeDbPath);
  }

  private initialize(opencodeDbPath: string): void {
    this.db.exec(CREATE_TABLES);

    // Apply incremental migrations for existing databases.
    // ALTER TABLE is idempotent via try/catch — SQLite throws if the column
    // already exists, which we safely ignore.
    this.runMigrations(opencodeDbPath);

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
  private runMigrations(opencodeDbPath: string): void {
    // v2: add total_entries_updated to consolidation_state
    try {
      this.db.exec(
        "ALTER TABLE consolidation_state ADD COLUMN total_entries_updated INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // Column already exists — no-op
    }

    // v2→v3: replace old consolidated_episode table (keyed by segment_index) with
    // the new message-ID-based schema (keyed by start_message_id, end_message_id).
    // Must run BEFORE v4 — v4 seeds last_message_time_created from consolidated_episode,
    // so the table must be in its correct schema before that query runs.
    // Only runs once: we detect whether the migration is needed by checking for the
    // presence of the new start_message_id column. If it's absent, the old schema
    // is in place and we drop + recreate. Old data was never actively written so
    // there is nothing to preserve.
    const hasNewSchema = (() => {
      // PRAGMA table_info returns 0 rows (not an error) when the table doesn't exist,
      // so this should never throw under normal conditions. Re-throw anything unexpected
      // rather than silently swallowing it and proceeding to DROP the table.
      const cols = this.db
        .prepare("PRAGMA table_info(consolidated_episode)")
        .all() as Array<{ name: string }>;
      return cols.some((c) => c.name === "start_message_id");
    })();

    if (!hasNewSchema) {
      // Wrap the DDL in a transaction so a mid-migration crash doesn't leave the
      // DB in a state where the table is dropped but not yet recreated.
      // Use db.transaction() (Bun's idiomatic API) rather than manual BEGIN/COMMIT
      // to avoid issues when called inside an existing transaction.
      // CONSOLIDATED_EPISODE_DDL is the single source of truth for this schema —
      // shared with CREATE_TABLES in schema.ts to prevent drift.
      this.db.transaction(() => {
        this.db.exec("DROP TABLE IF EXISTS consolidated_episode");
        // CONSOLIDATED_EPISODE_DDL uses CREATE TABLE IF NOT EXISTS — safe after the drop above.
        this.db.exec(CONSOLIDATED_EPISODE_DDL);
      })();
    }

    // v4: replace last_session_time_created with last_message_time_created.
    //
    // Must run AFTER v3 so consolidated_episode is in the correct schema before
    // we query it to seed the cursor value.
    //
    // We seed the new column to the max time_created of messages in the OpenCode DB
    // that belong to sessions already recorded in consolidated_episode. This avoids
    // a wasteful re-scan of all previously-consolidated sessions on the first post-
    // upgrade run: the cursor starts right where the old cursor effectively was, just
    // measured in message timestamps instead of session timestamps.
    //
    // If the OpenCode DB is unavailable (e.g. path misconfigured), we fall back to 0
    // and accept the one-time re-scan churn rather than failing startup.
    // v4 migration is two-phase because SQLite cannot roll back ALTER TABLE.
    // We track completion with a separate sentinel column so a crash between
    // ALTER TABLE and the seed UPDATE is recoverable on next startup:
    //   - Phase A: add last_message_time_created (idempotent ALTER TABLE)
    //   - Phase B: seed the value and mark completion via last_message_cursor_seeded
    // needsV4A and needsV4B are checked independently.
    const v4Cols = (() => {
      const cols = this.db
        .prepare("PRAGMA table_info(consolidation_state)")
        .all() as Array<{ name: string }>;
      return new Set(cols.map((c) => c.name));
    })();

    // Phase A: add the cursor column (safe to re-run — ALTER TABLE throws if column exists)
    if (!v4Cols.has("last_message_time_created")) {
      this.db.exec(
        "ALTER TABLE consolidation_state ADD COLUMN last_message_time_created INTEGER NOT NULL DEFAULT 0"
      );
    }

    // Phase B: add the seeded sentinel and run the seed query.
    // Runs if the sentinel column doesn't exist yet — meaning Phase B never completed.
    // This correctly re-runs the seed if the process crashed after Phase A but before
    // the UPDATE committed (the sentinel is written atomically in the same transaction).
    // Note: v4Cols is a pre-Phase-A snapshot. Using it for Phase B is safe because
    // Phase A only adds `last_message_time_created`, not `last_message_cursor_seeded`.
    if (!v4Cols.has("last_message_cursor_seeded")) {
      this.db.exec(
        "ALTER TABLE consolidation_state ADD COLUMN last_message_cursor_seeded INTEGER NOT NULL DEFAULT 0"
      );

      // Seed from OpenCode DB: max message.time_created for already-consolidated sessions.
      // Use the already-imported Database class — no require() needed.
      let seedValue = 0;
      if (existsSync(opencodeDbPath)) {
        try {
          const opencodeDb = new Database(opencodeDbPath, { readonly: true });
          try {
            const processedSessionIds = (
              this.db
                .prepare("SELECT DISTINCT session_id FROM consolidated_episode")
                .all() as Array<{ session_id: string }>
            ).map((r) => r.session_id);

            if (processedSessionIds.length > 0) {
              const row = opencodeDb
                .prepare(
                  `SELECT MAX(time_created) as max_time FROM message
                   WHERE session_id IN (SELECT value FROM json_each(?))`
                )
                .get(JSON.stringify(processedSessionIds)) as { max_time: number | null };
              seedValue = row?.max_time ?? 0;
            }
          } finally {
            opencodeDb.close();
          }
        } catch (err) {
          console.warn("[db] v4 migration: could not seed last_message_time_created from OpenCode DB:", err);
        }
      }

      // Write seed value and sentinel atomically so a crash here is detectable on retry.
      this.db.transaction(() => {
        this.db
          .prepare("UPDATE consolidation_state SET last_message_time_created = ?, last_message_cursor_seeded = 1 WHERE id = 1")
          .run(seedValue);
      })();

      if (seedValue > 0) {
        console.log(`[db] v4 migration: last_message_time_created seeded to ${seedValue}`);
      }
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
   * Get all active and conflicted entries that have embeddings (for similarity search).
   * Conflicted entries are included so they can be surfaced to the agent with a caveat
   * annotation, and so the contradiction scan can attempt to re-resolve them.
   */
  getActiveEntriesWithEmbeddings(): Array<KnowledgeEntry & { embedding: number[] }> {
    const rows = this.db
      .prepare(
        "SELECT * FROM knowledge_entry WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL ORDER BY strength DESC"
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
   * Get entries with optional server-side filtering — pushes status/type/scope
   * filters to SQL so we don't load the full table into memory just to slice it.
   */
  getEntries(filters: { status?: string; type?: string; scope?: string }): KnowledgeEntry[] {
    const conditions: string[] = [];
    const values: string[] = [];

    if (filters.status) {
      conditions.push("status = ?");
      values.push(filters.status);
    }
    if (filters.type) {
      conditions.push("type = ?");
      values.push(filters.type);
    }
    if (filters.scope) {
      conditions.push("scope = ?");
      values.push(filters.scope);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(`SELECT * FROM knowledge_entry ${where} ORDER BY created_at DESC`)
      .all(...values) as RawEntryRow[];

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

  // ── Contradiction detection ──

  /**
   * Find active and conflicted entries that share at least one topic with the given
   * topics list. Returns only entries that have embeddings (needed for similarity filtering).
   *
   * Conflicted entries are included so that new entries can re-attempt to resolve
   * existing irresolvable pairs — if a new entry clearly supports one side, the LLM
   * can supersede the loser and clear the conflict.
   *
   * Used by the post-extraction contradiction scan to find candidates in the
   * mid-similarity band (0.4–0.82) — entries that are topic-related but not
   * similar enough to have been caught by the reconsolidation threshold.
   *
   * Uses json_each() on both sides to avoid variable-limit issues.
   * Excludes a set of IDs already handled (e.g. the new entry itself, entries
   * already processed by decideMerge in this chunk).
   */
  getEntriesWithOverlappingTopics(
    topics: string[],
    excludeIds: string[]
  ): Array<KnowledgeEntry & { embedding: number[] }> {
    if (topics.length === 0) return [];

    const rows = this.db
      .prepare(
        `SELECT DISTINCT ke.*
         FROM knowledge_entry ke, json_each(ke.topics) t
         WHERE ke.status IN ('active', 'conflicted')
           AND t.value IN (SELECT value FROM json_each(?))
           AND ke.id NOT IN (SELECT value FROM json_each(?))
         ORDER BY ke.strength DESC`
      )
      .all(JSON.stringify(topics), JSON.stringify(excludeIds)) as RawEntryRow[];

    // Filter to entries with embeddings — similarity scoring in the contradiction scan
    // requires embeddings. Entries without embeddings are skipped (they'll get embeddings
    // on the next ensureEmbeddings pass and be checked on the next consolidation run).
    return rows
      .map((r) => this.rowToEntry(r))
      .filter((e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding);
  }

  /**
   * Record the outcome of a contradiction resolution between two entries.
   *
   * - "supersede_old": newEntryId wins — mark existingEntryId as superseded
   * - "supersede_new": existingEntryId wins — mark newEntryId as superseded
   * - "merge":         replace newEntryId content with merged, mark existingEntryId as superseded
   * - "irresolvable":  insert a contradicts relation, mark BOTH entries as conflicted for human review
   *
   * For supersede/merge resolutions: if the winning entry was previously 'conflicted'
   * (i.e. it was one half of an irresolvable pair), its contradicts relation is deleted
   * and its status is restored to 'active'. This enables automatic re-resolution when
   * a new entry clearly settles a previously unresolvable conflict.
   */
  applyContradictionResolution(
    resolution: "supersede_old" | "supersede_new" | "merge" | "irresolvable",
    newEntryId: string,
    existingEntryId: string,
    mergedData?: { content: string; type: string; topics: string[]; confidence: number }
  ): void {
    const now = Date.now();

    this.db.transaction(() => {
      switch (resolution) {
        case "supersede_old": {
          // Resolve any prior conflicts on BOTH entries BEFORE status changes.
          const loserConflictPartner1 = this.findConflictCounterpart(existingEntryId);
          const winnerConflictPartner1 = this.findConflictCounterpart(newEntryId);
          // New entry wins — mark existing as superseded
          this.db
            .prepare(
              `UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`
            )
            .run(newEntryId, now, existingEntryId);
          // Record the supersedes relation
          this.db
            .prepare(
              `INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`
            )
            .run(randomUUID(), newEntryId, existingEntryId, now);
          // If the loser was half of an irresolvable pair, its counterpart is now orphaned.
          // Restore the counterpart to 'active' and clean up the contradicts relation.
          if (loserConflictPartner1) this.restoreConflictCounterpart(loserConflictPartner1, existingEntryId, now);
          // If the winner was also conflicted, the new entry decisively settles it —
          // restore the winner to active and clean up its conflict counterpart too.
          if (winnerConflictPartner1) {
            this.db
              .prepare("UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'")
              .run(now, newEntryId);
            this.restoreConflictCounterpart(winnerConflictPartner1, newEntryId, now);
          }
          break;
        }

        case "supersede_new": {
          // Resolve any prior conflicts on BOTH entries before status changes.
          const loserConflictPartner2 = this.findConflictCounterpart(newEntryId);
          const winnerConflictPartner2 = this.findConflictCounterpart(existingEntryId);
          // Existing entry wins — mark new entry as superseded
          this.db
            .prepare(
              `UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`
            )
            .run(existingEntryId, now, newEntryId);
          this.db
            .prepare(
              `INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`
            )
            .run(randomUUID(), existingEntryId, newEntryId, now);
          // Restore the loser's orphaned conflict counterpart if any.
          if (loserConflictPartner2) this.restoreConflictCounterpart(loserConflictPartner2, newEntryId, now);
          // If the winner was also conflicted, the new entry decisively settles it —
          // restore the winner to active and clean up its conflict counterpart too.
          if (winnerConflictPartner2) {
            this.db
              .prepare("UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'")
              .run(now, existingEntryId);
            this.restoreConflictCounterpart(winnerConflictPartner2, existingEntryId, now);
          }
          break;
        }

        case "merge": {
          // Resolve any prior conflicts on both entries BEFORE status changes.
          const existingConflictPartner = this.findConflictCounterpart(existingEntryId);
          const newConflictPartner = this.findConflictCounterpart(newEntryId);
          // Merge into the new entry, supersede the old one.
          // If mergedData is absent (LLM truncation), newEntryId keeps its
          // original content — still a valid state, just unrefined.
          if (!mergedData) {
            console.warn(
              `[db] merge resolution missing mergedData — existingEntryId ${existingEntryId} ` +
              `will be superseded but newEntryId ${newEntryId} content unchanged`
            );
          } else {
            const safeType = clampKnowledgeType(mergedData.type);
            this.db
              .prepare(
                `UPDATE knowledge_entry
                 SET content = ?, type = ?, topics = ?, confidence = ?,
                     embedding = NULL, updated_at = ?
                 WHERE id = ?`
              )
              .run(
                mergedData.content,
                safeType,
                JSON.stringify(mergedData.topics),
                mergedData.confidence,
                now,
                newEntryId
              );
          }
          this.db
            .prepare(
              `UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`
            )
            .run(newEntryId, now, existingEntryId);
          this.db
            .prepare(
              `INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`
            )
            .run(randomUUID(), newEntryId, existingEntryId, now);
          // Restore orphaned conflict counterparts for both entries if applicable.
          // For the loser (existingEntryId): restore its counterpart.
          if (existingConflictPartner) this.restoreConflictCounterpart(existingConflictPartner, existingEntryId, now);
          // For the winner (newEntryId): if it was conflicted AND the merge content actually
          // landed (mergedData present), the conflict is decisively resolved — restore it and
          // its counterpart to active. If mergedData is absent (LLM truncation), the entry
          // retains its original unrefined content and should stay under review.
          if (newConflictPartner && mergedData) {
            this.db
              .prepare("UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'")
              .run(now, newEntryId);
            this.restoreConflictCounterpart(newConflictPartner, newEntryId, now);
          }
          break;
        }

        case "irresolvable":
          // Genuine tie — insert contradicts relation, mark BOTH entries as conflicted.
          // The /review endpoint surfaces all conflicted entries, so both halves of the
          // conflict must be visible there.
          this.db
            .prepare(
              `INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'contradicts', ?)`
            )
            .run(randomUUID(), newEntryId, existingEntryId, now);
          this.db
            .prepare(
              `UPDATE knowledge_entry SET status = 'conflicted', updated_at = ?
               WHERE id IN (?, ?)`
            )
            .run(now, newEntryId, existingEntryId);
          break;
      }
    })();
  }

  /**
   * Find the ID of the entry that shares a 'contradicts' relation with the given entry,
   * if one exists. Returns null if the entry has no contradicts relation.
   *
   * Must be called BEFORE the entry's status is changed (e.g. before superseding),
   * since the relation lookup does not depend on status but the caller needs the
   * counterpart ID before the original entry is modified.
   */
  private findConflictCounterpart(entryId: string): string | null {
    const entry = this.db
      .prepare("SELECT status FROM knowledge_entry WHERE id = ?")
      .get(entryId) as { status: string } | null;

    if (entry?.status !== "conflicted") return null;

    const rel = this.db
      .prepare(
        "SELECT source_id, target_id FROM knowledge_relation WHERE type = 'contradicts' AND (source_id = ? OR target_id = ?) LIMIT 1"
      )
      .get(entryId, entryId) as { source_id: string; target_id: string } | null;

    if (!rel) return null;
    return rel.source_id === entryId ? rel.target_id : rel.source_id;
  }

  /**
   * Restore a conflict counterpart to 'active' after its paired entry has been
   * superseded/resolved. Deletes the contradicts relation between them.
   *
   * @param counterpartId  The entry to restore (was orphaned when its partner was resolved)
   * @param resolvedId     The entry that was just superseded (used to target the relation delete)
   * @param now            Timestamp for updated_at
   */
  private restoreConflictCounterpart(counterpartId: string, resolvedId: string, now: number): void {
    this.db
      .prepare(
        "UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'"
      )
      .run(now, counterpartId);

    // Scope the delete to the specific pair (not all contradicts relations touching resolvedId)
    // to avoid accidentally orphaning unrelated conflicts if resolvedId has multiple pairs.
    this.db
      .prepare(
        `DELETE FROM knowledge_relation
         WHERE type = 'contradicts'
           AND ((source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?))`
      )
      .run(resolvedId, counterpartId, counterpartId, resolvedId);
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

  /**
   * Batch fetch all 'contradicts' relations that involve any of the given entry IDs.
   * Returns a map from each entry ID to its conflict counterpart ID.
   *
   * Used by the activation engine to annotate conflicted entries without N+1 queries.
   * Entries with no contradicts relation are absent from the returned map.
   */
  getContradictPairsForIds(entryIds: string[]): Map<string, string> {
    if (entryIds.length === 0) return new Map();

    const rows = this.db
      .prepare(
        `SELECT source_id, target_id FROM knowledge_relation
         WHERE type = 'contradicts'
           AND (source_id IN (SELECT value FROM json_each(?))
                OR target_id IN (SELECT value FROM json_each(?)))`
      )
      .all(JSON.stringify(entryIds), JSON.stringify(entryIds)) as Array<{
      source_id: string;
      target_id: string;
    }>;

    const result = new Map<string, string>();
    for (const row of rows) {
      // Map both directions so lookup works regardless of which end we query from
      result.set(row.source_id, row.target_id);
      result.set(row.target_id, row.source_id);
    }
    return result;
  }

  // ── Episode Tracking ──

  /**
   * Record a processed episode by its stable message ID range.
   * Called after the LLM call and DB writes for that episode succeed.
   * Uses INSERT OR IGNORE to be idempotent on retry.
   */
  recordEpisode(
    sessionId: string,
    startMessageId: string,
    endMessageId: string,
    contentType: "compaction_summary" | "messages",
    entriesCreated: number
  ): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO consolidated_episode
         (session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .run(sessionId, startMessageId, endMessageId, contentType, Date.now(), entriesCreated);
  }

  /**
   * Load already-processed message ID ranges for a set of session IDs.
   * Returns a Map<sessionId, ProcessedRange[]> for O(1) lookup during segmentation.
   *
   * Uses json_each() to pass IDs as a single JSON array parameter, avoiding the
   * SQLite SQLITE_MAX_VARIABLE_NUMBER limit (999) that a spread IN(?,?,...) would hit.
   */
  getProcessedEpisodeRanges(sessionIds: string[]): Map<string, ProcessedRange[]> {
    if (sessionIds.length === 0) return new Map();

    const rows = this.db
      .prepare(
        `SELECT session_id, start_message_id, end_message_id
         FROM consolidated_episode
         WHERE session_id IN (SELECT value FROM json_each(?))`
      )
      .all(JSON.stringify(sessionIds)) as Array<{
      session_id: string;
      start_message_id: string;
      end_message_id: string;
    }>;

    const result = new Map<string, ProcessedRange[]>();
    for (const row of rows) {
      const existing = result.get(row.session_id);
      if (existing) {
        existing.push({ startMessageId: row.start_message_id, endMessageId: row.end_message_id });
      } else {
        result.set(row.session_id, [{ startMessageId: row.start_message_id, endMessageId: row.end_message_id }]);
      }
    }
    return result;
  }

  // ── Consolidation State ──

  getConsolidationState(): ConsolidationState {
    const row = this.db
      .prepare("SELECT * FROM consolidation_state WHERE id = 1")
      .get() as {
      last_consolidated_at: number;
      last_message_time_created: number;
      total_sessions_processed: number;
      total_entries_created: number;
      total_entries_updated: number;
    };

    return {
      lastConsolidatedAt: row.last_consolidated_at,
      lastMessageTimeCreated: row.last_message_time_created,
      totalSessionsProcessed: row.total_sessions_processed,
      totalEntriesCreated: row.total_entries_created,
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
    if (state.lastMessageTimeCreated !== undefined) {
      fields.push("last_message_time_created = ?");
      values.push(state.lastMessageTimeCreated);
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

    const safeType = clampKnowledgeType(updates.type);

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
        safeType,
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
    // All four operations must succeed atomically — a crash mid-wipe would
    // leave entries deleted but the cursor not reset (or vice versa).
    this.db.transaction(() => {
      this.db.exec("DELETE FROM knowledge_relation");
      this.db.exec("DELETE FROM knowledge_entry");
      this.db.exec("DELETE FROM consolidated_episode");
      // last_message_cursor_seeded intentionally NOT reset — v4 migration does not need to re-run after a dev wipe.
      this.db.exec(
        `UPDATE consolidation_state SET 
          last_consolidated_at = 0,
          last_message_time_created = 0,
          total_sessions_processed = 0,
          total_entries_created = 0,
          total_entries_updated = 0
         WHERE id = 1`
      );
    })();
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
