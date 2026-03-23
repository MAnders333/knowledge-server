import { Database } from "bun:sqlite";
import type { SQLQueryBindings } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DEFAULT_SQLITE_PATH } from "../../config-file.js";
import { logger } from "../../logger.js";
import { clampKnowledgeType } from "../../types.js";
import type {
	Episode,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
} from "../../types.js";
import type { IKnowledgeStore } from "../interface.js";
import { MIGRATIONS } from "./migrations.js";
import {
	CREATE_TABLES,
	EXPECTED_TABLE_COLUMNS,
	SCHEMA_VERSION,
} from "./schema.js";

/**
 * SQLite database layer for the knowledge graph.
 *
 * Uses bun:sqlite (Bun's native SQLite binding) for all operations:
 * CRUD for entries/relations, embedding storage/retrieval,
 * and consolidation state management.
 *
 * Implements IKnowledgeStore — used by StoreRegistry for sqlite-kind stores.
 */
export class KnowledgeDB implements IKnowledgeStore {
	private db: Database;
	/** Absolute path to the SQLite file — exposed for migration tooling. */
	readonly dbPath: string;

	/**
	 * @param dbPath Path to the SQLite DB file. Defaults to DEFAULT_SQLITE_PATH
	 *               (~/.local/share/knowledge-server/knowledge.db).
	 *               Always pass an explicit path — the default exists only as a
	 *               safety net; production paths are resolved by StoreRegistry
	 *               via resolveSqlitePath().
	 */
	constructor(dbPath?: string) {
		const path = dbPath ?? DEFAULT_SQLITE_PATH;
		this.dbPath = path;
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
		this.initialize();
	}

	private initialize(): void {
		// Bootstrap schema_version table first so we can read the current version.
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL,
        applied_at INTEGER NOT NULL
      );
    `);

		const currentVersion =
			(
				this.db
					.prepare(
						"SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
					)
					.get() as { version: number } | null
			)?.version ?? 0;

		let migratedTo = currentVersion;
		for (const migration of MIGRATIONS) {
			if (migratedTo >= migration.version) continue;
			logger.log(
				`[db] Applying incremental migration v${migratedTo} → v${migration.version}: ${migration.label}.`,
			);
			this.db.transaction(() => {
				migration.up(this.db);
				this.db
					.prepare(
						"INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
					)
					.run(migration.version, Date.now());
			})();
			migratedTo = migration.version;
			logger.log(
				`[db] Incremental migration → v${migration.version} complete.`,
			);
		}
		// ─────────────────────────────────────────────────────────────────────────

		// After migrations, check whether the DB is still behind or has schema drift.
		// This handles DBs that are too old for incremental migration (pre-v6) or
		// where a migration left an inconsistent state.
		const postMigrationVersion = migratedTo;
		const remainingDrift =
			postMigrationVersion >= SCHEMA_VERSION ? [] : this.getSchemaDrift();

		const needsReset =
			(postMigrationVersion > 0 && postMigrationVersion < SCHEMA_VERSION) ||
			remainingDrift.length > 0;

		if (needsReset) {
			const driftDetail =
				remainingDrift.length > 0
					? ` (missing columns: ${remainingDrift.map((d) => `${d.table}.${d.column}`).join(", ")})`
					: "";
			logger.warn(
				`[db] Schema mismatch: DB is v${postMigrationVersion}, code expects v${SCHEMA_VERSION}${driftDetail}. Dropping and recreating all tables. All existing knowledge data has been cleared.`,
			);
			this.db.transaction(() => {
				this.db.exec("DROP TABLE IF EXISTS knowledge_cluster_member");
				this.db.exec("DROP TABLE IF EXISTS knowledge_cluster");
				this.db.exec("DROP TABLE IF EXISTS knowledge_relation");
				this.db.exec("DROP TABLE IF EXISTS knowledge_entry");
				this.db.exec("DROP TABLE IF EXISTS embedding_metadata");
				this.db.exec("DROP TABLE IF EXISTS schema_version");
				// Note: consolidated_episode, consolidation_state, pending_episodes,
				// and daemon_cursor are NOT dropped here — they live in server.db
				// (ServerLocalDB). Dropping them from legacy knowledge.db would be
				// a no-op on v13+ DBs and dangerous on pre-migration DBs.
			})();
		}

		// Create all tables (idempotent on a fresh DB; re-creates after reset).
		this.db.exec(CREATE_TABLES);

		// Stamp the current schema version if not already recorded.
		const stampedVersion =
			(
				this.db
					.prepare(
						"SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
					)
					.get() as { version: number } | null
			)?.version ?? 0;

		if (stampedVersion < SCHEMA_VERSION) {
			this.db
				.prepare(
					"INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
				)
				.run(SCHEMA_VERSION, Date.now());
		}
	}

	// ── Entry CRUD ──

	async insertEntry(
		entry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] },
	): Promise<void> {
		const embeddingBlob = entry.embedding
			? new Uint8Array(new Float32Array(entry.embedding).buffer)
			: null;

		this.db
			.prepare(
				`INSERT INTO knowledge_entry 
         (id, type, content, topics, confidence, source, scope, status, strength,
          created_at, updated_at, last_accessed_at, access_count, observation_count,
          superseded_by, derived_from, is_synthesized, embedding)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
				entry.observationCount,
				entry.supersededBy,
				JSON.stringify(entry.derivedFrom),
				entry.isSynthesized ? 1 : 0,
				embeddingBlob,
			);
	}

	async updateEntry(
		id: string,
		updates: Partial<KnowledgeEntry>,
	): Promise<void> {
		const fields: string[] = [];
		const values: SQLQueryBindings[] = [];

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
		if (updates.isSynthesized !== undefined) {
			fields.push("is_synthesized = ?");
			values.push(updates.isSynthesized ? 1 : 0);
		}

		// Always update timestamp
		fields.push("updated_at = ?");
		values.push(Date.now());

		values.push(id);

		this.db
			.prepare(`UPDATE knowledge_entry SET ${fields.join(", ")} WHERE id = ?`)
			.run(...values);
	}

	async getEntry(id: string): Promise<KnowledgeEntry | null> {
		const row = this.db
			.prepare("SELECT * FROM knowledge_entry WHERE id = ?")
			.get(id) as RawEntryRow | null;

		return row ? this.rowToEntry(row) : null;
	}

	async getActiveEntries(): Promise<KnowledgeEntry[]> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status = 'active' ORDER BY strength DESC",
			)
			.all() as RawEntryRow[];

		return rows.map((r) => this.rowToEntry(r));
	}

	/**
	 * Get all active and conflicted entries that have embeddings (for similarity search).
	 * Conflicted entries are included so they can be surfaced to the agent with a caveat
	 * annotation, and so the contradiction scan can attempt to re-resolve them.
	 */
	async getActiveEntriesWithEmbeddings(): Promise<
		Array<KnowledgeEntry & { embedding: number[] }>
	> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL ORDER BY strength DESC",
			)
			.all() as RawEntryRow[];

		return rows
			.map((r) => this.rowToEntry(r))
			.filter(
				(e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding,
			);
	}

	/**
	 * Get a single active or conflicted entry that has an embedding.
	 * Used to probe embedding dimensions without loading all entries into memory.
	 */
	async getOneEntryWithEmbedding(): Promise<
		(KnowledgeEntry & { embedding: number[] }) | null
	> {
		const row = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL LIMIT 1",
			)
			.get() as RawEntryRow | undefined;

		if (!row) return null;

		const entry = this.rowToEntry(row);
		if (!entry.embedding) return null;

		return entry as KnowledgeEntry & { embedding: number[] };
	}

	/**
	 * Get all active and conflicted entries in a single query.
	 * Used by applyDecay — avoids the TOCTOU window of two separate queries
	 * (an entry transitioning between statuses between calls could be processed twice).
	 */
	async getActiveAndConflictedEntries(): Promise<KnowledgeEntry[]> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status IN ('active', 'conflicted') ORDER BY updated_at DESC",
			)
			.all() as RawEntryRow[];
		return rows.map((r) => this.rowToEntry(r));
	}

	/**
	 * Get entries missing an embedding — used by ensureEmbeddings.
	 */
	async getEntriesMissingEmbeddings(): Promise<KnowledgeEntry[]> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status IN ('active', 'conflicted') AND embedding IS NULL ORDER BY updated_at DESC",
			)
			.all() as RawEntryRow[];

		return rows.map((r) => this.rowToEntry(r));
	}

	/**
	 * Get entries by status (for review, decay processing, etc.)
	 */
	async getEntriesByStatus(status: KnowledgeStatus): Promise<KnowledgeEntry[]> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_entry WHERE status = ? ORDER BY updated_at DESC",
			)
			.all(status) as RawEntryRow[];

		return rows.map((r) => this.rowToEntry(r));
	}

	/**
	 * Get entries with optional server-side filtering — pushes status/type/scope
	 * filters to SQL so we don't load the full table into memory just to slice it.
	 */
	async getEntries(filters: {
		status?: string;
		type?: string;
		scope?: string;
	}): Promise<KnowledgeEntry[]> {
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

		const where =
			conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
		const rows = this.db
			.prepare(
				`SELECT * FROM knowledge_entry ${where} ORDER BY created_at DESC`,
			)
			.all(...values) as RawEntryRow[];

		return rows.map((r) => this.rowToEntry(r));
	}

	/**
	 * Record an access (bump access_count and last_accessed_at).
	 * Retrieval-only signal — never called during consolidation.
	 */
	async recordAccess(id: string): Promise<void> {
		this.db
			.prepare(
				`UPDATE knowledge_entry 
         SET access_count = access_count + 1, last_accessed_at = ?, updated_at = ?
         WHERE id = ?`,
			)
			.run(Date.now(), Date.now(), id);
	}

	/**
	 * Reinforce an observation (bump observation_count and reset last_accessed_at).
	 * Evidence-only signal — called during consolidation when a `keep` decision
	 * confirms the same knowledge appeared in a new episode.
	 * Never called during activation/retrieval.
	 */
	async reinforceObservation(id: string): Promise<void> {
		this.db
			.prepare(
				`UPDATE knowledge_entry
         SET observation_count = observation_count + 1, last_accessed_at = ?, updated_at = ?
         WHERE id = ?`,
			)
			.run(Date.now(), Date.now(), id);
	}

	/**
	 * Batch update strength scores (used during decay).
	 */
	async updateStrength(id: string, strength: number): Promise<void> {
		this.db
			.prepare(
				"UPDATE knowledge_entry SET strength = ?, updated_at = ? WHERE id = ?",
			)
			.run(strength, Date.now(), id);
	}

	/**
	 * Count entries by status.
	 */
	async getStats(): Promise<Record<string, number>> {
		const rows = this.db
			.prepare(
				"SELECT status, COUNT(*) as count FROM knowledge_entry GROUP BY status",
			)
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
	 * mid-similarity band — entries that are topic-related but not similar enough
	 * to have been caught by the reconsolidation threshold.
	 *
	 * Uses json_each() on both sides to avoid variable-limit issues.
	 * Excludes a set of IDs already handled (e.g. the new entry itself, entries
	 * already processed by decideMerge in this chunk).
	 */
	async getEntriesWithOverlappingTopics(
		topics: string[],
		excludeIds: string[],
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>> {
		if (topics.length === 0) return [];

		const rows = this.db
			.prepare(
				`SELECT DISTINCT ke.*
         FROM knowledge_entry ke, json_each(ke.topics) t
         WHERE ke.status IN ('active', 'conflicted')
           AND t.value IN (SELECT value FROM json_each(?))
           AND ke.id NOT IN (SELECT value FROM json_each(?))
         ORDER BY ke.strength DESC`,
			)
			.all(JSON.stringify(topics), JSON.stringify(excludeIds)) as RawEntryRow[];

		// Filter to entries with embeddings — similarity scoring in the contradiction scan
		// requires embeddings. Entries without embeddings are skipped (they'll get embeddings
		// on the next ensureEmbeddings pass and be checked on the next consolidation run).
		return rows
			.map((r) => this.rowToEntry(r))
			.filter(
				(e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding,
			);
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
	async applyContradictionResolution(
		resolution: "supersede_old" | "supersede_new" | "merge" | "irresolvable",
		newEntryId: string,
		existingEntryId: string,
		mergedData?: {
			content: string;
			type: string;
			topics: string[];
			confidence: number;
		},
	): Promise<void> {
		const now = Date.now();

		this.db.transaction(() => {
			switch (resolution) {
				case "supersede_old": {
					// Resolve any prior conflicts on BOTH entries BEFORE status changes.
					const loserConflictPartner1 =
						this.findConflictCounterpart(existingEntryId);
					const winnerConflictPartner1 =
						this.findConflictCounterpart(newEntryId);
					// New entry wins — mark existing as superseded
					this.db
						.prepare(
							`UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`,
						)
						.run(newEntryId, now, existingEntryId);
					// Record the supersedes relation
					this.db
						.prepare(
							`INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`,
						)
						.run(randomUUID(), newEntryId, existingEntryId, now);
					// If the loser was half of an irresolvable pair, its counterpart is now orphaned.
					// Restore the counterpart to 'active' and clean up the contradicts relation.
					if (loserConflictPartner1)
						this.restoreConflictCounterpart(
							loserConflictPartner1,
							existingEntryId,
							now,
						);
					// If the winner was also conflicted, the new entry decisively settles it —
					// restore the winner to active and clean up its conflict counterpart too.
					if (winnerConflictPartner1) {
						this.db
							.prepare(
								"UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'",
							)
							.run(now, newEntryId);
						this.restoreConflictCounterpart(
							winnerConflictPartner1,
							newEntryId,
							now,
						);
					}
					break;
				}

				case "supersede_new": {
					// Resolve any prior conflicts on BOTH entries before status changes.
					const loserConflictPartner2 =
						this.findConflictCounterpart(newEntryId);
					const winnerConflictPartner2 =
						this.findConflictCounterpart(existingEntryId);
					// Existing entry wins — mark new entry as superseded
					this.db
						.prepare(
							`UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`,
						)
						.run(existingEntryId, now, newEntryId);
					this.db
						.prepare(
							`INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`,
						)
						.run(randomUUID(), existingEntryId, newEntryId, now);
					// Restore the loser's orphaned conflict counterpart if any.
					if (loserConflictPartner2)
						this.restoreConflictCounterpart(
							loserConflictPartner2,
							newEntryId,
							now,
						);
					// If the winner was also conflicted, the new entry decisively settles it —
					// restore the winner to active and clean up its conflict counterpart too.
					if (winnerConflictPartner2) {
						this.db
							.prepare(
								"UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'",
							)
							.run(now, existingEntryId);
						this.restoreConflictCounterpart(
							winnerConflictPartner2,
							existingEntryId,
							now,
						);
					}
					break;
				}

				case "merge": {
					// Resolve any prior conflicts on both entries BEFORE status changes.
					const existingConflictPartner =
						this.findConflictCounterpart(existingEntryId);
					const newConflictPartner = this.findConflictCounterpart(newEntryId);
					// Merge into the new entry, supersede the old one.
					// If mergedData is absent (LLM truncation), newEntryId keeps its
					// original content — still a valid state, just unrefined.
					if (!mergedData) {
						logger.warn(
							`[db] merge resolution missing mergedData — existingEntryId ${existingEntryId} ` +
								`will be superseded but newEntryId ${newEntryId} content unchanged`,
						);
					} else {
						const safeType = clampKnowledgeType(mergedData.type);
						this.db
							.prepare(
								`UPDATE knowledge_entry
                 SET content = ?, type = ?, topics = ?, confidence = ?,
                     embedding = NULL, updated_at = ?
                 WHERE id = ?`,
							)
							.run(
								mergedData.content,
								safeType,
								JSON.stringify(mergedData.topics),
								mergedData.confidence,
								now,
								newEntryId,
							);
					}
					this.db
						.prepare(
							`UPDATE knowledge_entry
               SET status = 'superseded', superseded_by = ?, updated_at = ?
               WHERE id = ?`,
						)
						.run(newEntryId, now, existingEntryId);
					this.db
						.prepare(
							`INSERT OR IGNORE INTO knowledge_relation
               (id, source_id, target_id, type, created_at)
               VALUES (?, ?, ?, 'supersedes', ?)`,
						)
						.run(randomUUID(), newEntryId, existingEntryId, now);
					// Restore orphaned conflict counterparts for both entries if applicable.
					// For the loser (existingEntryId): restore its counterpart.
					if (existingConflictPartner)
						this.restoreConflictCounterpart(
							existingConflictPartner,
							existingEntryId,
							now,
						);
					// For the winner (newEntryId): if it was conflicted AND the merge content actually
					// landed (mergedData present), the conflict is decisively resolved — restore it and
					// its counterpart to active. If mergedData is absent (LLM truncation), the entry
					// retains its original unrefined content and should stay under review.
					if (newConflictPartner && mergedData) {
						this.db
							.prepare(
								"UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'",
							)
							.run(now, newEntryId);
						this.restoreConflictCounterpart(
							newConflictPartner,
							newEntryId,
							now,
						);
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
               VALUES (?, ?, ?, 'contradicts', ?)`,
						)
						.run(randomUUID(), newEntryId, existingEntryId, now);
					this.db
						.prepare(
							`UPDATE knowledge_entry SET status = 'conflicted', updated_at = ?
               WHERE id IN (?, ?)`,
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
		// A 'contradicts' relation only exists when both entries are 'conflicted',
		// so querying the relation directly is sufficient — no separate status check needed.
		// This saves one DB round-trip per call (called up to 4 times per resolution).
		const rel = this.db
			.prepare(
				"SELECT source_id, target_id FROM knowledge_relation WHERE type = 'contradicts' AND (source_id = ? OR target_id = ?) LIMIT 1",
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
	private restoreConflictCounterpart(
		counterpartId: string,
		resolvedId: string,
		now: number,
	): void {
		this.db
			.prepare(
				"UPDATE knowledge_entry SET status = 'active', updated_at = ? WHERE id = ? AND status = 'conflicted'",
			)
			.run(now, counterpartId);

		// Scope the delete to the specific pair (not all contradicts relations touching resolvedId)
		// to avoid accidentally orphaning unrelated conflicts if resolvedId has multiple pairs.
		this.db
			.prepare(
				`DELETE FROM knowledge_relation
         WHERE type = 'contradicts'
           AND ((source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?))`,
			)
			.run(resolvedId, counterpartId, counterpartId, resolvedId);
	}

	/**
	 * Hard-delete an entry and all its relations.
	 * Intended for human review cleanup (junk/noise entries, irresolvable conflicts
	 * that aren't worth keeping). Returns true if an entry was deleted, false if not found.
	 */
	async deleteEntry(id: string): Promise<boolean> {
		let deleted = false;
		this.db.transaction(() => {
			this.db
				.prepare(
					"DELETE FROM knowledge_relation WHERE source_id = ? OR target_id = ?",
				)
				.run(id, id);
			const result = this.db
				.prepare("DELETE FROM knowledge_entry WHERE id = ?")
				.run(id);
			deleted = result.changes > 0;
		})();
		return deleted;
	}

	// ── Relations ──

	async insertRelation(relation: KnowledgeRelation): Promise<void> {
		this.db
			.prepare(
				"INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at) VALUES (?, ?, ?, ?, ?)",
			)
			.run(
				relation.id,
				relation.sourceId,
				relation.targetId,
				relation.type,
				relation.createdAt,
			);
	}

	async getRelationsFor(entryId: string): Promise<KnowledgeRelation[]> {
		const rows = this.db
			.prepare(
				"SELECT * FROM knowledge_relation WHERE source_id = ? OR target_id = ?",
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
	 * Batch fetch all entries that are sources for the given synthesized entry IDs.
	 *
	 * A synthesized `principle` or `pattern` entry has `supports` relations pointing
	 * to the source entries it was distilled from. This method returns those source
	 * entries for a set of synthesized IDs in a single query.
	 *
	 * Returns a Map<synthesizedId, sourceEntry[]>. Entries with no `supports`
	 * relations are absent from the map.
	 *
	 * Used by the activation engine for relation-aware retrieval: when a synthesized
	 * principle activates, its source entries are surfaced alongside it so the agent
	 * can see both the abstraction and the evidence that produced it.
	 */
	async getSupportSourcesForIds(
		synthesizedIds: string[],
	): Promise<Map<string, KnowledgeEntry[]>> {
		if (synthesizedIds.length === 0) return new Map();

		// `supports` relations: source_id = synthesized entry, target_id = source entry
		const rows = this.db
			.prepare(
				`SELECT kr.source_id AS synth_id, ke.*
         FROM knowledge_relation kr
         JOIN knowledge_entry ke ON ke.id = kr.target_id
         WHERE kr.type = 'supports'
           AND kr.source_id IN (SELECT value FROM json_each(?))
           AND ke.status IN ('active', 'conflicted')`,
			)
			.all(JSON.stringify(synthesizedIds)) as Array<
			RawEntryRow & { synth_id: string }
		>;

		const result = new Map<string, KnowledgeEntry[]>();
		for (const row of rows) {
			const { synth_id, ...entryRow } = row;
			const entry = this.rowToEntry(entryRow as RawEntryRow);
			const arr = result.get(synth_id);
			if (arr) arr.push(entry);
			else result.set(synth_id, [entry]);
		}
		return result;
	}

	/**
	 * Batch fetch all 'contradicts' relations that involve any of the given entry IDs.
	 * Returns a map from each entry ID to its conflict counterpart ID.
	 *
	 * Used by the activation engine to annotate conflicted entries without N+1 queries.
	 * Entries with no contradicts relation are absent from the returned map.
	 */
	async getContradictPairsForIds(
		entryIds: string[],
	): Promise<Map<string, string>> {
		if (entryIds.length === 0) return new Map();

		const rows = this.db
			.prepare(
				`SELECT source_id, target_id FROM knowledge_relation
         WHERE type = 'contradicts'
           AND (source_id IN (SELECT value FROM json_each(?))
                OR target_id IN (SELECT value FROM json_each(?)))`,
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

	// ── Helpers ──

	private rowToEntry(row: RawEntryRow): KnowledgeEntry {
		let embedding: number[] | undefined;
		if (row.embedding) {
			const buf = row.embedding as Uint8Array;
			const float32 = new Float32Array(
				buf.buffer,
				buf.byteOffset,
				buf.byteLength / 4,
			);
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
			observationCount: row.observation_count,
			supersededBy: row.superseded_by,
			derivedFrom: JSON.parse(row.derived_from),
			isSynthesized: row.is_synthesized === 1,
			embedding,
		};
	}

	/**
	 * Merge new content into an existing entry (reconsolidation).
	 * Updates content, type, topics, confidence, and timestamps in place.
	 * The entry's derivedFrom is expanded to include the new session IDs.
	 *
	 * @param embedding  Optional pre-computed embedding for the new content.
	 *   If supplied, it is written atomically in the same UPDATE statement so the
	 *   entry never passes through a NULL-embedding state. If omitted, embedding
	 *   is set to NULL and ensureEmbeddings will regenerate it at end of run.
	 */
	async mergeEntry(
		id: string,
		updates: {
			content: string;
			type: string;
			topics: string[];
			confidence: number;
			additionalSources: string[]; // session IDs from the new episode
		},
		embedding?: number[],
	): Promise<void> {
		const existing = await this.getEntry(id);
		if (!existing) return;

		const mergedSources = [
			...new Set([...existing.derivedFrom, ...updates.additionalSources]),
		];

		const safeType = clampKnowledgeType(updates.type);

		const embeddingBlob = embedding
			? new Uint8Array(new Float32Array(embedding).buffer)
			: null;

		const now = Date.now();
		this.db
			.prepare(
				`UPDATE knowledge_entry
         SET content = ?, type = ?, topics = ?, confidence = ?,
             derived_from = ?, updated_at = ?, last_accessed_at = ?,
             observation_count = observation_count + 1,
             embedding = ?
         WHERE id = ?`,
			)
			.run(
				updates.content,
				safeType,
				JSON.stringify(updates.topics),
				updates.confidence,
				JSON.stringify(mergedSources),
				now,
				now,
				embeddingBlob,
				id,
			);
	}

	/**
	 * Wipe all knowledge entries, relations, clusters, and embeddings.
	 * Staging and bookkeeping tables (consolidated_episode, consolidation_state,
	 * pending_episodes, daemon_cursor) are managed by ServerLocalDB — call
	 * serverLocalDb.reinitialize() separately for a complete reset.
	 */
	async reinitialize(): Promise<void> {
		// Wipe only knowledge tables — staging tables live in server.db (ServerLocalDB).
		this.db.transaction(() => {
			this.db.exec("DELETE FROM knowledge_cluster_member");
			this.db.exec("DELETE FROM knowledge_cluster");
			this.db.exec("DELETE FROM knowledge_relation");
			this.db.exec("DELETE FROM knowledge_entry");
			this.db.exec("DELETE FROM embedding_metadata");
		})();
	}

	// ── Embedding Metadata ──

	/**
	 * Get the stored embedding model metadata.
	 * Returns null if no metadata has been recorded yet (first run, or pre-v8 DB).
	 */
	async getEmbeddingMetadata(): Promise<{
		model: string;
		dimensions: number;
		recordedAt: number;
	} | null> {
		const row = this.db
			.prepare(
				"SELECT model, dimensions, recorded_at FROM embedding_metadata WHERE id = 1",
			)
			.get() as {
			model: string;
			dimensions: number;
			recorded_at: number;
		} | null;

		if (!row) return null;

		return {
			model: row.model,
			dimensions: row.dimensions,
			recordedAt: row.recorded_at,
		};
	}

	/**
	 * Record the embedding model and dimensions currently in use.
	 * Uses INSERT OR REPLACE so the first call creates the singleton row.
	 */
	async setEmbeddingMetadata(model: string, dimensions: number): Promise<void> {
		this.db
			.prepare(
				"INSERT OR REPLACE INTO embedding_metadata (id, model, dimensions, recorded_at) VALUES (1, ?, ?, ?)",
			)
			.run(model, dimensions, Date.now());
	}

	// ── Cluster Management ──

	/**
	 * Load all persisted clusters with their current member entry IDs.
	 */
	async getClustersWithMembers(): Promise<
		Array<{
			id: string;
			centroid: number[];
			memberCount: number;
			lastSynthesizedAt: number | null;
			lastMembershipChangedAt: number;
			createdAt: number;
			memberIds: string[];
		}>
	> {
		const clusterRows = this.db
			.prepare("SELECT * FROM knowledge_cluster ORDER BY created_at ASC")
			.all() as Array<{
			id: string;
			centroid: Uint8Array;
			member_count: number;
			last_synthesized_at: number | null;
			last_membership_changed_at: number;
			created_at: number;
		}>;

		if (clusterRows.length === 0) return [];

		const memberRows = this.db
			.prepare(
				`SELECT cluster_id, entry_id FROM knowledge_cluster_member
         WHERE cluster_id IN (SELECT value FROM json_each(?))`,
			)
			.all(JSON.stringify(clusterRows.map((r) => r.id))) as Array<{
			cluster_id: string;
			entry_id: string;
		}>;

		const membersByCluster = new Map<string, string[]>();
		for (const m of memberRows) {
			const arr = membersByCluster.get(m.cluster_id);
			if (arr) arr.push(m.entry_id);
			else membersByCluster.set(m.cluster_id, [m.entry_id]);
		}

		return clusterRows.map((r) => ({
			id: r.id,
			centroid: blobToFloats(r.centroid),
			memberCount: r.member_count,
			lastSynthesizedAt: r.last_synthesized_at,
			lastMembershipChangedAt: r.last_membership_changed_at,
			createdAt: r.created_at,
			memberIds: membersByCluster.get(r.id) ?? [],
		}));
	}

	/**
	 * Persist the full cluster state produced by a single clustering pass.
	 *
	 * Replaces cluster membership atomically:
	 * - Upserts each cluster row (insert new, update centroid/count/timestamps for existing).
	 * - Replaces membership rows for each cluster with the new member set.
	 * - Deletes cluster rows that are no longer present (entries have dispersed).
	 *
	 * @param clusters  New cluster state from the clustering pass.
	 */
	async persistClusters(
		clusters: Array<{
			id: string;
			centroid: number[];
			memberIds: string[];
			isNew: boolean;
			membershipChanged: boolean;
		}>,
	): Promise<void> {
		const now = Date.now();
		const newClusterIds = new Set(clusters.map((c) => c.id));

		this.db.transaction(() => {
			// Remove stale clusters (their entries have dispersed into other clusters).
			const existingIds = (
				this.db.prepare("SELECT id FROM knowledge_cluster").all() as Array<{
					id: string;
				}>
			).map((r) => r.id);

			for (const existingId of existingIds) {
				if (!newClusterIds.has(existingId)) {
					this.db
						.prepare("DELETE FROM knowledge_cluster WHERE id = ?")
						.run(existingId);
				}
			}

			for (const cluster of clusters) {
				const centroidBlob = new Uint8Array(
					new Float32Array(cluster.centroid).buffer,
				);

				if (cluster.isNew) {
					this.db
						.prepare(
							`INSERT INTO knowledge_cluster
               (id, centroid, member_count, last_synthesized_at, last_membership_changed_at, created_at)
               VALUES (?, ?, ?, NULL, ?, ?)`,
						)
						.run(cluster.id, centroidBlob, cluster.memberIds.length, now, now);
				} else {
					// Update centroid and member count; bump last_membership_changed_at only
					// if membership actually changed.
					if (cluster.membershipChanged) {
						this.db
							.prepare(
								`UPDATE knowledge_cluster
                 SET centroid = ?, member_count = ?, last_membership_changed_at = ?
                 WHERE id = ?`,
							)
							.run(centroidBlob, cluster.memberIds.length, now, cluster.id);
					} else {
						this.db
							.prepare(
								`UPDATE knowledge_cluster
                 SET centroid = ?, member_count = ?
                 WHERE id = ?`,
							)
							.run(centroidBlob, cluster.memberIds.length, cluster.id);
					}
				}

				// Replace membership: delete existing rows, insert new set.
				this.db
					.prepare("DELETE FROM knowledge_cluster_member WHERE cluster_id = ?")
					.run(cluster.id);

				for (const entryId of cluster.memberIds) {
					this.db
						.prepare(
							`INSERT OR IGNORE INTO knowledge_cluster_member (cluster_id, entry_id, joined_at)
               VALUES (?, ?, ?)`,
						)
						.run(cluster.id, entryId, now);
				}
			}
		})();
	}

	/**
	 * Stamp a cluster as synthesized at the current time.
	 * Called after a successful synthesis pass for that cluster.
	 */
	async markClusterSynthesized(clusterId: string): Promise<void> {
		this.db
			.prepare(
				"UPDATE knowledge_cluster SET last_synthesized_at = ? WHERE id = ?",
			)
			.run(Date.now(), clusterId);
	}

	/**
	 * NULL out all embeddings on active and conflicted entries.
	 *
	 * @internal — exposed for tests and manual recovery only. Production code
	 * should never call this: clearing embeddings without immediately regenerating
	 * them leaves similarity-based activation returning zero results until the
	 * next consolidation run. The normal model-change path uses in-place re-embed via
	 * checkAndReEmbed() instead.
	 *
	 * Returns the number of entries whose embeddings were cleared.
	 */
	async clearAllEmbeddings(): Promise<number> {
		const result = this.db
			.prepare(
				"UPDATE knowledge_entry SET embedding = NULL WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL",
			)
			.run();
		return result.changes;
	}

	async close(): Promise<void> {
		this.db.close();
	}

	/**
	 * Compare the actual DB column set against EXPECTED_TABLE_COLUMNS for every
	 * table. Returns a list of {table, column} pairs that are expected but absent.
	 *
	 * Only reports missing columns — extra columns in the DB (from a future schema
	 * that was partially rolled back) are ignored. The intent is to detect tables
	 * that are structurally incomplete, not to enforce exact parity.
	 *
	 * Tables that don't exist yet (empty PRAGMA result) are skipped — a missing
	 * table is a normal first-run condition, not drift. CREATE_TABLES handles it.
	 */
	private getSchemaDrift(): Array<{ table: string; column: string }> {
		const missing: Array<{ table: string; column: string }> = [];

		for (const [table, expectedCols] of Object.entries(
			EXPECTED_TABLE_COLUMNS,
		)) {
			const actualCols = new Set(
				(
					this.db.prepare(`PRAGMA table_info("${table}")`).all() as Array<{
						name: string;
					}>
				).map((c) => c.name),
			);

			// Table doesn't exist yet — not drift, just a fresh DB
			if (actualCols.size === 0) continue;

			for (const col of expectedCols) {
				if (!actualCols.has(col)) {
					missing.push({ table, column: col });
				}
			}
		}

		return missing;
	}
}

/**
 * Convert a SQLite BLOB (Uint8Array of raw float32 bytes) to a number[] array.
 * Used when reading centroid columns from knowledge_cluster rows.
 */
function blobToFloats(blob: Uint8Array): number[] {
	const float32 = new Float32Array(
		blob.buffer,
		blob.byteOffset,
		blob.byteLength / 4,
	);
	return Array.from(float32);
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
	observation_count: number;
	superseded_by: string | null;
	derived_from: string;
	is_synthesized: number;
	embedding: Uint8Array | null;
}
