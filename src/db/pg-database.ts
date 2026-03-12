import { randomUUID } from "node:crypto";
import postgres from "postgres";
import { logger } from "../logger.js";
import { clampKnowledgeType } from "../types.js";
import type {
	ConsolidationState,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
	ProcessedRange,
	SourceCursor,
} from "../types.js";
import type { IKnowledgeDB } from "./interface.js";
import { PG_CREATE_TABLES, SCHEMA_VERSION } from "./pg-schema.js";

/**
 * TypeScript's `Omit` on interfaces strips call signatures.
 * TransactionSql extends Omit<Sql, ...> which loses the tagged-template callable.
 * At runtime, the transaction sql object IS callable as a tagged template.
 * We use `any` for the transaction sql parameter to work around this.
 */
// biome-ignore lint: TS limitation with Omit stripping call signatures
type TxSql = any;

/**
 * Raw row shape from PostgreSQL (snake_case columns).
 */
interface RawEntryRow {
	id: string;
	type: string;
	content: string;
	topics: string[] | string;
	confidence: number | string;
	source: string;
	scope: string;
	status: string;
	strength: number | string;
	created_at: number | string;
	updated_at: number | string;
	last_accessed_at: number | string;
	access_count: number | string;
	observation_count: number | string;
	superseded_by: string | null;
	derived_from: string[] | string;
	is_synthesized: number | string;
	embedding: Buffer | Uint8Array | null;
}

/**
 * Convert a float32 number[] to a Buffer for PostgreSQL BYTEA storage.
 */
function floatsToBuffer(arr: number[]): Buffer {
	return Buffer.from(new Float32Array(arr).buffer);
}

/**
 * Convert a PostgreSQL BYTEA Buffer back to a number[] of float32 values.
 */
function bufferToFloats(buf: Buffer | Uint8Array): number[] {
	const uint8 =
		buf instanceof Buffer
			? new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength)
			: buf;
	const float32 = new Float32Array(
		uint8.buffer,
		uint8.byteOffset,
		uint8.byteLength / 4,
	);
	return Array.from(float32);
}

/**
 * Safely coerce a PG value (may be string or number) to a JS number.
 * BIGINT columns come back as strings from the pg driver.
 */
function toNum(val: number | string): number {
	return typeof val === "string" ? Number(val) : val;
}

/**
 * PostgreSQL database layer for the knowledge graph.
 *
 * Implements the same IKnowledgeDB interface as the SQLite KnowledgeDB class,
 * adapting all queries for PostgreSQL syntax (JSONB, BYTEA, ON CONFLICT, etc.).
 *
 * Activated when POSTGRES_CONNECTION_URI is set.
 */
export class PostgresKnowledgeDB implements IKnowledgeDB {
	private sql: postgres.Sql;
	/**
	 * Promise-based init lock: null = not started, pending Promise = in-flight,
	 * resolved Promise = complete. All callers await the same Promise so
	 * concurrent initialize() calls are safe.
	 */
	private initPromise: Promise<void> | null = null;

	/**
	 * @param connectionUri  Full postgres:// connection string.
	 * @param poolMax        Maximum pool connections. Defaults to POSTGRES_POOL_MAX
	 *                       env var if set, otherwise 10. Useful for hosted services
	 *                       with tight connection limits (Supabase free, Railway, Neon).
	 */
	constructor(
		connectionUri: string,
		poolMax = (() => {
			const v = Number.parseInt(process.env.POSTGRES_POOL_MAX ?? "", 10);
			return Number.isNaN(v) || v < 1 ? 10 : v;
		})(),
	) {
		this.sql = postgres(connectionUri, {
			max: poolMax,
			idle_timeout: 30, // seconds
			connect_timeout: 10, // seconds — surface cold-start failures early
		});
	}

	/**
	 * Initialize the database schema. Must be called after construction and
	 * awaited before any other operations.
	 *
	 * Concurrent calls are safe: all callers share the same Promise, so
	 * initialization runs exactly once.
	 *
	 * Bootstrap logic:
	 * - Fresh DB (currentVersion === 0): run PG_CREATE_TABLES directly and stamp
	 *   the current SCHEMA_VERSION. Migrations are skipped — they assume tables
	 *   already exist and are idempotent no-ops on a fresh DB, but the core
	 *   tables (knowledge_entry etc.) would never be created.
	 * - Existing DB below SCHEMA_VERSION: apply incremental migrations to bring
	 *   schema up to date.
	 * - After migrations, fall back to destructive drop+recreate only if the
	 *   schema is still behind (unreachable with current migration set, but kept
	 *   as a safety net for future schema changes that require it).
	 */
	async initialize(): Promise<void> {
		if (!this.initPromise) {
			// Attach the null-reset *before* any caller awaits, so concurrent
			// callers who share this promise all see the same rejection and the
			// next fresh call can retry rather than getting a stale rejected promise.
			this.initPromise = this._initialize().catch((err) => {
				this.initPromise = null;
				throw err;
			});
		}
		return this.initPromise;
	}

	private async _initialize(): Promise<void> {
		// Create schema_version table first
		await this.sql`
			CREATE TABLE IF NOT EXISTS schema_version (
				version INTEGER NOT NULL,
				applied_at BIGINT NOT NULL
			)
		`;

		const versionRows = await this.sql`
			SELECT version FROM schema_version ORDER BY version DESC LIMIT 1
		`;
		const currentVersion =
			versionRows.length > 0 ? Number(versionRows[0].version) : 0;

		// ── Fresh database: go straight to full create ──────────────────────
		// Migrations assume their target tables already exist (they add columns
		// to knowledge_entry, create cluster tables on top of it, etc.). On a
		// fresh PG instance with no tables, running migrations produces only
		// schema_version + embedding_metadata + cluster tables — the core
		// tables (knowledge_entry, knowledge_relation, ...) are never created.
		// Always bootstrap a fresh DB with PG_CREATE_TABLES and skip migrations.
		if (currentVersion === 0) {
			logger.log(
				`[pg-db] Fresh database — creating schema at v${SCHEMA_VERSION}.`,
			);
			// Wrap in a transaction so a crash between CREATE and INSERT leaves the
			// DB fully empty (schema_version still 0) rather than half-created.
			await this.sql.begin(async (sql: TxSql) => {
				await sql.unsafe(PG_CREATE_TABLES);
				await sql`
					INSERT INTO schema_version (version, applied_at)
					VALUES (${SCHEMA_VERSION}, ${Date.now()})
				`;
			});
			return;
		}

		// ── Existing database: apply incremental migrations ─────────────────
		// Each migration runs in its own transaction; a crash mid-migration
		// leaves the DB at the last committed version (re-runnable on restart).
		//
		// Rules for adding a new migration:
		//  1. Append a new { version, up } entry — never reorder or modify existing ones.
		//  2. `up` must be idempotent (IF NOT EXISTS, information_schema checks, etc.).
		//  3. Only additive changes (new tables, new nullable columns) are safe.
		//     Destructive changes that can't be expressed idempotently should fall
		//     through to the drop+recreate path below.
		if (currentVersion < SCHEMA_VERSION) {
			const PG_MIGRATIONS: Array<{
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
			];

			let migratedTo = currentVersion;
			for (const migration of PG_MIGRATIONS) {
				if (migratedTo >= migration.version) continue;
				logger.log(
					`[pg-db] Applying incremental migration v${migratedTo} → v${migration.version}: ${migration.label}.`,
				);
				await this.sql.begin(async (sql: TxSql) => {
					await migration.up(sql);
					await sql`
						INSERT INTO schema_version (version, applied_at)
						VALUES (${migration.version}, ${Date.now()})
					`;
				});
				migratedTo = migration.version;
				logger.log(
					`[pg-db] Incremental migration → v${migration.version} complete.`,
				);
			}

			// Safety net: if migrations still didn't reach SCHEMA_VERSION (e.g.
			// a future migration requires destructive changes), fall back to
			// drop+recreate. migratedTo > 0 is always true here (we already
			// handled 0 above), so we always warn before the destructive reset.
			if (migratedTo < SCHEMA_VERSION) {
				logger.warn(
					`[pg-db] Schema still at v${migratedTo} after migrations, code expects v${SCHEMA_VERSION}. Dropping and recreating all tables. All existing knowledge data has been cleared.`,
				);
				// Wrap the entire drop+recreate in a transaction so a crash mid-way
				// leaves the DB at the last committed migration version (re-runnable
				// on restart) rather than with a partially-dropped schema.
				await this.sql.begin(async (sql: TxSql) => {
					await sql`DROP TABLE IF EXISTS knowledge_cluster_member CASCADE`;
					await sql`DROP TABLE IF EXISTS knowledge_cluster CASCADE`;
					await sql`DROP TABLE IF EXISTS knowledge_relation CASCADE`;
					await sql`DROP TABLE IF EXISTS knowledge_entry CASCADE`;
					await sql`DROP TABLE IF EXISTS consolidated_episode CASCADE`;
					await sql`DROP TABLE IF EXISTS source_cursor CASCADE`;
					await sql`DROP TABLE IF EXISTS consolidation_state CASCADE`;
					await sql`DROP TABLE IF EXISTS embedding_metadata CASCADE`;
					await sql`DROP TABLE IF EXISTS schema_version CASCADE`;
					await sql.unsafe(PG_CREATE_TABLES);
					await sql`
						INSERT INTO schema_version (version, applied_at)
						VALUES (${SCHEMA_VERSION}, ${Date.now()})
					`;
				});
			}
		}
	}

	// ── Helpers ──

	private rowToEntry(row: RawEntryRow): KnowledgeEntry {
		let embedding: number[] | undefined;
		if (row.embedding) {
			embedding = bufferToFloats(row.embedding as Buffer | Uint8Array);
		}

		const topics =
			typeof row.topics === "string" ? JSON.parse(row.topics) : row.topics;
		const derivedFrom =
			typeof row.derived_from === "string"
				? JSON.parse(row.derived_from)
				: row.derived_from;

		return {
			id: row.id,
			type: row.type as KnowledgeEntry["type"],
			content: row.content,
			topics: Array.isArray(topics) ? topics : [],
			confidence: toNum(row.confidence),
			source: row.source,
			scope: row.scope as KnowledgeEntry["scope"],
			status: row.status as KnowledgeEntry["status"],
			strength: toNum(row.strength),
			createdAt: toNum(row.created_at),
			updatedAt: toNum(row.updated_at),
			lastAccessedAt: toNum(row.last_accessed_at),
			accessCount: toNum(row.access_count),
			observationCount: toNum(row.observation_count),
			supersededBy: row.superseded_by,
			derivedFrom: Array.isArray(derivedFrom) ? derivedFrom : [],
			isSynthesized: toNum(row.is_synthesized) === 1,
			embedding,
		};
	}

	// ── Entry CRUD ──

	async insertEntry(
		entry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] },
	): Promise<void> {
		const embeddingBuf = entry.embedding
			? floatsToBuffer(entry.embedding)
			: null;

		await this.sql`
			INSERT INTO knowledge_entry
			(id, type, content, topics, confidence, source, scope, status, strength,
			 created_at, updated_at, last_accessed_at, access_count, observation_count,
			 superseded_by, derived_from, is_synthesized, embedding)
			VALUES (
				${entry.id}, ${entry.type}, ${entry.content},
				${this.sql.json(entry.topics)},
				${entry.confidence}, ${entry.source}, ${entry.scope}, ${entry.status},
				${entry.strength}, ${entry.createdAt}, ${entry.updatedAt},
				${entry.lastAccessedAt}, ${entry.accessCount}, ${entry.observationCount},
				${entry.supersededBy}, ${this.sql.json(entry.derivedFrom)},
				${entry.isSynthesized ? 1 : 0}, ${embeddingBuf}
			)
		`;
	}

	async updateEntry(
		id: string,
		updates: Partial<KnowledgeEntry>,
	): Promise<void> {
		// Build SET clause dynamically. We use sql.unsafe for the dynamic query.
		const setClauses: string[] = [];
		const values: unknown[] = [];
		let idx = 1;

		if (updates.content !== undefined) {
			setClauses.push(`content = $${idx++}`);
			values.push(updates.content);
		}
		if (updates.topics !== undefined) {
			setClauses.push(`topics = $${idx++}::jsonb`);
			values.push(JSON.stringify(updates.topics));
		}
		if (updates.confidence !== undefined) {
			setClauses.push(`confidence = $${idx++}`);
			values.push(updates.confidence);
		}
		if (updates.status !== undefined) {
			setClauses.push(`status = $${idx++}`);
			values.push(updates.status);
		}
		if (updates.strength !== undefined) {
			setClauses.push(`strength = $${idx++}`);
			values.push(updates.strength);
		}
		if (updates.supersededBy !== undefined) {
			setClauses.push(`superseded_by = $${idx++}`);
			values.push(updates.supersededBy);
		}
		if (updates.scope !== undefined) {
			setClauses.push(`scope = $${idx++}`);
			values.push(updates.scope);
		}
		if (updates.embedding !== undefined) {
			setClauses.push(`embedding = $${idx++}`);
			values.push(floatsToBuffer(updates.embedding));
		}
		if (updates.isSynthesized !== undefined) {
			setClauses.push(`is_synthesized = $${idx++}`);
			values.push(updates.isSynthesized ? 1 : 0);
		}

		setClauses.push(`updated_at = $${idx++}`);
		values.push(Date.now());

		values.push(id);

		await this.sql.unsafe(
			`UPDATE knowledge_entry SET ${setClauses.join(", ")} WHERE id = $${idx}`,
			values as postgres.ParameterOrJSON<never>[],
		);
	}

	async getEntry(id: string): Promise<KnowledgeEntry | null> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry WHERE id = ${id}
		`;
		if (rows.length === 0) return null;
		return this.rowToEntry(rows[0] as unknown as RawEntryRow);
	}

	async getActiveEntries(): Promise<KnowledgeEntry[]> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry WHERE status = 'active' ORDER BY strength DESC
		`;
		return (rows as unknown as RawEntryRow[]).map((r) => this.rowToEntry(r));
	}

	async getActiveEntriesWithEmbeddings(): Promise<
		Array<KnowledgeEntry & { embedding: number[] }>
	> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry
			WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL
			ORDER BY strength DESC
		`;
		return (rows as unknown as RawEntryRow[])
			.map((r) => this.rowToEntry(r))
			.filter(
				(e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding,
			);
	}

	async getOneEntryWithEmbedding(): Promise<
		(KnowledgeEntry & { embedding: number[] }) | null
	> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry
			WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL
			LIMIT 1
		`;
		if (rows.length === 0) return null;
		const entry = this.rowToEntry(rows[0] as unknown as RawEntryRow);
		if (!entry.embedding) return null;
		return entry as KnowledgeEntry & { embedding: number[] };
	}

	async getActiveAndConflictedEntries(): Promise<KnowledgeEntry[]> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry
			WHERE status IN ('active', 'conflicted')
			ORDER BY updated_at DESC
		`;
		return (rows as unknown as RawEntryRow[]).map((r) => this.rowToEntry(r));
	}

	async getEntriesMissingEmbeddings(): Promise<KnowledgeEntry[]> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry
			WHERE status IN ('active', 'conflicted') AND embedding IS NULL
			ORDER BY updated_at DESC
		`;
		return (rows as unknown as RawEntryRow[]).map((r) => this.rowToEntry(r));
	}

	async getEntriesByStatus(status: KnowledgeStatus): Promise<KnowledgeEntry[]> {
		const rows = await this.sql`
			SELECT * FROM knowledge_entry WHERE status = ${status} ORDER BY updated_at DESC
		`;
		return (rows as unknown as RawEntryRow[]).map((r) => this.rowToEntry(r));
	}

	async getEntries(filters: {
		status?: string;
		type?: string;
		scope?: string;
	}): Promise<KnowledgeEntry[]> {
		const conditions: string[] = [];
		const values: unknown[] = [];
		let idx = 1;

		if (filters.status) {
			conditions.push(`status = $${idx++}`);
			values.push(filters.status);
		}
		if (filters.type) {
			conditions.push(`type = $${idx++}`);
			values.push(filters.type);
		}
		if (filters.scope) {
			conditions.push(`scope = $${idx++}`);
			values.push(filters.scope);
		}

		const where =
			conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
		const rows = await this.sql.unsafe(
			`SELECT * FROM knowledge_entry ${where} ORDER BY created_at DESC`,
			values as postgres.ParameterOrJSON<never>[],
		);
		return (rows as unknown as RawEntryRow[]).map((r) => this.rowToEntry(r));
	}

	async recordAccess(id: string): Promise<void> {
		const now = Date.now();
		await this.sql`
			UPDATE knowledge_entry
			SET access_count = access_count + 1, last_accessed_at = ${now}, updated_at = ${now}
			WHERE id = ${id}
		`;
	}

	async reinforceObservation(id: string): Promise<void> {
		const now = Date.now();
		await this.sql`
			UPDATE knowledge_entry
			SET observation_count = observation_count + 1, last_accessed_at = ${now}, updated_at = ${now}
			WHERE id = ${id}
		`;
	}

	async updateStrength(id: string, strength: number): Promise<void> {
		await this.sql`
			UPDATE knowledge_entry SET strength = ${strength}, updated_at = ${Date.now()} WHERE id = ${id}
		`;
	}

	async getStats(): Promise<Record<string, number>> {
		const rows = await this.sql`
			SELECT status, COUNT(*) as count FROM knowledge_entry GROUP BY status
		`;
		const stats: Record<string, number> = { total: 0 };
		for (const row of rows) {
			stats[row.status] = Number(row.count);
			stats.total += Number(row.count);
		}
		return stats;
	}

	// ── Contradiction detection ──

	async getEntriesWithOverlappingTopics(
		topics: string[],
		excludeIds: string[],
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>> {
		if (topics.length === 0) return [];

		// PostgreSQL equivalent of SQLite's json_each:
		// Use jsonb_array_elements_text to unnest the topics JSONB array
		const rows = await this.sql`
			SELECT DISTINCT ke.*
			FROM knowledge_entry ke,
			     jsonb_array_elements_text(ke.topics) AS t(value)
			WHERE ke.status IN ('active', 'conflicted')
			  AND t.value = ANY(${topics}::text[])
			  AND ke.id != ALL(${excludeIds}::text[])
			ORDER BY ke.strength DESC
		`;

		return (rows as unknown as RawEntryRow[])
			.map((r) => this.rowToEntry(r))
			.filter(
				(e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding,
			);
	}

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

		await this.sql.begin(async (sql: TxSql) => {
			switch (resolution) {
				case "supersede_old": {
					const loserPartner = await this.findConflictCounterpart(
						sql,
						existingEntryId,
					);
					const winnerPartner = await this.findConflictCounterpart(
						sql,
						newEntryId,
					);
					await sql`
						UPDATE knowledge_entry
						SET status = 'superseded', superseded_by = ${newEntryId}, updated_at = ${now}
						WHERE id = ${existingEntryId}
					`;
					await sql`
						INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
						VALUES (${randomUUID()}, ${newEntryId}, ${existingEntryId}, 'supersedes', ${now})
						ON CONFLICT DO NOTHING
					`;
					if (loserPartner)
						await this.restoreConflictCounterpart(
							sql,
							loserPartner,
							existingEntryId,
							now,
						);
					if (winnerPartner) {
						await sql`
							UPDATE knowledge_entry SET status = 'active', updated_at = ${now}
							WHERE id = ${newEntryId} AND status = 'conflicted'
						`;
						await this.restoreConflictCounterpart(
							sql,
							winnerPartner,
							newEntryId,
							now,
						);
					}
					break;
				}

				case "supersede_new": {
					const loserPartner = await this.findConflictCounterpart(
						sql,
						newEntryId,
					);
					const winnerPartner = await this.findConflictCounterpart(
						sql,
						existingEntryId,
					);
					await sql`
						UPDATE knowledge_entry
						SET status = 'superseded', superseded_by = ${existingEntryId}, updated_at = ${now}
						WHERE id = ${newEntryId}
					`;
					await sql`
						INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
						VALUES (${randomUUID()}, ${existingEntryId}, ${newEntryId}, 'supersedes', ${now})
						ON CONFLICT DO NOTHING
					`;
					if (loserPartner)
						await this.restoreConflictCounterpart(
							sql,
							loserPartner,
							newEntryId,
							now,
						);
					if (winnerPartner) {
						await sql`
							UPDATE knowledge_entry SET status = 'active', updated_at = ${now}
							WHERE id = ${existingEntryId} AND status = 'conflicted'
						`;
						await this.restoreConflictCounterpart(
							sql,
							winnerPartner,
							existingEntryId,
							now,
						);
					}
					break;
				}

				case "merge": {
					const existingPartner = await this.findConflictCounterpart(
						sql,
						existingEntryId,
					);
					const newPartner = await this.findConflictCounterpart(
						sql,
						newEntryId,
					);
					if (!mergedData) {
						logger.warn(
							`[pg-db] merge resolution missing mergedData — existingEntryId ${existingEntryId} ` +
								`will be superseded but newEntryId ${newEntryId} content unchanged`,
						);
					} else {
						const safeType = clampKnowledgeType(mergedData.type);
						await sql`
							UPDATE knowledge_entry
							SET content = ${mergedData.content}, type = ${safeType},
							    topics = ${sql.json(mergedData.topics)},
							    confidence = ${mergedData.confidence},
							    embedding = NULL, updated_at = ${now}
							WHERE id = ${newEntryId}
						`;
					}
					await sql`
						UPDATE knowledge_entry
						SET status = 'superseded', superseded_by = ${newEntryId}, updated_at = ${now}
						WHERE id = ${existingEntryId}
					`;
					await sql`
						INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
						VALUES (${randomUUID()}, ${newEntryId}, ${existingEntryId}, 'supersedes', ${now})
						ON CONFLICT DO NOTHING
					`;
					if (existingPartner)
						await this.restoreConflictCounterpart(
							sql,
							existingPartner,
							existingEntryId,
							now,
						);
					if (newPartner && mergedData) {
						await sql`
							UPDATE knowledge_entry SET status = 'active', updated_at = ${now}
							WHERE id = ${newEntryId} AND status = 'conflicted'
						`;
						await this.restoreConflictCounterpart(
							sql,
							newPartner,
							newEntryId,
							now,
						);
					}
					break;
				}

				case "irresolvable":
					await sql`
						INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
						VALUES (${randomUUID()}, ${newEntryId}, ${existingEntryId}, 'contradicts', ${now})
						ON CONFLICT DO NOTHING
					`;
					await sql`
						UPDATE knowledge_entry SET status = 'conflicted', updated_at = ${now}
						WHERE id IN (${newEntryId}, ${existingEntryId})
					`;
					break;
			}
		});
	}

	private async findConflictCounterpart(
		sql: TxSql,
		entryId: string,
	): Promise<string | null> {
		const rows = await sql`
			SELECT source_id, target_id FROM knowledge_relation
			WHERE type = 'contradicts' AND (source_id = ${entryId} OR target_id = ${entryId})
			LIMIT 1
		`;
		if (rows.length === 0) return null;
		return rows[0].source_id === entryId
			? rows[0].target_id
			: rows[0].source_id;
	}

	private async restoreConflictCounterpart(
		sql: TxSql,
		counterpartId: string,
		resolvedId: string,
		now: number,
	): Promise<void> {
		await sql`
			UPDATE knowledge_entry SET status = 'active', updated_at = ${now}
			WHERE id = ${counterpartId} AND status = 'conflicted'
		`;
		await sql`
			DELETE FROM knowledge_relation
			WHERE type = 'contradicts'
			  AND ((source_id = ${resolvedId} AND target_id = ${counterpartId})
			       OR (source_id = ${counterpartId} AND target_id = ${resolvedId}))
		`;
	}

	async deleteEntry(id: string): Promise<boolean> {
		return await this.sql.begin(async (sql: TxSql) => {
			await sql`
				DELETE FROM knowledge_relation WHERE source_id = ${id} OR target_id = ${id}
			`;
			const result = await sql`
				DELETE FROM knowledge_entry WHERE id = ${id}
			`;
			return result.count > 0;
		});
	}

	// ── Relations ──

	async insertRelation(relation: KnowledgeRelation): Promise<void> {
		await this.sql`
			INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
			VALUES (${relation.id}, ${relation.sourceId}, ${relation.targetId}, ${relation.type}, ${relation.createdAt})
		`;
	}

	async getRelationsFor(entryId: string): Promise<KnowledgeRelation[]> {
		const rows = await this.sql`
			SELECT * FROM knowledge_relation WHERE source_id = ${entryId} OR target_id = ${entryId}
		`;
		return rows.map((r) => ({
			id: r.id as string,
			sourceId: r.source_id as string,
			targetId: r.target_id as string,
			type: r.type as KnowledgeRelation["type"],
			createdAt: toNum(r.created_at as number | string),
		}));
	}

	async getSupportSourcesForIds(
		synthesizedIds: string[],
	): Promise<Map<string, KnowledgeEntry[]>> {
		if (synthesizedIds.length === 0) return new Map();

		// `supports` relations: source_id = synthesized entry, target_id = source entry
		const rows = await this.sql`
			SELECT kr.source_id AS synth_id, ke.*
			FROM knowledge_relation kr
			JOIN knowledge_entry ke ON ke.id = kr.target_id
			WHERE kr.type = 'supports'
			  AND kr.source_id = ANY(${synthesizedIds}::text[])
			  AND ke.status IN ('active', 'conflicted')
		`;

		const result = new Map<string, KnowledgeEntry[]>();
		for (const row of rows) {
			const synthId = row.synth_id as string;
			const entry = this.rowToEntry(row as unknown as RawEntryRow);
			const arr = result.get(synthId);
			if (arr) arr.push(entry);
			else result.set(synthId, [entry]);
		}
		return result;
	}

	async getContradictPairsForIds(
		entryIds: string[],
	): Promise<Map<string, string>> {
		if (entryIds.length === 0) return new Map();

		const rows = await this.sql`
			SELECT source_id, target_id FROM knowledge_relation
			WHERE type = 'contradicts'
			  AND (source_id = ANY(${entryIds}::text[]) OR target_id = ANY(${entryIds}::text[]))
		`;

		const result = new Map<string, string>();
		for (const row of rows) {
			result.set(row.source_id as string, row.target_id as string);
			result.set(row.target_id as string, row.source_id as string);
		}
		return result;
	}

	// ── Episode Tracking ──

	async recordEpisode(
		source: string,
		sessionId: string,
		startMessageId: string,
		endMessageId: string,
		contentType: "compaction_summary" | "messages" | "document",
		entriesCreated: number,
	): Promise<void> {
		await this.sql`
			INSERT INTO consolidated_episode
			(source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
			VALUES (${source}, ${sessionId}, ${startMessageId}, ${endMessageId}, ${contentType}, ${Date.now()}, ${entriesCreated})
			ON CONFLICT DO NOTHING
		`;
	}

	async getProcessedEpisodeRanges(
		source: string,
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		if (sessionIds.length === 0) return new Map();

		const rows = await this.sql`
			SELECT session_id, start_message_id, end_message_id
			FROM consolidated_episode
			WHERE source = ${source}
			  AND session_id = ANY(${sessionIds}::text[])
		`;

		const result = new Map<string, ProcessedRange[]>();
		for (const row of rows) {
			const sid = row.session_id as string;
			const range: ProcessedRange = {
				startMessageId: row.start_message_id as string,
				endMessageId: row.end_message_id as string,
			};
			const existing = result.get(sid);
			if (existing) existing.push(range);
			else result.set(sid, [range]);
		}
		return result;
	}

	// ── Source Cursor ──

	async getSourceCursor(source: string): Promise<SourceCursor> {
		const rows = await this.sql`
			SELECT last_message_time_created, last_consolidated_at
			FROM source_cursor WHERE source = ${source}
		`;

		if (rows.length === 0) {
			return { source, lastMessageTimeCreated: 0, lastConsolidatedAt: 0 };
		}

		return {
			source,
			lastMessageTimeCreated: toNum(
				rows[0].last_message_time_created as number | string,
			),
			lastConsolidatedAt: toNum(
				rows[0].last_consolidated_at as number | string,
			),
		};
	}

	async updateSourceCursor(
		source: string,
		cursor: Partial<Omit<SourceCursor, "source">>,
	): Promise<void> {
		const current = await this.getSourceCursor(source);
		const newLastMessageTime =
			cursor.lastMessageTimeCreated ?? current.lastMessageTimeCreated;
		const newLastConsolidated =
			cursor.lastConsolidatedAt ?? current.lastConsolidatedAt;

		await this.sql`
			INSERT INTO source_cursor (source, last_message_time_created, last_consolidated_at)
			VALUES (${source}, ${newLastMessageTime}, ${newLastConsolidated})
			ON CONFLICT (source) DO UPDATE SET
				last_message_time_created = EXCLUDED.last_message_time_created,
				last_consolidated_at = EXCLUDED.last_consolidated_at
		`;
	}

	// ── Consolidation State ──

	async getConsolidationState(): Promise<ConsolidationState> {
		const rows = await this.sql`
			SELECT * FROM consolidation_state WHERE id = 1
		`;

		if (rows.length === 0) {
			logger.warn(
				"[pg-db] consolidation_state row missing — returning zero state",
			);
			return {
				lastConsolidatedAt: 0,
				totalSessionsProcessed: 0,
				totalEntriesCreated: 0,
				totalEntriesUpdated: 0,
			};
		}

		return {
			lastConsolidatedAt: toNum(
				rows[0].last_consolidated_at as number | string,
			),
			totalSessionsProcessed: toNum(
				rows[0].total_sessions_processed as number | string,
			),
			totalEntriesCreated: toNum(
				rows[0].total_entries_created as number | string,
			),
			totalEntriesUpdated: toNum(
				rows[0].total_entries_updated as number | string,
			),
		};
	}

	async updateConsolidationState(
		state: Partial<ConsolidationState>,
	): Promise<void> {
		const setClauses: string[] = [];
		const values: unknown[] = [];
		let idx = 1;

		if (state.lastConsolidatedAt !== undefined) {
			setClauses.push(`last_consolidated_at = $${idx++}`);
			values.push(state.lastConsolidatedAt);
		}
		if (state.totalSessionsProcessed !== undefined) {
			setClauses.push(`total_sessions_processed = $${idx++}`);
			values.push(state.totalSessionsProcessed);
		}
		if (state.totalEntriesCreated !== undefined) {
			setClauses.push(`total_entries_created = $${idx++}`);
			values.push(state.totalEntriesCreated);
		}
		if (state.totalEntriesUpdated !== undefined) {
			setClauses.push(`total_entries_updated = $${idx++}`);
			values.push(state.totalEntriesUpdated);
		}

		if (setClauses.length === 0) return;

		await this.sql.unsafe(
			`UPDATE consolidation_state SET ${setClauses.join(", ")} WHERE id = 1`,
			values as postgres.ParameterOrJSON<never>[],
		);
	}

	// ── Entry Merge ──

	/**
	 * Merge new content into an existing entry (reconsolidation).
	 *
	 * When `embedding` is omitted the entry's embedding is set to NULL and
	 * ensureEmbeddings() will regenerate it at the end of the consolidation run.
	 * This means a crash between mergeEntry and ensureEmbeddings leaves the
	 * entry temporarily invisible to similarity queries — the same trade-off as
	 * the SQLite backend. Callers that have already computed the new embedding
	 * should pass it here to avoid the NULL window.
	 */
	async mergeEntry(
		id: string,
		updates: {
			content: string;
			type: string;
			topics: string[];
			confidence: number;
			additionalSources: string[];
		},
		embedding?: number[],
	): Promise<void> {
		const existing = await this.getEntry(id);
		if (!existing) return;

		const mergedSources = [
			...new Set([...existing.derivedFrom, ...updates.additionalSources]),
		];
		const safeType = clampKnowledgeType(updates.type);
		const embeddingBuf = embedding ? floatsToBuffer(embedding) : null;
		const now = Date.now();

		await this.sql`
			UPDATE knowledge_entry
			SET content = ${updates.content}, type = ${safeType},
			    topics = ${this.sql.json(updates.topics)},
			    confidence = ${updates.confidence},
			    derived_from = ${this.sql.json(mergedSources)},
			    updated_at = ${now}, last_accessed_at = ${now},
			    observation_count = observation_count + 1,
			    embedding = ${embeddingBuf}
			WHERE id = ${id}
		`;
	}

	async reinitialize(): Promise<void> {
		await this.sql.begin(async (sql: TxSql) => {
			await sql`DELETE FROM knowledge_cluster_member`;
			await sql`DELETE FROM knowledge_cluster`;
			await sql`DELETE FROM knowledge_relation`;
			await sql`DELETE FROM knowledge_entry`;
			await sql`DELETE FROM consolidated_episode`;
			await sql`DELETE FROM source_cursor`;
			await sql`DELETE FROM embedding_metadata`;
			await sql`
				UPDATE consolidation_state SET
					last_consolidated_at = 0,
					total_sessions_processed = 0,
					total_entries_created = 0,
					total_entries_updated = 0
				WHERE id = 1
			`;
		});
	}

	// ── Embedding Metadata ──

	async getEmbeddingMetadata(): Promise<{
		model: string;
		dimensions: number;
		recordedAt: number;
	} | null> {
		const rows = await this.sql`
			SELECT model, dimensions, recorded_at FROM embedding_metadata WHERE id = 1
		`;

		if (rows.length === 0) return null;

		return {
			model: rows[0].model as string,
			dimensions: toNum(rows[0].dimensions as number | string),
			recordedAt: toNum(rows[0].recorded_at as number | string),
		};
	}

	async setEmbeddingMetadata(model: string, dimensions: number): Promise<void> {
		await this.sql`
			INSERT INTO embedding_metadata (id, model, dimensions, recorded_at)
			VALUES (1, ${model}, ${dimensions}, ${Date.now()})
			ON CONFLICT (id) DO UPDATE SET
				model = EXCLUDED.model,
				dimensions = EXCLUDED.dimensions,
				recorded_at = EXCLUDED.recorded_at
		`;
	}

	// ── Cluster Management ──

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
		const clusterRows = await this.sql`
			SELECT * FROM knowledge_cluster ORDER BY created_at ASC
		`;

		if (clusterRows.length === 0) return [];

		const clusterIds = clusterRows.map((r) => r.id as string);
		const memberRows = await this.sql`
			SELECT cluster_id, entry_id FROM knowledge_cluster_member
			WHERE cluster_id = ANY(${clusterIds}::text[])
		`;

		const membersByCluster = new Map<string, string[]>();
		for (const m of memberRows) {
			const cid = m.cluster_id as string;
			const arr = membersByCluster.get(cid);
			if (arr) arr.push(m.entry_id as string);
			else membersByCluster.set(cid, [m.entry_id as string]);
		}

		return clusterRows.map((r) => ({
			id: r.id as string,
			centroid: bufferToFloats(r.centroid as Buffer),
			memberCount: toNum(r.member_count as number | string),
			lastSynthesizedAt:
				r.last_synthesized_at != null
					? toNum(r.last_synthesized_at as number | string)
					: null,
			lastMembershipChangedAt: toNum(
				r.last_membership_changed_at as number | string,
			),
			createdAt: toNum(r.created_at as number | string),
			memberIds: membersByCluster.get(r.id as string) ?? [],
		}));
	}

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

		await this.sql.begin(async (sql: TxSql) => {
			// Remove stale clusters in a single DELETE rather than N round-trips.
			// ON DELETE CASCADE on knowledge_cluster_member handles membership cleanup.
			const keepIds = [...newClusterIds];
			if (keepIds.length > 0) {
				await sql`
					DELETE FROM knowledge_cluster
					WHERE id != ALL(${sql.array(keepIds, "text")})
				`;
			} else {
				// No clusters remain — wipe everything
				await sql`DELETE FROM knowledge_cluster`;
			}

			for (const cluster of clusters) {
				const centroidBuf = floatsToBuffer(cluster.centroid);

				if (cluster.isNew) {
					await sql`
						INSERT INTO knowledge_cluster
						(id, centroid, member_count, last_synthesized_at, last_membership_changed_at, created_at)
						VALUES (${cluster.id}, ${centroidBuf}, ${cluster.memberIds.length}, NULL, ${now}, ${now})
					`;
				} else if (cluster.membershipChanged) {
					await sql`
						UPDATE knowledge_cluster
						SET centroid = ${centroidBuf}, member_count = ${cluster.memberIds.length},
						    last_membership_changed_at = ${now}
						WHERE id = ${cluster.id}
					`;
				} else {
					await sql`
						UPDATE knowledge_cluster
						SET centroid = ${centroidBuf}, member_count = ${cluster.memberIds.length}
						WHERE id = ${cluster.id}
					`;
				}

				// Replace membership
				await sql`DELETE FROM knowledge_cluster_member WHERE cluster_id = ${cluster.id}`;
				for (const entryId of cluster.memberIds) {
					await sql`
						INSERT INTO knowledge_cluster_member (cluster_id, entry_id, joined_at)
						VALUES (${cluster.id}, ${entryId}, ${now})
						ON CONFLICT DO NOTHING
					`;
				}
			}
		});
	}

	async markClusterSynthesized(clusterId: string): Promise<void> {
		await this.sql`
			UPDATE knowledge_cluster SET last_synthesized_at = ${Date.now()} WHERE id = ${clusterId}
		`;
	}

	async clearAllEmbeddings(): Promise<number> {
		const result = await this.sql`
			UPDATE knowledge_entry SET embedding = NULL
			WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL
		`;
		return result.count;
	}

	async close(): Promise<void> {
		await this.sql.end();
	}
}
