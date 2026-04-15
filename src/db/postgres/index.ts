import { randomUUID } from "node:crypto";
import postgres from "postgres";
import { logger } from "../../logger.js";
import { clampKnowledgeType } from "../../types.js";
import type {
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
} from "../../types.js";
import type { IKnowledgeStore } from "../interface.js";
import {
	bufferToFloats,
	floatsToBuffer,
	floatsToVectorLiteral,
} from "./embedding-codec.js";
import { PG_MIGRATIONS } from "./migrations.js";
import { PG_CREATE_TABLES, SCHEMA_VERSION } from "./schema.js";

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
 * Safely coerce a PG value (may be string or number) to a JS number.
 * BIGINT columns come back as strings from the pg driver.
 */
function toNum(val: number | string): number {
	return typeof val === "string" ? Number(val) : val;
}

/**
 * PostgreSQL database layer for the knowledge graph.
 *
 * Implements the same IKnowledgeStore interface as the SQLite KnowledgeDB class,
 * adapting all queries for PostgreSQL syntax (JSONB, BYTEA, ON CONFLICT, etc.).
 *
 * Used by StoreRegistry for postgres-kind stores.
 */
export class PostgresKnowledgeDB implements IKnowledgeStore {
	private sql: postgres.Sql;
	/**
	 * Promise-based init lock: null = not started, pending Promise = in-flight,
	 * resolved Promise = complete. All callers await the same Promise so
	 * concurrent initialize() calls are safe.
	 */
	private initPromise: Promise<void> | null = null;

	/**
	 * Advisory lock key derived from the Postgres database OID at init time.
	 * Null until initialize() completes. Using the OID makes the lock key
	 * config-name independent: any two processes pointing at the same physical
	 * database will compute the same key regardless of the local store name.
	 *
	 * Stored as bigint to avoid the signed 32-bit overflow that `oid::integer`
	 * would introduce on high-OID databases (OIDs are unsigned 32-bit; values
	 * ≥ 2^31 wrap negative under a signed cast, producing a different lock key
	 * across processes that receive the OID at different points in time).
	 * pg_try_advisory_lock(bigint) accepts the full 64-bit range.
	 */
	private advisoryLockKey: bigint | null = null;

	/** Reserved connection held for the duration of a consolidation lock. */
	private lockConnection: postgres.ReservedSql | null = null;

	/**
	 * True once ensureVectorColumn() has confirmed that:
	 *   1. The pgvector extension is installed.
	 *   2. The `embedding_vec vector(N)` column exists on knowledge_entry.
	 *   3. The ANN index path required by similarity queries is available.
	 *
	 * Set to false on construction; lazily set to true by ensureVectorColumn().
	 * When false, findSimilarEntries falls back gracefully (returns empty so
	 * callers revert to the in-process full scan) rather than throwing.
	 */
	private pgvectorReady = false;

	/** Active embedding dimensionality from embedding_metadata.id = 1. */
	private embeddingDims: number | null = null;

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
			// idle_timeout is intentionally disabled (0 = no timeout).
			// The advisory lock acquired by tryAcquireConsolidationLock() is
			// session-scoped: it is silently released if the underlying connection
			// is recycled by the pool. A consolidation run can hold the lock for
			// several minutes (LLM calls, embeddings, contradiction scan). Setting
			// idle_timeout > 0 risks losing the advisory lock mid-run and allowing
			// a second process to enter the critical section. Pool connections are
			// released explicitly after each operation so they do not accumulate.
			idle_timeout: 0,
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
		// Fetch the database OID for use as the per-DB advisory lock key.
		// This is config-name independent: any two processes connected to the
		// same physical database will compute the same key, regardless of what
		// local store name the user has assigned in their config.
		// Cast to bigint (not integer) to avoid signed 32-bit overflow: OIDs are
		// unsigned 32-bit values and can exceed 2^31-1 on active systems, which
		// would produce a negative (incorrect) value under a ::integer cast.
		const oidRows = await this.sql`
			SELECT oid::bigint AS oid FROM pg_database WHERE datname = current_database()
		`;
		this.advisoryLockKey = BigInt((oidRows[0] as { oid: string | number }).oid);

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

		// PG_MIGRATIONS is imported from ./migrations.ts — see that file for history.
		//
		// Gap-safe: migrations are filtered to version > currentVersion and applied
		// in ascending order.  If the DB is at v10 and migrations v11–v16 all exist,
		// every one of them runs in sequence even though intermediate versions (e.g.
		// v12, v13) were never individually stamped on this database.  Each migration
		// stamps its own version row, so a crash mid-sequence leaves the DB at the
		// last successfully committed version and restarts cleanly from there.
		if (currentVersion < SCHEMA_VERSION) {
			const pending = PG_MIGRATIONS
				.filter((m) => m.version > currentVersion)
				.sort((a, b) => a.version - b.version);

			if (pending.length > 0) {
				logger.log(
					`[pg-db] Schema at v${currentVersion}, target v${SCHEMA_VERSION}. Applying ${pending.length} migration(s): ${pending.map((m) => `v${m.version} (${m.label})`).join(", ")}.`,
				);
			}

			let migratedTo = currentVersion;
			for (const migration of pending) {
				logger.log(
					`[pg-db] Applying migration v${migration.version}: ${migration.label}.`,
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
					`[pg-db] Migration v${migration.version} complete.`,
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
					await sql`DROP TABLE IF EXISTS embedding_metadata CASCADE`;
					// Drop staging tables that may exist from pre-v14 Postgres schemas.
					// These now live in state.db (ServerStateDB) — safe to drop here
					// since migrateFromKnowledgeDb() would have already copied them.
					await sql`DROP TABLE IF EXISTS pending_episodes CASCADE`;
					await sql`DROP TABLE IF EXISTS consolidated_episode CASCADE`;
					await sql`DROP TABLE IF EXISTS consolidation_state CASCADE`;
					await sql`DROP TABLE IF EXISTS schema_version CASCADE`;
					await sql.unsafe(PG_CREATE_TABLES);
					await sql`
						INSERT INTO schema_version (version, applied_at)
						VALUES (${SCHEMA_VERSION}, ${Date.now()})
					`;
				});
			}
		}

		// ── pgvector: ensure embedding_vec column + HNSW index ───────────────────
		// Called unconditionally after every initialize() path (fresh, migrated,
		// or already up-to-date).  The method is idempotent: it checks for the
		// extension, column, and index before creating anything.  If embedding
		// dimensions are not yet known (no metadata row) it sets pgvectorReady=false
		// and defers column creation to the next setEmbeddingMetadata() call.
		await this.ensureVectorColumn();
	}

	// ── pgvector helpers ──

	/**
	 * Ensure the pgvector extension, `embedding_vec vector(N)` column, and HNSW
	 * index all exist.  Idempotent — safe to call on every startup.
	 *
	 * Defers silently when no embedding_metadata row exists yet (dimensions
	 * unknown).  setEmbeddingMetadata() calls this again once dimensions are
	 * recorded, completing the setup.
	 *
	 * Sets this.pgvectorReady = true on success, false when the extension is
	 * unavailable or dimensions are not yet known.
	 */
	private async ensureVectorColumn(): Promise<void> {
		// Check extension availability first — pgvector may not be installed.
		try {
			await this.sql`CREATE EXTENSION IF NOT EXISTS vector`;
		} catch {
			// pgvector not installed on this Postgres instance — degrade gracefully.
			logger.warn(
				"[pg-db] pgvector extension not available — vector search disabled. " +
					"Install pgvector to enable ANN search (see https://github.com/pgvector/pgvector).",
			);
			this.pgvectorReady = false;
			return;
		}

		// Read dimension from metadata singleton.
		const meta = await this.sql`
			SELECT dimensions FROM embedding_metadata WHERE id = 1
		`;
		if (meta.length === 0) {
			// Dimensions not yet known — defer until setEmbeddingMetadata() is called.
			this.embeddingDims = null;
			this.pgvectorReady = false;
			return;
		}
		const dims = Number(meta[0].dimensions);
		this.embeddingDims = dims;

		// Add embedding_vec column if absent.
		const colExists = await this.sql`
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name   = 'knowledge_entry'
			  AND column_name  = 'embedding_vec'
		`;

		if (colExists.length === 0) {
			// Add the column (DDL cannot run inside a transaction on some managed
			// Postgres providers, but ALTER TABLE ADD COLUMN is safe here because
			// the column did not exist a moment ago — no concurrent writers).
			await this.sql.unsafe(
				`ALTER TABLE knowledge_entry ADD COLUMN embedding_vec vector(${dims})`,
			);
			logger.log(`[pg-db] Added embedding_vec vector(${dims}) column.`);

			// Backfill embedding_vec from existing BYTEA embeddings via JS.
			// ::bit(32)::float4 is not a valid cast in standard Postgres 16, so we
			// fetch BYTEA rows, convert in JS, and write back in a single batched
			// UNNEST update (one round-trip regardless of row count).
			const toBackfill = await this.sql<{ id: string; embedding: Buffer }[]>`
				SELECT id, embedding FROM knowledge_entry WHERE embedding IS NOT NULL
			`;
			if (toBackfill.length > 0) {
				const ids: string[] = [];
				const vecs: string[] = [];
				for (const row of toBackfill) {
					ids.push(row.id);
					vecs.push(floatsToVectorLiteral(bufferToFloats(row.embedding)));
				}
				await this.sql.unsafe(
					"UPDATE knowledge_entry SET embedding_vec = v.vec::vector " +
					"FROM (SELECT UNNEST($1::text[]) AS id, UNNEST($2::text[]) AS vec) v " +
					"WHERE knowledge_entry.id = v.id",
					[ids, vecs],
				);
				logger.log(`[pg-db] Backfilled embedding_vec for ${toBackfill.length} rows.`);
			}
		} else {
			// Column exists — check its declared dimension matches metadata.
			// If the model changed (dims changed), drop and recreate the column.
			// checkAndReEmbed() calls setEmbeddingMetadata() after re-embedding all
			// rows, which calls ensureVectorColumn() again. We set pgvectorReady=false
			// here so activations during the re-embed window fall back to Path B
			// (full scan) rather than returning empty ANN results.
			const dimRows = await this.sql`
				SELECT format_type(atttypid, atttypmod) AS declared_type
				FROM pg_attribute
				WHERE attrelid = 'knowledge_entry'::regclass
				  AND attname  = 'embedding_vec'
				  AND attnum   > 0
			`;
			if (dimRows.length > 0) {
				// Parse declared type text (e.g. "vector(3072)") instead of relying on
				// atttypmod encoding, which differs across pgvector/Postgres builds.
				const declared = String((dimRows[0] as { declared_type: string }).declared_type);
				const m = declared.match(/\((\d+)\)/);
				const storedDim = m ? Number(m[1]) : -1;
				if (storedDim !== -1 && storedDim !== dims) {
					logger.log(
						`[pg-db] embedding_vec dimension mismatch (stored=${storedDim}, expected=${dims}) — recreating column. ANN search disabled until re-embed completes.`,
					);
					// Wrap the three DDL statements in a transaction so a crash mid-sequence
					// leaves the table in a consistent state (either old column present or
					// new empty column present — never column-absent with data still there).
					await this.sql.begin(async (sql: TxSql) => {
						await sql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
						await sql`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
						await sql.unsafe(
							"ALTER TABLE knowledge_entry DROP COLUMN embedding_vec",
						);
						await sql.unsafe(
							`ALTER TABLE knowledge_entry ADD COLUMN embedding_vec vector(${dims})`,
						);
					});
					logger.log(`[pg-db] Recreated embedding_vec as vector(${dims}). Rows will be backfilled by the re-embed cycle.`);
					// Column is empty — set false so Path B is used until re-embed
					// populates the column via updateEntry() calls.
					this.pgvectorReady = false;
					return;
				}
			}
		}

		// Self-heal: if embedding_vec exists but rows are missing (legacy rollout
		// edge case), backfill only the NULL subset from BYTEA embedding.
		const toBackfill = await this.sql<{ id: string; embedding: Buffer }[]>`
			SELECT id, embedding
			FROM knowledge_entry
			WHERE embedding IS NOT NULL
			  AND embedding_vec IS NULL
		`;
		if (toBackfill.length > 0) {
			const ids: string[] = [];
			const vecs: string[] = [];
			for (const row of toBackfill) {
				ids.push(row.id);
				vecs.push(floatsToVectorLiteral(bufferToFloats(row.embedding)));
			}
			await this.sql.unsafe(
				"UPDATE knowledge_entry SET embedding_vec = v.vec::vector " +
				"FROM (SELECT UNNEST($1::text[]) AS id, UNNEST($2::text[]) AS vec) v " +
				"WHERE knowledge_entry.id = v.id",
				[ids, vecs],
			);
			logger.log(`[pg-db] Self-healed embedding_vec backfill for ${toBackfill.length} rows.`);
		}

		// Create ANN index if absent (idempotent due to IF NOT EXISTS).
		//
		// We intentionally use halfvec HNSW + exact rerank to support common large
		// embedding models (>2000 dims) while keeping ANN. pgvector supports HNSW:
		//   - vector: up to 2000 dims
		//   - halfvec: up to 4000 dims
		//
		// Query plan in findSimilarEntries()/findContradictionCandidates():
		//   1) ANN candidate selection on (embedding_vec::halfvec(N))
		//   2) exact cosine rerank on embedding_vec
		//
		// Executed outside the backfill transaction so a transient index-build
		// failure (e.g. OOM on a large table) doesn't roll back the backfill.
		// If index creation fails, we degrade gracefully: pgvectorReady stays
		// false and activations continue on Path B (full scan) until the next
		// startup successfully builds the index.
		if (dims > 4000) {
			logger.warn(
				`[pg-db] embedding dimension ${dims} exceeds halfvec HNSW limit (4000). ANN search disabled for this store.`,
			);
			this.pgvectorReady = false;
			return;
		}
		try {
			await this.sql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
			await this.sql.unsafe(`
				CREATE INDEX IF NOT EXISTS idx_entry_embedding_halfvec_hnsw
				ON knowledge_entry
				USING hnsw ((embedding_vec::halfvec(${dims})) halfvec_cosine_ops)
				WITH (m = 16, ef_construction = 64)
			`);
		} catch (err) {
			logger.warn(
				`[pg-db] ANN index creation failed — ANN search disabled. Will retry on next startup. Error: ${err instanceof Error ? err.message : String(err)}`,
			);
			this.pgvectorReady = false;
			return;
		}

		this.pgvectorReady = true;
		logger.log(`[pg-db] pgvector ready (vector(${dims}), halfvec HNSW + exact rerank).`);
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
		const embeddingVec =
			this.pgvectorReady && entry.embedding
				? floatsToVectorLiteral(entry.embedding)
				: null;

		// Include embedding_vec in the INSERT only when pgvectorReady — the column
		// does not exist until ensureVectorColumn() creates it (which requires
		// embedding_metadata to be set first). Referencing a non-existent column
		// in the column list causes an immediate SQL error regardless of the value.
		if (this.pgvectorReady && embeddingVec !== null) {
			await this.sql.unsafe(
				`INSERT INTO knowledge_entry
				(id, type, content, topics, confidence, source, status, strength,
				 created_at, updated_at, last_accessed_at, access_count, observation_count,
				 superseded_by, derived_from, is_synthesized, embedding, embedding_vec)
				VALUES (
					$1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, $13,
					$14, $15::jsonb, $16, $17, $18::vector
				)`,
				[
					entry.id, entry.type, entry.content,
					this.sql.json(entry.topics),
					entry.confidence, entry.source, entry.status,
					entry.strength, entry.createdAt, entry.updatedAt,
					entry.lastAccessedAt, entry.accessCount, entry.observationCount,
					entry.supersededBy, this.sql.json(entry.derivedFrom),
					entry.isSynthesized ? 1 : 0, embeddingBuf,
					embeddingVec,
				] as postgres.ParameterOrJSON<never>[],
			);
		} else {
			await this.sql.unsafe(
				`INSERT INTO knowledge_entry
				(id, type, content, topics, confidence, source, status, strength,
				 created_at, updated_at, last_accessed_at, access_count, observation_count,
				 superseded_by, derived_from, is_synthesized, embedding)
				VALUES (
					$1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, $13,
					$14, $15::jsonb, $16, $17
				)`,
				[
					entry.id, entry.type, entry.content,
					this.sql.json(entry.topics),
					entry.confidence, entry.source, entry.status,
					entry.strength, entry.createdAt, entry.updatedAt,
					entry.lastAccessedAt, entry.accessCount, entry.observationCount,
					entry.supersededBy, this.sql.json(entry.derivedFrom),
					entry.isSynthesized ? 1 : 0, embeddingBuf,
				] as postgres.ParameterOrJSON<never>[],
			);
		}
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
			values.push(this.sql.json(updates.topics));
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
		if (updates.embedding !== undefined) {
			setClauses.push(`embedding = $${idx++}`);
			values.push(floatsToBuffer(updates.embedding));
			if (this.pgvectorReady) {
				// updates.embedding is always a non-null number[] in this branch
				// (guarded by `updates.embedding !== undefined` above). Cast directly.
				setClauses.push(`embedding_vec = $${idx++}::vector`);
				values.push(floatsToVectorLiteral(updates.embedding));
			}
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
		const embeddingVec =
			this.pgvectorReady && embedding
				? floatsToVectorLiteral(embedding)
				: null;
		const now = Date.now();

		// Three-way branch on pgvectorReady + whether an embedding was provided:
		//   A) pgvectorReady + embedding provided → set both BYTEA and vector columns
		//   B) pgvectorReady + no embedding       → null both columns (keeps them in sync)
		//   C) !pgvectorReady                     → omit embedding_vec (column absent)
		if (this.pgvectorReady && embeddingVec !== null) {
			// Case A: write new embedding to both columns.
			await this.sql.unsafe(
				`UPDATE knowledge_entry
				SET content = $1, type = $2,
				    topics = $3::jsonb,
				    confidence = $4,
				    derived_from = $5::jsonb,
				    updated_at = $6, last_accessed_at = $6,
				    observation_count = observation_count + 1,
				    embedding = $7,
				    embedding_vec = $8::vector
				WHERE id = $9`,
				[
					updates.content, safeType,
					this.sql.json(updates.topics),
					updates.confidence,
					this.sql.json(mergedSources),
					now, embeddingBuf,
					embeddingVec,
					id,
				] as postgres.ParameterOrJSON<never>[],
			);
		} else if (this.pgvectorReady) {
			// Case B: no embedding provided but column exists — null both to keep in sync.
			// Without this, embedding (BYTEA) → NULL but embedding_vec retains its old
			// value, causing ANN queries to return stale results until ensureEmbeddings runs.
			await this.sql.unsafe(
				`UPDATE knowledge_entry
				SET content = $1, type = $2,
				    topics = $3::jsonb,
				    confidence = $4,
				    derived_from = $5::jsonb,
				    updated_at = $6, last_accessed_at = $6,
				    observation_count = observation_count + 1,
				    embedding = $7,
				    embedding_vec = NULL
				WHERE id = $8`,
				[
					updates.content, safeType,
					this.sql.json(updates.topics),
					updates.confidence,
					this.sql.json(mergedSources),
					now, embeddingBuf,
					id,
				] as postgres.ParameterOrJSON<never>[],
			);
		} else {
			// Case C: embedding_vec column doesn't exist yet — omit it entirely.
			await this.sql.unsafe(
				`UPDATE knowledge_entry
				SET content = $1, type = $2,
				    topics = $3::jsonb,
				    confidence = $4,
				    derived_from = $5::jsonb,
				    updated_at = $6, last_accessed_at = $6,
				    observation_count = observation_count + 1,
				    embedding = $7
				WHERE id = $8`,
				[
					updates.content, safeType,
					this.sql.json(updates.topics),
					updates.confidence,
					this.sql.json(mergedSources),
					now, embeddingBuf,
					id,
				] as postgres.ParameterOrJSON<never>[],
			);
		}
	}

	async reinitialize(): Promise<void> {
		await this.sql.begin(async (sql: TxSql) => {
			await sql`DELETE FROM knowledge_cluster_member`;
			await sql`DELETE FROM knowledge_cluster`;
			await sql`DELETE FROM knowledge_relation`;
			await sql`DELETE FROM knowledge_entry`;
			await sql`DELETE FROM embedding_metadata`;
		});
		// Deleting embedding_metadata invalidates the stored dimensions.
		// Reset pgvectorReady so the next setEmbeddingMetadata() call re-runs
		// ensureVectorColumn(), preventing dimension-mismatch corruption when
		// a new model is configured after a knowledge reset.
		this.pgvectorReady = false;
		this.embeddingDims = null;
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
		// Lazily create (or recreate after a model/dimension change) the vector
		// column and HNSW index now that the dimension is known.
		await this.ensureVectorColumn();
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

		// Build the array parameter outside the transaction — postgres.js's
		// TransactionSql scope does not expose .array(); only the top-level sql does.
		const keepIdsParam =
			newClusterIds.size > 0
				? this.sql.array([...newClusterIds]) // postgres.js infers text (OID 25) for string[]
				: null;

		await this.sql.begin(async (sql: TxSql) => {
			// Remove stale clusters in a single DELETE rather than N round-trips.
			// ON DELETE CASCADE on knowledge_cluster_member handles membership cleanup.
			if (keepIdsParam !== null) {
				await sql`
					DELETE FROM knowledge_cluster
					WHERE id != ALL(${keepIdsParam})
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
		// Only clear embedding_vec when the column exists.
		const result = this.pgvectorReady
			? await this.sql`
				UPDATE knowledge_entry SET embedding = NULL, embedding_vec = NULL
				WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL
			`
			: await this.sql`
				UPDATE knowledge_entry SET embedding = NULL
				WHERE status IN ('active', 'conflicted') AND embedding IS NOT NULL
			`;
		return result.count;
	}

	// ── Vector Search ─────────────────────────────────────────────────────────

	/**
	 * Returns true when pgvector is fully operational: extension installed,
	 * embedding_vec column present, HNSW index built, and dimensions known.
	 * ActivationEngine checks this (via allStoresSupportAnn) before taking Path A.
	 */
	isVectorSearchReady(): boolean {
		return this.pgvectorReady;
	}

	/**
	 * Return the count of active+conflicted entries.
	 * Used by ActivationEngine (Path A) to report an accurate totalActive without
	 * loading all entries into memory.
	 */
	async getActiveEntryCount(): Promise<number> {
		const rows = await this.sql`
			SELECT COUNT(*) AS cnt FROM knowledge_entry
			WHERE status IN ('active', 'conflicted')
		`;
		return Number((rows[0] as { cnt: string | number }).cnt);
	}

	/**
	 * Find the `limit` entries most similar to `queryVector` using the HNSW index.
	 *
	 * Returns entries whose cosine similarity to `queryVector` is >= `threshold`,
	 * ordered by similarity descending (closest first).
	 *
	 * Falls back to returning an empty array when pgvector is not available
	 * (pgvectorReady = false), signalling callers to use the in-process full scan.
	 *
	 * The cosine distance operator `<=>` returns values in [0, 2]:
	 *   0 = identical, 2 = opposite.
	 * We convert to similarity: similarity = 1 − distance.
	 *
	 * `statuses` defaults to ['active', 'conflicted'] to match
	 * getActiveEntriesWithEmbeddings() behaviour.
	 */
	async findSimilarEntries(
		queryVector: number[],
		limit: number,
		threshold: number,
		statuses: KnowledgeStatus[] = ["active", "conflicted"],
	): Promise<
		Array<{ entry: KnowledgeEntry & { embedding: number[] }; similarity: number }>
	> {
		if (!this.pgvectorReady || this.embeddingDims === null) return [];

		const vectorLiteral = floatsToVectorLiteral(queryVector);
		// Two-step ANN + exact rerank:
		//  1) candidate_ids CTE uses halfvec HNSW index
		//  2) outer query reranks candidates with exact cosine on embedding_vec
		// This keeps ANN speed while preserving full-vector ranking quality.
		const maxDist = 1 - threshold; // distance <= maxDist ↔ similarity >= threshold
		const annCandidates = Math.max(limit * 8, 100);
		const rows = await this.sql.unsafe(
			`WITH candidate_ids AS (
			   SELECT id
			   FROM knowledge_entry
			   WHERE status = ANY($2::text[])
			     AND embedding_vec IS NOT NULL
			   ORDER BY (embedding_vec::halfvec(${this.embeddingDims}) <=> $1::halfvec(${this.embeddingDims}))
			   LIMIT $3
			 )
			 SELECT ke.*, (1 - (ke.embedding_vec <=> $4::vector)) AS similarity
			 FROM knowledge_entry ke
			 JOIN candidate_ids c ON c.id = ke.id
			 WHERE ke.embedding_vec <=> $4::vector <= $5
			 ORDER BY ke.embedding_vec <=> $4::vector
			 LIMIT $6`,
			[
				vectorLiteral,
				statuses,
				annCandidates,
				vectorLiteral,
				maxDist,
				limit,
			] as postgres.ParameterOrJSON<never>[],
		);

		return (rows as unknown as (RawEntryRow & { similarity: number })[])
			.map((r) => ({
				entry: this.rowToEntry(r) as KnowledgeEntry & { embedding: number[] },
				similarity: Number(r.similarity),
			}))
			.filter((r): r is { entry: KnowledgeEntry & { embedding: number[] }; similarity: number } =>
				!!r.entry.embedding,
			);
	}

	/**
	 * Find active/conflicted entries that share at least one topic with `topics`
	 * AND whose cosine similarity to `queryVector` falls in [minSimilarity, maxSimilarity).
	 * IDs in `excludeIds` are always excluded.
	 *
	 * Implemented with pgvector's `<=>` operator so both filters execute in the DB,
	 * avoiding the round-trip of loading all topic-overlapping rows into memory.
	 *
	 * Falls back to returning an empty array when pgvector is not ready — the caller
	 * (ContradictionScanner) then falls back to getEntriesWithOverlappingTopics().
	 */
	async findContradictionCandidates(
		queryVector: number[],
		topics: string[],
		excludeIds: string[],
		minSimilarity: number,
		maxSimilarity: number,
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>> {
		if (!this.pgvectorReady || this.embeddingDims === null || topics.length === 0)
			return [];

		const vectorLiteral = floatsToVectorLiteral(queryVector);
		// similarity = 1 − distance; band is [minSimilarity, maxSimilarity).
		// In distance space: distance ∈ (1−maxSimilarity, 1−minSimilarity].
		const minDist = 1 - maxSimilarity; // exclusive lower bound on distance
		const maxDist = 1 - minSimilarity; // inclusive upper bound on distance
		const annCandidates = 300;

		// Two-step ANN + exact rerank with topic pre-filtering.
		// topic_candidates limits the ANN search space to only overlapping-topic rows,
		// then candidate_ids uses halfvec HNSW, and the outer query applies the exact
		// contradiction similarity band on full vectors.
		// `id != ALL($2::text[])` with an empty array is always TRUE in Postgres, so
		// we always include the clause and always pass excludeIds — even when empty.
		// This keeps parameter numbering fixed regardless of excludeIds content.
		const rows = await this.sql.unsafe(
			`WITH topic_candidates AS (
			   SELECT DISTINCT ke.id, ke.embedding_vec
			   FROM knowledge_entry ke,
			        jsonb_array_elements_text(ke.topics) AS t(value)
			   WHERE ke.status IN ('active', 'conflicted')
			     AND ke.embedding_vec IS NOT NULL
			     AND t.value = ANY($1::text[])
			     AND ke.id != ALL($2::text[])
			 ),
			 candidate_ids AS (
			   SELECT id
			   FROM topic_candidates
			   ORDER BY (embedding_vec::halfvec(${this.embeddingDims}) <=> $3::halfvec(${this.embeddingDims}))
			   LIMIT $6
			 )
			 SELECT ke.*
			 FROM knowledge_entry ke
			 JOIN candidate_ids c ON c.id = ke.id
			 WHERE (ke.embedding_vec <=> $3::vector) >  $4
			   AND (ke.embedding_vec <=> $3::vector) <= $5
			 ORDER BY (ke.embedding_vec <=> $3::vector)`,
			[
				topics,
				excludeIds,
				vectorLiteral,
				minDist,
				maxDist,
				annCandidates,
			] as postgres.ParameterOrJSON<never>[],
		);

		return (rows as unknown as RawEntryRow[])
			.map((r) => this.rowToEntry(r))
			.filter(
				(e): e is KnowledgeEntry & { embedding: number[] } => !!e.embedding,
			);
	}

	// ── Consolidation Lock ────────────────────────────────────────────────────

	async tryAcquireConsolidationLock(): Promise<boolean> {
		await this.initialize();
		// advisoryLockKey is set during _initialize(). If it is somehow still null
		// after initialize() completes, that is a programming error — throw rather
		// than silently proceeding without any lock, which would allow concurrent
		// writes to the same database.
		if (this.advisoryLockKey === null) {
			throw new Error(
				"[pg-db] Advisory lock key not set after initialize() — cannot acquire consolidation lock.",
			);
		}
		// Double-acquire indicates a missing releaseConsolidationLock() call upstream.
		// Throw rather than returning false: the caller (consolidateExtractedToStore)
		// would treat false as "another process holds the lock" and silently skip the
		// store, masking the bug. A throw surfaces it immediately.
		if (this.lockConnection !== null) {
			throw new Error(
				"[pg-db] tryAcquireConsolidationLock called while lock already held. " +
					"Check for a missing releaseConsolidationLock() call.",
			);
		}
		// Reserve a dedicated connection for the advisory lock. The idle_timeout on
		// this connection is disabled (set in the pool constructor below) to prevent
		// Postgres from dropping the session while a long consolidation run is in
		// progress — advisory locks are session-scoped and would be silently released
		// if the underlying connection is recycled.
		this.lockConnection = await this.sql.reserve();
		try {
			// postgres.js does not accept bigint template parameters — cast to Number.
			// This is safe: OIDs are unsigned 32-bit values (max ~4.3B), which JS
			// Number represents exactly (doubles have 53-bit mantissa precision).
			const result = await this.lockConnection`
				SELECT pg_try_advisory_lock(${Number(this.advisoryLockKey)}) as acquired
			`;
			const acquired = (result[0] as { acquired: boolean }).acquired;
			if (!acquired) {
				this.lockConnection.release();
				this.lockConnection = null;
			}
			return acquired;
		} catch (e) {
			this.lockConnection?.release();
			this.lockConnection = null;
			throw e;
		}
	}

	async releaseConsolidationLock(): Promise<void> {
		if (!this.lockConnection || this.advisoryLockKey === null) return;
		// Always release the reserved connection, even if pg_advisory_unlock throws
		// (e.g. transient network error). A stale lockConnection would permanently
		// consume a pool slot and cause the double-acquire guard to throw on the
		// next consolidation run.
		try {
			await this.lockConnection`
				SELECT pg_advisory_unlock(${Number(this.advisoryLockKey)})
			`;
		} finally {
			this.lockConnection.release();
			this.lockConnection = null;
		}
	}

	async close(): Promise<void> {
		if (this.lockConnection) {
			await this.releaseConsolidationLock();
		}
		await this.sql.end();
	}
}
