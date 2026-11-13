import { logger } from "../../logger.js";
import { bufferToFloats, floatsToVectorLiteral } from "./embedding-codec.js";

// biome-ignore lint: TS limitation with Omit stripping call signatures from postgres Sql
type TxSql = any;

/** halfvec HNSW supports up to 4000 dimensions. */
const HALFVEC_HNSW_MAX_DIMS = 4000;

/**
 * Run the pgvector part of a migration under a SAVEPOINT.
 *
 * Postgres aborts the whole transaction on the first failed statement (SQLSTATE
 * 25P02). Catching the error in JS does not help: the schema_version stamp that
 * follows would fail too, the migration would roll back, and the store could never
 * start (no pgvector installed, a privilege error, or an index build that runs out
 * of memory). Under a savepoint only the vector work is rolled back. The version is
 * still stamped, and ensureVectorColumn() at startup, which runs outside any
 * transaction and degrades on its own, retries the vector setup on later starts.
 */
async function bestEffortVectorStep(
	outer: TxSql,
	version: number,
	step: (sql: TxSql) => Promise<void>,
): Promise<void> {
	try {
		await outer.savepoint(step);
	} catch (err) {
		logger.warn(
			`[pg-db] Migration v${version}: pgvector setup skipped — vector search stays off for this store until ensureVectorColumn() succeeds on a later startup. Error: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
}

/**
 * Run one owner-only statement under its own SAVEPOINT. A role that is not the
 * table owner gets 42501 (insufficient_privilege) from DROP INDEX / ALTER TABLE.
 * That is skipped with a warning so the store still opens; any other error stays
 * fatal. The savepoint keeps the transaction usable for the statements that follow.
 */
async function skipWithoutOwnership(
	outer: TxSql,
	version: number,
	statement: string,
	step: (sql: TxSql) => Promise<unknown>,
): Promise<void> {
	try {
		await outer.savepoint(step);
	} catch (err) {
		if ((err as { code?: string }).code !== "42501") throw err;
		logger.warn(
			`[pg-db] Migration v${version}: skipped \`${statement}\` — the connecting role does not own the object. Performance tuning stays unapplied until the owner runs it. Error: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
}

/** Create the halfvec HNSW index. Errors propagate to the caller's savepoint. */
async function createHalfvecIndex(sql: TxSql, dims: number): Promise<void> {
	await sql.unsafe(`
		CREATE INDEX IF NOT EXISTS idx_entry_embedding_halfvec_hnsw
		ON knowledge_entry
		USING hnsw ((embedding_vec::halfvec(${dims})) halfvec_cosine_ops)
		WITH (m = 16, ef_construction = 64)
	`);
}

/**
 * Incremental PostgreSQL migration registry.
 *
 * Rules for adding a new migration:
 *  1. Append a new { version, label, up } entry — never reorder or modify existing ones.
 *  2. `up` must be idempotent (IF NOT EXISTS, information_schema checks, etc.).
 *  3. Only additive changes (new tables, new nullable columns) are safe.
 *     Destructive changes that can't be expressed idempotently should fall
 *     through to the drop+recreate path.
 */
export const PG_MIGRATIONS: Array<{
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
	{
		version: 11,
		label:
			"add user_id to source_cursor and consolidated_episode for multi-user support",
		up: async (sql: TxSql) => {
			// source_cursor: add user_id column, rebuild PK to (source, user_id).
			// Add table_schema filter so we don't match a same-named table in a
			// different schema (e.g. when multiple apps share one PG instance).
			const cursorCols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'source_cursor'
				  AND column_name = 'user_id'
			`;
			if (cursorCols.length === 0) {
				await sql`
					ALTER TABLE source_cursor
					ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'
				`;
				// Look up the actual PK constraint name rather than assuming
				// the auto-generated name — avoids failure on restored dumps
				// or manually named constraints.
				const cursorPkRows = await sql`
					SELECT constraint_name FROM information_schema.table_constraints
					WHERE table_schema = current_schema()
					  AND table_name = 'source_cursor'
					  AND constraint_type = 'PRIMARY KEY'
				`;
				const cursorPkName =
					cursorPkRows.length > 0
						? String(cursorPkRows[0].constraint_name ?? "").trim()
						: "";
				if (cursorPkName) {
					await sql`ALTER TABLE source_cursor DROP CONSTRAINT ${sql(cursorPkName)}`;
				} else {
					// No PK found via information_schema — log a warning so operators
					// can investigate, then attempt ADD PRIMARY KEY anyway. If a PK
					// actually exists under an unexpected name, the ADD will fail with
					// a clear Postgres error rather than silently corrupting state.
					logger.warn(
						"[pg-db] v11 migration: could not find source_cursor primary key via information_schema. Proceeding with ADD PRIMARY KEY — will fail if an unnamed PK already exists.",
					);
				}
				await sql`ALTER TABLE source_cursor ADD PRIMARY KEY (source, user_id)`;
			}

			// consolidated_episode: add user_id column, rebuild PK.
			const episodeCols = await sql`
				SELECT column_name FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'consolidated_episode'
				  AND column_name = 'user_id'
			`;
			if (episodeCols.length === 0) {
				await sql`
					ALTER TABLE consolidated_episode
					ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default'
				`;
				const episodePkRows = await sql`
					SELECT constraint_name FROM information_schema.table_constraints
					WHERE table_schema = current_schema()
					  AND table_name = 'consolidated_episode'
					  AND constraint_type = 'PRIMARY KEY'
				`;
				const episodePkName =
					episodePkRows.length > 0
						? String(episodePkRows[0].constraint_name ?? "").trim()
						: "";
				if (episodePkName) {
					await sql`ALTER TABLE consolidated_episode DROP CONSTRAINT ${sql(episodePkName)}`;
				} else {
					logger.warn(
						"[pg-db] v11 migration: could not find consolidated_episode primary key via information_schema. Proceeding with ADD PRIMARY KEY — will fail if an unnamed PK already exists.",
					);
				}
				await sql`
					ALTER TABLE consolidated_episode
					ADD PRIMARY KEY (source, user_id, session_id, start_message_id, end_message_id)
				`;
				// Update index to include user_id
				await sql`DROP INDEX IF EXISTS idx_episode_source_session`;
				await sql`
					CREATE INDEX IF NOT EXISTS idx_episode_source_user_session
					ON consolidated_episode(source, user_id, session_id)
				`;
			}
		},
	},
	{
		version: 12,
		label: "add pending_episodes table for episode uploader daemon",
		up: async (sql: TxSql) => {
			// pending_episodes: staging table for daemon-uploaded episodes.
			// daemon_cursor lives in local SQLite only — not created in Postgres.
			const tableExists = await sql`
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = current_schema()
				  AND table_name = 'pending_episodes'
			`;
			if (tableExists.length === 0) {
				await sql`
					CREATE TABLE pending_episodes (
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
						session_timestamp BIGINT  NOT NULL DEFAULT 0,
						max_message_time  BIGINT  NOT NULL DEFAULT 0,
						approx_tokens    INTEGER NOT NULL DEFAULT 0,
						uploaded_at      BIGINT  NOT NULL
					)
				`;
				await sql`
				CREATE INDEX IF NOT EXISTS idx_pending_source_user_time
				ON pending_episodes(source, user_id, max_message_time)
			`;
			}
		},
	},
	{
		version: 13,
		label:
			"daemon-only consolidation: drop source_cursor, remove user_id from consolidated_episode",
		up: async (sql: TxSql) => {
			// Drop source_cursor — no longer needed with daemon-only consolidation.
			await sql`DROP TABLE IF EXISTS source_cursor`;

			// Remove user_id from consolidated_episode PK.
			// Guard: only run if user_id column still exists.
			const userIdCol = await sql`
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'consolidated_episode'
				  AND column_name = 'user_id'
			`;
			if (userIdCol.length > 0) {
				// Rename old table, create new without user_id, copy data, drop old.
				await sql`ALTER TABLE consolidated_episode RENAME TO consolidated_episode_old`;
				await sql`
					CREATE TABLE consolidated_episode (
						source           TEXT    NOT NULL,
						session_id       TEXT    NOT NULL,
						start_message_id TEXT    NOT NULL,
						end_message_id   TEXT    NOT NULL,
						content_type     TEXT    NOT NULL,
						processed_at     BIGINT  NOT NULL,
						entries_created  INTEGER NOT NULL DEFAULT 0,
						PRIMARY KEY (source, session_id, start_message_id, end_message_id)
					)
				`;
				// INSERT IGNORE equivalent in Postgres: ON CONFLICT DO NOTHING deduplicates
				// in case the same (source, session_id, start, end) existed for multiple user_ids.
				await sql`
					INSERT INTO consolidated_episode
					SELECT source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created
					FROM consolidated_episode_old
					ON CONFLICT (source, session_id, start_message_id, end_message_id) DO NOTHING
				`;
				await sql`DROP TABLE consolidated_episode_old`;
				await sql`DROP INDEX IF EXISTS idx_episode_source_user_session`;
				await sql`
					CREATE INDEX IF NOT EXISTS idx_episode_source_session
					ON consolidated_episode(source, session_id)
				`;
			}
		},
	},
	{
		version: 14,
		label:
			"retire staging tables from Postgres — they now live in state.db (ServerStateDB)",
		up: async (sql: TxSql) => {
			// consolidated_episode, consolidation_state and pending_episodes moved to the
			// server state DB. Three cases decide what happens to them here:
			//  1. A Postgres state DB (STATE_DB_URI) in this same schema owns the tables.
			//     Its init creates them before the knowledge stores open, so they are
			//     live state, not leftovers: leave them alone.
			//  2. Empty: drop them, as before.
			//  3. Rows: ServerStateDB cannot read them (it copies only a legacy SQLite
			//     knowledge.db), and a Postgres migration cannot reach state.db to copy
			//     them. Dropping would destroy unconsolidated episodes, so the table is
			//     renamed to legacy_<table> and kept for manual recovery.
			const stateOwned = await sql`
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = current_schema() AND table_name = 'state_schema_version'
			`;
			if (stateOwned.length > 0) return;

			for (const table of [
				"pending_episodes",
				"consolidated_episode",
				"consolidation_state",
			]) {
				const exists = await sql`
					SELECT 1 FROM information_schema.tables
					WHERE table_schema = current_schema() AND table_name = ${table}
				`;
				if (exists.length === 0) continue;
				// The lock is held until this transaction commits, so no row can land
				// between the count and the drop or rename.
				await sql.unsafe(`LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE`);
				const [{ n }] = await sql.unsafe(
					`SELECT COUNT(*)::int AS n FROM ${table}`,
				);
				if (n === 0) {
					await sql.unsafe(`DROP TABLE ${table} CASCADE`);
					continue;
				}
				await sql.unsafe(`ALTER TABLE ${table} RENAME TO legacy_${table}`);
				logger.warn(
					`[pg-db] Migration v14: ${table} held ${n} row(s). They are not copied to state.db and are no longer read; kept as legacy_${table} for manual recovery.`,
				);
			}
		},
	},
	{
		version: 15,
		label:
			"drop scope column — domain routing replaces personal/team scope distinction",
		up: async (sql: TxSql) => {
			// scope was a 'personal|team' classification that the LLM assigned at
			// extraction time. With multi-store domain routing, the domain field carries
			// this signal more precisely. Dropping it removes a confusing field that
			// the LLM was conflating with domain assignment.
			const col = await sql`
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = 'knowledge_entry'
				  AND column_name = 'scope'
			`;
			if (col.length > 0) {
				await sql`ALTER TABLE knowledge_entry DROP COLUMN scope`;
			}
		},
	},
	{
		version: 16,
		label: "pgvector: add embedding_vec column and ANN index",
		up: (outer: TxSql) =>
			bestEffortVectorStep(outer, 16, async (sql: TxSql) => {
				// Install pgvector before anything references its type. CREATE EXTENSION
				// runs inside the savepoint; if the server cannot provide the extension,
				// the savepoint rolls back and ensureVectorColumn() degrades at startup.
				await sql`CREATE EXTENSION IF NOT EXISTS vector`;

				// The BYTEA `embedding` column stays the source of truth (SQLite
				// compatibility, crash-recovery, re-embed path). `embedding_vec` is a
				// derived, Postgres-only column used for ANN index search. Its dimension
				// comes from embedding_metadata. Without that row the column is created
				// later by setEmbeddingMetadata() → ensureVectorColumn().
				const meta = await sql`
					SELECT dimensions FROM embedding_metadata WHERE id = 1
				`;
				if (meta.length === 0) return;
				const dims = Number(meta[0].dimensions);

				const vecColExists = await sql`
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = current_schema()
					  AND table_name   = 'knowledge_entry'
					  AND column_name  = 'embedding_vec'
				`;
				if (vecColExists.length === 0) {
					// ALTER TABLE with a dynamic dimension: use sql.unsafe so we can
					// interpolate `dims` into the DDL string. The value comes from our
					// own DB row (not user input) so this is safe.
					await sql.unsafe(
						`ALTER TABLE knowledge_entry ADD COLUMN embedding_vec vector(${dims})`,
					);

					// Backfill embedding_vec from existing BYTEA embeddings. ::bit(32)::float4
					// is not a valid cast in standard Postgres 16, so we fetch BYTEA rows and
					// convert in JS, then write back in a single batched UNNEST UPDATE.
					const toBackfill = await sql<
						{ id: string; embedding: Buffer }[]
					>`SELECT id, embedding FROM knowledge_entry WHERE embedding IS NOT NULL`;

					if (toBackfill.length > 0) {
						const ids: string[] = [];
						const vecs: string[] = [];
						for (const row of toBackfill) {
							ids.push(row.id);
							vecs.push(floatsToVectorLiteral(bufferToFloats(row.embedding)));
						}
						await sql.unsafe(
							"UPDATE knowledge_entry SET embedding_vec = v.vec::vector " +
								"FROM (SELECT UNNEST($1::text[]) AS id, UNNEST($2::text[]) AS vec) v " +
								"WHERE knowledge_entry.id = v.id",
							[ids, vecs],
						);
						logger.log(
							`[pg-db] Migration v16: backfilled embedding_vec for ${toBackfill.length} rows.`,
						);
					}
				}

				// ANN index: halfvec HNSW + exact rerank, which supports large embeddings.
				if (dims <= HALFVEC_HNSW_MAX_DIMS) {
					await createHalfvecIndex(sql, dims);
				} else {
					logger.warn(
						`[pg-db] Migration v16: embedding dimension ${dims} exceeds halfvec HNSW limit (${HALFVEC_HNSW_MAX_DIMS}). ANN index skipped for this store.`,
					);
				}
			}),
	},
	{
		version: 17,
		label: "pgvector: repair embedding_vec backfill + halfvec ANN index",
		up: (outer: TxSql) =>
			bestEffortVectorStep(outer, 17, async (sql: TxSql) => {
				await sql`CREATE EXTENSION IF NOT EXISTS vector`;

				const meta = await sql`
					SELECT dimensions FROM embedding_metadata WHERE id = 1
				`;
				if (meta.length === 0) {
					// No embedding model configured yet; column/index are created lazily when
					// setEmbeddingMetadata() runs.
					return;
				}

				const dims = Number(meta[0].dimensions);

				// Ensure embedding_vec exists with the correct dimensions.
				const vecColExists = await sql`
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = current_schema()
					  AND table_name   = 'knowledge_entry'
					  AND column_name  = 'embedding_vec'
				`;

				if (vecColExists.length === 0) {
					await sql.unsafe(
						`ALTER TABLE knowledge_entry ADD COLUMN embedding_vec vector(${dims})`,
					);
				} else {
					const dimRows = await sql`
						SELECT format_type(atttypid, atttypmod) AS declared_type
						FROM pg_attribute
						WHERE attrelid = 'knowledge_entry'::regclass
						  AND attname  = 'embedding_vec'
						  AND attnum   > 0
					`;
					if (dimRows.length > 0) {
						const declared = String(
							(dimRows[0] as { declared_type: string }).declared_type,
						);
						const m = declared.match(/\((\d+)\)/);
						const storedDim = m ? Number(m[1]) : -1;
						if (storedDim !== -1 && storedDim !== dims) {
							await sql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
							await sql`DROP INDEX IF EXISTS idx_entry_embedding_halfvec_hnsw`;
							await sql`ALTER TABLE knowledge_entry DROP COLUMN embedding_vec`;
							await sql.unsafe(
								`ALTER TABLE knowledge_entry ADD COLUMN embedding_vec vector(${dims})`,
							);
						}
					}
				}

				// Repair backfill for rows that have BYTEA embedding but missing embedding_vec.
				const toBackfill = await sql<{ id: string; embedding: Buffer }[]>`
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
					await sql.unsafe(
						"UPDATE knowledge_entry SET embedding_vec = v.vec::vector " +
							"FROM (SELECT UNNEST($1::text[]) AS id, UNNEST($2::text[]) AS vec) v " +
							"WHERE knowledge_entry.id = v.id",
						[ids, vecs],
					);
					logger.log(
						`[pg-db] Migration v17: backfilled embedding_vec for ${toBackfill.length} rows.`,
					);
				}

				// Replace legacy vector HNSW index with halfvec ANN index when supported.
				await sql`DROP INDEX IF EXISTS idx_entry_embedding_vec_hnsw`;
				if (dims <= HALFVEC_HNSW_MAX_DIMS) {
					// Skip CREATE INDEX if the halfvec HNSW index is already present.
					// CREATE INDEX IF NOT EXISTS still requires table ownership and fails
					// with "must be owner of table" on managed Postgres where the schema was
					// bootstrapped by a separate admin user. When the index exists from a
					// prior run, treat that as success.
					const hnswExists = await sql`
						SELECT 1 FROM pg_class c
						JOIN pg_namespace n ON n.oid = c.relnamespace
						WHERE c.relkind = 'i'
						  AND n.nspname = current_schema()
						  AND c.relname = 'idx_entry_embedding_halfvec_hnsw'
					`;
					if (hnswExists.length === 0) {
						await createHalfvecIndex(sql, dims);
					}
				} else {
					logger.warn(
						`[pg-db] Migration v17: embedding dimension ${dims} exceeds halfvec HNSW limit (${HALFVEC_HNSW_MAX_DIMS}). ANN index skipped for this store.`,
					);
				}
			}),
	},
	{
		version: 18,
		label:
			"drop strength and last_accessed_at indexes (HNSW churn on every update); fillfactor 85",
		up: async (sql: TxSql) => {
			// Strength is computed on read, so its index is unused. Both indexes were
			// rewritten on each decay pass and access, which is a non-HOT update that
			// also re-inserts the row into the embedding HNSW index. fillfactor leaves
			// room on each page so counter updates can be HOT.
			// Owner-only DDL: a non-owner role (managed Postgres, separate admin) must
			// still open the store, so each statement is best-effort.
			await skipWithoutOwnership(
				sql,
				18,
				"DROP INDEX idx_entry_strength",
				(s) => s`DROP INDEX IF EXISTS idx_entry_strength`,
			);
			await skipWithoutOwnership(
				sql,
				18,
				"DROP INDEX idx_entry_accessed",
				(s) => s`DROP INDEX IF EXISTS idx_entry_accessed`,
			);
			await skipWithoutOwnership(
				sql,
				18,
				"ALTER TABLE knowledge_entry SET (fillfactor)",
				(s) => s`ALTER TABLE knowledge_entry SET (fillfactor = 85)`,
			);
		},
	},
	{
		version: 19,
		label: "add injection_count to knowledge_entry (access_count frozen)",
		up: async (sql: TxSql) => {
			// Check first: a role that does not own the table cannot ALTER it, and the
			// column is already there when the table was created by the owner.
			const present = await sql`
				SELECT 1 FROM pg_attribute a
				WHERE a.attrelid = to_regclass('knowledge_entry')
				  AND a.attname = 'injection_count' AND a.attnum > 0 AND NOT a.attisdropped
			`;
			if (present.length === 0) {
				try {
					await sql`ALTER TABLE knowledge_entry ADD COLUMN injection_count INTEGER NOT NULL DEFAULT 0`;
				} catch (err) {
					if ((err as { code?: string }).code !== "42501") throw err;
					// Not skippable: the write path increments injection_count. The
					// migration aborts, so the store stays at its version until the owner runs the fix.
					throw new Error(
						"Migration v19 needs the table owner: the connecting role cannot add knowledge_entry.injection_count. " +
							"Owner-only fix: as the owner of knowledge_entry, run `ALTER TABLE knowledge_entry ADD COLUMN injection_count INTEGER NOT NULL DEFAULT 0`, then start the server again.",
						{ cause: err },
					);
				}
			}
		},
	},
	{
		version: 20,
		label:
			"cluster members joined before their cluster's last synthesis are baseline, not new",
		up: async (sql: TxSql) => {
			// See the SQLite v19 migration: clamp join times that older builds
			// rewrote, so existing members do not count as new.
			await sql`
				UPDATE knowledge_cluster_member m
				SET joined_at = LEAST(m.joined_at, c.last_synthesized_at)
				FROM knowledge_cluster c
				WHERE c.id = m.cluster_id
				  AND c.last_synthesized_at IS NOT NULL
				  AND m.joined_at > c.last_synthesized_at
			`;
		},
	},
];
