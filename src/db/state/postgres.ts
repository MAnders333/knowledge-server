import postgres from "postgres";
import { config } from "../../config.js";
import { logger } from "../../logger.js";
import type {
	ActivationEventRow,
	ConsolidationState,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import { PENDING_EPISODE_MAX_ATTEMPTS } from "../interface.js";
import type { IServerStateDB } from "../interface.js";
import {
	assertNotNewerThanCode,
	latestVersion,
	pendingMigrations,
} from "../migration-registry.js";
import { resolvePostgresTls, warnNoTlsOnce } from "../postgres/tls.js";
import {
	ACTIVATION_EVENT_COLUMNS,
	type ActivationEventValue,
	activationEventColumnDefs,
} from "./activation-columns.js";

// biome-ignore lint: TS limitation with Omit stripping call signatures
type TxSql = any;

/** Attempt counts for pending episodes (v2). Kept out of pending_episodes. */
const PG_ATTEMPTS_TABLE = `
  CREATE TABLE IF NOT EXISTS pending_episode_attempts (
    id              TEXT    PRIMARY KEY,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT
  );
`;

// Content-free measurement rows: ids, counts, scores, latency, and a session
// hash. Never prompt text. entry_ids / raw_sims / rerank_scores are JSON text.
// Columns come from ACTIVATION_EVENT_COLUMNS. A version limits them to the ones
// that version introduces, so the v3 table is created without the later columns.
function pgActivationEventTable(version?: number): string {
	return `
  CREATE TABLE IF NOT EXISTS activation_event (
    id BIGSERIAL PRIMARY KEY,
    ${activationEventColumnDefs("pg", version)}
  );
`;
}

/**
 * CREATE INDEX checks table ownership before IF NOT EXISTS skips, so an index is
 * created only when it is missing from the table's own schema. The table is found
 * through pg_class, not assumed to live in current_schema().
 */
function createIndexIfMissing(
	table: string,
	index: string,
	columns: string,
): string {
	return `
  DO $$
  DECLARE table_schema name;
  BEGIN
    SELECT n.nspname INTO table_schema
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass('${table}');
    IF NOT EXISTS (SELECT 1 FROM pg_indexes
                   WHERE schemaname = table_schema AND indexname = '${index}') THEN
      CREATE INDEX ${index} ON ${table}(${columns});
    END IF;
  END $$;
`;
}

const PG_ACTIVATION_EVENT_INDEXES =
	createIndexIfMissing("activation_event", "idx_activation_event_ts", "ts") +
	createIndexIfMissing(
		"activation_event",
		"idx_activation_event_client_ts",
		"client, ts",
	);

// Serves the duplicate check on staged episodes and queryRanges (both filter by session_id).
const PG_PENDING_SESSION_INDEX = createIndexIfMissing(
	"pending_episodes",
	"idx_pending_session",
	"session_id",
);

/**
 * Run an index step under its own SAVEPOINT. A role that does not own the table
 * gets 42501 (insufficient_privilege) from CREATE INDEX, and without a savepoint
 * that error aborts the whole upgrade transaction. An index is only a performance
 * aid, so the error is logged and the step skipped; the version is still stamped.
 * Any other error stays fatal.
 */
async function skipWithoutOwnership(
	tx: TxSql,
	version: number,
	statement: string,
	step: (sql: TxSql) => Promise<unknown>,
): Promise<void> {
	try {
		await tx.savepoint(step);
	} catch (err) {
		if ((err as { code?: string }).code !== "42501") throw err;
		logger.warn(
			`[pg-state-db] Migration v${version}: skipped \`${statement}\` — the connecting role does not own the table. Lookups stay unindexed until the owner creates the index. Error: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
}

const PG_CREATE_STATE_TABLES = `
  CREATE TABLE IF NOT EXISTS state_schema_version (
    version    INTEGER NOT NULL UNIQUE,
    applied_at BIGINT  NOT NULL
  );

  CREATE TABLE IF NOT EXISTS pending_episodes (
    id                TEXT    PRIMARY KEY,
    user_id           TEXT    NOT NULL DEFAULT 'default',
    source            TEXT    NOT NULL,
    session_id        TEXT    NOT NULL,
    start_message_id  TEXT    NOT NULL,
    end_message_id    TEXT    NOT NULL,
    session_title     TEXT    NOT NULL DEFAULT '',
    project_name      TEXT    NOT NULL DEFAULT '',
    directory         TEXT    NOT NULL DEFAULT '',
    content           TEXT    NOT NULL,
    content_type      TEXT    NOT NULL CHECK(content_type IN ('messages', 'compaction_summary', 'document')),
    session_timestamp BIGINT  NOT NULL DEFAULT 0,
    max_message_time  BIGINT  NOT NULL DEFAULT 0,
    approx_tokens     INTEGER NOT NULL DEFAULT 0,
    uploaded_at       BIGINT  NOT NULL
  );

  ${PG_ATTEMPTS_TABLE}

  CREATE INDEX IF NOT EXISTS idx_pending_source_time
    ON pending_episodes(source, max_message_time);

  ${PG_PENDING_SESSION_INDEX}

  CREATE TABLE IF NOT EXISTS consolidated_episode (
    source            TEXT    NOT NULL,
    session_id        TEXT    NOT NULL,
    start_message_id  TEXT    NOT NULL,
    end_message_id    TEXT    NOT NULL,
    content_type      TEXT    NOT NULL,
    processed_at      BIGINT  NOT NULL,
    entries_created   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id, start_message_id, end_message_id)
  );

  CREATE INDEX IF NOT EXISTS idx_episode_source_session
    ON consolidated_episode(source, session_id);

  CREATE TABLE IF NOT EXISTS consolidation_state (
    -- id is always 1 — this is a single-row singleton table.
    -- DEFAULT is intentionally omitted: Postgres ignores DEFAULT on a PK column
    -- unless it's GENERATED/serial. The INSERT below hard-codes 1.
    id                       INTEGER PRIMARY KEY CHECK(id = 1),
    last_consolidated_at     BIGINT  NOT NULL DEFAULT 0,
    total_sessions_processed BIGINT  NOT NULL DEFAULT 0,
    total_entries_created    BIGINT  NOT NULL DEFAULT 0,
    total_entries_updated    BIGINT  NOT NULL DEFAULT 0
  );

  INSERT INTO consolidation_state
    (id, last_consolidated_at, total_sessions_processed, total_entries_created, total_entries_updated)
  VALUES (1, 0, 0, 0, 0)
  ON CONFLICT (id) DO NOTHING;

  ${pgActivationEventTable()}
  ${PG_ACTIVATION_EVENT_INDEXES}
`;

/**
 * Versioned migrations for the Postgres state DB. Version 1 is the baseline
 * created by PG_CREATE_STATE_TABLES; each later entry runs once, in order.
 */
const PG_STATE_MIGRATIONS: Array<{
	version: number;
	label: string;
	up?: (tx: TxSql) => Promise<void>;
}> = [
	{ version: 1, label: "baseline state schema" },
	{
		version: 2,
		label:
			"pending_episode_attempts table (attempt counts kept out of pending_episodes)",
		up: async (tx: TxSql) => {
			await tx.unsafe(PG_ATTEMPTS_TABLE);
		},
	},
	{
		version: 3,
		label: "activation_event table (content-free measurement)",
		up: async (tx: TxSql) => {
			await tx.unsafe(pgActivationEventTable(3));
			await skipWithoutOwnership(
				tx,
				3,
				"CREATE INDEX idx_activation_event_ts, idx_activation_event_client_ts",
				(sql) => sql.unsafe(PG_ACTIVATION_EVENT_INDEXES),
			);
		},
	},
	{
		version: 4,
		label: "activation_event gate and budget counts (precision gate)",
		up: (tx: TxSql) => addActivationEventColumns(tx, 4),
	},
	{
		version: 5,
		label: "activation_event rerank model (scorer of rerank_scores)",
		up: (tx: TxSql) => addActivationEventColumns(tx, 5),
	},
	{
		version: 6,
		label: "pending_episodes session_id index (duplicate check, queryRanges)",
		up: (tx: TxSql) =>
			skipWithoutOwnership(tx, 6, "CREATE INDEX idx_pending_session", (sql) =>
				sql.unsafe(PG_PENDING_SESSION_INDEX),
			),
	},
];
const STATE_SCHEMA_VERSION = latestVersion(PG_STATE_MIGRATIONS);

/** Add the activation_event columns introduced in `version` that the table lacks. */
async function addActivationEventColumns(
	tx: TxSql,
	version: number,
): Promise<void> {
	// Only add what is missing: a non-owner role cannot ALTER a table another role
	// created. pg_attribute, not information_schema: the latter hides columns from
	// roles without privileges on the table, which would look like "missing".
	const present = new Set(
		(
			await tx`SELECT a.attname FROM pg_attribute a
			         WHERE a.attrelid = to_regclass('activation_event')
			           AND a.attnum > 0 AND NOT a.attisdropped`
		).map((r: { attname: string }) => r.attname),
	);
	for (const column of ACTIVATION_EVENT_COLUMNS) {
		if (column.since === version && !present.has(column.name)) {
			await tx.unsafe(
				`ALTER TABLE activation_event ADD COLUMN ${column.name} ${column.pg}`,
			);
		}
	}
}

/** One activation_event row keyed by column name, for the multi-row Postgres insert. */
function activationEventRecord(
	r: ActivationEventRow,
): Record<string, ActivationEventValue> {
	return Object.fromEntries(
		ACTIVATION_EVENT_COLUMNS.map((c) => [c.name, c.value(r)]),
	);
}

/**
 * PostgresServerStateDB — Postgres-backed implementation of IServerStateDB.
 *
 * Enables a fully remote/cloud consolidation server: the daemon on each
 * developer machine writes pending_episodes to shared Postgres; the remote
 * server drains it from there.
 *
 * Does NOT hold daemon_cursor — that lives in DaemonDB (always local SQLite).
 */
export class PostgresServerStateDB implements IServerStateDB {
	private readonly sql: postgres.Sql;
	private initPromise: Promise<void> | null = null;

	constructor(uri: string, poolMax = config.postgres.poolMax) {
		const tls = resolvePostgresTls(uri);
		warnNoTlsOnce(tls.warnNoTls);
		this.sql = postgres(tls.uri, {
			...(tls.ssl ? { ssl: tls.ssl } : {}),
			max: poolMax,
			idle_timeout: 30,
			connect_timeout: 10,
		});
	}

	async initialize(): Promise<void> {
		if (!this.initPromise) {
			this.initPromise = this._initialize().catch((err) => {
				this.initPromise = null;
				throw err;
			});
		}
		return this.initPromise;
	}

	private async _initialize(): Promise<void> {
		// Check if state_schema_version table exists by querying it.
		// If it doesn't exist yet, Postgres throws code 42P01 (undefined_table).
		// We catch only that specific error to distinguish "fresh DB" from
		// "connection error / permission denied" — any other error propagates.
		// The transaction is safe to retry (CREATE TABLE IF NOT EXISTS,
		// INSERT ... ON CONFLICT DO NOTHING) if initPromise is reset on failure.
		//
		// Known limitation: two processes starting simultaneously against a fresh DB
		// can both observe the table as absent and both attempt DDL. This is safe
		// because all statements use IF NOT EXISTS / ON CONFLICT DO NOTHING, but
		// a proper serialization would require a pre-existing lock table or a
		// separate advisory lock session (chicken-and-egg with a fresh DB).
		// In practice this race window is negligible for team setups.
		let v: number | null = null;
		try {
			const result = await this.sql`
				SELECT MAX(version) as v FROM state_schema_version
			`;
			v = (result[0] as { v: number | null }).v;
		} catch (e: unknown) {
			const code = (e as { code?: string }).code;
			if (code !== "42P01") {
				// Not a "table doesn't exist" error — propagate as a real failure.
				throw e;
			}
			// 42P01 = undefined_table: fresh DB, schema not yet created.
		}

		if (v === null) {
			// Note: sql.unsafe() uses the simple query protocol and does not support
			// true DDL atomicity even inside a transaction. DDL auto-commits individually.
			// Safety relies on idempotency: CREATE TABLE IF NOT EXISTS and
			// INSERT ... ON CONFLICT DO NOTHING. If init is interrupted mid-way,
			// the next startup retries safely.
			await this.sql.unsafe(PG_CREATE_STATE_TABLES);
			await this.sql`
				INSERT INTO state_schema_version (version, applied_at)
				VALUES (${STATE_SCHEMA_VERSION}, ${Date.now()})
				ON CONFLICT DO NOTHING
			`;
			logger.log("[pg-state-db] Initialized Postgres state DB (fresh schema).");
		} else if (v < STATE_SCHEMA_VERSION) {
			await this.upgradeSchema();
		} else {
			assertNotNewerThanCode(v, STATE_SCHEMA_VERSION, "Postgres state DB");
			logger.log(`[pg-state-db] Postgres state DB at schema v${v}.`);
		}
	}

	/**
	 * Upgrade an existing state DB to STATE_SCHEMA_VERSION. Runs in one transaction
	 * under a transaction-scoped advisory lock, and re-reads the version under the
	 * lock, so concurrent server/daemon starts do not both run the ALTER.
	 * v2: pending_episode_attempts table (dead-lettering attempt counts).
	 * v3: activation_event table (content-free measurement); v4-v5: its later columns.
	 * v6: pending_episodes session_id index.
	 */
	private async upgradeSchema(): Promise<void> {
		await this.sql.begin(async (tx: TxSql) => {
			await tx`SELECT pg_advisory_xact_lock(hashtext('knowledge-server:state-schema'))`;
			const rows = await tx`SELECT MAX(version) AS v FROM state_schema_version`;
			const versionRow = rows[0] as { v: number | null } | undefined;
			const current = Number(versionRow?.v ?? 0);
			for (const migration of pendingMigrations(
				PG_STATE_MIGRATIONS,
				current,
				STATE_SCHEMA_VERSION,
				"Postgres state DB",
			)) {
				if (migration.up) await migration.up(tx);
				await tx`
					INSERT INTO state_schema_version (version, applied_at)
					VALUES (${migration.version}, ${Date.now()})
					ON CONFLICT DO NOTHING
				`;
				logger.log(
					`[pg-state-db] Applied state migration v${migration.version}: ${migration.label}.`,
				);
			}
		});
	}

	// ── Pending Episodes ──────────────────────────────────────────────────────

	async insertPendingEpisode(episode: PendingEpisode): Promise<void> {
		await this.initialize();
		// Skip a range already staged (see the SQLite implementation). Plain SQL: no
		// unique index or table ownership needed.
		await this.sql`
			INSERT INTO pending_episodes
			(id, user_id, source, session_id, start_message_id, end_message_id,
			 session_title, project_name, directory, content, content_type,
			 session_timestamp, max_message_time, approx_tokens, uploaded_at)
			SELECT ${episode.id}, ${episode.userId}, ${episode.source},
				${episode.sessionId}, ${episode.startMessageId}, ${episode.endMessageId},
				${episode.sessionTitle ?? ""}, ${episode.projectName ?? ""},
				${episode.directory ?? ""}, ${episode.content},
				${episode.contentType}, ${episode.timeCreated ?? 0}::bigint,
				${episode.maxMessageTime}::bigint, ${episode.approxTokens ?? 0}::integer,
				${episode.uploadedAt}::bigint
			WHERE NOT EXISTS (
				SELECT 1 FROM pending_episodes
				WHERE user_id = ${episode.userId} AND source = ${episode.source} AND session_id = ${episode.sessionId}
				  AND start_message_id = ${episode.startMessageId}
				  AND end_message_id = ${episode.endMessageId}
			)
			ON CONFLICT (id) DO NOTHING
		`;
	}

	async getPendingEpisodes(
		afterMaxMessageTime: number,
		limit = 500,
	): Promise<PendingEpisode[]> {
		await this.initialize();
		const rows = await this.sql`
			SELECT p.*, COALESCE(a.failed_attempts, 0) AS failed_attempts FROM pending_episodes p LEFT JOIN pending_episode_attempts a ON a.id = p.id WHERE p.max_message_time > ${afterMaxMessageTime} AND COALESCE(a.failed_attempts, 0) < ${PENDING_EPISODE_MAX_ATTEMPTS} ORDER BY p.max_message_time ASC LIMIT ${limit}
		`;
		return rows.map((r) => ({
			id: r.id as string,
			userId: r.user_id as string,
			source: r.source as string,
			sessionId: r.session_id as string,
			startMessageId: r.start_message_id as string,
			endMessageId: r.end_message_id as string,
			sessionTitle: r.session_title as string,
			projectName: r.project_name as string,
			directory: r.directory as string,
			content: r.content as string,
			contentType: r.content_type as PendingEpisode["contentType"],
			timeCreated: Number(r.session_timestamp),
			maxMessageTime: Number(r.max_message_time),
			approxTokens: Number(r.approx_tokens),
			uploadedAt: Number(r.uploaded_at),
			failedAttempts: Number(r.failed_attempts),
		}));
	}

	async deletePendingEpisodes(ids: string[]): Promise<void> {
		if (ids.length === 0) return;
		await this.initialize();
		await this.sql.begin(async (sql: TxSql) => {
			await sql`DELETE FROM pending_episode_attempts WHERE id = ANY(${this.sql.array(ids)})`;
			await sql`DELETE FROM pending_episodes WHERE id = ANY(${this.sql.array(ids)})`;
		});
	}

	async recordPendingEpisodeFailures(
		ids: string[],
		error: string,
	): Promise<void> {
		if (ids.length === 0) return;
		await this.initialize();
		await this.sql`
			INSERT INTO pending_episode_attempts (id, failed_attempts, last_error)
			SELECT u, 1, ${error} FROM unnest(${this.sql.array(ids)}::text[]) AS u
			ON CONFLICT (id) DO UPDATE SET
				failed_attempts = pending_episode_attempts.failed_attempts + 1,
				last_error = EXCLUDED.last_error
		`;
	}

	async countDeadLetteredEpisodes(): Promise<number> {
		await this.initialize();
		const result = await this.sql`
			SELECT COUNT(*) AS n FROM pending_episodes p
			JOIN pending_episode_attempts a ON a.id = p.id
			WHERE a.failed_attempts >= ${PENDING_EPISODE_MAX_ATTEMPTS}
		`;
		return Number((result[0] as { n: number }).n);
	}

	async retryDeadLetteredEpisodes(): Promise<number> {
		await this.initialize();
		const result = await this.sql`
			DELETE FROM pending_episode_attempts
			WHERE failed_attempts >= ${PENDING_EPISODE_MAX_ATTEMPTS}
		`;
		return result.count;
	}

	async countPendingSessions(): Promise<number> {
		await this.initialize();
		const result = await this.sql`
			SELECT COUNT(DISTINCT p.session_id) as n FROM pending_episodes p LEFT JOIN pending_episode_attempts a ON a.id = p.id WHERE COALESCE(a.failed_attempts, 0) < ${PENDING_EPISODE_MAX_ATTEMPTS}
		`;
		return Number((result[0] as { n: number }).n);
	}

	// ── Episode Tracking ──────────────────────────────────────────────────────

	async recordEpisode(
		source: string,
		sessionId: string,
		startMessageId: string,
		endMessageId: string,
		contentType: "compaction_summary" | "messages" | "document",
		entriesCreated: number,
	): Promise<void> {
		await this.initialize();
		await this.sql`
			INSERT INTO consolidated_episode
			(source, session_id, start_message_id, end_message_id, content_type,
			 processed_at, entries_created)
			VALUES (
				${source}, ${sessionId}, ${startMessageId}, ${endMessageId},
				${contentType}, ${Date.now()}, ${entriesCreated}
			)
			ON CONFLICT (source, session_id, start_message_id, end_message_id) DO NOTHING
		`;
	}

	async getProcessedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		if (sessionIds.length === 0) return new Map();
		await this.initialize();
		const rows = await this.sql`
			SELECT source, session_id, start_message_id, end_message_id
			FROM consolidated_episode
			WHERE session_id = ANY(${this.sql.array(sessionIds)})
		`;
		return this.pgRowsToRangeMap(rows);
	}

	async getUploadedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		if (sessionIds.length === 0) return new Map();
		await this.initialize();
		const rows = await this.sql`
			SELECT source, session_id, start_message_id, end_message_id
			FROM consolidated_episode
			WHERE session_id = ANY(${this.sql.array(sessionIds)})
			UNION
			SELECT source, session_id, start_message_id, end_message_id
			FROM pending_episodes
			WHERE session_id = ANY(${this.sql.array(sessionIds)})
		`;
		return this.pgRowsToRangeMap(rows);
	}

	private pgRowsToRangeMap(
		rows: postgres.RowList<postgres.Row[]>,
	): Map<string, ProcessedRange[]> {
		const map = new Map<string, ProcessedRange[]>();
		for (const r of rows) {
			const sid = r.session_id as string;
			const ranges = map.get(sid) ?? [];
			ranges.push({
				source: r.source as string,
				startMessageId: r.start_message_id as string,
				endMessageId: r.end_message_id as string,
			});
			map.set(sid, ranges);
		}
		return map;
	}

	// ── Consolidation State ───────────────────────────────────────────────────

	async getConsolidationState(): Promise<ConsolidationState> {
		await this.initialize();
		const rows = await this.sql`SELECT * FROM consolidation_state WHERE id = 1`;
		const r = rows[0] as
			| {
					last_consolidated_at: number;
					total_sessions_processed: number;
					total_entries_created: number;
					total_entries_updated: number;
			  }
			| undefined;
		if (!r) {
			logger.warn(
				"[pg-state-db] consolidation_state row missing — returning zero state. " +
					"This indicates DB misconfiguration or a failed schema init.",
			);
		}
		return {
			lastConsolidatedAt: r ? Number(r.last_consolidated_at) : 0,
			totalSessionsProcessed: r ? Number(r.total_sessions_processed) : 0,
			totalEntriesCreated: r ? Number(r.total_entries_created) : 0,
			totalEntriesUpdated: r ? Number(r.total_entries_updated) : 0,
		};
	}

	/**
	 * Update consolidation state counters atomically.
	 * Fields not present in `state` (i.e. undefined) are preserved via COALESCE.
	 * Explicitly passing `0` will set the field to 0.
	 * To zero all counters atomically, use reinitialize() instead.
	 */
	async updateConsolidationState(
		state: Partial<ConsolidationState>,
	): Promise<void> {
		await this.initialize();
		if (Object.keys(state).length === 0) return;
		// Use COALESCE to preserve existing values for fields not supplied by the
		// caller — single atomic UPDATE avoids a read-then-write TOCTOU window.
		await this.sql`
			UPDATE consolidation_state SET
				last_consolidated_at     = COALESCE(${state.lastConsolidatedAt ?? null},     last_consolidated_at),
				total_sessions_processed = COALESCE(${state.totalSessionsProcessed ?? null}, total_sessions_processed),
				total_entries_created    = COALESCE(${state.totalEntriesCreated ?? null},    total_entries_created),
				total_entries_updated    = COALESCE(${state.totalEntriesUpdated ?? null},    total_entries_updated)
			WHERE id = 1
		`;
	}

	// ── Reinitialize ──────────────────────────────────────────────────────────

	async reinitialize(userId?: string): Promise<void> {
		await this.initialize();
		await this.sql.begin(async (sql: TxSql) => {
			if (userId === undefined) {
				await sql`DELETE FROM pending_episode_attempts`;
				await sql`DELETE FROM pending_episodes`;
			} else {
				await sql`DELETE FROM pending_episode_attempts WHERE id IN (SELECT id FROM pending_episodes WHERE user_id = ${userId})`;
				await sql`DELETE FROM pending_episodes WHERE user_id = ${userId}`;
			}
			await sql`DELETE FROM consolidated_episode`;
			// Use UPSERT rather than UPDATE so the singleton row is restored even if
			// a previous reinitialize or failed init left the table without a row.
			await sql`
				INSERT INTO consolidation_state
					(id, last_consolidated_at, total_sessions_processed,
					 total_entries_created, total_entries_updated)
				VALUES (1, 0, 0, 0, 0)
				ON CONFLICT (id) DO UPDATE SET
					last_consolidated_at     = 0,
					total_sessions_processed = 0,
					total_entries_created    = 0,
					total_entries_updated    = 0
			`;
		});
	}

	// ── Activation Events (measurement, content-free) ─────────────────────────

	async insertActivationEvents(rows: ActivationEventRow[]): Promise<void> {
		if (rows.length === 0) return;
		await this.initialize();
		// One multi-row INSERT is a single atomic statement. Arrays are stored as
		// JSON text, the same encoding the SQLite backend uses.
		await this.sql`
			INSERT INTO activation_event ${this.sql(rows.map(activationEventRecord))}
		`;
	}

	async pruneActivationEvents(cutoffMs: number): Promise<number> {
		await this.initialize();
		const result = await this.sql`
			DELETE FROM activation_event WHERE ts < ${cutoffMs}
		`;
		return result.count;
	}

	/** Injected activations per client since sinceMs: only those that returned entries and were not cut off by the client deadline. */
	async countActivationsByClient(
		sinceMs: number,
	): Promise<Record<string, number>> {
		await this.initialize();
		const rows = await this.sql`
			SELECT client, COUNT(*) AS n FROM activation_event
			WHERE ts >= ${sinceMs} AND n_returned > 0 AND client_deadline_exceeded = FALSE
			GROUP BY client
		`;
		return Object.fromEntries(
			rows.map((r) => [r.client as string, Number(r.n)]),
		);
	}

	async countEpisodesBySource(
		sinceMs: number,
	): Promise<Record<string, number>> {
		await this.initialize();
		const rows = await this.sql`
			SELECT source, COUNT(*) AS n FROM consolidated_episode
			WHERE processed_at >= ${sinceMs}
			GROUP BY source
		`;
		return Object.fromEntries(
			rows.map((r) => [r.source as string, Number(r.n)]),
		);
	}

	// ── Close ─────────────────────────────────────────────────────────────────

	async close(): Promise<void> {
		await this.sql.end();
	}
}
