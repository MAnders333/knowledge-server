import postgres from "postgres";
import { logger } from "../../logger.js";
import type {
	ConsolidationState,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import type { IServerStateDB } from "../interface.js";

// biome-ignore lint: TS limitation with Omit stripping call signatures
type TxSql = any;

const STATE_SCHEMA_VERSION = 1;

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

  CREATE INDEX IF NOT EXISTS idx_pending_source_time
    ON pending_episodes(source, max_message_time);

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
`;

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

	constructor(uri: string) {
		this.sql = postgres(uri, {
			max: 10,
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
		} else {
			logger.log(`[pg-state-db] Postgres state DB at schema v${v}.`);
		}
	}

	// ── Pending Episodes ──────────────────────────────────────────────────────

	async insertPendingEpisode(episode: PendingEpisode): Promise<void> {
		await this.initialize();
		await this.sql`
			INSERT INTO pending_episodes
			(id, user_id, source, session_id, start_message_id, end_message_id,
			 session_title, project_name, directory, content, content_type,
			 session_timestamp, max_message_time, approx_tokens, uploaded_at)
			VALUES (
				${episode.id}, ${episode.userId}, ${episode.source},
				${episode.sessionId}, ${episode.startMessageId}, ${episode.endMessageId},
				${episode.sessionTitle ?? ""}, ${episode.projectName ?? ""},
				${episode.directory ?? ""}, ${episode.content},
				${episode.contentType}, ${episode.timeCreated ?? 0},
				${episode.maxMessageTime}, ${episode.approxTokens ?? 0},
				${episode.uploadedAt}
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
			SELECT * FROM pending_episodes
			WHERE max_message_time > ${afterMaxMessageTime}
			ORDER BY max_message_time ASC
			LIMIT ${limit}
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
		}));
	}

	async deletePendingEpisodes(ids: string[]): Promise<void> {
		if (ids.length === 0) return;
		await this.initialize();
		await this
			.sql`DELETE FROM pending_episodes WHERE id = ANY(${this.sql.array(ids)})`;
	}

	async countPendingSessions(): Promise<number> {
		await this.initialize();
		const result = await this.sql`
			SELECT COUNT(DISTINCT session_id) as n FROM pending_episodes
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
			if (!map.has(sid)) map.set(sid, []);
			map.get(sid)!.push({
				source: r.source as string,
				startMessageId: r.start_message_id as string,
				endMessageId: r.end_message_id as string,
			});
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

	async reinitialize(): Promise<void> {
		await this.initialize();
		await this.sql.begin(async (sql: TxSql) => {
			await sql`DELETE FROM pending_episodes`;
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

	// ── Close ─────────────────────────────────────────────────────────────────

	async close(): Promise<void> {
		await this.sql.end();
	}
}
