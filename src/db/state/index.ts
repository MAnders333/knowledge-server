import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { logger } from "../../logger.js";
import { tryAcquireProcessLock } from "../../process-lock.js";
import type {
	ActivationEventRow,
	ConsolidationState,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import { ensurePrivateFile } from "../../utils.js";
import {
	type IServerStateDB,
	PENDING_EPISODE_MAX_ATTEMPTS,
} from "../interface.js";
import {
	assertNotNewerThanCode,
	pendingMigrations,
} from "../migration-registry.js";
import { SQLITE_BUSY_TIMEOUT_MS } from "../sqlite-busy.js";
import { runStateMigrations } from "./migrations.js";
import {
	ACTIVATION_EVENT_V4_COLUMNS,
	SERVER_LOCAL_MIGRATIONS,
} from "./schema.js";
import {
	SERVER_LOCAL_CREATE_TABLES,
	SERVER_LOCAL_SCHEMA_VERSION,
} from "./schema.js";

/**
 * Default path for state.db — the server-local staging/bookkeeping database.
 * Always a local SQLite file on the machine where knowledge-server runs.
 */
export const DEFAULT_SERVER_STATE_PATH = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"state.db",
);
/** Outcome of ServerStateDB.compactIfNeeded(). */
export interface CompactionResult {
	ran: boolean;
	reason: string;
	freedPages?: number;
}

/** compactIfNeeded() runs only for files at least this large … */
const COMPACT_MIN_BYTES = 50 * 1024 * 1024;
/** … and only when more than this fraction of their pages are free. */
const COMPACT_MIN_FREE_RATIO = 0.5;
/** PRAGMA auto_vacuum value for INCREMENTAL. */
const AUTO_VACUUM_INCREMENTAL = 2;

/**
 * ServerStateDB — the server-local SQLite database.
 *
 * Holds staging and bookkeeping tables:
 *   - pending_episodes: daemon writes, server drains
 *   - consolidated_episode: idempotency log
 *   - consolidation_state: global server counters
 *
 * daemon_cursor lives in DaemonDB (src/db/daemon/index.ts), not here.
 *
 * The actual knowledge (knowledge_entry, etc.) lives in the configured
 * knowledge stores (IKnowledgeStore), which can be local SQLite or remote Postgres.
 */
export class ServerStateDB implements IServerStateDB {
	private readonly db: Database;
	/**
	 * @param dbPath state.db file; created with mode 0600 when missing.
	 * @param legacyKnowledgeDbPath knowledge.db to copy v3 staging tables from
	 *   (and drop them from). Omit to skip — no other file is ever touched.
	 */
	constructor(
		dbPath = DEFAULT_SERVER_STATE_PATH,
		legacyKnowledgeDbPath?: string,
	) {
		const dir = dirname(dbPath);
		if (!existsSync(dir)) {
			mkdirSync(dir, { recursive: true });
		}
		ensurePrivateFile(dbPath);
		this.db = new Database(dbPath);
		// Before journal_mode: the WAL switch takes a lock of its own.
		this.db.exec(`PRAGMA busy_timeout=${SQLITE_BUSY_TIMEOUT_MS}`);
		this.db.exec("PRAGMA journal_mode=WAL");
		this.db.exec("PRAGMA foreign_keys=ON");
		this.initialize(legacyKnowledgeDbPath);
		logger.log(`[db] Server state DB: SQLite at ${dbPath}`);
	}

	private initialize(legacyKnowledgeDbPath: string | undefined): void {
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

		assertNotNewerThanCode(
			currentVersion,
			SERVER_LOCAL_SCHEMA_VERSION,
			"state.db",
		);
		if (currentVersion > 0) {
			// Every version after the stored one must be registered. The state tables
			// themselves are created idempotently below.
			pendingMigrations(
				SERVER_LOCAL_MIGRATIONS,
				currentVersion,
				SERVER_LOCAL_SCHEMA_VERSION,
				"state.db",
			);
		}

		// Always ensure applied_migrations exists — it was added in v1 but may be
		// absent from existing v1 DBs created before this table was introduced.
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS applied_migrations (
        name       TEXT    PRIMARY KEY,
        applied_at INTEGER NOT NULL
      );
    `);

		// v4: activation_event gate and budget counts. CREATE TABLE IF NOT EXISTS does
		// not add columns to a table that already exists, so add any that are missing.
		const eventColumns = new Set(
			(
				this.db.prepare("PRAGMA table_info(activation_event)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name),
		);
		if (eventColumns.size > 0) {
			for (const column of ACTIVATION_EVENT_V4_COLUMNS) {
				if (!eventColumns.has(column)) {
					this.db.exec(
						`ALTER TABLE activation_event ADD COLUMN ${column} INTEGER NOT NULL DEFAULT 0`,
					);
				}
			}
		}
		// v5: the scorer that produced rerank_scores. Nullable: earlier rows have none.
		if (eventColumns.size > 0 && !eventColumns.has("rerank_model")) {
			this.db.exec("ALTER TABLE activation_event ADD COLUMN rerank_model TEXT");
		}
		// Fresh DB or already at current version — create/verify tables.
		// Always run CREATE TABLE IF NOT EXISTS for all tables — additive and
		// idempotent. Schema migrations for state.db are always additive; tables
		// are never dropped (pending_episodes may have unsent daemon uploads).
		this.db.exec(SERVER_LOCAL_CREATE_TABLES);

		// Stamp the current version idempotently. INSERT OR IGNORE avoids a
		// duplicate-row error if the table already has a row for this version.
		if (currentVersion < SERVER_LOCAL_SCHEMA_VERSION) {
			this.db
				.prepare(
					"INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (?, ?)",
				)
				.run(SERVER_LOCAL_SCHEMA_VERSION, Date.now());
			if (currentVersion === 0) {
				logger.log(
					`[db] Server state DB: created at v${SERVER_LOCAL_SCHEMA_VERSION}`,
				);
			}
		}
		// Run one-time data migrations (idempotent — each guards itself via applied_migrations).
		runStateMigrations(this.db, legacyKnowledgeDbPath);
	}

	// ── Pending Episodes ──────────────────────────────────────────────────────

	async insertPendingEpisode(episode: PendingEpisode): Promise<void> {
		// Skip a range already staged, e.g. two daemons uploading the same session.
		// Plain SQL: no unique index or table ownership needed.
		this.db
			.prepare(
				`INSERT OR IGNORE INTO pending_episodes
         (id, user_id, source, session_id, start_message_id, end_message_id,
          session_title, project_name, directory, content, content_type,
          session_timestamp, max_message_time, approx_tokens, uploaded_at)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE NOT EXISTS (
           SELECT 1 FROM pending_episodes
           WHERE user_id = ? AND source = ? AND session_id = ? AND start_message_id = ? AND end_message_id = ?
         )`,
			)
			.run(
				episode.id,
				episode.userId,
				episode.source,
				episode.sessionId,
				episode.startMessageId,
				episode.endMessageId,
				episode.sessionTitle,
				episode.projectName,
				episode.directory,
				episode.content,
				episode.contentType,
				episode.timeCreated,
				episode.maxMessageTime,
				episode.approxTokens,
				episode.uploadedAt,
				episode.userId,
				episode.source,
				episode.sessionId,
				episode.startMessageId,
				episode.endMessageId,
			);
	}

	async getPendingEpisodes(
		afterMaxMessageTime: number,
		limit = 500,
	): Promise<PendingEpisode[]> {
		const rows = this.db
			.prepare(
				"SELECT p.*, COALESCE(a.failed_attempts, 0) AS failed_attempts FROM pending_episodes p LEFT JOIN pending_episode_attempts a ON a.id = p.id WHERE p.max_message_time > ? AND COALESCE(a.failed_attempts, 0) < ? ORDER BY p.max_message_time ASC LIMIT ?",
			)
			.all(afterMaxMessageTime, PENDING_EPISODE_MAX_ATTEMPTS, limit) as Array<{
			id: string;
			user_id: string;
			source: string;
			session_id: string;
			start_message_id: string;
			end_message_id: string;
			session_title: string;
			project_name: string;
			directory: string;
			content: string;
			content_type: string;
			session_timestamp: number;
			max_message_time: number;
			approx_tokens: number;
			uploaded_at: number;
			failed_attempts: number;
		}>;

		return rows.map((r) => ({
			id: r.id,
			userId: r.user_id,
			source: r.source,
			sessionId: r.session_id,
			startMessageId: r.start_message_id,
			endMessageId: r.end_message_id,
			sessionTitle: r.session_title,
			projectName: r.project_name,
			directory: r.directory,
			content: r.content,
			contentType: r.content_type as PendingEpisode["contentType"],
			timeCreated: r.session_timestamp,
			maxMessageTime: r.max_message_time,
			approxTokens: r.approx_tokens,
			uploadedAt: r.uploaded_at,
			failedAttempts: r.failed_attempts,
		}));
	}

	async countPendingSessions(): Promise<number> {
		const row = this.db
			.prepare(
				"SELECT COUNT(DISTINCT p.session_id) as n FROM pending_episodes p LEFT JOIN pending_episode_attempts a ON a.id = p.id WHERE COALESCE(a.failed_attempts, 0) < ?",
			)
			.get(PENDING_EPISODE_MAX_ATTEMPTS) as { n: number } | undefined;
		return row?.n ?? 0;
	}

	async deletePendingEpisodes(ids: string[]): Promise<void> {
		if (ids.length === 0) return;
		const json = JSON.stringify(ids);
		this.db.transaction(() => {
			this.db
				.prepare(
					"DELETE FROM pending_episode_attempts WHERE id IN (SELECT value FROM json_each(?))",
				)
				.run(json);
			this.db
				.prepare(
					"DELETE FROM pending_episodes WHERE id IN (SELECT value FROM json_each(?))",
				)
				.run(json);
		})();
	}

	async recordPendingEpisodeFailures(
		ids: string[],
		error: string,
	): Promise<void> {
		if (ids.length === 0) return;
		this.db
			.prepare(
				`INSERT INTO pending_episode_attempts (id, failed_attempts, last_error)
         SELECT value, 1, ? FROM json_each(?) WHERE true
         ON CONFLICT (id) DO UPDATE SET
           failed_attempts = failed_attempts + 1,
           last_error = excluded.last_error`,
			)
			.run(error, JSON.stringify(ids));
	}

	async countDeadLetteredEpisodes(): Promise<number> {
		const row = this.db
			.prepare(
				`SELECT COUNT(*) as n FROM pending_episodes p
         JOIN pending_episode_attempts a ON a.id = p.id
         WHERE a.failed_attempts >= ?`,
			)
			.get(PENDING_EPISODE_MAX_ATTEMPTS) as { n: number } | undefined;
		return row?.n ?? 0;
	}

	async retryDeadLetteredEpisodes(): Promise<number> {
		return this.db
			.prepare(
				"DELETE FROM pending_episode_attempts WHERE failed_attempts >= ?",
			)
			.run(PENDING_EPISODE_MAX_ATTEMPTS).changes;
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
		this.db
			.prepare(
				`INSERT OR IGNORE INTO consolidated_episode
         (source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
			)
			.run(
				source,
				sessionId,
				startMessageId,
				endMessageId,
				contentType,
				Date.now(),
				entriesCreated,
			);
	}

	async getProcessedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		if (sessionIds.length === 0) return new Map();
		return this.queryRanges(sessionIds, false);
	}

	async getUploadedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		if (sessionIds.length === 0) return new Map();
		return this.queryRanges(sessionIds, true);
	}

	private queryRanges(
		sessionIds: string[],
		includePending: boolean,
	): Map<string, ProcessedRange[]> {
		const sql = includePending
			? `SELECT source, session_id, start_message_id, end_message_id
           FROM consolidated_episode
           WHERE session_id IN (SELECT value FROM json_each(?))
           UNION
           SELECT source, session_id, start_message_id, end_message_id
           FROM pending_episodes
           WHERE session_id IN (SELECT value FROM json_each(?))`
			: `SELECT source, session_id, start_message_id, end_message_id
           FROM consolidated_episode
           WHERE session_id IN (SELECT value FROM json_each(?))`;

		const params = includePending
			? [JSON.stringify(sessionIds), JSON.stringify(sessionIds)]
			: [JSON.stringify(sessionIds)];

		const rows = this.db.prepare(sql).all(...params) as Array<{
			source: string;
			session_id: string;
			start_message_id: string;
			end_message_id: string;
		}>;

		const result = new Map<string, ProcessedRange[]>();
		for (const row of rows) {
			const range: ProcessedRange = {
				source: row.source,
				startMessageId: row.start_message_id,
				endMessageId: row.end_message_id,
			};
			const existing = result.get(row.session_id);
			if (existing) existing.push(range);
			else result.set(row.session_id, [range]);
		}
		return result;
	}

	// ── Consolidation State ───────────────────────────────────────────────────

	async getConsolidationState(): Promise<ConsolidationState> {
		const row = this.db
			.prepare("SELECT * FROM consolidation_state WHERE id = 1")
			.get() as {
			last_consolidated_at: number;
			total_sessions_processed: number;
			total_entries_created: number;
			total_entries_updated: number;
		} | null;

		if (!row) {
			return {
				lastConsolidatedAt: 0,
				totalSessionsProcessed: 0,
				totalEntriesCreated: 0,
				totalEntriesUpdated: 0,
			};
		}
		return {
			lastConsolidatedAt: row.last_consolidated_at,
			totalSessionsProcessed: row.total_sessions_processed,
			totalEntriesCreated: row.total_entries_created,
			totalEntriesUpdated: row.total_entries_updated,
		};
	}

	async updateConsolidationState(
		state: Partial<ConsolidationState>,
	): Promise<void> {
		const fields: string[] = [];
		const values: (string | number)[] = [];

		if (state.lastConsolidatedAt !== undefined) {
			fields.push("last_consolidated_at = ?");
			values.push(state.lastConsolidatedAt);
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
			.prepare(
				`UPDATE consolidation_state SET ${fields.join(", ")} WHERE id = 1`,
			)
			.run(...values);
	}

	/**
	 * Wipe staging data and reset consolidation_state counters. Pending episodes
	 * are deleted for `userId` only when given (see IServerStateDB.reinitialize).
	 */
	async reinitialize(userId?: string): Promise<void> {
		this.db.transaction(() => {
			if (userId === undefined) {
				this.db.exec("DELETE FROM pending_episode_attempts");
				this.db.exec("DELETE FROM pending_episodes");
			} else {
				this.db
					.prepare(
						"DELETE FROM pending_episode_attempts WHERE id IN (SELECT id FROM pending_episodes WHERE user_id = ?)",
					)
					.run(userId);
				this.db
					.prepare("DELETE FROM pending_episodes WHERE user_id = ?")
					.run(userId);
			}
			this.db.exec("DELETE FROM consolidated_episode");
			this.db.exec(
				`UPDATE consolidation_state SET
           last_consolidated_at = 0,
           total_sessions_processed = 0,
           total_entries_created = 0,
           total_entries_updated = 0
         WHERE id = 1`,
			);
		})();
	}

	// ── Activation events (measurement, content-free) ─────────────────────────

	async insertActivationEvents(rows: ActivationEventRow[]): Promise<void> {
		if (rows.length === 0) return;
		const stmt = this.db.prepare(
			`INSERT INTO activation_event
			   (ts, client, session_hash, prompt_chars, cue_count, n_candidates, n_returned,
			    entry_ids, raw_sims, rerank_scores, rerank_model, gate_mode, latency_ms, client_deadline_exceeded,
			    gate_dropped, budget_dropped, deduped, injected_tokens)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		);
		this.db.transaction(() => {
			for (const r of rows) {
				stmt.run(
					r.ts,
					r.client,
					r.sessionHash,
					r.promptChars,
					r.cueCount,
					r.nCandidates,
					r.nReturned,
					JSON.stringify(r.entryIds),
					JSON.stringify(r.rawSims),
					r.rerankScores === null ? null : JSON.stringify(r.rerankScores),
					r.rerankModel,
					r.gateMode,
					r.latencyMs,
					r.clientDeadlineExceeded ? 1 : 0,
					r.gateDropped,
					r.budgetDropped,
					r.deduped,
					r.injectedTokens,
				);
			}
		})();
	}

	async pruneActivationEvents(cutoffMs: number): Promise<number> {
		const res = this.db
			.prepare("DELETE FROM activation_event WHERE ts < ?")
			.run(cutoffMs);
		return Number(res.changes);
	}

	async countActivationsByClient(
		sinceMs: number,
	): Promise<Record<string, number>> {
		const rows = this.db
			.prepare(
				"SELECT client, COUNT(*) AS n FROM activation_event WHERE ts >= ? GROUP BY client",
			)
			.all(sinceMs) as Array<{ client: string; n: number }>;
		return Object.fromEntries(rows.map((r) => [r.client, Number(r.n)]));
	}

	async countEpisodesBySource(
		sinceMs: number,
	): Promise<Record<string, number>> {
		const rows = this.db
			.prepare(
				"SELECT source, COUNT(*) AS n FROM consolidated_episode WHERE processed_at >= ? GROUP BY source",
			)
			.all(sinceMs) as Array<{ source: string; n: number }>;
		return Object.fromEntries(rows.map((r) => [r.source, Number(r.n)]));
	}

	/**
	 * Storage housekeeping for state.db, run once at server startup.
	 *
	 * Consolidated episodes are deleted from pending_episodes, and SQLite never
	 * returns those pages to the OS, so a long-lived file can be mostly free space.
	 * When the file is at least `minBytes` and more than `minFreeRatio` of its pages
	 * are free, this switches the file to incremental auto_vacuum (a one-time full
	 * VACUUM is needed for that) and reclaims the free pages.
	 *
	 * Skipped when another process holds `lockPath` (another server, the daemon's
	 * maintenance or a second CLI), and when SQLite reports the file busy: the
	 * housekeeping is best effort and never blocks startup.
	 */
	compactIfNeeded(options: {
		lockPath: string;
		minBytes?: number;
		minFreeRatio?: number;
	}): CompactionResult {
		const minBytes = options.minBytes ?? COMPACT_MIN_BYTES;
		const minFreeRatio = options.minFreeRatio ?? COMPACT_MIN_FREE_RATIO;
		const pageSize = this.pragmaNumber("page_size");
		const pageCount = this.pragmaNumber("page_count");
		const freeBefore = this.pragmaNumber("freelist_count");
		const sizeBytes = pageSize * pageCount;
		if (sizeBytes < minBytes) {
			return { ran: false, reason: "below size threshold" };
		}
		if (pageCount === 0 || freeBefore / pageCount <= minFreeRatio) {
			return { ran: false, reason: "free space below threshold" };
		}
		const lock = tryAcquireProcessLock(options.lockPath);
		if (lock === null) {
			return {
				ran: false,
				reason: "another process holds the maintenance lock",
			};
		}
		try {
			// Full VACUUM only to convert an existing file to incremental mode.
			if (this.pragmaNumber("auto_vacuum") !== AUTO_VACUUM_INCREMENTAL) {
				this.db.exec(`PRAGMA auto_vacuum = ${AUTO_VACUUM_INCREMENTAL}`);
				this.db.exec("VACUUM");
			}
			this.db.exec("PRAGMA incremental_vacuum");
			// Move the freed pages out of the WAL so the main file actually shrinks.
			this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
			const freeAfter = this.pragmaNumber("freelist_count");
			logger.log(
				`[db] state.db housekeeping: reclaimed ${freeBefore - freeAfter} of ${pageCount} pages (${Math.round((sizeBytes * (freeBefore - freeAfter)) / pageCount / 1048576)} MB).`,
			);
			return {
				ran: true,
				reason: "compacted",
				freedPages: freeBefore - freeAfter,
			};
		} catch (err) {
			const reason = err instanceof Error ? err.message : String(err);
			logger.warn(`[db] state.db housekeeping skipped: ${reason}`);
			return { ran: false, reason: `failed: ${reason}` };
		} finally {
			lock.release();
		}
	}

	private pragmaNumber(name: string): number {
		const row = this.db.prepare(`PRAGMA ${name}`).get() as Record<
			string,
			number
		>;
		return Number(Object.values(row)[0]);
	}

	async close(): Promise<void> {
		this.db.close();
	}
}
