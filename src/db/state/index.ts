import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { defaultDataDir } from "../../config-file.js";
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
import {
	ACTIVATION_EVENT_COLUMNS,
	type ActivationEventValue,
} from "./activation-columns.js";
import { runStateMigrations } from "./migrations.js";
import {
	SERVER_LOCAL_MIGRATIONS,
	SERVER_LOCAL_SCHEMA_VERSION,
} from "./schema.js";

const INSERT_ACTIVATION_EVENT_SQL = `INSERT INTO activation_event (${ACTIVATION_EVENT_COLUMNS.map((c) => c.name).join(", ")})
	VALUES (${ACTIVATION_EVENT_COLUMNS.map(() => "?").join(", ")})`;

/** SQLite has no boolean type: booleans are bound as 1 or 0. */
function sqliteBinding(value: ActivationEventValue): string | number | null {
	return typeof value === "boolean" ? Number(value) : value;
}

/**
 * Default path for state.db — the server-local staging/bookkeeping database.
 * Always a local SQLite file on the machine where knowledge-server runs.
 */
export function defaultStateDbPath(): string {
	return join(defaultDataDir(), "state.db");
}
/** Outcome of ServerStateDB.compactIfNeeded(). */
interface CompactionResult {
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
	constructor(dbPath = defaultStateDbPath(), legacyKnowledgeDbPath?: string) {
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
		const storedVersion = this.migrateSchema();
		if (storedVersion === 0) {
			logger.log(
				`[db] Server state DB: created at v${SERVER_LOCAL_SCHEMA_VERSION}`,
			);
		}
		// Bookkeeping for the one-time data migrations below. Kept out of the registry
		// so a file stamped before this table existed still gets it.
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS applied_migrations (
        name       TEXT    PRIMARY KEY,
        applied_at INTEGER NOT NULL
      );
    `);
		// Run one-time data migrations (idempotent — each guards itself via applied_migrations).
		runStateMigrations(this.db, legacyKnowledgeDbPath);
	}

	/**
	 * Apply each registered schema migration above the stored version, in order.
	 * Runs under one write lock and re-reads the version under it, so a daemon and a
	 * server starting together cannot both run the same ALTER. Returns the version
	 * found before migrating (0 for a new file).
	 */
	private migrateSchema(): number {
		this.db.exec("BEGIN IMMEDIATE");
		try {
			this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL,
        applied_at INTEGER NOT NULL
      );
    `);
			const storedVersion =
				(
					this.db
						.prepare(
							"SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
						)
						.get() as { version: number } | null
				)?.version ?? 0;
			assertNotNewerThanCode(
				storedVersion,
				SERVER_LOCAL_SCHEMA_VERSION,
				"state.db",
			);
			for (const migration of pendingMigrations(
				SERVER_LOCAL_MIGRATIONS,
				storedVersion,
				SERVER_LOCAL_SCHEMA_VERSION,
				"state.db",
			)) {
				migration.up(this.db);
				this.db
					.prepare(
						"INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
					)
					.run(migration.version, Date.now());
			}
			this.db.exec("COMMIT");
			return storedVersion;
		} catch (err) {
			// A failed statement can already have rolled the transaction back.
			if (this.db.inTransaction) this.db.exec("ROLLBACK");
			throw err;
		}
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
		const stmt = this.db.prepare(INSERT_ACTIVATION_EVENT_SQL);
		this.db.transaction(() => {
			for (const r of rows) {
				stmt.run(
					...ACTIVATION_EVENT_COLUMNS.map((c) => sqliteBinding(c.value(r))),
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

	/** Injected activations per client since sinceMs: only those that returned entries and were not cut off by the client deadline. */
	async countActivationsByClient(
		sinceMs: number,
	): Promise<Record<string, number>> {
		const rows = this.db
			.prepare(
				"SELECT client, COUNT(*) AS n FROM activation_event WHERE ts >= ? AND n_returned > 0 AND client_deadline_exceeded = 0 GROUP BY client",
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
