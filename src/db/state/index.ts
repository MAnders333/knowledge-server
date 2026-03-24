import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { IServerStateDB } from "../interface.js";
import { logger } from "../../logger.js";
import type {
	ConsolidationState,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import {
	SERVER_LOCAL_CREATE_TABLES,
	SERVER_LOCAL_SCHEMA_VERSION,
} from "./schema.js";
import { runStateMigrations } from "./migrations.js";

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
	private _consolidationLockHeld = false;

	constructor(dbPath = DEFAULT_SERVER_STATE_PATH) {
		const dir = dirname(dbPath);
		if (!existsSync(dir)) {
			mkdirSync(dir, { recursive: true });
		}
		this.db = new Database(dbPath);
		this.db.exec("PRAGMA journal_mode=WAL");
		this.db.exec("PRAGMA foreign_keys=ON");
		this.initialize();
		logger.log(`[db] Server state DB: SQLite at ${dbPath}`);
	}

	private initialize(): void {
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

		// Always ensure applied_migrations exists — it was added in v1 but may be
		// absent from existing v1 DBs created before this table was introduced.
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS applied_migrations (
        name       TEXT    PRIMARY KEY,
        applied_at INTEGER NOT NULL
      );
    `);

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
		runStateMigrations(this.db);
	}

	// ── Pending Episodes ──────────────────────────────────────────────────────

	async insertPendingEpisode(episode: PendingEpisode): Promise<void> {
		this.db
			.prepare(
				`INSERT OR IGNORE INTO pending_episodes
         (id, user_id, source, session_id, start_message_id, end_message_id,
          session_title, project_name, directory, content, content_type,
          session_timestamp, max_message_time, approx_tokens, uploaded_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
			);
	}

	async getPendingEpisodes(
		afterMaxMessageTime: number,
		limit = 500,
	): Promise<PendingEpisode[]> {
		const rows = this.db
			.prepare(
				`SELECT * FROM pending_episodes
         WHERE max_message_time > ?
         ORDER BY max_message_time ASC
         LIMIT ?`,
			)
			.all(afterMaxMessageTime, limit) as Array<{
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
		}));
	}

	async countPendingSessions(): Promise<number> {
		const row = this.db
			.prepare("SELECT COUNT(DISTINCT session_id) as n FROM pending_episodes")
			.get() as { n: number } | undefined;
		return row?.n ?? 0;
	}

	async deletePendingEpisodes(ids: string[]): Promise<void> {
		if (ids.length === 0) return;
		this.db
			.prepare(
				"DELETE FROM pending_episodes WHERE id IN (SELECT value FROM json_each(?))",
			)
			.run(JSON.stringify(ids));
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

	// ── Consolidation Lock ────────────────────────────────────────────────────

	async tryAcquireConsolidationLock(): Promise<boolean> {
		if (this._consolidationLockHeld) return false;
		this._consolidationLockHeld = true;
		return true;
	}

	async releaseConsolidationLock(): Promise<void> {
		this._consolidationLockHeld = false;
	}

	/**
	 * Wipe all staging data: pending_episodes, consolidated_episode, and reset
	 * consolidation_state counters. Called when reinitializing the knowledge store.
	 */
	async reinitialize(): Promise<void> {
		this.db.transaction(() => {
			this.db.exec("DELETE FROM pending_episodes");
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

	async close(): Promise<void> {
		this.db.close();
	}
}
