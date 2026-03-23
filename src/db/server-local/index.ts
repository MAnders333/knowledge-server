import { type SQLQueryBindings, Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { IServerLocalDB } from "../interface.js";
import { logger } from "../../logger.js";
import type {
	ConsolidationState,
	DaemonCursor,
	Episode,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import {
	SERVER_LOCAL_CREATE_TABLES,
	SERVER_LOCAL_SCHEMA_VERSION,
} from "./schema.js";

/**
 * Default path for server.db — the server-local staging/bookkeeping database.
 * Always a local SQLite file on the machine where knowledge-server runs.
 */
export const DEFAULT_SERVER_LOCAL_PATH = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"server.db",
);

/**
 * ServerLocalDB — the server-local SQLite database.
 *
 * Holds staging and bookkeeping tables:
 *   - pending_episodes: daemon writes, server drains
 *   - consolidated_episode: idempotency log
 *   - consolidation_state: global server counters
 *   - daemon_cursor: daemon upload progress
 *
 * The actual knowledge (knowledge_entry, etc.) lives in the configured
 * knowledge stores (IKnowledgeStore), which can be local SQLite or remote Postgres.
 */
export class ServerLocalDB implements IServerLocalDB {
	private readonly db: Database;
	private _consolidationLockHeld = false;

	constructor(dbPath = DEFAULT_SERVER_LOCAL_PATH) {
		const dir = dirname(dbPath);
		if (!existsSync(dir)) {
			mkdirSync(dir, { recursive: true });
		}
		this.db = new Database(dbPath);
		this.db.exec("PRAGMA journal_mode=WAL");
		this.db.exec("PRAGMA foreign_keys=ON");
		this.initialize();
		logger.log(`[db] Server local DB: SQLite at ${dbPath}`);
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
		// idempotent. Schema migrations for server.db are always additive; tables
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
					`[db] Server local DB: created at v${SERVER_LOCAL_SCHEMA_VERSION}`,
				);
			}
		}
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
			contentType: r.content_type as Episode["contentType"],
			timeCreated: r.session_timestamp,
			maxMessageTime: r.max_message_time,
			approxTokens: r.approx_tokens,
			uploadedAt: r.uploaded_at,
		}));
	}

	async countPendingSessions(): Promise<number> {
		const row = this.db
			.prepare("SELECT COUNT(DISTINCT session_id) as n FROM pending_episodes")
			.get() as { n: number };
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

		const rows = this.db
			.prepare(
				`SELECT source, session_id, start_message_id, end_message_id
         FROM consolidated_episode
         WHERE session_id IN (SELECT value FROM json_each(?))`,
			)
			.all(JSON.stringify(sessionIds)) as Array<{
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

	// ── Daemon Cursor ─────────────────────────────────────────────────────────

	async getDaemonCursor(source: string): Promise<DaemonCursor> {
		const row = this.db
			.prepare(
				"SELECT last_message_time_created, last_uploaded_at FROM daemon_cursor WHERE source = ?",
			)
			.get(source) as {
			last_message_time_created: number;
			last_uploaded_at: number;
		} | null;

		if (!row) return { source, lastMessageTimeCreated: 0, lastUploadedAt: 0 };
		return {
			source,
			lastMessageTimeCreated: row.last_message_time_created,
			lastUploadedAt: row.last_uploaded_at,
		};
	}

	async updateDaemonCursor(
		source: string,
		cursor: Partial<Omit<DaemonCursor, "source">>,
	): Promise<void> {
		// Single atomic upsert using COALESCE to preserve existing values for
		// fields not provided by the caller — avoids a TOCTOU race from read-then-write.
		const newLastMessageTime = cursor.lastMessageTimeCreated ?? null;
		const newLastUploaded = cursor.lastUploadedAt ?? null;
		this.db
			.prepare(
				`INSERT INTO daemon_cursor (source, last_message_time_created, last_uploaded_at)
         VALUES (?, COALESCE(?, 0), COALESCE(?, 0))
         ON CONFLICT (source) DO UPDATE SET
           last_message_time_created = COALESCE(?, last_message_time_created),
           last_uploaded_at = COALESCE(?, last_uploaded_at)`,
			)
			.run(
				source,
				newLastMessageTime,
				newLastUploaded,
				newLastMessageTime,
				newLastUploaded,
			);
	}

	// ── Migration helper ──────────────────────────────────────────────────────

	/**
	 * Copy staging/bookkeeping data from an existing knowledge.db into this server.db.
	 * Called once on first startup when migrating from the old single-file architecture.
	 *
	 * @param sourceDbPath Path to the existing knowledge.db file.
	 */
	migrateFromKnowledgeDb(sourceDbPath: string): void {
		if (!existsSync(sourceDbPath)) return;

		// Guard: only migrate once. If this path has already been migrated, skip.
		const migrationKey = `knowledge_db:${sourceDbPath}`;
		const alreadyMigrated = this.db
			.prepare("SELECT 1 FROM applied_migrations WHERE name = ? LIMIT 1")
			.get(migrationKey);
		if (alreadyMigrated) return;

		logger.log(
			`[db] Migrating staging tables from ${sourceDbPath} → server.db...`,
		);

		const src = new Database(sourceDbPath, { readonly: true });
		try {
			// Check if source has the tables we need
			const tables = (
				src
					.prepare("SELECT name FROM sqlite_master WHERE type='table'")
					.all() as Array<{ name: string }>
			).map((r) => r.name);

			this.db.transaction(() => {
				if (tables.includes("pending_episodes")) {
					const rows = src.prepare("SELECT * FROM pending_episodes").all();
					const insert = this.db.prepare(
						`INSERT OR IGNORE INTO pending_episodes
             (id, user_id, source, session_id, start_message_id, end_message_id,
              session_title, project_name, directory, content, content_type,
              session_timestamp, max_message_time, approx_tokens, uploaded_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					);
					for (const r of rows as Record<string, SQLQueryBindings>[]) {
						insert.run(
							r.id,
							r.user_id,
							r.source,
							r.session_id,
							r.start_message_id,
							r.end_message_id,
							r.session_title ?? "",
							r.project_name ?? "",
							r.directory ?? "",
							r.content,
							r.content_type,
							r.session_timestamp ?? 0,
							r.max_message_time ?? 0,
							r.approx_tokens ?? 0,
							r.uploaded_at,
						);
					}
					logger.log(`[db] Migrated ${rows.length} pending_episodes rows.`);
				}

				if (tables.includes("consolidated_episode")) {
					const rows = src.prepare("SELECT * FROM consolidated_episode").all();
					const insert = this.db.prepare(
						`INSERT OR IGNORE INTO consolidated_episode
             (source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
					);
					for (const r of rows as Record<string, SQLQueryBindings>[]) {
						insert.run(
							r.source,
							r.session_id,
							r.start_message_id,
							r.end_message_id,
							r.content_type,
							r.processed_at,
							r.entries_created ?? 0,
						);
					}
					logger.log(`[db] Migrated ${rows.length} consolidated_episode rows.`);
				}

				if (tables.includes("consolidation_state")) {
					const row = src
						.prepare("SELECT * FROM consolidation_state WHERE id = 1")
						.get() as Record<string, SQLQueryBindings> | null;
					if (row) {
						this.db
							.prepare(
								`UPDATE consolidation_state SET
               last_consolidated_at = ?, total_sessions_processed = ?,
               total_entries_created = ?, total_entries_updated = ?
               WHERE id = 1`,
							)
							.run(
								row.last_consolidated_at ?? 0,
								row.total_sessions_processed ?? 0,
								row.total_entries_created ?? 0,
								row.total_entries_updated ?? 0,
							);
						logger.log("[db] Migrated consolidation_state.");
					}
				}

				if (tables.includes("daemon_cursor")) {
					const rows = src.prepare("SELECT * FROM daemon_cursor").all();
					const insert = this.db.prepare(
						`INSERT OR REPLACE INTO daemon_cursor (source, last_message_time_created, last_uploaded_at)
             VALUES (?, ?, ?)`,
					);
					for (const r of rows as Record<string, SQLQueryBindings>[]) {
						insert.run(
							r.source,
							r.last_message_time_created ?? 0,
							r.last_uploaded_at ?? 0,
						);
					}
					logger.log(`[db] Migrated ${rows.length} daemon_cursor rows.`);
				}
			})();

			// Record that this source has been migrated so subsequent startups skip it.
			this.db
				.prepare(
					"INSERT OR IGNORE INTO applied_migrations (name, applied_at) VALUES (?, ?)",
				)
				.run(migrationKey, Date.now());

			logger.log("[db] Migration from knowledge.db complete.");
		} catch (err) {
			logger.warn(
				`[db] Migration from knowledge.db failed — starting with empty staging tables. Error: ${err instanceof Error ? err.message : String(err)}`,
			);
		} finally {
			src.close();
		}
	}

	/**
	 * Wipe all staging data: pending_episodes, consolidated_episode, and reset
	 * consolidation_state. Called when reinitializing the knowledge store.
	 */
	async reinitializeLocal(): Promise<void> {
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
