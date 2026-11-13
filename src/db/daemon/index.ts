import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { logger } from "../../logger.js";
import type { DaemonCursor } from "../../types.js";
import { ensurePrivateFile } from "../../utils.js";
import { SQLITE_BUSY_TIMEOUT_MS } from "../sqlite-busy.js";

/**
 * Default path for daemon.db — the daemon-local SQLite database.
 * Always a local SQLite file on the machine where knowledge-daemon runs.
 * Never shared or remote.
 */
export const DEFAULT_DAEMON_DB_PATH = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"daemon.db",
);

const CREATE_TABLES = `
  CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER NOT NULL,
    applied_at INTEGER NOT NULL
  );

  -- Daemon cursor — tracks how far the daemon has scanned each source.
  -- Per-machine, never replicated. The daemon uses this to avoid re-uploading
  -- episodes it has already staged in pending_episodes.
  CREATE TABLE IF NOT EXISTS daemon_cursor (
    source                    TEXT    PRIMARY KEY,
    last_message_time_created INTEGER NOT NULL DEFAULT 0,
    last_uploaded_at          INTEGER NOT NULL DEFAULT 0
  );

  -- Per-session upload watermark — the last message ID the daemon has staged
  -- for each (source, session). A grown session is read from this message on,
  -- so only its new tail is uploaded. Per-machine, like daemon_cursor.
  CREATE TABLE IF NOT EXISTS daemon_session_cursor (
    source          TEXT    NOT NULL,
    session_id      TEXT    NOT NULL,
    last_message_id TEXT    NOT NULL,
    updated_at      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id)
  );
`;

/**
 * Schema history:
 *   1 — schema_version + daemon_cursor.
 *   2 — daemon_session_cursor (incremental upload watermark per session).
 * CREATE TABLE IF NOT EXISTS applies new tables on open; the version is recorded.
 */
const SCHEMA_VERSION = 2;

/** Max session IDs per IN (...) query; keeps well under SQLite's bound-parameter limit. */
const SESSION_LOOKUP_CHUNK = 500;

/**
 * DaemonDB — the daemon-local SQLite database.
 *
 * Contains daemon_cursor and daemon_session_cursor. This is intentionally separate
 * from state.db (the server's staging/bookkeeping DB) because:
 *
 * 1. daemon_cursor is per-machine and never needs to be seen by the server.
 * 2. state.db (pending_episodes) can be backed by Postgres in remote setups,
 *    while daemon_cursor must always remain local.
 *
 * By keeping them separate, the daemon can write episodes to a remote
 * state.db (Postgres) while keeping its local cursor in daemon.db (SQLite).
 */
export class DaemonDB {
	private readonly db: Database;

	constructor(dbPath = DEFAULT_DAEMON_DB_PATH) {
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
		this.initialize();
		logger.log(`[db] Daemon DB: SQLite at ${dbPath}`);
	}

	private initialize(): void {
		this.db.exec(CREATE_TABLES);
		const existing = this.db
			.prepare("SELECT MAX(version) as v FROM schema_version")
			.get() as { v: number | null };
		if (existing?.v == null || existing.v < SCHEMA_VERSION) {
			this.db
				.prepare(
					"INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (?, ?)",
				)
				.run(SCHEMA_VERSION, Date.now());
		}
	}

	/** False until the source has a daemon_cursor row, i.e. before its first run. */
	async hasDaemonCursor(source: string): Promise<boolean> {
		const row = this.db
			.prepare("SELECT 1 FROM daemon_cursor WHERE source = ?")
			.get(source);
		return row !== null && row !== undefined;
	}

	/** Returns zeros for a source with no daemon_cursor row. */
	async getDaemonCursor(source: string): Promise<DaemonCursor> {
		const row = this.db
			.prepare("SELECT * FROM daemon_cursor WHERE source = ?")
			.get(source) as {
			source: string;
			last_message_time_created: number;
			last_uploaded_at: number;
		} | null;
		if (!row) {
			return { source, lastMessageTimeCreated: 0, lastUploadedAt: 0 };
		}
		return {
			source: row.source,
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

	/**
	 * Last message ID staged for each of `sessionIds` (sessions never uploaded are
	 * absent from the map).
	 */
	async getSessionCursors(
		source: string,
		sessionIds: string[],
	): Promise<Map<string, string>> {
		const result = new Map<string, string>();
		for (let i = 0; i < sessionIds.length; i += SESSION_LOOKUP_CHUNK) {
			const chunk = sessionIds.slice(i, i + SESSION_LOOKUP_CHUNK);
			const placeholders = chunk.map(() => "?").join(", ");
			const rows = this.db
				.prepare(
					`SELECT session_id, last_message_id FROM daemon_session_cursor
					 WHERE source = ? AND session_id IN (${placeholders})`,
				)
				.all(source, ...chunk) as Array<{
				session_id: string;
				last_message_id: string;
			}>;
			for (const row of rows) result.set(row.session_id, row.last_message_id);
		}
		return result;
	}

	/** Record that messages of `sessionId` up to and including `lastMessageId` are staged. */
	async setSessionCursor(
		source: string,
		sessionId: string,
		lastMessageId: string,
	): Promise<void> {
		this.db
			.prepare(
				`INSERT INTO daemon_session_cursor (source, session_id, last_message_id, updated_at)
				 VALUES (?, ?, ?, ?)
				 ON CONFLICT (source, session_id) DO UPDATE SET
				   last_message_id = excluded.last_message_id,
				   updated_at = excluded.updated_at`,
			)
			.run(source, sessionId, lastMessageId, Date.now());
	}

	/** Forget every cursor (source-level and per-session), so the next run rescans from scratch. */
	async resetDaemonCursors(): Promise<void> {
		this.db.exec("DELETE FROM daemon_cursor");
		this.db.exec("DELETE FROM daemon_session_cursor");
	}

	close(): void {
		this.db.close();
	}
}
