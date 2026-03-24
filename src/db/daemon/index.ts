import { Database } from "bun:sqlite";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { logger } from "../../logger.js";
import type { DaemonCursor } from "../../types.js";

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
`;

const SCHEMA_VERSION = 1;

/**
 * DaemonDB — the daemon-local SQLite database.
 *
 * Contains only the daemon_cursor table. This is intentionally separate
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
		this.db = new Database(dbPath);
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

	async resetDaemonCursors(): Promise<void> {
		this.db.exec("DELETE FROM daemon_cursor");
	}

	close(): void {
		this.db.close();
	}
}
