/**
 * Migrations for state.db (ServerStateDB).
 *
 * All one-time data migrations live here. `runStateMigrations()` is called
 * from ServerStateDB.initialize() after schema tables are created.
 * Each migration is idempotent — guarded by applied_migrations.
 *
 * Adding a new migration: add a function below and call it in runStateMigrations().
 */

import { Database, type SQLQueryBindings } from "bun:sqlite";
import { existsSync } from "node:fs";
import { logger } from "../../logger.js";

/**
 * Run all pending state.db data migrations.
 * Called from ServerStateDB.initialize() — no external coordination needed.
 *
 * @param db The state.db Database handle (schema tables already created).
 * @param legacyKnowledgeDbPath The knowledge.db to copy v3 staging tables from.
 *   Omit to skip the legacy steps: no other file is ever opened or modified.
 */
export function runStateMigrations(
	db: Database,
	legacyKnowledgeDbPath?: string,
): void {
	// Each step is independent with its own applied_migrations guard.
	copyKnowledgeDbStagingTables(db, legacyKnowledgeDbPath);
	dropKnowledgeDbStagingTables(db, legacyKnowledgeDbPath);
	dropOrphanedDaemonCursorFromStateDb(db);
	addPendingEpisodeFailureColumns(db);
}

/**
 * v3 migration step 1: copy staging tables from knowledge.db → state.db.
 *
 * In v2 and early v3, pending_episodes, consolidated_episode, consolidation_state,
 * and daemon_cursor lived in knowledge.db alongside knowledge entries.
 * From v3.x onwards they live exclusively in state.db.
 *
 * Idempotent — the guard check and data copy are inside the same transaction
 * so a crash between commit and key stamp cannot cause a double-run.
 */
function copyKnowledgeDbStagingTables(
	db: Database,
	legacyPath: string | undefined,
): void {
	if (!legacyPath || !existsSync(legacyPath)) return;

	const migrationKey = "v3_copy_staging_tables";
	const src = new Database(legacyPath, { readonly: true });

	try {
		const tables = new Set(
			(
				src
					.prepare("SELECT name FROM sqlite_master WHERE type='table'")
					.all() as Array<{ name: string }>
			).map((r) => r.name),
		);

		// Guard + data copy + key stamp all inside one transaction — atomic.
		const alreadyDone = db.transaction(() => {
			if (
				db
					.prepare("SELECT 1 FROM applied_migrations WHERE name = ? LIMIT 1")
					.get(migrationKey)
			) {
				return true;
			}

			logger.log(
				"[migration] Copying staging tables from knowledge.db → state.db...",
			);

			if (tables.has("pending_episodes")) {
				const rows = src.prepare("SELECT * FROM pending_episodes").all();
				const insert = db.prepare(
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
				logger.log(`[migration] Copied ${rows.length} pending_episodes rows.`);
			}

			if (tables.has("consolidated_episode")) {
				const rows = src.prepare("SELECT * FROM consolidated_episode").all();
				const insert = db.prepare(
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
				logger.log(
					`[migration] Copied ${rows.length} consolidated_episode rows.`,
				);
			}

			if (tables.has("consolidation_state")) {
				const row = src
					.prepare("SELECT * FROM consolidation_state WHERE id = 1")
					.get() as Record<string, SQLQueryBindings> | null;
				if (row) {
					db.prepare(
						`UPDATE consolidation_state SET
             last_consolidated_at = ?, total_sessions_processed = ?,
             total_entries_created = ?, total_entries_updated = ?
             WHERE id = 1`,
					).run(
						row.last_consolidated_at ?? 0,
						row.total_sessions_processed ?? 0,
						row.total_entries_created ?? 0,
						row.total_entries_updated ?? 0,
					);
					logger.log("[migration] Copied consolidation_state.");
				}
			}

			// daemon_cursor is NOT migrated into state.db — it was removed from
			// state.db when DaemonDB was introduced (src/db/daemon/index.ts).
			// daemon_cursor now lives in daemon.db (always local SQLite per-machine).
			// If knowledge.db has daemon_cursor or source_cursor rows, we skip them
			// here. The daemon will start from cursor=0 on first run and re-upload
			// historical episodes (safe — the server deduplicates via consolidated_episode).
			if (tables.has("daemon_cursor") || tables.has("source_cursor")) {
				logger.log(
					"[migration] Skipping daemon_cursor migration — cursor now lives in daemon.db. " +
						"Daemon will re-upload history on first run (safe).",
				);
			}

			// Stamp atomically with the data — a crash before this line rolls back
			// the entire transaction including data, so no partial state can occur.
			db.prepare(
				"INSERT OR IGNORE INTO applied_migrations (name, applied_at) VALUES (?, ?)",
			).run(migrationKey, Date.now());

			return false;
		})();

		if (!alreadyDone) {
			logger.log("[migration] Staging table copy complete.");
		}
	} catch (err) {
		logger.warn(
			`[migration] Copy failed — state.db starts with empty staging tables. Error: ${err instanceof Error ? err.message : String(err)}`,
		);
	} finally {
		src.close();
	}
}

/**
 * v3 migration step 2: drop orphaned staging tables from knowledge.db.
 *
 * Independent from the copy step — has its own guard and checks that the
 * copy was recorded before dropping (never drops if copy failed).
 */
function dropKnowledgeDbStagingTables(
	db: Database,
	legacyPath: string | undefined,
): void {
	if (!legacyPath || !existsSync(legacyPath)) return;

	const dropKey = "v3_drop_staging_tables";
	if (
		db
			.prepare("SELECT 1 FROM applied_migrations WHERE name = ? LIMIT 1")
			.get(dropKey)
	)
		return;

	// Only drop if copy succeeded — do not drop if copy failed or never ran.
	if (
		!db
			.prepare(
				"SELECT 1 FROM applied_migrations WHERE name = 'v3_copy_staging_tables' LIMIT 1",
			)
			.get()
	)
		return;

	const rw = new Database(legacyPath);
	try {
		rw.transaction(() => {
			rw.exec("DROP TABLE IF EXISTS pending_episodes");
			rw.exec("DROP TABLE IF EXISTS consolidated_episode");
			rw.exec("DROP TABLE IF EXISTS consolidation_state");
			rw.exec("DROP TABLE IF EXISTS daemon_cursor");
			rw.exec("DROP TABLE IF EXISTS source_cursor");
		})();
		db.prepare(
			"INSERT OR IGNORE INTO applied_migrations (name, applied_at) VALUES (?, ?)",
		).run(dropKey, Date.now());
		logger.log(
			"[migration] Dropped orphaned staging tables from knowledge.db.",
		);
	} catch (err) {
		logger.warn(
			`[migration] Could not drop orphaned tables from knowledge.db: ${err instanceof Error ? err.message : String(err)}`,
		);
	} finally {
		rw.close();
	}
}

/**
 * v3.x migration: drop orphaned daemon_cursor table from state.db.
 *
 * When DaemonDB was introduced (src/db/daemon/index.ts), daemon_cursor was
 * moved out of state.db into a separate daemon.db. Existing state.db files
 * from prior v3.x installs still contain the daemon_cursor table — it is
 * now inert (nothing reads or writes it from state.db) but takes space and
 * is confusing. Drop it.
 *
 * This migration is safe to run on any state.db version:
 * - Fresh installs: no daemon_cursor table → DROP IF EXISTS is a no-op.
 * - Pre-DaemonDB v3.x installs: table present → dropped.
 *
 * Note: the cursor data is intentionally NOT migrated to daemon.db here.
 * That would require DaemonDB to be open and writable during state.db
 * migration, creating a cross-DB dependency. The cost of discarding the
 * cursor is a one-time re-upload of historical episodes (safe — the server
 * deduplicates via consolidated_episode).
 */
function dropOrphanedDaemonCursorFromStateDb(db: Database): void {
	const key = "v3_drop_daemon_cursor_from_state_db";
	const already = db
		.prepare("SELECT 1 FROM applied_migrations WHERE name = ? LIMIT 1")
		.get(key);
	if (already) return;

	db.transaction(() => {
		// Check if the table exists — a simple row lookup is enough since the
		// query already filters by name (no need for a Set).
		const exists = db
			.prepare(
				"SELECT 1 FROM sqlite_master WHERE type='table' AND name='daemon_cursor' LIMIT 1",
			)
			.get();

		if (exists) {
			db.exec("DROP TABLE IF EXISTS daemon_cursor");
			logger.log(
				"[migration] Dropped orphaned daemon_cursor table from state.db — cursor now lives in daemon.db.",
			);
		}

		db.prepare(
			"INSERT OR IGNORE INTO applied_migrations (name, applied_at) VALUES (?, ?)",
		).run(key, Date.now());
	})();
}

/**
 * v2 schema step: add failed_attempts / last_error to pending_episodes.
 *
 * Fresh DBs get these columns from SERVER_LOCAL_CREATE_TABLES. Existing v1 DBs
 * do not (CREATE TABLE IF NOT EXISTS is a no-op for them), so they are added
 * here. The column check makes the step safe whichever path created the table.
 */
function addPendingEpisodeFailureColumns(db: Database): void {
	const key = "v2_pending_episode_failure_columns";
	if (
		db
			.prepare("SELECT 1 FROM applied_migrations WHERE name = ? LIMIT 1")
			.get(key)
	)
		return;

	db.transaction(() => {
		const cols = new Set(
			(
				db.prepare("PRAGMA table_info(pending_episodes)").all() as Array<{
					name: string;
				}>
			).map((c) => c.name),
		);
		if (!cols.has("failed_attempts")) {
			db.exec(
				"ALTER TABLE pending_episodes ADD COLUMN failed_attempts INTEGER NOT NULL DEFAULT 0",
			);
		}
		if (!cols.has("last_error")) {
			db.exec("ALTER TABLE pending_episodes ADD COLUMN last_error TEXT");
		}
		db.prepare(
			"INSERT OR IGNORE INTO applied_migrations (name, applied_at) VALUES (?, ?)",
		).run(key, Date.now());
	})();
}
