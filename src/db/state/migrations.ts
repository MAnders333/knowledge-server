/**
 * Migrations for state.db (ServerStateDB).
 *
 * All one-time data migrations live here. `runStateMigrations()` is called
 * from ServerStateDB.initialize() after schema tables are created.
 * Each migration is idempotent — guarded by applied_migrations.
 *
 * Adding a new migration: add a function below and call it in runStateMigrations().
 */

import { type SQLQueryBindings, Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { logger } from "../../logger.js";

/**
 * The legacy knowledge.db path. Both knowledge.db and state.db share the same
 * data directory (~/.local/share/knowledge-server/) by convention — the
 * staging tables that need migrating were always at this fixed location.
 */
const LEGACY_KNOWLEDGE_DB_PATH = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"knowledge.db",
);

/**
 * Run all pending state.db data migrations.
 * Called from ServerStateDB.initialize() — no external coordination needed.
 *
 * @param db The state.db Database handle (schema tables already created).
 */
export function runStateMigrations(db: Database): void {
	// Two independent steps — each has its own applied_migrations guard.
	copyKnowledgeDbStagingTables(db);
	dropKnowledgeDbStagingTables(db);
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
function copyKnowledgeDbStagingTables(db: Database): void {
	if (!existsSync(LEGACY_KNOWLEDGE_DB_PATH)) return;

	const migrationKey = "v3_copy_staging_tables";
	const src = new Database(LEGACY_KNOWLEDGE_DB_PATH, { readonly: true });

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

			if (tables.has("daemon_cursor")) {
				const rows = src.prepare("SELECT * FROM daemon_cursor").all();
				const insert = db.prepare(
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
				logger.log(`[migration] Copied ${rows.length} daemon_cursor rows.`);
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
function dropKnowledgeDbStagingTables(db: Database): void {
	if (!existsSync(LEGACY_KNOWLEDGE_DB_PATH)) return;

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

	const rw = new Database(LEGACY_KNOWLEDGE_DB_PATH);
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
