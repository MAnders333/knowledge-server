/**
 * Database module entry point.
 *
 * Re-exports the IKnowledgeDB interface and provides a factory function
 * that creates the appropriate implementation based on configuration:
 *
 * - POSTGRES_CONNECTION_URI set → PostgresKnowledgeDB
 * - Otherwise → KnowledgeDB (SQLite, default)
 */

export type { IKnowledgeDB } from "./interface.js";
export { KnowledgeDB } from "./database.js";
export { PostgresKnowledgeDB } from "./pg-database.js";

import { config } from "../config.js";
import { logger } from "../logger.js";
import { KnowledgeDB } from "./database.js";
import type { IKnowledgeDB } from "./interface.js";
import { PostgresKnowledgeDB } from "./pg-database.js";

/**
 * Create and initialize a KnowledgeDB instance based on configuration.
 *
 * When POSTGRES_CONNECTION_URI is set, creates a PostgresKnowledgeDB.
 * Otherwise, creates the default SQLite KnowledgeDB.
 *
 * @param dbPath Optional override for the SQLite database path (ignored for PostgreSQL).
 */
export async function createKnowledgeDB(
	dbPath?: string,
): Promise<IKnowledgeDB> {
	const pgUri = config.postgresConnectionUri;

	if (pgUri) {
		logger.log("[db] Using PostgreSQL backend.");
		const db = new PostgresKnowledgeDB(pgUri);
		await db.initialize();
		return db;
	}

	logger.log("[db] Using SQLite backend.");
	// SQLite KnowledgeDB initializes synchronously in the constructor
	return new KnowledgeDB(dbPath);
}
