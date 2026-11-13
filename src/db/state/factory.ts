/**
 * Thin factory for IServerStateDB — used by the daemon binary to avoid
 * importing StoreRegistry (which pulls in the full knowledge store surface:
 * KnowledgeDB, PostgresKnowledgeDB, DomainRouter, etc.).
 *
 * The daemon only needs IServerStateDB for pending_episodes. Using this
 * factory keeps the daemon binary small.
 */
import {
	DEFAULT_CONFIG,
	defaultConfigPath,
	loadConfigFile,
	resolveLegacyKnowledgeDbPath,
} from "../../config-file.js";
import { logger } from "../../logger.js";
import { redactUriCredentials } from "../../redact-uri.js";
import { errCode } from "../../utils.js";
import type { IServerStateDB } from "../interface.js";
import { ServerStateDB } from "./index.js";
import { PostgresServerStateDB } from "./postgres.js";

/**
 * Create the correct IServerStateDB implementation based on config.
 * Reads config.jsonc (or uses DEFAULT_CONFIG if absent).
 */
export async function createServerStateDB(
	configPath = defaultConfigPath(),
): Promise<IServerStateDB> {
	let config = DEFAULT_CONFIG;
	try {
		const fileConfig = loadConfigFile(configPath);
		if (fileConfig) config = fileConfig;
	} catch (e: unknown) {
		// Only swallow "file not found" errors — propagate config validation errors
		// so a malformed stateDb block doesn't silently fall back to SQLite and
		// route the daemon's episodes to the wrong DB.
		const code = errCode(e);
		if (code !== "ENOENT" && code !== "ENOTDIR") {
			throw e;
		}
		// Config file absent — use defaults (local SQLite)
	}

	// STATE_DB_URI is already applied to config.stateDb by config-file.ts.
	if (config.stateDb?.kind === "postgres") {
		const stateDbUri = config.stateDb.uri;
		if (!stateDbUri) {
			throw new Error('stateDb.kind is "postgres" but no uri configured');
		}
		logger.log(
			`[db] Server state DB: Postgres at ${redactUriCredentials(stateDbUri)}`,
		);
		const db = new PostgresServerStateDB(stateDbUri);
		await db.initialize();
		return db;
	}

	return new ServerStateDB(
		config.stateDb?.path,
		resolveLegacyKnowledgeDbPath(config),
	);
}
