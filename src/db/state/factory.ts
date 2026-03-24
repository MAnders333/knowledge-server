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
	loadConfigFile,
	DEFAULT_CONFIG_PATH,
} from "../../config-file.js";
import type { IServerStateDB } from "../interface.js";
import { errCode } from "../../utils.js";
import { logger } from "../../logger.js";
import { ServerStateDB } from "./index.js";
import { PostgresServerStateDB } from "./postgres.js";

/**
 * Create the correct IServerStateDB implementation based on config.
 * Reads config.jsonc (or uses DEFAULT_CONFIG if absent).
 */
export async function createServerStateDB(
	configPath = DEFAULT_CONFIG_PATH,
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

	const stateDbUri = process.env.STATE_DB_URI ?? config.stateDb?.uri;

	if (config.stateDb?.kind === "postgres" || stateDbUri) {
		if (!stateDbUri) {
			throw new Error(
				'stateDb.kind is "postgres" but no uri configured and STATE_DB_URI env var is not set',
			);
		}
		logger.log(
			`[db] Server state DB: Postgres at ${stateDbUri.replace(/:\/\/[^@]*@/, "://<redacted>@")}`,
		);
		const db = new PostgresServerStateDB(stateDbUri);
		await db.initialize();
		return db;
	}

	return new ServerStateDB(config.stateDb?.path);
}
