/**
 * knowledge-daemon entry point.
 *
 * A thin binary that reads local AI tool session files and uploads episodes
 * to the pending_episodes staging table. The knowledge server drains this
 * table during consolidation.
 *
 * This entry point imports ONLY what the daemon needs:
 *   - Episode readers (file parsers for OpenCode, Claude Code, etc.)
 *   - EpisodeUploader (the upload loop)
 *   - DB client (local SQLite + optional remote Postgres target)
 *   - Config resolution
 *
 * Intentionally does NOT import:
 *   - HTTP server (Hono)
 *   - MCP server
 *   - LLM client (Anthropic, OpenAI, etc.)
 *   - Embedding client
 *   - Consolidation engine
 *   - Reconsolidation / synthesis
 *
 * This keeps the daemon binary small — a developer machine only needs the
 * daemon installed, not the full knowledge server.
 */

import { config, validateConfig } from "../config.js";
import { createEpisodeReaders } from "../consolidation/readers/index.js";
import { KnowledgeDB } from "../db/database.js";
import { StoreRegistry } from "../db/store-registry.js";
import { EpisodeUploader } from "./uploader.js";
import { logger } from "../logger.js";

// Parse CLI args
const args = process.argv.slice(2);
const intervalArg = args.find((a) => a.startsWith("--interval="));
const parsedInterval = intervalArg
	? Number.parseInt(intervalArg.split("=")[1], 10)
	: Number.NaN;
const intervalMs =
	!Number.isNaN(parsedInterval) && parsedInterval > 0
		? parsedInterval * 1000
		: 5 * 60 * 1000; // default: 5 minutes

const onceFlag = args.includes("--once");

// Validate config
const errors = validateConfig();
if (errors.length > 0) {
	for (const err of errors) console.error(`  ✗ ${err}`);
	process.exit(1);
}

// Local SQLite DB — always used for daemon cursor storage.
// Uses the default SQLite path if not configured explicitly.
const localDb = new KnowledgeDB();

// Target DB — where pending_episodes are written.
// In single-machine setups this is the same DB as the server uses.
// In remote setups this is a Postgres instance the server also connects to.
const registry = await StoreRegistry.create();
const targetDb = registry.writableStore();

// Resolve user ID from StoreRegistry (KNOWLEDGE_USER_ID → config.jsonc → hostname → "default")
const userId = registry.userId;

// Episode readers — same set as the consolidation engine uses locally.
const readers = createEpisodeReaders();

if (readers.length === 0) {
	logger.warn(
		"[daemon] No episode readers initialised. Check that at least one AI tool " +
			"is installed and enabled (OPENCODE_ENABLED, CLAUDE_ENABLED, etc.).",
	);
}

const uploader = new EpisodeUploader(readers, localDb, targetDb, userId);

if (onceFlag) {
	// One-shot mode: upload once and exit. Useful for cron / launchd OnDemand.
	try {
		const result = await uploader.upload();
		logger.log(
			`[daemon] Done. ${result.episodesUploaded} episodes uploaded from ${result.sessionsProcessed} sessions.`,
		);
	} finally {
		for (const reader of readers) reader.close();
		await localDb.close();
		await registry.close();
	}
	process.exit(0);
} else {
	// Polling mode: run until SIGTERM/SIGINT.
	// Pass an onShutdown callback so DB connections and readers are properly
	// closed before process.exit, avoiding SQLite WAL and Postgres pool leaks.
	await uploader.runPolling(intervalMs, async () => {
		for (const reader of readers) reader.close();
		await localDb.close();
		await registry.close();
	});
}
