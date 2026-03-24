/**
 * knowledge-daemon entry point.
 *
 * A thin binary that reads local AI tool session files and uploads episodes
 * to the server-local pending_episodes staging table.
 *
 * The daemon always writes to the server-local DB (state.db on the same
 * machine as the server). The server consolidates from there and routes
 * extracted knowledge to the appropriate knowledge store (SQLite or Postgres)
 * based on domain configuration.
 *
 * This entry point imports ONLY what the daemon needs:
 *   - Episode readers (file parsers for OpenCode, Claude Code, etc.)
 *   - EpisodeUploader (the upload loop)
 *   - DaemonDB (daemon cursor, always local SQLite)
 *   - StoreRegistry (to get IServerStateDB for pending_episodes)
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
import { createEpisodeReaders } from "./readers/index.js";
import { DaemonDB } from "../db/daemon/index.js";
import { createServerStateDB } from "../db/state/factory.js";
import { resolveUserId } from "../config-file.js";
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

// Server state DB — holds pending_episodes. Can be local SQLite or Postgres
// depending on stateDb config. Daemon writes episodes here; server reads them.
// Uses createServerStateDB (thin factory) rather than StoreRegistry to avoid
// importing the full knowledge-store surface, keeping the daemon binary small.
const serverStateDb = await createServerStateDB();

// Daemon-local DB — holds daemon_cursor only. Always local SQLite.
const daemonDb = new DaemonDB();

const userId = resolveUserId();

// Episode readers — file parsers for local AI tool session files.
const readers = createEpisodeReaders();

if (readers.length === 0) {
	logger.warn(
		"[daemon] No episode readers initialised. Check that at least one AI tool " +
			"is installed and enabled (OPENCODE_ENABLED, CLAUDE_ENABLED, etc.).",
	);
}

const uploader = new EpisodeUploader(readers, serverStateDb, daemonDb, userId);

if (onceFlag) {
	// One-shot mode: upload once and exit. Useful for cron / launchd OnDemand.
	logger.log(`[daemon] Running once. User: ${userId}`);
	try {
		const result = await uploader.upload();
		logger.log(
			`[daemon] Done. ${result.episodesUploaded} episodes uploaded from ${result.sessionsProcessed} sessions.`,
		);
	} finally {
		for (const reader of readers) reader.close();
		await serverStateDb.close();
		daemonDb.close();
	}
	process.exit(0);
} else {
	// Polling mode: run until SIGTERM/SIGINT.
	await uploader.runPolling(intervalMs, async () => {
		for (const reader of readers) reader.close();
		await serverStateDb.close();
		daemonDb.close();
	});
}
