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
 *   - createServerStateDB (thin factory, avoids importing StoreRegistry)
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

// Load .env from default config location for binary installs — must be first.
import "../env.js";
import pkg from "../../package.json" with { type: "json" };
import { DaemonDB } from "../db/daemon/index.js";
import { createServerStateDB } from "../db/state/factory.js";
import { logger } from "../logger.js";
import { tightenOwnedPrivatePaths } from "../private-files.js";
import { currentUserId } from "../user-id.js";
import { parseInitialLookbackDays } from "./lookback.js";
import { createEpisodeReaders } from "./readers/index.js";
import { EpisodeUploader } from "./uploader.js";

// Parse CLI args
const args = process.argv.slice(2);

// Side-effect-free exits, before any DB connection. `knowledge-server update`
// runs `--version` on staged binaries before swapping them in.
if (args.includes("--version")) {
	console.log(`knowledge-daemon v${pkg.version}`);
	process.exit(0);
}
if (args.includes("--help") || args.includes("-h")) {
	console.log(`knowledge-daemon v${pkg.version}

Usage: knowledge-daemon [--interval=SECONDS] [--once]

Uploads local AI tool session episodes to the server state DB.
  --interval=SECONDS  Polling interval (default 1800)
  --once              Upload once and exit
  --version           Print version and exit
  -h, --help          Show this help message and exit
`);
	process.exit(0);
}
const intervalArg = args.find((a) => a.startsWith("--interval="));
const parsedInterval = intervalArg
	? Number.parseInt(intervalArg.split("=")[1], 10)
	: Number.NaN;
const envInterval = process.env.DAEMON_UPLOAD_INTERVAL_SECONDS
	? Number.parseInt(process.env.DAEMON_UPLOAD_INTERVAL_SECONDS, 10)
	: Number.NaN;
const intervalMs =
	!Number.isNaN(parsedInterval) && parsedInterval > 0
		? parsedInterval * 1000
		: !Number.isNaN(envInterval) && envInterval > 0
			? envInterval * 1000
			: 30 * 60 * 1000; // default: 30 minutes — gives sessions time to reach a natural pause

const onceFlag = args.includes("--once");

// Upgraded installs may have group/other-readable state, DB or log files.
tightenOwnedPrivatePaths();

// No validateConfig() call here — the daemon does not use LLM or embedding.
// It only needs state.db (SQLite or Postgres), which createServerStateDB()
// resolves from config.jsonc. Connection failures surface naturally at runtime.

// Server state DB — holds pending_episodes. Can be local SQLite or Postgres
// depending on stateDb config. Daemon writes episodes here; server reads them.
// Uses createServerStateDB (thin factory) rather than StoreRegistry to avoid
// importing the full knowledge-store surface, keeping the daemon binary small.
let serverStateDb: Awaited<ReturnType<typeof createServerStateDB>>;
try {
	serverStateDb = await createServerStateDB();
} catch (err) {
	console.error(
		`  ✗ Failed to connect to state DB: ${err instanceof Error ? err.message : String(err)}`,
	);
	console.error(
		"  Check stateDb config in config.jsonc (or STATE_DB_URI env var for Postgres).",
	);
	process.exit(1);
}

// Daemon-local DB — holds daemon_cursor only. Always local SQLite.
const daemonDb = new DaemonDB();

const userId = currentUserId();

// Episode readers — file parsers for local AI tool session files.
const readers = createEpisodeReaders();

if (readers.length === 0) {
	logger.warn(
		"[daemon] No episode readers initialised. Check that at least one AI tool " +
			"is installed and enabled (OPENCODE_ENABLED, CLAUDE_ENABLED, etc.).",
	);
}

const uploader = new EpisodeUploader(readers, serverStateDb, daemonDb, userId, {
	initialLookbackDays: parseInitialLookbackDays(
		process.env.INITIAL_LOOKBACK_DAYS,
	),
});

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
