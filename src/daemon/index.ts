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
 * Only one daemon may run per data directory: the process takes daemon.lock
 * at startup and exits cleanly when another live daemon already holds it.
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
import { resolveDataDir } from "../config-file.js";
import { config } from "../config.js";
import { DaemonDB } from "../db/daemon/index.js";
import { createServerStateDB } from "../db/state/factory.js";
import { logger, parseLogLevel } from "../logger.js";
import { tightenOwnedPrivatePaths } from "../private-files.js";
import {
	type LockHandle,
	lockHolder,
	tryAcquireProcessLock,
} from "../process-lock.js";
import {
	daemonLockPath,
	daemonLogPath,
	daemonStatusPath,
} from "../runtime-files.js";
import { currentUserId } from "../user-id.js";
import { parseInitialLookbackDays } from "./lookback.js";
import { createEpisodeReaders } from "./readers/index.js";
import { DAEMON_LOCK_RETRY_MS, acquireDaemonLock } from "./single-instance.js";
import { type DaemonStatus, writeDaemonStatus } from "./status.js";
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

// Data directory: state.db, the lockfiles and the status file live here.
const dataDir = resolveDataDir();
// KNOWLEDGE_DAEMON_LOG_PATH="" keeps daemon lines on stdout only; the server sets
// it when it auto-spawns the daemon and forwards those lines into server.log.
logger.init(
	process.env.KNOWLEDGE_DAEMON_LOG_PATH ?? daemonLogPath(dataDir),
	parseLogLevel(process.env.LOG_LEVEL),
	config.logMaxBytes,
);

// Single instance per data directory. Taken before any DB connection. A polling
// daemon that finds another one running idles and takes over when it exits; it
// does not exit, so launchd/systemd do not restart it in a loop. A one-shot run
// (--once, cron/on-demand) has nothing to wait for and exits instead.
const daemonLockFile = daemonLockPath(dataDir);
// A daemon that finds its lock taken over stops at once: another daemon now owns
// the data directory. launchd/systemd restart this process, and the restart waits.
const onDaemonLockLost = () => {
	logger.error(
		"[daemon] daemon.lock was taken over by another process. Exiting so only one daemon writes.",
	);
	process.exit(1);
};
let daemonLock: LockHandle;
if (onceFlag) {
	const lock = tryAcquireProcessLock(daemonLockFile, {
		onLost: onDaemonLockLost,
	});
	if (!lock) {
		const holder = lockHolder(daemonLockFile);
		const who = holder && holder.pid > 0 ? ` (PID ${holder.pid})` : "";
		logger.warn(
			`[daemon] Another knowledge-daemon${who} already runs for ${dataDir}. Exiting: only one daemon may run per data directory.`,
		);
		process.exit(0);
	}
	daemonLock = lock;
} else {
	const { lock, waited } = await acquireDaemonLock(daemonLockFile, {
		onWaiting: (pid) => {
			const who = pid > 0 ? ` (PID ${pid})` : "";
			logger.warn(
				`[daemon] Another knowledge-daemon${who} already runs for ${dataDir}. Waiting to take over; retrying every ${DAEMON_LOCK_RETRY_MS / 1000} s.`,
			);
		},
		onLost: onDaemonLockLost,
	});
	daemonLock = lock;
	if (waited) logger.log("[daemon] Took over the daemon lock; starting.");
}
const releaseDaemonLock = () => daemonLock.release();

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
	releaseDaemonLock();
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

const closeResources = async () => {
	for (const reader of readers) reader.close();
	await serverStateDb.close();
	daemonDb.close();
};

if (onceFlag) {
	// One-shot mode: upload once and exit. Useful for cron / launchd OnDemand.
	logger.log(`[daemon] Running once. User: ${userId}`);
	try {
		const result = await uploader.upload();
		logger.log(
			`[daemon] Done. ${result.episodesUploaded} episodes uploaded from ${result.sessionsProcessed} sessions.`,
		);
	} finally {
		await closeResources();
		releaseDaemonLock();
	}
	process.exit(0);
} else {
	// Polling mode: run until SIGTERM/SIGINT. Each cycle updates the status file the
	// server reads for /status.
	const statusPath = daemonStatusPath(dataDir);
	const status: DaemonStatus = {
		pid: process.pid,
		startedAt: Date.now(),
		intervalMs,
		lastCycleAt: null,
		consecutiveFailures: 0,
		lastError: null,
		lastUploadAt: null,
	};
	const publish = (patch: Partial<DaemonStatus>) => {
		Object.assign(status, patch);
		try {
			writeDaemonStatus(statusPath, status);
		} catch (err) {
			logger.warn("[daemon] Could not write status file:", err);
		}
	};
	publish({});
	await uploader.runPolling(
		intervalMs,
		async () => {
			await closeResources();
			releaseDaemonLock();
		},
		(outcome) => {
			const now = Date.now();
			publish({
				lastCycleAt: now,
				consecutiveFailures: outcome.ok ? 0 : status.consecutiveFailures + 1,
				lastError: outcome.ok ? null : (outcome.error ?? "unknown error"),
				...(outcome.uploaded > 0 && { lastUploadAt: now }),
			});
		},
	);
}
