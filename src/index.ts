import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { serve } from "bun";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../package.json" with { type: "json" };
import { ActivationEngine } from "./activation/activate.js";
import { createApp } from "./api/server.js";
import { runActivate } from "./commands/activate.js";
import { runCalibrate } from "./commands/calibrate.js";
import { runConsolidate } from "./commands/consolidate.js";
import { runReinitialize } from "./commands/reinitialize.js";
import { runStatus } from "./commands/status.js";
import { config, validateConfig } from "./config.js";
import { ConsolidationEngine } from "./consolidation/consolidate.js";
import { createEpisodeReaders } from "./consolidation/readers/index.js";
import { KnowledgeDB } from "./db/database.js";
import { logger } from "./logger.js";
import { main as mcpMain } from "./mcp/index.js";
import { runSetupTool } from "./setup-tool.js";
import { runStop } from "./stop.js";
import { runUpdate } from "./update.js";

// Bun normalises process.argv the same way for both compiled binaries and `bun run`:
//   argv[0] = "bun"
//   argv[1] = entry point (real .ts path or virtual /$bunfs/... bundle path)
//   argv[2] = first user argument (the subcommand)
// So argv[2] is always the subcommand regardless of how the binary is invoked.
const subcommand = process.argv[2];
const subcommandArgs = process.argv.slice(3);

/**
 * Knowledge Server — main entry point.
 *
 * Starts the HTTP API server that provides:
 * - /activate — embedding-based knowledge activation (used by plugin + MCP)
 * - /consolidate — triggers episodic → knowledge consolidation
 * - /review — surfaces entries needing human attention
 * - /status — health check and stats
 */
async function main() {
	// Handle subcommands before starting the HTTP server.
	// All early-exit paths use return so we avoid process.exit() where possible,
	// which allows the event loop to drain naturally (important for the mcp branch).

	// `knowledge-server mcp` — MCP stdio proxy.
	// mcpMain() calls server.connect(StdioServerTransport), which returns immediately
	// after the protocol handshake. The event loop stays alive because StdioServerTransport
	// holds stdin open; returning here lets it drain naturally when the client disconnects.
	if (subcommand === "mcp") {
		// Let errors propagate to the outer main().catch handler, which logs and exits 1.
		// return (not process.exit) lets the StdioServerTransport drain naturally.
		await mcpMain();
		return;
	}

	// `knowledge-server setup-tool <tool>`
	if (subcommand === "setup-tool") {
		runSetupTool(subcommandArgs);
		process.exit(0);
	}

	// `knowledge-server update [--version v1.2.3]`
	if (subcommand === "update") {
		console.log("┌─────────────────────────────────────┐");
		console.log("│  knowledge-server update             │");
		console.log("└─────────────────────────────────────┘");
		console.log("");
		await runUpdate(subcommandArgs, `v${pkg.version}`);
		process.exit(0);
	}

	// `knowledge-server stop`
	if (subcommand === "stop") {
		await runStop(config.pidPath);
		process.exit(0);
	}

	// `knowledge-server status`
	if (subcommand === "status") {
		runStatus(config.pidPath);
		process.exit(0);
	}

	// `knowledge-server consolidate`
	if (subcommand === "consolidate") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runConsolidate();
		process.exit(0);
	}

	// `knowledge-server activate <query>`
	if (subcommand === "activate") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runActivate(subcommandArgs[0] ?? "");
		process.exit(0);
	}

	// `knowledge-server calibrate`
	if (subcommand === "calibrate") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runCalibrate();
		process.exit(0);
	}

	// `knowledge-server reinitialize [--confirm|--dry-run]`
	if (subcommand === "reinitialize") {
		runReinitialize(subcommandArgs);
		process.exit(0);
	}

	// `knowledge-server --help` / `knowledge-server help`
	if (subcommand === "--help" || subcommand === "-h" || subcommand === "help") {
		console.log(`knowledge-server v${pkg.version}

Usage: knowledge-server [command]

Commands:
  (no command)              Start the HTTP server
  stop                      Stop the running HTTP server
  status                    Show server state and knowledge graph stats
  consolidate               Run a manual consolidation cycle
  activate <query>          Test knowledge activation for a query
  calibrate                 Recommend similarity thresholds for the active embedding model
  reinitialize              Wipe all knowledge and reset consolidation cursor
  setup-tool <tool>         Set up integration (opencode|claude-code|cursor|codex|vscode)
  update [--version v1.2.3] Update to the latest (or specified) release
  mcp                       Start the MCP stdio proxy (used by tool integrations)

Options:
  -h, --help                Show this help message
`);
		process.exit(0);
	}

	// Unknown subcommand guard
	if (subcommand !== undefined) {
		console.error(`Unknown command: "${subcommand}". Run \`knowledge-server --help\` for usage.`);
		process.exit(1);
	}

	// Initialize logger first so all subsequent output (including config errors) is captured.
	logger.init(config.logPath);

	logger.raw("┌─────────────────────────────────────┐");
	logger.raw(`│  Knowledge Server v${pkg.version.padEnd(17)}│`);
	logger.raw("│  Consolidation-aware knowledge       │");
	logger.raw("│  system for AI coding agents         │");
	logger.raw("└─────────────────────────────────────┘");

	// Validate config
	const errors = validateConfig();
	if (errors.length > 0) {
		logger.error("Configuration errors:");
		for (const err of errors) {
			logger.error(`  ✗ ${err}`);
		}
		process.exit(1);
	}

	// Initialize components
	const db = new KnowledgeDB();
	const activation = new ActivationEngine(db);
	const readers = createEpisodeReaders();
	const consolidation = new ConsolidationEngine(db, activation, readers);

	// Check if this is a first run (no knowledge yet, but episodes exist)
	const stats = db.getStats();
	const consolidationState = db.getConsolidationState();

	logger.log(
		`Knowledge graph: ${stats.total || 0} entries (${stats.active || 0} active)`,
	);
	logger.log(
		`Last consolidation: ${consolidationState.lastConsolidatedAt ? new Date(consolidationState.lastConsolidatedAt).toISOString() : "never"}`,
	);
	if (config.consolidation.includeToolOutputs.length > 0) {
		logger.log(
			`Tool output extraction: ${config.consolidation.includeToolOutputs.join(", ")}`,
		);
	} else {
		logger.log(
			"Tool output extraction: disabled (set CONSOLIDATION_INCLUDE_TOOL_OUTPUTS to enable)",
		);
	}

	// Check if the embedding model has changed and re-embed if necessary.
	// Runs before consolidation so all vectors are consistent when the
	// reconsolidation step compares new extractions against existing entries.
	try {
		const didReEmbed = await activation.checkAndReEmbed();
		if (didReEmbed) {
			logger.log("All embeddings are now consistent with the configured model.");
		}
	} catch (e) {
		logger.error(
			"[embedding] Re-embed check failed — activation and consolidation may produce dimension mismatch errors.",
			e,
		);
	}

	// Check for pending sessions
	const pending = consolidation.checkPending();
	if (pending.pendingSessions > 0) {
		logger.log(
			`⚡ ${pending.pendingSessions} sessions pending consolidation` +
				` (${config.consolidation.maxSessionsPerRun} per batch).`,
		);
		logger.log("  Starting background consolidation...");
	} else {
		logger.log("✓ Knowledge graph is up to date.");
	}

	// Admin token: use KNOWLEDGE_ADMIN_TOKEN env var if set (stable, useful for scripting),
	// otherwise generate a random token per process lifetime (more secure for interactive use).
	const adminTokenIsStable = !!config.adminToken;
	const adminToken = config.adminToken ?? randomBytes(24).toString("hex");

	// PID file guard — check before serve() so we never bind the port if another
	// instance is already running. If the file exists but the PID is dead (crash
	// without cleanup), treat it as stale and continue. The actual write happens
	// after serve() succeeds so the file only exists when a port is truly bound.
	if (config.pidPath && existsSync(config.pidPath)) {
		const stalePid = Number.parseInt(
			readFileSync(config.pidPath, "utf8").trim(),
			10,
		);
		const isAlive =
			!Number.isNaN(stalePid) &&
			(() => {
				try {
					process.kill(stalePid, 0);
					return true;
				} catch (e) {
					// EPERM = process exists but we can't signal it (still alive).
					// ESRCH = no such process (truly dead).
					return (e as NodeJS.ErrnoException).code === "EPERM";
				}
			})();
		if (isAlive) {
			logger.error(
				`Server already running at PID ${stalePid}. Run \`knowledge-server stop\` first.`,
			);
			process.exit(1);
		}
		// Stale file — remove and continue. Wrapped in try/catch to handle
		// a TOCTOU race where two concurrent starts both pass the liveness
		// check and the second unlinkSync hits ENOENT.
		try {
			unlinkSync(config.pidPath);
		} catch (e) {
			if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
		}
	}

	// Create HTTP app
	const app = createApp(db, activation, consolidation, adminToken, adminTokenIsStable);

	// Start server — PID file written after this succeeds so it only exists
	// when a port is truly bound.
	let server: ReturnType<typeof serve>;
	try {
		server = serve({
			fetch: app.fetch,
			port: config.port,
			hostname: config.host,
			idleTimeout: 255, // max allowed by Bun — consolidation can take a while
		});
	} catch (e) {
		if ((e as NodeJS.ErrnoException).code === "EADDRINUSE") {
			logger.error(
				`Port ${config.port} is already in use. Is another process running on that port?`,
			);
		} else {
			logger.error(
				`Failed to start HTTP server: ${e instanceof Error ? e.message : String(e)}`,
			);
		}
		process.exit(1);
	}

	if (config.pidPath) {
		writeFileSync(config.pidPath, String(process.pid), "utf8");
	}

	logger.raw(`\n✓ HTTP API listening on http://${config.host}:${config.port}`);
	logger.raw("  GET  /activate?q=...                  — Activate knowledge");
	logger.raw(
		"  POST /consolidate                      — Run consolidation   [admin token required]",
	);
	logger.raw("  GET  /review                           — Review entries");
	logger.raw("  GET  /status                           — Health check");
	logger.raw("  GET  /entries                          — List entries");
	logger.raw(
		"  POST /hooks/claude-code/user-prompt    — Claude Code hook (unauthenticated)",
	);
	logger.raw(
		`  ALL  /mcp                              — MCP streamable-http${adminTokenIsStable ? " [admin token required]" : " (unauthenticated — local only)"}`,
	);
	logger.rawStdoutOnly(`\n  Admin token (keep this private): ${adminToken}`);
	logger.rawStdoutOnly(
		`  curl -X POST -H "Authorization: Bearer <token>" http://${config.host}:${config.port}/consolidate`,
	);
	if (config.logPath) {
		logger.raw(`\n  Logs: ${config.logPath}`);
	}

	// Background consolidation — runs after the server starts listening so the
	// HTTP API is available immediately. Two modes:
	//
	//   Startup loop  — drains all pending sessions in batches on first start.
	//                   Runs once and exits when sessionsProcessed === 0.
	//
	//   Polling loop  — periodic check while server runs.
	//                   Enabled by CONSOLIDATION_POLL_INTERVAL_MS (default: disabled).
	//                   Fires only when pending sessions exist; skips silently otherwise.
	//
	// Both loops share the same tryLock() / unlock() so they can never run concurrently
	// with each other or with a manual API-triggered consolidation.
	let shutdownRequested = false;
	const activeLoops: Promise<void>[] = [];

	/**
	 * Run one complete consolidation drain: repeatedly calls consolidate() until
	 * sessionsProcessed === 0 or shutdown is requested.
	 * Caller is responsible for acquiring/releasing the lock.
	 */
	async function runConsolidationDrain(label: string): Promise<void> {
		let batch = 1;
		let consecutiveErrors = 0;
		const MAX_CONSECUTIVE_ERRORS = 3;
		const BASE_RETRY_DELAY_MS = 5_000;

		while (!shutdownRequested) {
			try {
				if (!consolidation.tryLock()) {
					// Another path holds the lock — yield and retry.
					await new Promise((resolve) => setTimeout(resolve, 2000));
					continue;
				}
				logger.log(`[${label}] Batch ${batch}...`);
				let result: Awaited<ReturnType<typeof consolidation.consolidate>>;
				try {
					result = await consolidation.consolidate();
				} finally {
					consolidation.unlock();
				}
				consecutiveErrors = 0;
				if (result.sessionsProcessed === 0) {
					logger.log(`[${label}] Complete — all sessions processed.`);
					break;
				}
				logger.log(
					`[${label}] Batch ${batch} done: ${result.sessionsProcessed} sessions, ` +
						`${result.entriesCreated} created, ${result.entriesUpdated} updated.`,
				);
				batch++;
			} catch (err) {
				consecutiveErrors++;
				if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
					logger.error(
						`[${label}] ${MAX_CONSECUTIVE_ERRORS} consecutive errors — giving up. Last error:`,
						err,
					);
					break;
				}
				const delay = BASE_RETRY_DELAY_MS * 2 ** (consecutiveErrors - 1);
				logger.error(
					`[${label}] Error (attempt ${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}), ` +
						`retrying in ${delay / 1000}s:`,
					err,
				);
				await new Promise((resolve) => setTimeout(resolve, delay));
			}
		}
	}

	// Startup drain
	if (pending.pendingSessions > 0) {
		activeLoops.push(runConsolidationDrain("startup consolidation"));
	}

	// Polling loop (opt-in)
	const pollIntervalMs = config.consolidation.pollIntervalMs;
	if (pollIntervalMs > 0) {
		logger.log(
			`✓ Auto-consolidation polling enabled (every ${pollIntervalMs / 1000}s).`,
		);
		activeLoops.push(
			(async () => {
				while (!shutdownRequested) {
					await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
					if (shutdownRequested) break;
					const { pendingSessions } = consolidation.checkPending();
					if (pendingSessions > 0) {
						logger.log(
							`[poll] ${pendingSessions} pending sessions — starting consolidation.`,
						);
						await runConsolidationDrain("poll");
					}
				}
			})(),
		);
	}

	// Maximum time (ms) to wait for an in-flight consolidation batch to finish
	// before the process exits on SIGINT / SIGTERM. 30 s is generous enough for
	// a typical LLM round-trip but short enough to not hang interactive shutdown.
	const SHUTDOWN_TIMEOUT_MS = 30_000;

	// Graceful shutdown — signal loops to stop, wait up to SHUTDOWN_TIMEOUT_MS
	// for the current batch to finish before closing the DB. Prevents losing
	// in-flight LLM results.
	async function shutdown(signal: string) {
		logger.log(`[${signal}] Shutting down gracefully...`);
		shutdownRequested = true;
		const TIMED_OUT = Symbol("timed_out");
		if (activeLoops.length > 0) {
			logger.log(
				`[shutdown] Waiting for ${activeLoops.length} active consolidation loop(s) to finish...`,
			);
		}
		const result = await Promise.race([
			Promise.all(activeLoops).then(() => null),
			new Promise<typeof TIMED_OUT>((r) =>
				setTimeout(() => r(TIMED_OUT), SHUTDOWN_TIMEOUT_MS),
			),
		]);
		if (result === TIMED_OUT) {
			logger.warn(
				`[shutdown] ${SHUTDOWN_TIMEOUT_MS / 1000}s timeout reached — in-flight consolidation batch abandoned.`,
			);
		}
		consolidation.close();
		db.close();
		// Clean up PID file on graceful shutdown — only if it still points to us.
		// Guards against a race where a second instance already overwrote the file.
		if (config.pidPath && existsSync(config.pidPath)) {
			try {
				const stored = Number.parseInt(
					readFileSync(config.pidPath, "utf8").trim(),
					10,
				);
				if (stored === process.pid) unlinkSync(config.pidPath);
			} catch {
				// Non-fatal — process is exiting anyway.
			}
		}
		process.exit(0);
	}

	process.on("SIGINT", () => {
		shutdown("SIGINT").catch((e) =>
			logger.error("[SIGINT] Unexpected shutdown error:", e),
		);
	});
	process.on("SIGTERM", () => {
		shutdown("SIGTERM").catch((e) =>
			logger.error("[SIGTERM] Unexpected shutdown error:", e),
		);
	});
}

main().catch((e) => {
	logger.error("Fatal error:", e);
	process.exit(1);
});
