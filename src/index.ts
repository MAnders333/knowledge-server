import { randomBytes } from "node:crypto";
import { serve } from "bun";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../package.json" with { type: "json" };
import { ActivationEngine } from "./activation/activate.js";
import { createApp } from "./api/server.js";
import { config, validateConfig } from "./config.js";
import { ConsolidationEngine } from "./consolidation/consolidate.js";
import { createEpisodeReaders } from "./consolidation/readers/index.js";
import { KnowledgeDB } from "./db/database.js";
import { logger } from "./logger.js";
import { main as mcpMain } from "./mcp/index.js";
import { runSetupTool } from "./setup-tool.js";
import { runUpdate } from "./update.js";

// Determine the subcommand offset in process.argv.
// - Compiled binary:  argv = ["/path/to/knowledge-server", "mcp", ...]  → subcommand at [1]
// - bun run src/...:  argv = ["/path/to/bun", "src/index.ts", "mcp", ...]  → subcommand at [2]
// Bun.main is a virtual bundle path (/$bunfs/...) in compiled binaries, a real .ts path otherwise.
const isBinaryInstall = !Bun.main.endsWith(".ts");
const subcommand = isBinaryInstall ? process.argv[1] : process.argv[2];
const subcommandArgs = isBinaryInstall ? process.argv.slice(2) : process.argv.slice(3);

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

	// Initialize logger first so all subsequent output (including config errors) is captured.
	logger.init(config.logPath);

	logger.raw("┌─────────────────────────────────────┐");
	logger.raw(`│  Knowledge Server v${pkg.version.padEnd(17)}│`);
	logger.raw("│  Consolidation-aware knowledge       │");
	logger.raw("│  system for OpenCode agents          │");
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

	// Create HTTP app
	const app = createApp(db, activation, consolidation, adminToken, adminTokenIsStable);

	// Start server
	const server = serve({
		fetch: app.fetch,
		port: config.port,
		hostname: config.host,
		idleTimeout: 255, // max allowed by Bun — consolidation can take a while
	});

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
