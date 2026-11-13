// Load .env from default config location for binary installs — must be first.
import "./env.js";
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { serve } from "bun";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../package.json" with { type: "json" };
import { ActivationEngine } from "./activation/activate.js";
import { ActivationEventRecorder } from "./activation/events.js";
import { createRerankerFromConfig } from "./activation/rerank.js";
import { createApp } from "./api/server.js";
import { runActivate } from "./commands/activate.js";
import { runCalibrate } from "./commands/calibrate.js";
import { runClaudeAuth } from "./commands/claude-auth.js";
import { runConsolidate } from "./commands/consolidate.js";
import { runMigrateConfig } from "./commands/migrate-config.js";
import { runProcessInbox } from "./commands/process-inbox.js";
import { runReinitialize } from "./commands/reinitialize.js";
import { runReview } from "./commands/review.js";
import { runSetupTool } from "./commands/setup-tool.js";
import { runStatus } from "./commands/status.js";
import { runStop } from "./commands/stop.js";
import { downloadAndInstallDaemon, runUpdate } from "./commands/update.js";
import { resolveDataDir } from "./config-file.js";
import { config, spawnedDaemonIntervalArgs, validateConfig } from "./config.js";
import { ConsolidationEngine } from "./consolidation/consolidate.js";
import { runDecayTick, runPollTick } from "./consolidation/poll.js";
import { PendingEpisodesReader } from "./consolidation/readers/pending.js";
import { ServerStateDB } from "./db/state/index.js";
import { StoreRegistry } from "./db/store-registry.js";
import { logger, parseLogLevel } from "./logger.js";
import { main as mcpMain } from "./mcp/index.js";
import { tightenOwnedPrivatePaths } from "./private-files.js";
import { lockHolder } from "./process-lock.js";
import {
	consolidationLockPath,
	daemonLockPath,
	stateMaintenanceLockPath,
} from "./runtime-files.js";
import { errCode } from "./utils.js";

// Bun normalises process.argv the same way for both compiled binaries and `bun run`:
//   argv[0] = "bun"
//   argv[1] = entry point (real .ts path or virtual /$bunfs/... bundle path)
//   argv[2] = first user argument (the subcommand)
// So argv[2] is always the subcommand regardless of how the binary is invoked.
const subcommand = process.argv[2];
const subcommandArgs = process.argv.slice(3);

/**
 * Release resources on a fatal exit: kill the auto-spawned daemon child and
 * remove this process's PID file. Set by main() once the PID file is written.
 */
let releaseOnFatal: (() => void) | null = null;

/** Remove the PID file only if it still names this process (guards a racing second instance). */
function removeOwnPidFile(pidPath: string | undefined): void {
	if (!pidPath || !existsSync(pidPath)) return;
	try {
		const stored = Number.parseInt(readFileSync(pidPath, "utf8").trim(), 10);
		if (stored === process.pid) unlinkSync(pidPath);
	} catch {
		// Non-fatal — process is exiting anyway.
	}
}

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
		await runStatus(config.pidPath);
		process.exit(0);
	}

	// `knowledge-server consolidate`
	if (subcommand === "consolidate") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runConsolidate({
			retryDead: subcommandArgs.includes("--retry-dead"),
		});
		process.exit(0);
	}

	// `knowledge-server process-inbox [dir]`
	if (subcommand === "process-inbox") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runProcessInbox(subcommandArgs);
		process.exit(0);
	}

	// `knowledge-server claude-auth [--status|--revoke]`
	// Note: no validateConfig() check — claude-auth is used to SET UP credentials,
	// so it must work even when no credentials are configured yet.
	if (subcommand === "claude-auth") {
		await runClaudeAuth(subcommandArgs);
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

	// `knowledge-server review [--filter <conflicted|stale|all>]`
	if (subcommand === "review") {
		const errors = validateConfig();
		if (errors.length > 0) {
			for (const err of errors) console.error(`  ✗ ${err}`);
			process.exit(1);
		}
		await runReview(subcommandArgs);
		process.exit(0);
	}

	// `knowledge-server migrate-config`
	if (subcommand === "migrate-config") {
		runMigrateConfig();
		process.exit(0);
	}

	// `knowledge-server reinitialize [--confirm|--dry-run]`
	if (subcommand === "reinitialize") {
		await runReinitialize(subcommandArgs);
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
  consolidate [--retry-dead] Run a manual consolidation cycle (--retry-dead requeues dead-lettered episodes)
  process-inbox [dir]       Upload local files and run full pipeline (upload → consolidate → synthesize)
  claude-auth               Authenticate with a Claude Pro/Max subscription (optional)
  activate <query>          Test knowledge activation for a query
  review [--filter <f>]     Interactively review entries (filter: conflicted|stale|all)
  calibrate                 Recommend similarity thresholds for the active embedding model
  reinitialize              Reset daemon cursor, state, or store entries (see --reset-state, --reset-store)
  setup-tool <tool>         Set up integration: opencode|claude-code|cursor|codex|vscode,
                            pi|oh-my-pi (extensions), daemon (background service)
  update [--version v1.2.3] Update to the latest (or specified) release
  mcp                       Start the MCP stdio proxy (used by tool integrations)

Options:
  -h, --help                Show this help message

Run \`knowledge-server help-advanced\` for additional commands.
`);
		process.exit(0);
	}

	// `knowledge-server help-advanced` — less-common commands, co-located with --help
	if (subcommand === "help-advanced") {
		console.log(`knowledge-server v${pkg.version} — advanced commands

  migrate-config            Idempotent. Generates ~/.config/knowledge-server/config.jsonc
                            from env vars: POSTGRES_CONNECTION_URI, KNOWLEDGE_DB_PATH,
                            KNOWLEDGE_USER_ID, KNOWLEDGE_PORT, KNOWLEDGE_HOST, DAEMON_AUTO_SPAWN.
                            Run once after upgrading from a pre-config.jsonc release.
`);
		process.exit(0);
	}

	// Unknown subcommand guard
	if (subcommand !== undefined) {
		console.error(
			`Unknown command: "${subcommand}". Run \`knowledge-server --help\` for usage.`,
		);
		process.exit(1);
	}

	// Initialize logger first so all subsequent output (including config errors) is captured.
	logger.init(
		config.logPath,
		parseLogLevel(process.env.LOG_LEVEL),
		config.logMaxBytes,
	);
	// Upgraded installs may have group/other-readable logs, DBs or .env (server.log can hold an old admin token).
	tightenOwnedPrivatePaths();

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

	// Initialize store registry and components
	const registry = await StoreRegistry.create();
	const db = registry.writableStore();
	const { serverStateDb } = registry;
	// Data directory: lockfiles and the daemon status file live here, shared with the daemon and CLI.
	const dataDir = resolveDataDir();
	// Housekeeping before anything else writes heavily: reclaim state.db free pages
	// when the file is large and mostly empty. Skipped if another process is busy with it.
	if (serverStateDb instanceof ServerStateDB) {
		serverStateDb.compactIfNeeded({
			lockPath: stateMaintenanceLockPath(dataDir),
		});
	}
	const activation = new ActivationEngine(
		db,
		registry.readStores(),
		registry.writableStores(),
		createRerankerFromConfig(),
	);
	const consolidation = new ConsolidationEngine(
		db,
		serverStateDb,
		activation,
		[new PendingEpisodesReader(serverStateDb)],
		registry.domainRouter,
		{ lockPath: consolidationLockPath(dataDir) },
	);

	// Check if this is a first run (no knowledge yet, but episodes exist)
	const stats = await db.getStats();
	const consolidationState = await serverStateDb.getConsolidationState();

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
			logger.log(
				"All embeddings are now consistent with the configured model.",
			);
		}
	} catch (e) {
		logger.error(
			"[embedding] Re-embed check failed — activation and consolidation may produce dimension mismatch errors.",
			e,
		);
	}

	// Check for pending sessions
	const pending = await consolidation.checkPending();
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
					return errCode(e) === "EPERM";
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
			if (errCode(e) !== "ENOENT") throw e;
		}
	}

	// Content-free activation telemetry: pruned at startup and daily, flushed on shutdown.
	const activationEvents = new ActivationEventRecorder(serverStateDb);
	activationEvents.start();

	// Create HTTP app
	const app = createApp(
		db,
		serverStateDb,
		activation,
		consolidation,
		adminToken,
		adminTokenIsStable,
		registry.unavailableStoreIds,
		registry.readStores(),
		registry.writableStores(),
		registry.port,
		activationEvents,
		registry.host,
		config.allowedHosts,
		{ dataDir, daemonAutoSpawn: registry.daemonAutoSpawn },
	);

	// port/host/daemonAutoSpawn come from the registry (config.jsonc with env var override)
	// rather than config.ts, so they reflect the config file if one exists.
	const { port, host, daemonAutoSpawn } = registry;

	// Loopback-host check runs post-registry so it covers values set via config.jsonc
	// (not just KNOWLEDGE_HOST env var). Checked here rather than in validateConfig()
	// which runs before the registry is created.
	// Bracketed IPv6 (`[::1]`, as written in a URL or config) is normalised to the
	// bare address so both spellings pass the check and bind correctly.
	const bindHost = host.replace(/^\[(.*)\]$/, "$1");
	const loopbackHosts = ["127.0.0.1", "::1", "localhost"];
	if (!loopbackHosts.includes(bindHost)) {
		logger.error(
			`"host" is set to "${host}", which exposes the server on non-loopback interfaces with no authentication. Only use 127.0.0.1 unless you have added authentication and understand the security implications.`,
		);
		process.exit(1);
	}

	// Start server — PID file written after this succeeds so it only exists
	// when a port is truly bound.
	let server: ReturnType<typeof serve>;
	try {
		server = serve({
			fetch: app.fetch,
			port,
			hostname: bindHost,
			idleTimeout: 255, // max allowed by Bun — consolidation can take a while
		});
	} catch (e) {
		if (errCode(e) === "EADDRINUSE") {
			logger.error(
				`Port ${port} is already in use. Is another process running on that port?`,
			);
		} else {
			logger.error(
				`Failed to start HTTP server: ${e instanceof Error ? e.message : String(e)}`,
			);
		}
		process.exit(1);
	}

	let daemonChild: ReturnType<typeof Bun.spawn> | null = null;

	if (config.pidPath) {
		writeFileSync(config.pidPath, String(process.pid), {
			encoding: "utf8",
			mode: 0o600,
		});
	}
	releaseOnFatal = () => {
		daemonChild?.kill();
		removeOwnPidFile(config.pidPath);
	};

	logger.raw(`\n✓ HTTP API listening on http://${host}:${port}`);
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
	if (process.stdout.isTTY) {
		logger.rawStdoutOnly(`\n  Admin token (keep this private): ${adminToken}`);
	} else {
		// Non-terminal stdout is usually a log file (launchd/systemd/nohup redirect),
		// so the token is never printed there.
		logger.raw(
			adminTokenIsStable
				? "\n  Admin token: from KNOWLEDGE_ADMIN_TOKEN (not printed: stdout is not a terminal)."
				: "\n  Admin token: random per process (not printed: stdout is not a terminal). Set KNOWLEDGE_ADMIN_TOKEN for a stable token.",
		);
	}
	logger.rawStdoutOnly(
		`  curl -X POST -H "Authorization: Bearer <token>" http://${host}:${port}/consolidate`,
	);
	if (config.logPath) {
		logger.raw(`\n  Logs: ${config.logPath}`);
	}

	// ── Daemon auto-spawn ────────────────────────────────────────────────────
	//
	// Automatically start knowledge-daemon as a child process so users don't
	// need to manage two separate services. The daemon reads local AI tool
	// session files and uploads episodes to pending_episodes; the server
	// consolidates from there.
	//
	// The server doesn't try to second-guess whether there are local session
	// files — the daemon handles that itself (warns and polls quietly when
	// nothing is found). The only question is whether the daemon binary exists.
	//
	// Disabled when:
	//   - DAEMON_AUTO_SPAWN=false — user manages the daemon themselves
	//     (e.g. via launchd/systemd through `knowledge-server setup-tool daemon`)
	//   - Running in dev mode (bun run src/index.ts) — no compiled binary present
	//   - The daemon binary can't be found next to the server binary
	//
	// The daemon child inherits the server's environment so it picks up the
	// same KNOWLEDGE_* env vars. Its stdout/stderr are forwarded to the server log.

	// One daemon per data directory. A service-managed (launchd/systemd) or manually
	// started daemon already holding daemon.lock means the server must not add a second.
	const runningDaemon = lockHolder(daemonLockPath(dataDir));
	if (runningDaemon) {
		logger.log(
			`[daemon] Auto-spawn skipped — a knowledge-daemon${runningDaemon.pid > 0 ? ` (PID ${runningDaemon.pid})` : ""} already runs for ${dataDir}. Not starting a second one.`,
		);
	}

	if (daemonAutoSpawn && !runningDaemon) {
		// Resolve the daemon command to spawn.
		//
		// Compiled install:   process.execPath = .../libexec/knowledge-server
		//                     basename starts with "knowledge-server"
		//                     cmd = [".../libexec/knowledge-daemon", ...interval args]
		//
		// Source install:     process.execPath = .../bun (basename starts with "bun")
		//                     process.argv[1]  = .../repo/src/index.ts
		//                     cmd = [".../bun", "run", ".../repo/src/daemon/index.ts", ...interval args]
		//
		// Uses basename(process.execPath).startsWith("bun") — same convention as
		// resolveEnvFilePath() and setup-tool.ts for source-vs-compiled detection.
		const isSourceRun = basename(process.execPath).startsWith("bun");
		const daemonBinPath = join(dirname(process.execPath), "knowledge-daemon");

		// For compiled installs where the daemon binary is missing (e.g. upgrading
		// from v2 which only downloaded knowledge-server), try to install it
		// automatically using the same version as the running server.
		if (!isSourceRun && !existsSync(daemonBinPath)) {
			logger.log(
				`[daemon] knowledge-daemon not found at ${daemonBinPath} — downloading v${pkg.version}...`,
			);
			try {
				await downloadAndInstallDaemon(`v${pkg.version}`, daemonBinPath);
				logger.log("[daemon] knowledge-daemon installed successfully.");
			} catch (err) {
				logger.warn(
					`[daemon] Auto-install failed: ${err instanceof Error ? err.message : String(err)}. Run \`knowledge-server update\` to install it manually, or set DAEMON_AUTO_SPAWN=false to suppress this.`,
				);
			}
		}

		// Interval arguments for the spawned daemon (--interval=300 unless
		// DAEMON_UPLOAD_INTERVAL_SECONDS is set); see spawnedDaemonIntervalArgs.
		const spawnedIntervalArgs = spawnedDaemonIntervalArgs(
			config.daemon.uploadIntervalSeconds,
		);
		const daemonCmd: string[] = isSourceRun
			? process.argv[1]
				? [
						process.execPath,
						"run",
						join(dirname(process.argv[1]), "daemon", "index.ts"),
						...spawnedIntervalArgs,
					]
				: [] // argv[1] missing — can't locate daemon entry point
			: existsSync(daemonBinPath)
				? [daemonBinPath, ...spawnedIntervalArgs]
				: [];

		if (daemonCmd.length === 0) {
			logger.warn(
				`[daemon] Auto-spawn skipped — daemon binary not found at ${daemonBinPath}. Run \`knowledge-server update\` to install it, or set DAEMON_AUTO_SPAWN=false to suppress this warning.`,
			);
		} else {
			try {
				daemonChild = Bun.spawn(daemonCmd, {
					stdout: "pipe",
					stderr: "pipe",
					// The daemon's lines are forwarded into server.log below; it must not
					// also write its own daemon.log.
					env: { ...process.env, KNOWLEDGE_DAEMON_LOG_PATH: "" },
				});
				// Forward daemon output to the server log.
				// TextDecoder is stateful: using { stream: true } preserves carry-over
				// state so multi-byte UTF-8 sequences split across chunk boundaries
				// are decoded correctly rather than replaced with \uFFFD.
				// Forward daemon output to the server log without double-stamping.
				// Daemon lines are already structured (have their own timestamp + level).
				const forwardStream = async (
					stream: ReadableStream<Uint8Array> | null,
				) => {
					if (!stream) return;
					const decoder = new TextDecoder();
					const emit = (text: string) => {
						for (const line of text.split("\n")) {
							const trimmed = line.trimEnd();
							if (trimmed) logger.passthrough(trimmed);
						}
					};
					for await (const chunk of stream) {
						emit(decoder.decode(chunk, { stream: true }));
					}
					// Flush decoder carry-over buffer so multi-byte sequences at the
					// end of a chunk boundary are not silently dropped on stream close.
					emit(decoder.decode());
				};
				forwardStream(
					daemonChild.stdout as ReadableStream<Uint8Array> | null,
				).catch((e) => logger.warn("[daemon] stdout forward error:", e));
				forwardStream(
					daemonChild.stderr as ReadableStream<Uint8Array> | null,
				).catch((e) => logger.warn("[daemon] stderr forward error:", e));
				// Observe daemon exit so unexpected crashes are surfaced in the server log.
				daemonChild.exited
					.then((code) => {
						if (code !== 0 && !shutdownRequested) {
							logger.warn(
								`[daemon] Exited unexpectedly with code ${code}. Restart the server or run knowledge-daemon manually.`,
							);
						}
					})
					.catch((e) => logger.warn("[daemon] exit watch error:", e));
				logger.log(
					`[daemon] Auto-spawned (PID ${daemonChild.pid}). Set DAEMON_AUTO_SPAWN=false to manage the daemon yourself.`,
				);
			} catch (err) {
				logger.warn(
					`[daemon] Auto-spawn failed: ${err instanceof Error ? err.message : String(err)}. Start knowledge-daemon manually or run \`knowledge-server setup-tool daemon\`.`,
				);
			}
		}
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
	//   Decay loop    — strength decay and archiving, every DECAY_INTERVAL_MS (default 24 h),
	//                   whether or not sessions are pending.
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
		const attempted = new Set<string>();
		let batch = 1;
		let consecutiveErrors = 0;
		const MAX_CONSECUTIVE_ERRORS = 3;
		const BASE_RETRY_DELAY_MS = 5_000;

		while (!shutdownRequested) {
			try {
				// Nothing pending: finish without an empty batch (which would still scan
				// every store to ensure embeddings).
				if ((await consolidation.checkPending()).pendingSessions === 0) {
					logger.log(`[${label}] Complete — all sessions processed.`);
					break;
				}
				if (!consolidation.tryLock()) {
					// Another path holds the lock — yield and retry.
					await new Promise((resolve) => setTimeout(resolve, 2000));
					continue;
				}
				logger.log(`[${label}] Batch ${batch}...`);
				let result: Awaited<ReturnType<typeof consolidation.consolidate>>;
				try {
					result = await consolidation.consolidate({ attempted });
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

		// Run KB synthesis once after all batches complete (unconditional — synthesis
		// short-circuits cheaply in runKBSynthesis when no clusters are ripe).
		// Runs even if no new sessions were processed this drain: cluster membership
		// can change from a previous drain, and synthesis should catch up regardless.
		if (!shutdownRequested) {
			logger.log(`[${label}] Running KB synthesis pass...`);
			try {
				if (consolidation.tryLock()) {
					try {
						await consolidation.runSynthesis();
					} finally {
						consolidation.unlock();
					}
				} else {
					logger.error(
						`[${label}] Could not acquire lock for synthesis — skipping.`,
					);
				}
			} catch (err) {
				logger.error(`[${label}] KB synthesis failed:`, err);
			}
		}
	}

	// Startup drain
	if (pending.pendingSessions > 0) {
		activeLoops.push(runConsolidationDrain("startup consolidation"));
	}

	// Interruptible sleep: checks shutdownRequested every second rather than
	// sleeping for the full interval. This lets shutdown complete immediately
	// instead of waiting up to an interval (e.g. 8h) for the setTimeout.
	const interruptibleSleep = async (ms: number) => {
		const end = Date.now() + ms;
		while (!shutdownRequested) {
			const remaining = end - Date.now();
			if (remaining <= 0) break;
			await new Promise((resolve) =>
				setTimeout(resolve, Math.min(1000, remaining)),
			);
		}
	};

	// Decay schedule — independent of pending work, so idle installs still archive.
	// The first run waits a minute so startup consolidation is not competing with it.
	const DECAY_FIRST_RUN_DELAY_MS = 60_000;
	const DECAY_LOCK_RETRY_MS = 5 * 60_000;
	const decayIntervalMs = config.decay.intervalMs;
	if (decayIntervalMs > 0) {
		activeLoops.push(
			(async () => {
				await interruptibleSleep(DECAY_FIRST_RUN_DELAY_MS);
				while (!shutdownRequested) {
					const tick = await runDecayTick(consolidation);
					await interruptibleSleep(
						tick.status === "busy" ? DECAY_LOCK_RETRY_MS : decayIntervalMs,
					);
				}
			})(),
		);
	}

	// Polling loop — default 8h, skips silently when no sessions are pending.
	const pollIntervalMs = config.consolidation.pollIntervalMs;
	if (pollIntervalMs > 0) {
		const intervalLabel =
			pollIntervalMs >= 3_600_000
				? `${pollIntervalMs / 3_600_000}h`
				: `${pollIntervalMs / 60_000}m`;
		logger.log(
			`✓ Auto-consolidation polling enabled (every ${intervalLabel}). Set CONSOLIDATION_POLL_INTERVAL_MS=0 to disable.`,
		);

		activeLoops.push(
			(async () => {
				while (!shutdownRequested) {
					await interruptibleSleep(pollIntervalMs);
					if (shutdownRequested) break;
					await runPollTick(consolidation, runConsolidationDrain);
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
		// Only log a waiting message when consolidation is actually running —
		// the polling loop is always in activeLoops but is idle most of the time.
		if (consolidation.isConsolidating) {
			logger.log("[shutdown] Waiting for in-flight consolidation to finish...");
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
		// Terminate auto-spawned daemon child if running and wait briefly for
		// it to exit cleanly before closing the DB (avoids concurrent SQLite access).
		if (daemonChild) {
			try {
				daemonChild.kill();
				await Promise.race([
					daemonChild.exited,
					new Promise<void>((r) => setTimeout(r, 2000)),
				]);
				logger.log("[daemon] Auto-spawned daemon terminated.");
			} catch {
				// Non-fatal — process may have already exited.
			}
		}
		consolidation.close();
		await activationEvents.stop();
		await registry.close();
		removeOwnPidFile(config.pidPath);
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
	releaseOnFatal?.();
	process.exit(1);
});

// Any rejection that escapes its caller would otherwise end the process without
// cleanup (Bun exits 1 with no PID or daemon teardown). Log it and release first.
process.on("unhandledRejection", (reason) => {
	logger.error("[fatal] Unhandled promise rejection:", reason);
	releaseOnFatal?.();
	process.exit(1);
});
