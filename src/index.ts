import { serve } from "bun";
import { randomBytes } from "node:crypto";
import { KnowledgeDB } from "./db/database.js";
import { ActivationEngine } from "./activation/activate.js";
import { ConsolidationEngine } from "./consolidation/consolidate.js";
import { createApp } from "./api/server.js";
import { config, validateConfig } from "./config.js";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../package.json" with { type: "json" };

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
  console.log("┌─────────────────────────────────────┐");
  console.log(`│  Knowledge Server v${pkg.version.padEnd(17)}│`);
  console.log("│  Consolidation-aware knowledge       │");
  console.log("│  system for OpenCode agents          │");
  console.log("└─────────────────────────────────────┘");

  // Validate config
  const errors = validateConfig();
  if (errors.length > 0) {
    console.error("\nConfiguration errors:");
    for (const err of errors) {
      console.error(`  ✗ ${err}`);
    }
    process.exit(1);
  }

  // Initialize components
  const db = new KnowledgeDB();
  const activation = new ActivationEngine(db);
  const consolidation = new ConsolidationEngine(db, activation);

  // Check if this is a first run (no knowledge yet, but episodes exist)
  const stats = db.getStats();
  const consolidationState = db.getConsolidationState();

  console.log(`\nKnowledge graph: ${stats.total || 0} entries (${stats.active || 0} active)`);
  console.log(
    `Last consolidation: ${consolidationState.lastConsolidatedAt ? new Date(consolidationState.lastConsolidatedAt).toISOString() : "never"}`
  );

  // Check for pending sessions
  const pending = consolidation.checkPending();
  if (pending.pendingSessions > 0) {
    console.log(
      `\n⚡ ${pending.pendingSessions} sessions pending consolidation` +
      ` (${config.consolidation.maxSessionsPerRun} per batch).`
    );
    console.log("  Starting background consolidation...");
  } else {
    console.log("\n✓ Knowledge graph is up to date.");
  }

  // Admin token: use KNOWLEDGE_ADMIN_TOKEN env var if set (stable, useful for scripting),
  // otherwise generate a random token per process lifetime (more secure for interactive use).
  const adminToken = config.adminToken ?? randomBytes(24).toString("hex");

  // Create HTTP app
  const app = createApp(db, activation, consolidation, adminToken);

  // Start server
  const server = serve({
    fetch: app.fetch,
    port: config.port,
    hostname: config.host,
    idleTimeout: 255, // max allowed by Bun — consolidation can take a while
  });

  console.log(`\n✓ HTTP API listening on http://${config.host}:${config.port}`);
  console.log("  GET  /activate?q=...  — Activate knowledge");
  console.log("  POST /consolidate     — Run consolidation   [admin token required]");
  console.log("  GET  /review          — Review entries");
  console.log("  GET  /status          — Health check");
  console.log("  GET  /entries         — List entries");
  console.log(`\n  Admin token (keep this private): ${adminToken}`);
  console.log(`  curl -X POST -H "Authorization: Bearer <token>" http://${config.host}:${config.port}/consolidate`);

  // Background consolidation loop — runs after server is listening
  // so the server is available immediately while consolidation proceeds.
  // Promise is stored so SIGTERM can await it before closing the DB.
  let shutdownRequested = false;
  let startupLoopDone: Promise<void> = Promise.resolve();

  if (pending.pendingSessions > 0) {
    startupLoopDone = (async () => {
      let batch = 1;
      let consecutiveErrors = 0;
      const MAX_CONSECUTIVE_ERRORS = 3;
      const BASE_RETRY_DELAY_MS = 5_000;

      while (!shutdownRequested) {
        try {
          // Claim the flag synchronously (no await between check and set) so an
          // API call cannot slip in between and start a concurrent consolidation.
          if (!consolidation.tryLock()) {
            // Another path (API) already holds the lock — yield and retry.
            // tryLock() is synchronous so no await between check and claim.
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
          console.log(`\n[startup consolidation] Batch ${batch}...`);
          let result: Awaited<ReturnType<typeof consolidation.consolidate>>;
          try {
            result = await consolidation.consolidate();
          } finally {
            consolidation.unlock();
          }
          consecutiveErrors = 0; // reset on success
          if (result.sessionsProcessed === 0) {
            console.log("[startup consolidation] Complete — all sessions processed.");
            break;
          }
          console.log(
            `[startup consolidation] Batch ${batch} done: ${result.sessionsProcessed} sessions, ` +
            `${result.entriesCreated} created, ${result.entriesUpdated} updated.`
          );
          batch++;
        } catch (err) {
          consecutiveErrors++;
          if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            console.error(
              `[startup consolidation] ${MAX_CONSECUTIVE_ERRORS} consecutive errors — giving up. Last error:`,
              err
            );
            break;
          }
          const delay = BASE_RETRY_DELAY_MS * (2 ** (consecutiveErrors - 1));
          console.error(
            `[startup consolidation] Error (attempt ${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}), ` +
            `retrying in ${delay / 1000}s:`,
            err
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    })();
  }

  // Graceful shutdown — signal the loop to stop, wait up to 30s for it to finish
  // the current batch before closing the DB. Prevents losing in-flight LLM results.
  async function shutdown(signal: string) {
    console.log(`\n[${signal}] Shutting down gracefully...`);
    shutdownRequested = true;
    const TIMED_OUT = Symbol("timed_out");
    const result = await Promise.race([
      startupLoopDone.then(() => null),
      new Promise<typeof TIMED_OUT>((r) => setTimeout(() => r(TIMED_OUT), 30_000)),
    ]);
    if (result === TIMED_OUT) {
      console.warn("[shutdown] 30s timeout reached — in-flight consolidation batch abandoned.");
    }
    consolidation.close();
    db.close();
    process.exit(0);
  }

  process.on("SIGINT", () => { shutdown("SIGINT").catch(console.error); });
  process.on("SIGTERM", () => { shutdown("SIGTERM").catch(console.error); });
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
