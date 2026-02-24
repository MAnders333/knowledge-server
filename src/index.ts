import { serve } from "bun";
import { randomBytes } from "node:crypto";
import { KnowledgeDB } from "./db/database.js";
import { ActivationEngine } from "./activation/activate.js";
import { ConsolidationEngine } from "./consolidation/consolidate.js";
import { createApp } from "./api/server.js";
import { config, validateConfig } from "./config.js";

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
  console.log("│  Knowledge Server v0.1.0            │");
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

  // Generate a random admin token for this process lifetime.
  // Required on POST /consolidate and POST /reinitialize.
  // Printed once to the console — store it if you need to call those endpoints manually.
  const adminToken = randomBytes(24).toString("hex");

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
  console.log(`\n  Admin token: ${adminToken}`);
  console.log("  Usage: curl -X POST -H \"Authorization: Bearer <token>\" http://127.0.0.1:3179/consolidate");

  // Background consolidation loop — runs after server is listening
  // so the server is available immediately while consolidation proceeds.
  if (pending.pendingSessions > 0) {
    (async () => {
      let batch = 1;
      let consecutiveErrors = 0;
      const MAX_CONSECUTIVE_ERRORS = 3;
      const BASE_RETRY_DELAY_MS = 5_000;

      while (true) {
        try {
          console.log(`\n[startup consolidation] Batch ${batch}...`);
          const result = await consolidation.consolidate();
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

  // Graceful shutdown
  process.on("SIGINT", () => {
    console.log("\nShutting down...");
    consolidation.close();
    db.close();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    consolidation.close();
    db.close();
    process.exit(0);
  });
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
