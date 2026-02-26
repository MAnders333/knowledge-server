import { Hono } from "hono";
import type { Context } from "hono";
import { timingSafeEqual } from "node:crypto";
import type { KnowledgeDB } from "../db/database.js";
import type { ActivationEngine } from "../activation/activate.js";
import type { ConsolidationEngine } from "../consolidation/consolidate.js";
import { config } from "../config.js";
// @ts-ignore — Bun supports JSON imports natively; tsc may warn without resolveJsonModule
import pkg from "../../package.json" with { type: "json" };

/**
 * HTTP API for the knowledge server.
 *
 * Endpoints:
 * - GET  /activate?q=...     -- Activate knowledge entries by query (used by plugin)
 * - POST /consolidate         -- Run consolidation cycle          [requires admin token]
 * - POST /reinitialize        -- Wipe knowledge DB and reset cursor [requires admin token]
 * - GET  /review              -- List entries needing attention
 * - GET  /status              -- Server health and stats
 * - GET  /entries             -- List all entries (with filters)
 * - GET  /entries/:id         -- Get a specific entry
 *
 * Admin token:
 * A random token is generated at startup and printed to the console once.
 * Pass it as `Authorization: Bearer <token>` on protected endpoints.
 * This guards against CSRF and other local-process abuse of destructive operations.
 */
export function createApp(
  db: KnowledgeDB,
  activation: ActivationEngine,
  consolidation: ConsolidationEngine,
  adminToken: string
): Hono {
  const app = new Hono();

  // -- Auth helper --

  // Pre-encode the expected token once so timingSafeEqual can compare buffers.
  // "Bearer " + 48-char hex = a public constant length, so the early length
  // check leaks nothing meaningful while keeping the comparison simple.
  const expectedToken = Buffer.from(`Bearer ${adminToken}`);

  function requireAdminToken(c: Context): boolean {
    const auth = c.req.header("Authorization") ?? "";
    const provided = Buffer.from(auth);
    if (provided.length !== expectedToken.length) return false;
    return timingSafeEqual(provided, expectedToken);
  }

  // -- Activation --

  app.get("/activate", async (c) => {
    const query = c.req.query("q");
    if (!query) {
      return c.json({ error: "Missing query parameter 'q'" }, 400);
    }

    try {
      const result = await activation.activate(query);
      return c.json(result);
    } catch (e) {
      console.error("[activate] Error:", e);
      return c.json({ error: "Internal server error" }, 500);
    }
  });

  // -- Consolidation --

  app.post("/consolidate", async (c) => {
    if (!requireAdminToken(c)) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    if (!consolidation.tryLock()) {
      return c.json({ error: "Consolidation already in progress" }, 409);
    }

    try {
      const result = await consolidation.consolidate();
      return c.json(result);
    } catch (e) {
      console.error("[consolidate] Error:", e);
      return c.json({ error: "Internal server error" }, 500);
    } finally {
      consolidation.unlock();
    }
  });

  // -- Re-initialization --

  app.post("/reinitialize", async (c) => {
    if (!requireAdminToken(c)) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    try {
      const confirm = c.req.query("confirm");
      if (confirm !== "yes") {
        return c.json(
          {
            error:
              "This will DELETE all knowledge entries and reset the consolidation cursor. Add ?confirm=yes to proceed.",
          },
          400
        );
      }

      db.reinitialize();

      console.log("[reinitialize] Knowledge DB wiped and cursor reset.");
      return c.json({
        status: "reinitialized",
        message:
          "All knowledge entries deleted and consolidation cursor reset to 0. Run POST /consolidate to rebuild.",
      });
    } catch (e) {
      console.error("[reinitialize] Error:", e);
      return c.json({ error: "Internal server error" }, 500);
    }
  });

  // -- Review --

  app.get("/review", (c) => {
    const conflicted = db.getEntriesByStatus("conflicted");
    const active = db.getActiveEntries();

    // Find stale entries (active but low strength)
    const stale = active
      .filter((e) => e.strength < 0.3)
      .sort((a, b) => a.strength - b.strength);

    // Find team-relevant entries that might need external documentation
    const teamRelevant = active.filter(
      (e) => e.scope === "team" && e.confidence >= 0.7
    );

    return c.json({
      conflicted: conflicted.map(stripEmbedding),
      stale: stale.map(stripEmbedding),
      teamRelevant: teamRelevant.map(stripEmbedding),
      summary: {
        conflictedCount: conflicted.length,
        staleCount: stale.length,
        teamRelevantCount: teamRelevant.length,
      },
    });
  });

  // -- Status --

  app.get("/status", (c) => {
    const stats = db.getStats();
    const consolidationState = db.getConsolidationState();

    return c.json({
      status: "ok",
      version: pkg.version,
      knowledge: stats,
      consolidation: {
        lastRun: consolidationState.lastConsolidatedAt
          ? new Date(consolidationState.lastConsolidatedAt).toISOString()
          : null,
        totalSessionsProcessed:
          consolidationState.totalSessionsProcessed,
        totalEntriesCreated: consolidationState.totalEntriesCreated,
      },
      config: {
        port: config.port,
        embeddingModel: config.embedding.model,
        extractionModel: config.llm.extractionModel,
        mergeModel: config.llm.mergeModel,
        contradictionModel: config.llm.contradictionModel,
      },
    });
  });

  // -- Entries CRUD --

  app.get("/entries", (c) => {
    const status = c.req.query("status") || undefined;
    const type = c.req.query("type") || undefined;
    const scope = c.req.query("scope") || undefined;

    // Filtering is pushed to SQL — no full-table load + JS filter
    const entries = db.getEntries({ status, type, scope });

    return c.json({
      entries: entries.map(stripEmbedding),
      count: entries.length,
    });
  });

  app.get("/entries/:id", (c) => {
    const entry = db.getEntry(c.req.param("id"));
    if (!entry) {
      return c.json({ error: "Entry not found" }, 404);
    }

    const relations = db.getRelationsFor(entry.id);
    return c.json({
      entry: stripEmbedding(entry),
      relations,
    });
  });

  return app;
}

/**
 * Strip the embedding vector from entries before sending over API.
 * Embeddings are large (3072 floats) and not useful to consumers.
 */
function stripEmbedding(entry: { embedding?: number[]; [key: string]: unknown }) {
  const { embedding: _embedding, ...rest } = entry;
  return rest;
}
