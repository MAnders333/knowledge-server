import { Hono } from "hono";
import type { KnowledgeDB } from "../db/database.js";
import type { ActivationEngine } from "../activation/activate.js";
import type { ConsolidationEngine } from "../consolidation/consolidate.js";
import { config } from "../config.js";

/**
 * HTTP API for the knowledge server.
 *
 * Endpoints:
 * - GET  /activate?q=...     -- Activate knowledge entries by query (used by plugin)
 * - POST /consolidate         -- Run consolidation cycle
 * - POST /reinitialize        -- Wipe knowledge DB and reset consolidation cursor
 * - GET  /review              -- List entries needing attention
 * - GET  /status              -- Server health and stats
 * - GET  /entries             -- List all entries (with filters)
 * - GET  /entries/:id         -- Get a specific entry
 */
export function createApp(
  db: KnowledgeDB,
  activation: ActivationEngine,
  consolidation: ConsolidationEngine
): Hono {
  const app = new Hono();

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
      return c.json({ error: String(e) }, 500);
    }
  });

  // -- Consolidation --

  app.post("/consolidate", async (c) => {
    try {
      const result = await consolidation.consolidate();
      return c.json(result);
    } catch (e) {
      console.error("[consolidate] Error:", e);
      return c.json({ error: String(e) }, 500);
    }
  });

  // -- Re-initialization --

  app.post("/reinitialize", async (c) => {
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
      return c.json({ error: String(e) }, 500);
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
      version: "0.2.0",
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
        consolidationModel: config.llm.model,
      },
    });
  });

  // -- Entries CRUD --

  app.get("/entries", (c) => {
    const status = c.req.query("status");
    const type = c.req.query("type");
    const scope = c.req.query("scope");

    let entries = db.getAllEntries();

    if (status) {
      entries = entries.filter((e) => e.status === status);
    }
    if (type) {
      entries = entries.filter((e) => e.type === type);
    }
    if (scope) {
      entries = entries.filter((e) => e.scope === scope);
    }

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
  const { embedding, ...rest } = entry;
  return rest;
}
