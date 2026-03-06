import { timingSafeEqual } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { Hono } from "hono";
import type { Context } from "hono";
import { bodyLimit } from "hono/body-limit";
// @ts-ignore — Bun supports JSON imports natively; tsc may warn without resolveJsonModule
import pkg from "../../package.json" with { type: "json" };
import type { ActivationEngine } from "../activation/activate.js";
import { contradictionTagBlock, contradictionTagInline, staleTag } from "../activation/format.js";
import { config } from "../config.js";
import type { ConsolidationEngine } from "../consolidation/consolidate.js";
import type { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";
import { activateInputSchema } from "../mcp/index.js";
import type { KnowledgeEntry, KnowledgeStatus } from "../types.js";

/**
 * HTTP API for the knowledge server.
 *
 * Endpoints:
 * - GET  /activate?q=...                   -- Activate knowledge entries by query (used by plugin)
 * - POST /consolidate                       -- Run consolidation cycle          [requires admin token]
 * - POST /reinitialize                      -- Wipe knowledge DB and reset cursor [requires admin token]
 * - GET  /review                            -- List entries needing attention
 * - GET  /status                            -- Server health and stats
 * - GET  /entries                           -- List all entries (with filters)
 * - GET  /entries/:id                       -- Get a specific entry
 * - PATCH /entries/:id                      -- Update fields on an entry       [requires admin token]
 * - POST /entries/:id/resolve               -- Resolve a conflicted entry pair [requires admin token]
 * - DELETE /entries/:id                     -- Hard-delete an entry            [requires admin token]
 * - POST /hooks/claude-code/user-prompt     -- Claude Code UserPromptSubmit hook (unauthenticated)
 * - ALL  /mcp                               -- MCP streamable-http endpoint (auth optional, see below)
 *
 * Admin token:
 * A random token is generated at startup and printed to the console once.
 * Pass it as `Authorization: Bearer <token>` on protected endpoints.
 * This guards against CSRF and other local-process abuse of destructive operations.
 *
 * /mcp auth:
 * When KNOWLEDGE_ADMIN_TOKEN is set, the /mcp endpoint requires the same Bearer
 * token. When unset (random token per process), /mcp is unauthenticated — suitable
 * for local use where the server is only accessible on 127.0.0.1. For hosted/shared
 * deployments, always set KNOWLEDGE_ADMIN_TOKEN so remote MCP clients must authenticate.
 */
export function createApp(
	db: KnowledgeDB,
	activation: ActivationEngine,
	consolidation: ConsolidationEngine,
	adminToken: string,
	/** Whether adminToken was explicitly configured (vs randomly generated for local use). */
	adminTokenIsStable = false,
): Hono {
	const app = new Hono();

	// -- /mcp streamable-http transport --
	//
	// One stateless transport instance shared across all requests. Stateless mode
	// is appropriate here because every `activate` call is independent — there is
	// no conversation state to maintain between MCP requests.
	//
	// For hosted deployments: set KNOWLEDGE_ADMIN_TOKEN and point MCP clients at
	// https://your-server.com/mcp with `Authorization: Bearer <token>`.
	// For local use: no token required (server binds to 127.0.0.1 only).
	const mcpTransport = new WebStandardStreamableHTTPServerTransport({
		sessionIdGenerator: undefined, // stateless
	});

	const mcpServer = new McpServer({
		name: "knowledge-server",
		version: pkg.version,
	});

	mcpServer.tool(
		"activate",
		"Activate associated knowledge by providing cues. Returns knowledge entries that are semantically related to the provided cues. Use this when you need to recall what has been learned from prior sessions about a specific topic. Provide descriptive cues — topics, questions, or keywords — and receive relevant knowledge entries ranked by association strength.",
		activateInputSchema,
		async ({ cues, limit, threshold }) => {
			try {
				const result = await activation.activate(
					cues,
					{
						limit: limit ?? undefined,
						threshold: threshold ?? undefined,
					},
				);

				if (result.entries.length === 0) {
					return {
						content: [{ type: "text" as const, text: "No relevant knowledge found for these cues." }],
					};
				}

				const formatted = result.entries
					.map(
						(r, i) =>
							`${i + 1}. [${r.entry.type}] ${r.entry.content}${staleTag(r.staleness)}${contradictionTagBlock(r.contradiction)}\n` +
							`   Topics: ${r.entry.topics.join(", ")}\n` +
							`   Confidence: ${r.entry.confidence} | Scope: ${r.entry.scope} | Semantic match: ${r.rawSimilarity.toFixed(3)} | Score: ${r.similarity.toFixed(3)}`,
					)
					.join("\n\n");

				const conflictCount = result.entries.filter((r) => r.contradiction).length;
				const conflictNote = conflictCount > 0
					? ` — ${conflictCount} conflicted, do not act on those without clarifying which version is correct`
					: "";

				return {
					content: [{
						type: "text" as const,
						text: `## Activated Knowledge (${result.entries.length} entries, ${result.totalActive} total active${conflictNote})\n\n${formatted}`,
					}],
				};
			} catch (e) {
				logger.error("[mcp/activate] Error:", e);
				return {
					content: [{ type: "text" as const, text: `Error activating knowledge: ${e}` }],
					isError: true,
				};
			}
		},
	);

	// Connect McpServer to transport (async; safe to fire-and-forget here since
	// connect() only sets up event listeners and does not block).
	mcpServer.connect(mcpTransport).catch((e) => {
		logger.error("[mcp] Failed to connect MCP server to transport:", e);
	});

	// Route all methods on /mcp to the transport.
	// Auth: required only when a stable admin token is configured (hosted mode).
	// In local mode (random per-process token) /mcp is open — the server already
	// binds to 127.0.0.1 so network access is not a concern.
	app.all("/mcp", async (c) => {
		if (adminTokenIsStable && !requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}
		return mcpTransport.handleRequest(c.req.raw);
	});

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
		// Accept one or more `q` params (repeated: ?q=seg1&q=seg2&q=full).
		// Single ?q=... still works — queries() returns a one-element array.
		const queries = c.req.queries("q");
		if (!queries || queries.length === 0) {
			return c.json({ error: "Missing query parameter 'q'" }, 400);
		}

		// Optional overrides — callers (plugin, MCP) can tune per their needs.
		// Defaults come from config so the server admin controls the baseline.
		const limitParam = c.req.query("limit");
		const thresholdParam = c.req.query("threshold");

		const parsedLimit = limitParam
			? Number.parseInt(limitParam, 10)
			: Number.NaN;
		const limit = !Number.isNaN(parsedLimit)
			? Math.max(1, Math.min(50, parsedLimit))
			: undefined;

		const parsedThreshold = thresholdParam
			? Number.parseFloat(thresholdParam)
			: Number.NaN;
		const threshold = !Number.isNaN(parsedThreshold)
			? Math.max(0, Math.min(1, parsedThreshold))
			: undefined;

		try {
			const result = await activation.activate(
				queries.length === 1 ? queries[0] : queries,
				{ limit, threshold },
			);
			return c.json(result);
		} catch (e) {
			logger.error("[activate] Error:", e);
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
			logger.error("[consolidate] Error:", e);
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
					400,
				);
			}

			db.reinitialize();

			logger.log("[reinitialize] Knowledge DB wiped and cursor reset.");
			return c.json({
				status: "reinitialized",
				message:
					"All knowledge entries deleted and consolidation cursor reset to 0. Run POST /consolidate to rebuild.",
			});
		} catch (e) {
			logger.error("[reinitialize] Error:", e);
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
			(e) => e.scope === "team" && e.confidence >= 0.7,
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

		// Per-source cursor info — surfaced so operators can see each source's
		// last-consolidated timestamp and high-water mark without tailing logs.
		const sourceCursors = ["opencode", "claude-code"].map((source) => {
			const cursor = db.getSourceCursor(source);
			return {
				source,
				lastConsolidatedAt: cursor.lastConsolidatedAt
					? new Date(cursor.lastConsolidatedAt).toISOString()
					: null,
			};
		});

		// Config block (model names, port) is gated behind the admin token.
		// Unauthenticated callers (e.g. healthcheck scripts) still get version +
		// knowledge stats, but don't learn which models / endpoint are in use.
		// Intentional: non-blocking — unauthenticated callers still receive a 200
		// with partial data; the config block is simply omitted.
		const isAdmin = requireAdminToken(c);

		return c.json({
			status: "ok",
			version: pkg.version,
			knowledge: stats,
			consolidation: {
				lastRun: consolidationState.lastConsolidatedAt
					? new Date(consolidationState.lastConsolidatedAt).toISOString()
					: null,
				totalSessionsProcessed: consolidationState.totalSessionsProcessed,
				totalEntriesCreated: consolidationState.totalEntriesCreated,
				sources: sourceCursors,
			},
			...(isAdmin && {
				config: {
					port: config.port,
					embeddingModel: config.embedding.model,
					extractionModel: config.llm.extractionModel,
					mergeModel: config.llm.mergeModel,
					contradictionModel: config.llm.contradictionModel,
				},
			}),
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

	// PATCH /entries/:id — update mutable fields on any entry.
	// Useful for human review: correcting content, changing scope/type, marking stale entries active, etc.
	// Accepts any subset of: content, topics, confidence, status, scope.
	// Embedding is NOT re-computed — callers that change content should be aware similarity
	// searches will use the old embedding until the next reconsolidation pass touches the entry.
	app.patch("/entries/:id", async (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const entry = db.getEntry(c.req.param("id"));
		if (!entry) {
			return c.json({ error: "Entry not found" }, 404);
		}

		let body: Record<string, unknown>;
		try {
			body = await c.req.json();
		} catch {
			return c.json({ error: "Invalid JSON body" }, 400);
		}

		const allowed = [
			"content",
			"topics",
			"confidence",
			"status",
			"scope",
		] as const;
		type AllowedField = (typeof allowed)[number];
		const updates: Partial<KnowledgeEntry> = {};

		for (const field of allowed) {
			if (field in body) {
				(updates as Record<AllowedField, unknown>)[field] = body[field];
			}
		}

		if (Object.keys(updates).length === 0) {
			return c.json(
				{
					error: `No updatable fields provided. Allowed: ${allowed.join(", ")}`,
				},
				400,
			);
		}

		// Validate all fields before touching the DB.
		if (updates.content !== undefined) {
			if (typeof updates.content !== "string" || !updates.content.trim()) {
				return c.json({ error: "content must be a non-empty string" }, 400);
			}
		}
		if (updates.topics !== undefined) {
			if (
				!Array.isArray(updates.topics) ||
				!(updates.topics as unknown[]).every((t) => typeof t === "string")
			) {
				return c.json({ error: "topics must be an array of strings" }, 400);
			}
		}
		const validStatuses: KnowledgeStatus[] = [
			"active",
			"archived",
			"superseded",
			"conflicted",
			"tombstoned",
		];
		if (
			updates.status !== undefined &&
			!validStatuses.includes(updates.status)
		) {
			return c.json(
				{
					error: `Invalid status. Must be one of: ${validStatuses.join(", ")}`,
				},
				400,
			);
		}
		if (
			updates.scope !== undefined &&
			updates.scope !== "personal" &&
			updates.scope !== "team"
		) {
			return c.json(
				{ error: `Invalid scope. Must be 'personal' or 'team'` },
				400,
			);
		}
		if (updates.confidence !== undefined) {
			const c_ = updates.confidence as unknown;
			if (typeof c_ !== "number" || c_ < 0 || c_ > 1) {
				return c.json(
					{ error: "confidence must be a number between 0 and 1" },
					400,
				);
			}
		}

		db.updateEntry(entry.id, updates);
		const updated = db.getEntry(entry.id);
		return c.json({ entry: updated ? stripEmbedding(updated) : null });
	});

	// POST /entries/:id/resolve — resolve a conflicted entry pair via one of three outcomes:
	//   supersede_this  — the entry identified by :id is the loser; its conflict counterpart wins
	//   supersede_other — the entry identified by :id wins; its conflict counterpart is superseded
	//   merge           — replace :id's content with mergedContent; supersede the counterpart
	//   delete          — hard-delete this entry (useful for noise/junk that shouldn't be kept)
	//
	// The entry must have status='conflicted'. Its counterpart is looked up automatically
	// via the contradicts relation.
	app.post("/entries/:id/resolve", async (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const entry = db.getEntry(c.req.param("id"));
		if (!entry) {
			return c.json({ error: "Entry not found" }, 404);
		}
		if (entry.status !== "conflicted") {
			return c.json(
				{ error: `Entry is not conflicted (status: ${entry.status})` },
				400,
			);
		}

		let body: Record<string, unknown>;
		try {
			body = await c.req.json();
		} catch {
			return c.json({ error: "Invalid JSON body" }, 400);
		}

		const resolution = body.resolution as string;
		const validResolutions = [
			"supersede_this",
			"supersede_other",
			"merge",
			"delete",
		];
		if (!validResolutions.includes(resolution)) {
			return c.json(
				{
					error: `Invalid resolution. Must be one of: ${validResolutions.join(", ")}`,
				},
				400,
			);
		}

		// All resolutions need the counterpart. For 'delete', we also restore the counterpart
		// to 'active' before deleting — otherwise it stays 'conflicted' forever with no partner.
		const relations = db.getRelationsFor(entry.id);
		const conflictRelation = relations.find((r) => r.type === "contradicts");
		const counterpartId = conflictRelation
			? conflictRelation.sourceId === entry.id
				? conflictRelation.targetId
				: conflictRelation.sourceId
			: null;

		if (resolution === "delete") {
			// Restore the counterpart to active (deleteEntry cascades and removes the relation)
			if (counterpartId) {
				db.updateEntry(counterpartId, { status: "active" });
			}
			db.deleteEntry(entry.id);
			return c.json({
				ok: true,
				deleted: entry.id,
				restoredCounterpart: counterpartId,
			});
		}

		if (!counterpartId) {
			return c.json(
				{
					error:
						"No contradicts relation found — cannot locate conflict counterpart",
				},
				422,
			);
		}

		if (resolution === "merge") {
			const mergedContent = body.mergedContent as string | undefined;
			if (
				!mergedContent ||
				typeof mergedContent !== "string" ||
				!mergedContent.trim()
			) {
				return c.json(
					{ error: "mergedContent is required for merge resolution" },
					400,
				);
			}
			// In applyContradictionResolution: "merge" means newEntryId content gets mergedData,
			// existingEntryId is superseded. We treat :id as the winner (newEntryId).
			db.applyContradictionResolution("merge", entry.id, counterpartId, {
				content: mergedContent,
				type: entry.type,
				topics: entry.topics,
				confidence: entry.confidence,
			});
			return c.json({
				ok: true,
				resolution: "merge",
				winner: entry.id,
				superseded: counterpartId,
			});
		}

		if (resolution === "supersede_this") {
			// :id loses — counterpart wins
			// applyContradictionResolution("supersede_new", newEntryId=:id, existingEntryId=counterpart)
			// means existingEntryId (counterpart) wins, newEntryId (:id) is superseded
			db.applyContradictionResolution("supersede_new", entry.id, counterpartId);
			return c.json({
				ok: true,
				resolution: "supersede_this",
				winner: counterpartId,
				superseded: entry.id,
			});
		}

		// supersede_other — :id wins, counterpart loses
		// applyContradictionResolution("supersede_old", newEntryId=:id, existingEntryId=counterpart)
		// means newEntryId (:id) wins, existingEntryId (counterpart) is superseded
		db.applyContradictionResolution("supersede_old", entry.id, counterpartId);
		return c.json({
			ok: true,
			resolution: "supersede_other",
			winner: entry.id,
			superseded: counterpartId,
		});
	});

	// -- Claude Code hook --

	// POST /hooks/claude-code/user-prompt
	//
	// Called by Claude Code's UserPromptSubmit hook before each user prompt is sent
	// to the model. Activates relevant knowledge and returns it as additionalContext
	// so Claude Code injects it into the conversation automatically.
	//
	// This endpoint is intentionally unauthenticated (same as GET /activate) because:
	// - The server binds to 127.0.0.1 only — loopback is the security boundary
	// - Claude Code hooks run as the local user, not a remote caller
	// - Adding auth would require storing a token in ~/.claude/settings.json in plaintext
	//
	// On any error (bad body, activation failure, etc.) we return {} so Claude Code
	// continues normally without the context — hook errors are always non-blocking.
	//
	// Body size is capped at 1 MB — prompts larger than that are pathological and
	// would cause excessive memory pressure. The limit is generous enough that no
	// normal Claude Code prompt will ever hit it.
	//
	// Request body: { prompt: string, session_id?: string, cwd?: string }
	// Response:     { hookSpecificOutput: { hookEventName: "UserPromptSubmit", additionalContext: string } }
	//           or  {} on error (Claude Code ignores empty hookSpecificOutput gracefully)
	app.post(
		"/hooks/claude-code/user-prompt",
		bodyLimit({ maxSize: 1 * 1024 * 1024, onError: (c) => c.json({}) }),
		async (c) => {
			let prompt: string;
			try {
				const body = (await c.req.json()) as Record<string, unknown>;
				if (typeof body.prompt !== "string" || !body.prompt.trim()) {
					return c.json({});
				}
				prompt = body.prompt;
			} catch {
				return c.json({});
			}

			try {
				const result = await activation.activate(prompt, { limit: 8 });

				if (result.entries.length === 0) {
					return c.json({});
				}

				// Format activated entries using the same inline style as the passive plugin.
				const lines = result.entries.map((r) => {
					const stale = staleTag(r.staleness);
					const contradiction = contradictionTagInline(r.contradiction);
					return `- [${r.entry.type}] ${r.entry.content}${stale}${contradiction}`;
				});

				const additionalContext = [
					"Relevant knowledge from your knowledge base:",
					...lines,
				].join("\n");

				logger.log(
					`[hooks/claude-code] Activated ${result.entries.length} entries for prompt: "${prompt.slice(0, 60)}..."`,
				);

				return c.json({
					hookSpecificOutput: {
						hookEventName: "UserPromptSubmit",
						additionalContext,
					},
				});
			} catch (e) {
				logger.error("[hooks/claude-code] Activation error:", e);
				// Return {} so Claude Code continues without context (non-blocking)
				return c.json({});
			}
		},
	);

	// DELETE /entries/:id — hard-delete an entry and all its relations.
	// Use for noise, junk extractions, or entries you simply don't want in the store.
	// Irreversible. For soft removal prefer PATCH with status='superseded'.
	app.delete("/entries/:id", (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const entry = db.getEntry(c.req.param("id"));
		if (!entry) {
			return c.json({ error: "Entry not found" }, 404);
		}

		// If the entry is conflicted, restore its counterpart to active before deleting.
		// deleteEntry cascades and removes the contradicts relation, which would otherwise
		// leave the counterpart stuck in 'conflicted' status with no resolvable partner.
		let restoredCounterpart: string | null = null;
		if (entry.status === "conflicted") {
			const relations = db.getRelationsFor(entry.id);
			const conflictRel = relations.find((r) => r.type === "contradicts");
			if (conflictRel) {
				restoredCounterpart =
					conflictRel.sourceId === entry.id
						? conflictRel.targetId
						: conflictRel.sourceId;
				db.updateEntry(restoredCounterpart, { status: "active" });
			}
		}

		db.deleteEntry(entry.id);
		return c.json({ ok: true, deleted: entry.id, restoredCounterpart });
	});

	return app;
}

/**
 * Strip the embedding vector from entries before sending over API.
 * Embeddings are large (3072 floats) and not useful to consumers.
 */
function stripEmbedding(
	entry: KnowledgeEntry,
): Omit<KnowledgeEntry, "embedding"> {
	const { embedding: _embedding, ...rest } = entry;
	return rest;
}
