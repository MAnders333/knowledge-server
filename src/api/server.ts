import { createHash, timingSafeEqual } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { Hono } from "hono";
import type { Context } from "hono";
import { bodyLimit } from "hono/body-limit";
// @ts-ignore — Bun supports JSON imports natively; tsc may warn without resolveJsonModule
import pkg from "../../package.json" with { type: "json" };
import type { ActivationEngine } from "../activation/activate.js";
import { splitIntoCues } from "../activation/activate.js";
import {
	CLIENT_HEADER,
	type ClientTag,
	parseClientTag,
} from "../activation/client.js";
import {
	ActivationEventRecorder,
	hashSessionId,
} from "../activation/events.js";
import { formatActivatedKnowledge } from "../activation/format.js";
import { DEFAULT_CONFIG } from "../config-file.js";
import { REVIEW_STALE_STRENGTH_THRESHOLD, config } from "../config.js";
import type { ConsolidationEngine } from "../consolidation/consolidate.js";
import { type DaemonStatus, readDaemonStatus } from "../daemon/status.js";
import type { IKnowledgeStore } from "../db/index.js";
import { componentHealth, evaluateHealth } from "../health.js";
import { logger } from "../logger.js";
import { activateInputSchema } from "../mcp/index.js";
import { lockHolder } from "../process-lock.js";
import { daemonLockPath, daemonStatusPath } from "../runtime-files.js";
import { KnowledgeService } from "../services/knowledge-service.js";
import type {
	ActivationResult,
	ConsolidationState,
	KnowledgeEntry,
	KnowledgeStatus,
} from "../types.js";
import { withTimeout } from "../utils.js";
import { AuthFailureThrottle } from "./auth-throttle.js";
import { buildHostAllowlist } from "./host-allowlist.js";
import { MAX_BODY_BYTES, boundCues, capText } from "./input-limits.js";

/** Process context /status needs beyond the stores: the data dir and the daemon setting. */
export interface StatusRuntime {
	/** Data directory holding daemon.lock and daemon.status.json. Omit to skip daemon checks. */
	dataDir?: string;
	/** The server auto-spawns the daemon (DAEMON_AUTO_SPAWN), so a daemon is expected. */
	daemonAutoSpawn?: boolean;
}

/** Providers whose circuit state /status reports. */
const PROVIDER_NAMES = ["embedding", "rerank"] as const;

/** Daemon liveness as /status reports it: the lock holder plus the status file. */
function readDaemonView(dataDir: string): {
	running: boolean;
	pid: number;
	status: DaemonStatus | null;
} {
	const holder = lockHolder(daemonLockPath(dataDir));
	return {
		running: holder !== null,
		pid: holder?.pid ?? 0,
		status: readDaemonStatus(daemonStatusPath(dataDir)),
	};
}

/**
 * The socket address of the request, as set by the server's fetch wrapper, or
 * "unknown". Forwarding headers are ignored because clients control them. Behind a
 * reverse proxy every request shares the proxy's address, so the throttle is shared.
 */
function clientAddressOf(c: Context): string {
	const env: { remoteAddress?: string } | undefined = c.env;
	return env?.remoteAddress ?? "unknown";
}

/**
 * HTTP API for the knowledge server.
 *
 * Endpoints:
 * - GET  /health                            -- Liveness probe, no store access (always open)
 * - GET  /activate?q=...                   -- Activate knowledge entries by query (used by plugin)
 * - POST /consolidate                       -- Run consolidation cycle          [admin token]
 * - POST /reinitialize                      -- Wipe knowledge DB and reset cursor [admin token]
 * - GET  /review                            -- List entries needing attention
 * - GET  /status                            -- Server health and stats (config block: admin)
 * - GET  /entries                           -- List all entries (with filters)
 * - GET  /entries/:id                       -- Get a specific entry
 * - PATCH /entries/:id                      -- Update fields on an entry       [admin token]
 * - POST /entries/:id/flag                  -- Queue an entry for review       [read auth]
 * - POST /entries/:id/resolve               -- Resolve a conflicted entry pair [admin token]
 * - DELETE /entries/:id                     -- Hard-delete an entry            [admin token]
 * - POST /hooks/claude-code/user-prompt     -- Claude Code UserPromptSubmit hook
 * - ALL  /mcp                               -- MCP streamable-http endpoint
 *
 * Auth:
 * - Random token (default, per process): printed to the console once, and only when
 *   stdout is a terminal. Mutation routes require `Authorization: Bearer <token>`.
 *   Other routes are open to loopback clients that pass the Host and Origin checks.
 * - Stable token (KNOWLEDGE_ADMIN_TOKEN): every route except /health requires the same
 *   Bearer token. Failed attempts are throttled per client address (429, Retry-After).
 */
/**
 * The request's JSON body when it is an object. Null for malformed JSON and for
 * valid JSON that is not an object (`null`, a string, an array): `in` and
 * property reads on those would throw and surface as 500.
 */
async function readJsonObject(
	c: Context,
): Promise<Record<string, unknown> | null> {
	let body: unknown;
	try {
		body = await c.req.json();
	} catch {
		return null;
	}
	if (body === null || typeof body !== "object" || Array.isArray(body)) {
		return null;
	}
	return body as Record<string, unknown>;
}

export function createApp(
	db: IKnowledgeStore,
	serverStateDb: import("../db/interface.js").IServerStateDB,
	activation: ActivationEngine,
	consolidation: ConsolidationEngine,
	adminToken: string,
	/** Whether adminToken was explicitly configured (vs randomly generated for local use). */
	adminTokenIsStable = false,
	/** Store IDs that failed to connect at startup and are currently unavailable. */
	unavailableStoreIds: ReadonlySet<string> = new Set(),
	/**
	 * All stores to fan out read operations across (/entries, /review, /status).
	 * Defaults to [db] for backwards compatibility (single-store mode).
	 * In multi-store setups, pass registry.readStores() so entries in secondary
	 * stores are visible via the API.
	 */
	readDbs: IKnowledgeStore[] = [db],
	/**
	 * All writable stores for /reinitialize fan-out.
	 * Defaults to [db]. Pass registry.writableStores() in multi-store setups
	 * so all domain stores are wiped atomically on reinitialize.
	 */
	writableDbs: IKnowledgeStore[] = [db],
	/** Port the server is bound to; reported by the admin /status. */
	serverPort: number = DEFAULT_CONFIG.port,
	/**
	 * Content-free activation telemetry sink. The caller owns its lifecycle
	 * (start/stop). Defaults to a recorder on serverStateDb with no timers.
	 */
	activationEvents: ActivationEventRecorder = new ActivationEventRecorder(
		serverStateDb,
	),
	/** Address the server is bound to; always accepted in the Host allow-list. */
	bindHost = "127.0.0.1",
	/** Extra Host values (KNOWLEDGE_ALLOWED_HOSTS), each `name` or `name:port`. */
	extraAllowedHosts: readonly string[] = [],
	/** Where the daemon's lockfile and status file live; enables the daemon checks in /status. */
	runtime: StatusRuntime = {},
): Hono {
	const app = new Hono();
	const allowlist = buildHostAllowlist(bindHost, extraAllowedHosts);

	// Every route, /mcp included, refuses a Host or Origin that does not name this
	// machine (or an allowed host). This blocks DNS rebinding and cross-origin
	// browser pages. Browsers always send Host, so a missing Host is refused too.
	// Non-browser clients that send no Origin are unaffected.
	app.use("*", async (c, next) => {
		if (!allowlist.matchesHost(c.req.header("host") ?? "")) {
			return c.json({ error: "Forbidden: Host not allowed" }, 403);
		}
		const origin = c.req.header("origin");
		if (origin !== undefined && !allowlist.matchesOrigin(origin)) {
			return c.json({ error: "Forbidden: Origin not allowed" }, 403);
		}
		// A cross-site page can still issue a no-cors GET (<img>, <script>,
		// no-cors fetch) with no Origin header, and it would run the activation
		// and flag side effects. Browsers send Sec-Fetch-Site on every request; only
		// same-origin and user-initiated navigations (none) are let through.
		const fetchSite = c.req.header("sec-fetch-site");
		if (
			fetchSite !== undefined &&
			fetchSite.toLowerCase() !== "same-origin" &&
			fetchSite.toLowerCase() !== "none"
		) {
			return c.json({ error: "Forbidden: cross-site request" }, 403);
		}
		await next();
	});

	// -- Admin token --

	// Compare SHA-256 digests, not the raw strings: both digests are 32 bytes, so the
	// comparison does not reveal the length of the token that was sent.
	const expectedAuthDigest = createHash("sha256")
		.update(`Bearer ${adminToken}`)
		.digest();

	function requireAdminToken(c: Context): boolean {
		const provided = createHash("sha256")
			.update(c.req.header("Authorization") ?? "")
			.digest();
		return timingSafeEqual(provided, expectedAuthDigest);
	}

	// Stable admin token: every route except /health needs its Bearer token, reads
	// included. Failed attempts are throttled per client address. In random-token
	// mode reads stay open to loopback clients, and only the mutation routes check
	// the token.
	const authFailures = new AuthFailureThrottle();
	if (adminTokenIsStable) {
		app.use("*", async (c, next) => {
			if (c.req.path === "/health") return next();
			const client = clientAddressOf(c);
			const blockedMs = authFailures.blockedForMs(client);
			if (blockedMs > 0) {
				c.header("Retry-After", String(Math.ceil(blockedMs / 1000)));
				return c.json(
					{ error: "Too many failed authentication attempts" },
					429,
				);
			}
			if (!requireAdminToken(c)) {
				authFailures.recordFailure(client);
				return c.json({ error: "Unauthorized" }, 401);
			}
			await next();
		});
	}

	// Liveness probe that touches no store. Open even with a stable admin token.
	app.get("/health", (c) => c.json({ status: "ok" }));

	// Body cap for JSON routes. Oversized bodies are rejected before parsing.
	const jsonBodyLimit = bodyLimit({
		maxSize: MAX_BODY_BYTES,
		onError: (c) => c.json({ error: "Payload too large" }, 413),
	});
	// Reuse ActivationEngine's EmbeddingClient to avoid a second model connection.
	const service = new KnowledgeService(db, activation.embeddings);

	// Pre-build a service instance for each readable store so PATCH /entries/:id
	// can route writes to the correct store without per-request allocation.
	const serviceByStore = new Map<IKnowledgeStore, KnowledgeService>(
		[...new Set([...readDbs, ...writableDbs])].map((s) => [
			s,
			s === db ? service : new KnowledgeService(s, activation.embeddings),
		]),
	);

	// -- /mcp streamable-http transport --
	//
	// Stateless: every request gets a fresh McpServer and transport. The SDK
	// refuses to reuse a stateless transport, and a shared server would let
	// JSON-RPC ids from concurrent clients collide.
	//
	// With a stable admin token (KNOWLEDGE_ADMIN_TOKEN) clients send
	// `Authorization: Bearer <token>`, as for every other route except /health.
	// For local use: no token required (server binds to 127.0.0.1 only).
	const createMcpServer = (): McpServer => {
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
					const started = performance.now();
					const result = await activation.activate(capText(cues), {
						limit,
						threshold,
					});
					const latencyMs = Math.round(performance.now() - started);
					const cueStr = Array.isArray(cues) ? cues.join(" | ") : cues;
					const prompt = Array.isArray(cues)
						? (cues[cues.length - 1] ?? "")
						: cues;
					logActivation("mcp", cueStr, result.entries);
					recordActivationEvent({
						client: "mcp",
						promptChars: prompt.length,
						result,
						latencyMs,
						sessionId: null,
						clientGaveUp: false,
					});
					// The agent receives these entries, so they count as injections.
					activation.commitInjections(result.injection);

					if (result.entries.length === 0) {
						return {
							content: [
								{
									type: "text" as const,
									text: "No relevant knowledge found for these cues.",
								},
							],
						};
					}

					const formatted = formatActivatedKnowledge(result.entries);

					const conflictCount = result.entries.filter(
						(r) => r.contradiction,
					).length;
					const conflictNote =
						conflictCount > 0
							? ` — ${conflictCount} conflicted, do not act on those without clarifying which version is correct`
							: "";

					return {
						content: [
							{
								type: "text" as const,
								text: `## Activated Knowledge (${result.entries.length} entries, ${result.totalActive} total active${conflictNote})\n\n${formatted}`,
							},
						],
					};
				} catch (e) {
					logger.error("[mcp/activate] Error:", e);
					return {
						content: [
							{
								type: "text" as const,
								text: `Error activating knowledge: ${e}`,
							},
						],
						isError: true,
					};
				}
			},
		);
		return mcpServer;
	};

	// Auth: with a stable admin token, the gate near the top already required Bearer
	// auth. In random-token mode /mcp is open; the server binds to loopback and the
	// Host/Origin checks above apply.
	const mcpHostHeaders = allowlist.sdkHostHeaders(serverPort);
	app.all("/mcp", jsonBodyLimit, async (c) => {
		const transport = new WebStandardStreamableHTTPServerTransport({
			sessionIdGenerator: undefined, // stateless
			enableDnsRebindingProtection: true,
			allowedHosts: mcpHostHeaders,
		});
		const mcpServer = createMcpServer();
		await mcpServer.connect(transport);
		const response = await transport.handleRequest(c.req.raw);
		// The SDK streams JSON-RPC responses; release the server once they are sent.
		return releaseWhenDone(response, () => {
			void mcpServer.close();
		});
	});

	/**
	 * Find an entry for a mutating admin route. Only writable stores are searched.
	 * An entry that exists only in a read-only store yields "read-only" (403)
	 * instead of being changed.
	 */
	async function findWritableEntry(
		id: string,
	): Promise<
		{ entry: KnowledgeEntry; store: IKnowledgeStore } | "read-only" | null
	> {
		const found = await findEntryAcrossStores(writableDbs, id);
		if (found) return found;
		const inReadOnly = await findEntryAcrossStores(readDbs, id);
		return inReadOnly ? "read-only" : null;
	}

	/**
	 * Find a writable entry by full id or by the short id shown in injected lines
	 * (a prefix of at least 8 hex characters). A prefix that matches more than one
	 * entry yields "ambiguous" so the caller never flags the wrong one.
	 */
	async function findEntryByRef(
		ref: string,
	): Promise<
		| { entry: KnowledgeEntry; store: IKnowledgeStore }
		| "read-only"
		| "ambiguous"
		| null
	> {
		if (ref.length === 36 || !/^[0-9a-f-]{8,35}$/i.test(ref)) {
			return findWritableEntry(ref);
		}
		const needle = ref.toLowerCase();
		const matches: Array<{ entry: KnowledgeEntry; store: IKnowledgeStore }> =
			[];
		for (const store of readDbs) {
			for (const entry of await store.getEntries({})) {
				if (entry.id.toLowerCase().startsWith(needle)) {
					matches.push({ entry, store });
				}
			}
		}
		if (matches.length === 0) return null;
		if (matches.length > 1) return "ambiguous";
		const [only] = matches;
		return writableDbs.includes(only.store) ? only : "read-only";
	}

	// -- Helpers --

	/**
	 * Log one activation to the server log. Content-light by default:
	 *
	 *   [activation/opencode] chars=129 → 3 entries
	 *   [activation/opencode]   0.72 fact  <id>
	 *
	 * At LOG_LEVEL=debug the query and entry text are added:
	 *
	 *   [activation/opencode] q="<full query>" → 3 entries
	 *   [activation/opencode]   0.72 fact  <id>  "full entry content"
	 *
	 * caller is the client tag (opencode, pi, claude-code, mcp, unknown, …).
	 * Measurement lives in activation_event (see recordActivationEvent).
	 */
	function logActivation(
		caller: string,
		query: string,
		entries: ActivationResult["entries"],
	): void {
		const prefix = `[activation/${caller}]`;
		const debug = logger.isEnabled("debug");
		const head = debug ? `q=${JSON.stringify(query)}` : `chars=${query.length}`;
		if (entries.length === 0) {
			logger.log(`${prefix} ${head} → 0 entries`);
			return;
		}
		logger.log(
			`${prefix} ${head} → ${entries.length} entr${entries.length === 1 ? "y" : "ies"}`,
		);
		for (const r of entries) {
			const content = debug ? `  ${JSON.stringify(r.entry.content)}` : "";
			logger.log(
				`${prefix}   ${r.rawSimilarity.toFixed(3)} ${r.entry.type.padEnd(10)} ${r.entry.id}${content}`,
			);
		}
	}

	/**
	 * Queue one content-free activation_event row. Never throws, never waits on the
	 * database: the recorder batches writes off the request path.
	 */
	function recordActivationEvent(args: {
		client: ClientTag;
		promptChars: number;
		result: ActivationResult;
		latencyMs: number;
		sessionId: string | null | undefined;
		/** True when the client disconnected before the result was ready. */
		clientGaveUp: boolean;
	}): void {
		const { client, promptChars, result, latencyMs, sessionId, clientGaveUp } =
			args;
		try {
			const gateMode = result.telemetry?.gateMode ?? "off";
			activationEvents.record({
				ts: Date.now(),
				client,
				sessionHash: hashSessionId(sessionId),
				promptChars,
				cueCount: result.telemetry?.cueCount ?? 0,
				nCandidates: result.telemetry?.nCandidates ?? 0,
				nReturned: result.entries.length,
				entryIds: result.entries.map((r) => r.entry.id),
				rawSims: result.entries.map(
					(r) => Math.round(r.rawSimilarity * 1e4) / 1e4,
				),
				rerankScores:
					gateMode === "rerank"
						? result.entries.map((r) =>
								r.rerankScore === undefined
									? null
									: Math.round(r.rerankScore * 1e4) / 1e4,
							)
						: null,
				gateMode,
				rerankModel:
					gateMode === "rerank"
						? (result.telemetry?.rerankModel ?? null)
						: null,
				latencyMs,
				clientDeadlineExceeded: clientGaveUp,
				gateDropped: result.telemetry?.gateDropped ?? 0,
				budgetDropped: result.telemetry?.budgetDropped ?? 0,
				deduped: result.telemetry?.deduped ?? 0,
				injectedTokens: result.telemetry?.injectedTokens ?? 0,
			});
		} catch {
			// Measurement must never fail a request.
		}
	}

	/**
	 * Per-session dedupe key, scoped to the client. Undefined when the client sent
	 * no session id: such requests are not deduplicated (see session-dedupe.ts).
	 * Ids longer than 256 characters are replaced by their SHA-256 hex digest.
	 */
	function sessionDedupeKey(
		client: string,
		sessionId: string | null | undefined,
	): string | undefined {
		const id = sessionId?.trim();
		if (!id) return undefined;
		// Long ids are hashed so a client-chosen id cannot make the key arbitrarily large.
		const key =
			id.length <= 256 ? id : createHash("sha256").update(id).digest("hex");
		return `${client}\u0000${key}`;
	}

	// -- Activation --

	// Access records are written for every returned entry, including for
	// unauthenticated callers. Gating that is deliberately out of scope here.
	app.get("/activate", async (c) => {
		// Accept one or more `q` params (repeated: ?q=seg1&q=seg2&q=full).
		// Single ?q=... still works — queries() returns a one-element array.
		const rawQueries = c.req.queries("q");
		if (!rawQueries || rawQueries.length === 0) {
			return c.json({ error: "Missing query parameter 'q'" }, 400);
		}
		const queries = boundCues(rawQueries);

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

		const client = parseClientTag(
			c.req.query("client") ?? c.req.header(CLIENT_HEADER),
		);
		const sessionId = c.req.header("x-knowledge-session");
		const prompt = queries[queries.length - 1] ?? "";
		try {
			const started = performance.now();
			const result = await activation.activate(
				queries.length === 1 ? queries[0] : queries,
				{ limit, threshold, session: sessionDedupeKey(client, sessionId) },
			);
			const latencyMs = Math.round(performance.now() - started);
			logActivation(client, queries.join(" | "), result.entries);
			const clientGaveUp = c.req.raw.signal.aborted;
			recordActivationEvent({
				client,
				promptChars: prompt.length,
				result,
				latencyMs,
				sessionId,
				clientGaveUp,
			});
			// telemetry and injection are internal; neither is part of the response.
			const { telemetry: _telemetry, injection: _injection, ...body } = result;
			// `context` is the ready-to-inject block. Plugins prefer it over their own
			// formatting, so the escaping and labelling live in one place (format.ts).
			const response = c.json({
				...body,
				context: formatActivatedKnowledge(result.entries),
			});
			// Abandoned requests are not recorded as injections (client_deadline_exceeded).
			if (!clientGaveUp) activation.commitInjections(result.injection);
			return response;
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

			// Fan out reinitialize to all writable stores so secondary domain stores
			// are wiped together with the primary. Without this, post-reinitialize
			// consolidation re-inserts everything from pending_episodes while secondary
			// stores retain their old entries, producing duplicates.
			for (const store of writableDbs) {
				await store.reinitialize();
			}
			await serverStateDb.reinitialize();

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

	app.get("/review", async (c) => {
		// Fan out across all readable stores and merge results.
		// fanOutReads tolerates per-store failures — a dead store is skipped.
		const allConflicted = (
			await fanOutReads(
				readDbs,
				(s) => s.getEntriesByStatus("conflicted"),
				"review getEntriesByStatus",
			)
		).flat();
		const allActive = (
			await fanOutReads(
				readDbs,
				(s) => s.getActiveEntries(),
				"review getActiveEntries",
			)
		).flat();

		// Find stale entries (active but low strength)
		const stale = allActive
			.filter((e) => e.strength < REVIEW_STALE_STRENGTH_THRESHOLD)
			.sort((a, b) => a.strength - b.strength);

		return c.json({
			conflicted: allConflicted.map(stripEmbedding),
			stale: stale.map(stripEmbedding),
			summary: {
				conflictedCount: allConflicted.length,
				staleCount: stale.length,
			},
		});
	});

	// -- Status --

	app.get("/status", async (c) => {
		// Fan out stats across all stores and sum counts.
		// fanOutReads tolerates per-store failures — a dead store is skipped
		// and its counts simply omitted from the sum. Each store's outcome is
		// recorded, so `health` below reflects this request's reads.
		const allStats = await fanOutReads(
			readDbs,
			(s) => s.getStats(),
			"status getStats",
		);
		const stats = allStats.reduce(
			(acc, s) => ({
				total: acc.total + s.total,
				active: acc.active + s.active,
				superseded: acc.superseded + (s.superseded ?? 0),
				archived: acc.archived + (s.archived ?? 0),
				conflicted: acc.conflicted + (s.conflicted ?? 0),
				tombstoned: acc.tombstoned + (s.tombstoned ?? 0),
			}),
			{
				total: 0,
				active: 0,
				superseded: 0,
				archived: 0,
				conflicted: 0,
				tombstoned: 0,
			},
		);

		// The state DB is the one dependency every path needs. If it does not answer,
		// the server is down; the response still carries what is known.
		let stateDbReachable = true;
		let consolidationState: ConsolidationState | null = null;
		let pendingSessions = 0;
		let deadLettered = 0;
		try {
			consolidationState = await serverStateDb.getConsolidationState();
			pendingSessions = await serverStateDb.countPendingSessions();
			deadLettered = await serverStateDb.countDeadLetteredEpisodes();
		} catch (err) {
			stateDbReachable = false;
			logger.error("[status] State DB unreachable:", err);
		}

		const outcome = consolidation.consolidationOutcome();
		const daemon = runtime.dataDir ? readDaemonView(runtime.dataDir) : null;
		const daemonExpected =
			runtime.daemonAutoSpawn === true || daemon?.status != null;

		const storeInputs = [
			...readDbs.map((s) => ({
				id: s.id,
				failing: componentHealth.snapshot(`store:${s.id}`).failing,
			})),
			...[...unavailableStoreIds].map((id) => ({ id, failing: true })),
		];
		const providerSnapshots = PROVIDER_NAMES.map((name) =>
			componentHealth.snapshot(name),
		);
		const llmHealth = componentHealth.snapshot("llm");
		const { health, reasons } = evaluateHealth({
			stateDbReachable,
			stores: storeInputs,
			providers: providerSnapshots.map((p) => ({
				name: p.name,
				open: p.open,
			})),
			daemon: {
				expected: daemonExpected,
				running: daemon?.running ?? false,
				consecutiveFailures: daemon?.status?.consecutiveFailures ?? 0,
			},
			consolidation: {
				consecutiveFailures: outcome.consecutiveFailures,
				deadLettered,
				pendingSessions,
			},
			llm: {
				failingRuns: llmHealth.consecutiveFailures,
				failingForMs:
					llmHealth.failingSince === null
						? 0
						: Date.now() - llmHealth.failingSince,
				failureClass: llmHealth.lastError,
			},
		});

		// Config block (model names, port) and diagnostics are admin-only. In
		// random-token mode, unauthenticated callers still get version, stats and
		// health, and the blocks are omitted. With a stable token the gate near the
		// top has already required the token, so every caller here is admin.
		const isAdmin = requireAdminToken(c);

		// Use primary store's embedding metadata (model is shared across all stores).
		const embeddingMeta = await db.getEmbeddingMetadata();

		return c.json({
			// Liveness: the server answered. `health` is the operational verdict.
			status: "ok",
			health,
			reasons,
			version: pkg.version,
			knowledge: stats,
			consolidation: {
				lastRun: consolidationState?.lastConsolidatedAt
					? new Date(consolidationState.lastConsolidatedAt).toISOString()
					: null,
				totalSessionsProcessed:
					consolidationState?.totalSessionsProcessed ?? null,
				totalEntriesCreated: consolidationState?.totalEntriesCreated ?? null,
				consecutiveFailures: outcome.consecutiveFailures,
				deadLettered,
				pendingSessions,
			},
			daemon: daemon
				? {
						running: daemon.running,
						lastUploadAt: daemon.status?.lastUploadAt ?? null,
						lastCycleAt: daemon.status?.lastCycleAt ?? null,
					}
				: null,
			embedding: embeddingMeta
				? {
						model: embeddingMeta.model,
						dimensions: embeddingMeta.dimensions,
						recordedAt: new Date(embeddingMeta.recordedAt).toISOString(),
					}
				: null,
			...(unavailableStoreIds.size > 0 && {
				unavailableStores: [...unavailableStoreIds],
			}),
			...(isAdmin && {
				config: {
					port: serverPort,
					embeddingModel: config.embedding.model,
					extractionModel: config.llm.extractionModel,
					mergeModel: config.llm.mergeModel,
					contradictionModel: config.llm.contradictionModel,
				},
				// Admin-only detail: raw error text and process ids stay off the public view.
				diagnostics: {
					daemon: daemon
						? {
								pid: daemon.pid,
								startedAt: daemon.status?.startedAt ?? null,
								lastError: daemon.status?.lastError ?? null,
							}
						: null,
					consolidation: {
						lastError: outcome.lastError,
						lastErrorAt: outcome.lastErrorAt
							? new Date(outcome.lastErrorAt).toISOString()
							: null,
						lastSuccessAt: outcome.lastSuccessAt
							? new Date(outcome.lastSuccessAt).toISOString()
							: null,
					},
					stores: readDbs.map((s) => ({
						id: s.id,
						...componentHealth.snapshot(`store:${s.id}`),
					})),
					providers: providerSnapshots,
				},
			}),
		});
	});

	// -- Entries CRUD --

	app.get("/entries", async (c) => {
		const status = c.req.query("status") || undefined;
		const type = c.req.query("type") || undefined;

		// Fan out across all stores — entries in secondary domain stores are included.
		// fanOutReads tolerates per-store failures — a dead store is skipped.
		const allEntries = (
			await fanOutReads(
				readDbs,
				(s) => s.getEntries({ status, type }),
				"entries list",
			)
		).flat();

		return c.json({
			entries: allEntries.map(stripEmbedding),
			count: allEntries.length,
		});
	});

	app.get("/entries/:id", async (c) => {
		// Search across all stores — the entry may live in any domain store.
		const id = c.req.param("id");
		const found = await findEntryAcrossStores(readDbs, id);
		if (!found) {
			return c.json({ error: "Entry not found" }, 404);
		}
		const { entry, store: entryStore } = found;

		const relations = await entryStore.getRelationsFor(entry.id);
		return c.json({
			entry: stripEmbedding(entry),
			relations,
		});
	});

	// PATCH /entries/:id — update mutable fields on any entry.
	// Useful for human review: correcting content, changing type, marking stale entries active, etc.
	// Accepts any subset of: content, topics, confidence, status.
	// If content or topics change, re-compute the embedding immediately so activation
	// and reconsolidation continue using a semantically correct vector.
	app.patch("/entries/:id", jsonBodyLimit, async (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const id = c.req.param("id");
		const located = await findWritableEntry(id);
		if (located === "read-only") {
			return c.json({ error: "Entry is in a read-only store" }, 403);
		}
		if (!located) {
			return c.json({ error: "Entry not found" }, 404);
		}
		const { entry, store: entryStore } = located;

		const body = await readJsonObject(c);
		if (!body) return c.json({ error: "Invalid JSON body" }, 400);

		const allowed = ["content", "topics", "confidence", "status"] as const;
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
		if (updates.confidence !== undefined) {
			const c_ = updates.confidence as unknown;
			if (typeof c_ !== "number" || c_ < 0 || c_ > 1) {
				return c.json(
					{ error: "confidence must be a number between 0 and 1" },
					400,
				);
			}
		}

		try {
			// Route the update to whichever store holds this entry.
			const entryService = serviceByStore.get(entryStore) ?? service;
			await entryService.updateEntry(entry.id, updates);
			const updated = await entryStore.getEntry(entry.id);
			if (!updated) {
				return c.json({ error: "Entry not found after update" }, 500);
			}
			return c.json({ entry: stripEmbedding(updated) });
		} catch (e) {
			logger.error("[entries/patch] Failed to update entry:", e);
			return c.json({ error: "Failed to update entry" }, 500);
		}
	});

	// POST /entries/:id/flag — an agent reports an entry as wrong, outdated or irrelevant.
	// The entry is queued for review by moving it to status 'conflicted' (it stays in
	// the store and in /review). Only active entries can be flagged. :id is the full
	// entry id or the short id shown in injected lines. Flagging is reversible, so it
	// takes read-level auth and needs no admin token of its own.
	app.post("/entries/:id/flag", jsonBodyLimit, async (c) => {
		const located = await findEntryByRef(c.req.param("id"));
		if (located === "read-only") {
			return c.json({ error: "Entry is in a read-only store" }, 403);
		}
		if (located === "ambiguous") {
			return c.json(
				{ error: "Id prefix matches more than one entry; use the full id" },
				409,
			);
		}
		if (!located) {
			return c.json({ error: "Entry not found" }, 404);
		}
		const { entry, store: entryStore } = located;

		const body = await readJsonObject(c);
		if (!body) return c.json({ error: "Invalid JSON body" }, 400);
		const reasons = ["wrong", "outdated", "irrelevant"];
		if (typeof body.reason !== "string" || !reasons.includes(body.reason)) {
			return c.json(
				{ error: `reason must be one of: ${reasons.join(", ")}` },
				400,
			);
		}
		if (entry.status !== "active") {
			return c.json(
				{ error: `Entry is not active (status: ${entry.status})` },
				409,
			);
		}
		try {
			const entryService = serviceByStore.get(entryStore) ?? service;
			await entryService.updateEntry(entry.id, { status: "conflicted" });
		} catch (e) {
			logger.error("[entries/flag] Failed to flag entry:", e);
			return c.json({ error: "Failed to flag entry" }, 500);
		}
		logger.log(`[entries/flag] ${entry.id.slice(0, 8)} reason=${body.reason}`);
		return c.json({ ok: true, id: entry.id, status: "conflicted" });
	});

	// POST /entries/:id/resolve — resolve a conflicted entry pair via one of three outcomes:
	//   supersede_this  — the entry identified by :id is the loser; its conflict counterpart wins
	//   supersede_other — the entry identified by :id wins; its conflict counterpart is superseded
	//   merge           — replace :id's content with mergedContent; supersede the counterpart
	//   delete          — hard-delete this entry (useful for noise/junk that shouldn't be kept)
	//
	// The entry must have status='conflicted'. Its counterpart is looked up automatically
	// via the contradicts relation.
	app.post("/entries/:id/resolve", jsonBodyLimit, async (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const resolveId = c.req.param("id");
		const located = await findWritableEntry(resolveId);
		if (located === "read-only") {
			return c.json({ error: "Entry is in a read-only store" }, 403);
		}
		if (!located) {
			return c.json({ error: "Entry not found" }, 404);
		}
		const { entry, store: entryStore } = located;
		if (entry.status !== "conflicted") {
			return c.json(
				{ error: `Entry is not conflicted (status: ${entry.status})` },
				400,
			);
		}

		const body = await readJsonObject(c);
		if (!body) return c.json({ error: "Invalid JSON body" }, 400);

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
		const relations = await entryStore.getRelationsFor(entry.id);
		const conflictRelation = relations.find((r) => r.type === "contradicts");
		const counterpartId = conflictRelation
			? conflictRelation.sourceId === entry.id
				? conflictRelation.targetId
				: conflictRelation.sourceId
			: null;

		// Guard against cross-store conflicts — applies to all resolutions including
		// delete (which needs to restore the counterpart to active).
		let counterpart: KnowledgeEntry | null = null;
		if (counterpartId) {
			counterpart = await entryStore.getEntry(counterpartId);
			if (!counterpart) {
				return c.json(
					{
						error:
							"Cross-store conflict resolution is not supported — entry and counterpart live in different stores. Use knowledge-server review to resolve manually.",
					},
					422,
				);
			}
		}

		if (resolution === "delete") {
			// Restore the counterpart to active (deleteEntry cascades and removes the relation).
			// Only a counterpart that is still conflicted is restored: decay may have archived
			// or tombstoned it since, and reviving that would undo the decay.
			const restoreCounterpart = counterpart?.status === "conflicted";
			if (counterpartId && restoreCounterpart) {
				const entryService = serviceByStore.get(entryStore) ?? service;
				await entryService.updateEntry(counterpartId, { status: "active" });
			}
			await entryStore.deleteEntry(entry.id);
			return c.json({
				ok: true,
				deleted: entry.id,
				restoredCounterpart: restoreCounterpart ? counterpartId : null,
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
			await entryStore.applyContradictionResolution(
				"merge",
				entry.id,
				counterpartId,
				{
					content: mergedContent,
					type: entry.type,
					topics: entry.topics,
					confidence: entry.confidence,
				},
			);
			return c.json({
				ok: true,
				resolution: "merge",
				winner: entry.id,
				superseded: counterpartId,
			});
		}

		if (resolution === "supersede_this") {
			// :id loses — counterpart wins
			await entryStore.applyContradictionResolution(
				"supersede_new",
				entry.id,
				counterpartId,
			);
			return c.json({
				ok: true,
				resolution: "supersede_this",
				winner: counterpartId,
				superseded: entry.id,
			});
		}

		// supersede_other — :id wins, counterpart loses
		await entryStore.applyContradictionResolution(
			"supersede_old",
			entry.id,
			counterpartId,
		);
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
	// In random-token mode this endpoint is unauthenticated, like GET /activate: the
	// server binds to 127.0.0.1 only, and loopback is the security boundary. With a
	// stable admin token the gate near the top requires Bearer auth, and clients
	// send the token from KNOWLEDGE_ADMIN_TOKEN.
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
		bodyLimit({ maxSize: MAX_BODY_BYTES, onError: (c) => c.json({}) }),
		async (c) => {
			let prompt: string;
			let sessionIdBody: unknown;
			try {
				const body = (await c.req.json()) as Record<string, unknown>;
				if (typeof body.prompt !== "string" || !body.prompt.trim()) {
					return c.json({});
				}
				prompt = body.prompt;
				sessionIdBody = body.session_id;
			} catch {
				return c.json({});
			}

			try {
				// Split prompt into per-line cues + full prompt holistic cue,
				// matching the same multi-cue strategy used by the OpenCode plugin.
				const cues = boundCues(splitIntoCues(capText(prompt)));
				const started = performance.now();
				const client = parseClientTag(
					c.req.query("client") ?? c.req.header(CLIENT_HEADER) ?? "claude-code",
				);
				const sessionId =
					typeof sessionIdBody === "string" ? sessionIdBody : null;
				const result = await activation.activate(cues, {
					limit: 8,
					session: sessionDedupeKey(client, sessionId),
				});
				const latencyMs = Math.round(performance.now() - started);
				logActivation(client, prompt, result.entries);
				const clientGaveUp = c.req.raw.signal.aborted;
				recordActivationEvent({
					client,
					promptChars: prompt.length,
					result,
					latencyMs,
					sessionId,
					clientGaveUp,
				});

				const response = c.json(
					result.entries.length === 0
						? {}
						: {
								hookSpecificOutput: {
									hookEventName: "UserPromptSubmit",
									additionalContext: formatActivatedKnowledge(result.entries),
								},
							},
				);
				// Abandoned requests are not recorded as injections (client_deadline_exceeded).
				if (!clientGaveUp) activation.commitInjections(result.injection);
				return response;
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
	app.delete("/entries/:id", async (c) => {
		if (!requireAdminToken(c)) {
			return c.json({ error: "Unauthorized" }, 401);
		}

		const deleteId = c.req.param("id");
		const located = await findWritableEntry(deleteId);
		if (located === "read-only") {
			return c.json({ error: "Entry is in a read-only store" }, 403);
		}
		if (!located) {
			return c.json({ error: "Entry not found" }, 404);
		}
		const { entry, store: entryStore } = located;

		// If the entry is conflicted, restore its counterpart to active before deleting.
		// deleteEntry cascades and removes the contradicts relation, which would otherwise
		// leave the counterpart stuck in 'conflicted' status with no resolvable partner.
		// Only a counterpart that is still conflicted is restored: decay may have archived
		// or tombstoned it since, and reviving that would undo the decay.
		let restoredCounterpart: string | null = null;
		if (entry.status === "conflicted") {
			const relations = await entryStore.getRelationsFor(entry.id);
			const conflictRel = relations.find((r) => r.type === "contradicts");
			if (conflictRel) {
				const counterpartId =
					conflictRel.sourceId === entry.id
						? conflictRel.targetId
						: conflictRel.sourceId;
				// Guard against cross-store counterpart — same as /resolve endpoint.
				// If the counterpart is in a different store we can't safely restore it here.
				const counterpart = await entryStore.getEntry(counterpartId);
				if (!counterpart) {
					return c.json(
						{
							error:
								"Cross-store conflict: entry and counterpart live in different stores. Cannot safely restore counterpart on delete.",
						},
						422,
					);
				}
				if (counterpart.status === "conflicted") {
					const entryService = serviceByStore.get(entryStore) ?? service;
					await entryService.updateEntry(counterpartId, { status: "active" });
					restoredCounterpart = counterpartId;
				}
			}
		}

		await entryStore.deleteEntry(entry.id);
		return c.json({ ok: true, deleted: entry.id, restoredCounterpart });
	});

	return app;
}

/**
 * Fan out a read operation across stores, tolerating per-store failures.
 *
 * Each store call is wrapped in a per-store timeout. Results are collected via
 * Promise.allSettled so one dead store (e.g. remote Postgres behind a VPN that
 * went down) cannot block or reject the entire request — the remaining stores'
 * results are returned. Failed stores are logged as warnings.
 *
 * Used by /status, /review, and /entries which all fan out reads across
 * readDbs and previously used Promise.all (fail-fast, no timeout).
 */
async function fanOutReads<T>(
	stores: IKnowledgeStore[],
	fn: (db: IKnowledgeStore) => Promise<T>,
	label: string,
	timeoutMs = 5_000,
): Promise<T[]> {
	if (stores.length === 0) return [];
	const results = await Promise.allSettled(
		stores.map((db) =>
			Promise.race([
				fn(db),
				new Promise<never>((_, reject) =>
					setTimeout(
						() => reject(new Error(`timeout after ${timeoutMs / 1000}s`)),
						timeoutMs,
					),
				),
			]),
		),
	);
	const ok: T[] = [];
	for (let i = 0; i < results.length; i++) {
		const r = results[i];
		const name = `store:${stores[i].id}`;
		if (r.status === "fulfilled") {
			componentHealth.recordSuccess(name);
			ok.push(r.value);
		} else {
			componentHealth.recordFailure(name, r.reason);
			logger.warn(
				`[api] Store "${stores[i].id}" failed during ${label} — skipping. ` +
					`Reason: ${r.reason instanceof Error ? r.reason.message : String(r.reason)}`,
			);
		}
	}
	return ok;
}

/**
 * Search for a single entry across stores, tolerating per-store failures.
 *
 * Iterates stores sequentially (breaks on first hit) with a per-store timeout
 * so a dead store is skipped after 5 s rather than hanging the request.
 * Used by /entries/:id GET, PATCH, resolve, and DELETE handlers.
 */
async function findEntryAcrossStores(
	stores: IKnowledgeStore[],
	id: string,
): Promise<{ entry: KnowledgeEntry; store: IKnowledgeStore } | null> {
	for (const store of stores) {
		try {
			const found = await withTimeout(
				store.getEntry(id),
				5_000,
				`getEntry "${id}"`,
			);
			if (found) return { entry: found, store };
		} catch (e) {
			logger.warn(
				`[api] Store "${store.id}" failed during getEntry — skipping. ` +
					`Reason: ${e instanceof Error ? e.message : String(e)}`,
			);
		}
	}
	return null;
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

/**
 * Run `onDone` once the response body has been fully streamed or cancelled.
 * The per-request MCP server must stay alive until its responses are written.
 */
function releaseWhenDone(response: Response, onDone: () => void): Response {
	if (!response.body) {
		onDone();
		return response;
	}
	const reader = response.body.getReader();
	const body = new ReadableStream<Uint8Array>({
		async pull(controller) {
			try {
				const { done, value } = await reader.read();
				if (done) {
					controller.close();
					onDone();
				} else {
					controller.enqueue(value);
				}
			} catch (e) {
				controller.error(e);
				onDone();
			}
		},
		cancel(reason) {
			void reader.cancel(reason);
			onDone();
		},
	});
	return new Response(body, {
		status: response.status,
		statusText: response.statusText,
		headers: response.headers,
	});
}
