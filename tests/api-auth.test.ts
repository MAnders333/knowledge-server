import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Hono } from "hono";
import { ActivationEngine } from "../src/activation/activate";
import { AuthFailureThrottle } from "../src/api/auth-throttle";
import { createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";

const STABLE_TOKEN = "stable-admin-token-0123456789";
const HOST = { Host: "127.0.0.1:3179" };

function consolidationStub(): ConsolidationEngine {
	return {
		consolidate: async () => ({}),
		get isConsolidating() {
			return false;
		},
		tryLock: () => true,
		consolidationOutcome: () => ({
			consecutiveFailures: 0,
			lastError: null,
			lastErrorAt: null,
			lastSuccessAt: null,
		}),
		unlock: () => {},
		close: () => {},
	} as unknown as ConsolidationEngine;
}

describe("admin token gate", () => {
	let tempDir: string;
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let activation: ActivationEngine;
	let app: Hono;

	function build(adminTokenIsStable: boolean, readsProtected = false): void {
		app = createApp({
			db,
			serverStateDb,
			activation,
			consolidation: consolidationStub(),
			adminToken: STABLE_TOKEN,
			adminTokenIsStable,
			readsProtected,
		});
	}

	/** Loopback Host, optional Bearer token, optional socket address (the throttle key). */
	function call(
		path: string,
		init: RequestInit & { token?: string } = {},
		remoteAddress?: string,
	): Response | Promise<Response> {
		const { token, ...rest } = init;
		const headers: Record<string, string> = {
			...HOST,
			...(init.headers as Record<string, string>),
		};
		if (token !== undefined) headers.Authorization = `Bearer ${token}`;
		return app.request(
			path,
			{ ...rest, headers },
			remoteAddress === undefined ? undefined : { remoteAddress },
		);
	}

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-auth-test-"));
		db = new KnowledgeDB(
			join(tempDir, "test.db"),
			join(tempDir, "opencode-fake.db"),
		);
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
		activation = new ActivationEngine(db);
		spyOn(activation.embeddings, "embed").mockResolvedValue([0.1, 0.2, 0.3]);
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	describe("stable token, reads protected", () => {
		beforeEach(() => {
			// The routes under test call the activation engine: stub it, no embedder.
			spyOn(activation, "activate").mockResolvedValue({
				entries: [],
				query: "",
				totalActive: 0,
			});
			build(true, true);
		});

		it("answers /health without a token", async () => {
			const res = await call("/health");
			expect(res.status).toBe(200);
			expect(await res.json()).toEqual({ status: "ok" });
		});

		it.each([
			["GET", "/status"],
			["GET", "/activate?q=churn"],
			["GET", "/review"],
			["GET", "/entries"],
			["GET", "/entries/c0ffee00"],
			["POST", "/entries/c0ffee00/flag"],
			["POST", "/hooks/claude-code/user-prompt"],
			["POST", "/mcp"],
		])("refuses %s %s without a token", async (method, path) => {
			const res = await call(path, {
				method,
				headers: { "Content-Type": "application/json" },
				body: method === "POST" ? JSON.stringify({ prompt: "hello" }) : null,
			});
			expect(res.status).toBe(401);
		});

		it("accepts the token on reads and on the flag route", async () => {
			expect((await call("/entries", { token: STABLE_TOKEN })).status).toBe(
				200,
			);
			expect((await call("/review", { token: STABLE_TOKEN })).status).toBe(200);
			expect((await call("/status", { token: STABLE_TOKEN })).status).toBe(200);
			// Past the gate: a missing entry is a 404, not a 401.
			const flag = await call("/entries/ffffffff/flag", {
				method: "POST",
				token: STABLE_TOKEN,
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ reason: "wrong" }),
			});
			expect(flag.status).toBe(404);
		});

		it("refuses wrong, partial, longer and differently sized tokens", async () => {
			for (const token of [
				"x",
				STABLE_TOKEN.slice(0, -1),
				`${STABLE_TOKEN}x`,
				"stable-admin-token-0123456789-but-much-much-longer-than-the-real-one",
				"",
			]) {
				expect((await call("/entries", { token })).status).toBe(401);
			}
			// The bare token without the Bearer scheme is not accepted either.
			const bare = await call("/entries", {
				headers: { Authorization: STABLE_TOKEN },
			});
			expect(bare.status).toBe(401);
		});

		it.each([
			["GET", "/status", undefined, 200],
			["GET", "/activate?q=churn", undefined, 200],
			["GET", "/review", undefined, 200],
			["GET", "/entries", undefined, 200],
			["GET", "/entries/c0ffee00", undefined, 404],
			["POST", "/entries/c0ffee00/flag", { reason: "wrong" }, 404],
			[
				"POST",
				"/hooks/claude-code/user-prompt",
				{ prompt: "how do we rotate the admin token" },
				200,
			],
		])("accepts the token on %s %s", async (method, path, body, status) => {
			const res = await call(path, {
				method,
				token: STABLE_TOKEN,
				headers: { "Content-Type": "application/json" },
				body: body === undefined ? null : JSON.stringify(body),
			});
			expect(res.status).toBe(status);
		});

		it("accepts the token on /mcp", async () => {
			const res = await call("/mcp", {
				method: "POST",
				token: STABLE_TOKEN,
				headers: {
					"Content-Type": "application/json",
					Accept: "application/json, text/event-stream",
				},
				body: JSON.stringify({
					jsonrpc: "2.0",
					id: 1,
					method: "initialize",
					params: {
						protocolVersion: "2025-03-26",
						capabilities: {},
						clientInfo: { name: "auth-test", version: "1.0.0" },
					},
				}),
			});
			expect(res.status).toBe(200);
		});

		it("treats an empty bearer as no token, and reads the scheme case-insensitively", async () => {
			for (const authorization of ["Bearer", "Bearer    "]) {
				const res = await call("/entries", {
					headers: { Authorization: authorization },
				});
				expect(res.status).toBe(401);
			}
			const lower = await call("/entries", {
				headers: { Authorization: `bearer ${STABLE_TOKEN}` },
			});
			expect(lower.status).toBe(200);
		});

		it("throttles failed attempts per client address with 429 and Retry-After", async () => {
			for (let i = 0; i < 10; i++) {
				expect(
					(await call("/entries", { token: "wrong-token-0000" }, "10.0.0.1"))
						.status,
				).toBe(401);
			}
			const blocked = await call(
				"/entries",
				{ token: "wrong-token-0000" },
				"10.0.0.1",
			);
			expect(blocked.status).toBe(429);
			const retryAfter = Number(blocked.headers.get("Retry-After"));
			expect(retryAfter).toBeGreaterThan(0);
			expect(retryAfter).toBeLessThanOrEqual(60);

			// A correct token is never refused by the throttle. The valid holder on the
			// same address gets through, wrong tokens there stay refused, and another
			// address is unaffected.
			expect(
				(await call("/entries", { token: STABLE_TOKEN }, "10.0.0.1")).status,
			).toBe(200);
			expect(
				(await call("/entries", { token: "wrong-token-0000" }, "10.0.0.1"))
					.status,
			).toBe(429);
			expect(
				(await call("/entries", { token: STABLE_TOKEN }, "10.0.0.2")).status,
			).toBe(200);
		});
	});

	describe("stable token, reads open (default)", () => {
		beforeEach(() => {
			spyOn(activation, "activate").mockResolvedValue({
				entries: [],
				query: "",
				totalActive: 0,
			});
			build(true, false);
		});

		it.each([
			["GET", "/status", undefined, 200],
			["GET", "/activate?q=churn", undefined, 200],
			["GET", "/review", undefined, 200],
			["GET", "/entries", undefined, 200],
			["GET", "/entries/c0ffee00", undefined, 404],
			["POST", "/entries/c0ffee00/flag", { reason: "wrong" }, 404],
			[
				"POST",
				"/hooks/claude-code/user-prompt",
				{ prompt: "how do we rotate the admin token" },
				200,
			],
		])("answers %s %s without a token", async (method, path, body, status) => {
			const res = await call(path, {
				method,
				headers: { "Content-Type": "application/json" },
				body: body === undefined ? null : JSON.stringify(body),
			});
			expect(res.status).toBe(status);
		});

		it("requires the token for /mcp in default mode, as on earlier releases", async () => {
			const options = {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Accept: "application/json, text/event-stream",
				},
				body: JSON.stringify({
					jsonrpc: "2.0",
					id: 1,
					method: "initialize",
					params: {
						protocolVersion: "2025-03-26",
						capabilities: {},
						clientInfo: { name: "auth-test", version: "1.0.0" },
					},
				}),
			};
			expect((await call("/mcp", options)).status).toBe(401);
			expect(
				(await call("/mcp", { ...options, token: STABLE_TOKEN })).status,
			).toBe(200);
		});

		it.each([
			["POST", "/consolidate", undefined, 200],
			["POST", "/reinitialize?confirm=yes", undefined, 200],
			["PATCH", "/entries/c0ffee00", { content: "edited" }, 404],
			["DELETE", "/entries/c0ffee00", undefined, 404],
			["POST", "/entries/c0ffee00/resolve", { resolution: "delete" }, 404],
		])(
			"requires the token for %s %s, and accepts it",
			async (method, path, body, status) => {
				const options = {
					method,
					headers: { "Content-Type": "application/json" },
					body: body === undefined ? null : JSON.stringify(body),
				};
				expect((await call(path, options)).status).toBe(401);
				expect(
					(await call(path, { ...options, token: STABLE_TOKEN })).status,
				).toBe(status);
			},
		);

		it("keeps the flag route open without a token", async () => {
			const res = await call("/entries/ffffffff/flag", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ reason: "wrong" }),
			});
			expect(res.status).toBe(404);
		});

		it("throttles failed attempts on admin routes, and never throttles reads", async () => {
			const bad = { method: "DELETE", token: "wrong-token-0000" };
			for (let i = 0; i < 10; i++) {
				expect((await call("/entries/c0ffee00", bad, "10.0.0.1")).status).toBe(
					401,
				);
			}
			expect((await call("/entries/c0ffee00", bad, "10.0.0.1")).status).toBe(
				429,
			);
			// A correct token still gets through on that address; reads are not gated.
			expect(
				(
					await call(
						"/entries/c0ffee00",
						{ method: "DELETE", token: STABLE_TOKEN },
						"10.0.0.1",
					)
				).status,
			).toBe(404);
			expect((await call("/entries", {}, "10.0.0.1")).status).toBe(200);
		});
	});

	describe("random token (default)", () => {
		beforeEach(() => build(false));

		it("leaves reads open without a token", async () => {
			expect((await call("/entries")).status).toBe(200);
			expect((await call("/status")).status).toBe(200);
		});

		it("keeps mutation routes behind the token and never throttles them", async () => {
			for (let i = 0; i < 15; i++) {
				const res = await call(
					"/consolidate",
					{ method: "POST", token: "wrong-token-0000" },
					"10.0.0.1",
				);
				expect(res.status).toBe(401);
			}
		});
	});

	describe("session key", () => {
		it("bounds the dedupe key for very long session ids and keeps ids apart", async () => {
			build(false);
			const activate = spyOn(activation, "activate");
			const long = "s".repeat(100_000);
			const sessionKey = async (sessionId: string) => {
				activate.mockClear();
				await call("/activate?q=churn", {
					headers: { "x-knowledge-session": sessionId },
				});
				return activate.mock.calls[0][1]?.session;
			};

			const first = await sessionKey(long);
			expect(first).toBeDefined();
			expect(first?.length).toBeLessThan(200);
			expect(await sessionKey(long)).toBe(first);
			expect(await sessionKey(`${long}x`)).not.toBe(first);
		});
	});
});

describe("AuthFailureThrottle", () => {
	it("blocks after the limit and lifts the block when the window ends", () => {
		const throttle = new AuthFailureThrottle(2, 1000);
		throttle.recordFailure("a", 0);
		expect(throttle.blockedForMs("a", 1)).toBe(0);
		throttle.recordFailure("a", 1);
		expect(throttle.blockedForMs("a", 2)).toBe(998);
		expect(throttle.blockedForMs("a", 1000)).toBe(0);
	});

	it("forgets the least recently created client when full", () => {
		const throttle = new AuthFailureThrottle(1, 1000, 2);
		throttle.recordFailure("a", 0);
		throttle.recordFailure("b", 0);
		throttle.recordFailure("c", 0);
		expect(throttle.blockedForMs("a", 1)).toBe(0);
		expect(throttle.blockedForMs("b", 1)).toBeGreaterThan(0);
		expect(throttle.blockedForMs("c", 1)).toBeGreaterThan(0);
	});

	it("keeps a renewed window at the back of the eviction order", () => {
		const throttle = new AuthFailureThrottle(1, 1000, 2);
		throttle.recordFailure("a", 0);
		throttle.recordFailure("b", 0);
		// a's window has ended, so this failure opens a new one and a moves to the back.
		throttle.recordFailure("a", 1000);
		throttle.recordFailure("c", 1000);
		expect(throttle.blockedForMs("a", 1001)).toBeGreaterThan(0);
		expect(throttle.blockedForMs("b", 1001)).toBe(0);
	});
});
