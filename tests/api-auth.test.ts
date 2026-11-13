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

	function build(adminTokenIsStable: boolean): void {
		app = createApp(
			db,
			serverStateDb,
			activation,
			consolidationStub(),
			STABLE_TOKEN,
			adminTokenIsStable,
		);
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

	describe("stable token", () => {
		beforeEach(() => build(true));

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

			// The valid holder on the same address is refused until the window ends,
			// while another address is unaffected.
			expect(
				(await call("/entries", { token: STABLE_TOKEN }, "10.0.0.1")).status,
			).toBe(429);
			expect(
				(await call("/entries", { token: STABLE_TOKEN }, "10.0.0.2")).status,
			).toBe(200);
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
});
