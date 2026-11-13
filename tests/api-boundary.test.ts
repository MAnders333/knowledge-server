/**
 * HTTP-level tests for the API boundary: Host/Origin validation, MCP transport
 * (per-request server, DNS-rebinding check, admin-token gating), input caps,
 * body limits, and read-only store enforcement.
 *
 * Requests go over a real loopback socket (Bun.serve on an ephemeral port).
 * Bun.fetch forwards an explicit Host header, which is how a DNS-rebinding
 * request is simulated.
 */
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Hono } from "hono";
import { ActivationEngine } from "../src/activation/activate";
import { createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { KnowledgeDB } from "../src/db/sqlite/index";
import { KnowledgeDB as KnowledgeDBImpl } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import type { KnowledgeEntry } from "../src/types";

const TOKEN = "stable-admin-token-0123456789";

/**
 * Oversized bodies are answered 413 before the server reads them. Bun 1.3.9's
 * fetch keeps such a socket in its pool while it is still uploading, so the
 * next request on it stalls until the test timeout (about 1 in 5 full runs).
 * `keepalive: false` gives each oversized request its own socket.
 */
const OVERSIZED_REQUEST = { keepalive: false } as RequestInit;
const MCP_HEADERS = {
	"Content-Type": "application/json",
	Accept: "application/json, text/event-stream",
};

function consolidationStub(): ConsolidationEngine {
	return {
		consolidate: async () => ({
			sessionsProcessed: 0,
			segmentsProcessed: 0,
			entriesCreated: 0,
			entriesUpdated: 0,
			entriesArchived: 0,
			conflictsDetected: 0,
			conflictsResolved: 0,
			duration: 0,
		}),
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

function entryFixture(id: string, content: string): KnowledgeEntry {
	const now = Date.now();
	return {
		id,
		type: "fact",
		content,
		topics: ["test"],
		confidence: 0.9,
		source: "test",
		status: "active",
		strength: 1.0,
		createdAt: now,
		updatedAt: now,
		lastAccessedAt: now,
		accessCount: 0,
		observationCount: 1,
		supersededBy: null,
		derivedFrom: [],
		embedding: [0.1, 0.2, 0.3],
	} as KnowledgeEntry;
}

/** Parse a JSON-RPC reply sent either as plain JSON or as one SSE message. */
async function readRpc(res: Response): Promise<Record<string, unknown>> {
	const text = await res.text();
	if ((res.headers.get("content-type") ?? "").includes("text/event-stream")) {
		const line = text.split("\n").find((l) => l.startsWith("data:"));
		if (!line) throw new Error(`no data line in SSE body: ${text}`);
		return JSON.parse(line.slice("data:".length));
	}
	return JSON.parse(text);
}

describe("HTTP API boundary", () => {
	let tempDir: string;
	let db: KnowledgeDB;
	let roDb: KnowledgeDB | undefined;
	let serverStateDb: ServerStateDB;
	let activation: ActivationEngine;
	let server: { port?: number; stop(force?: boolean): void };
	let base: string;
	let port: number;
	let app: Hono;
	let activateSpy: ReturnType<typeof spyOn>;

	/**
	 * Build the app for the bound port. The port is needed up front because the
	 * MCP SDK compares full Host header strings, so the server is started first
	 * with a fetch that delegates to `app`, which is assigned afterwards.
	 */
	function buildApp(
		options: {
			adminTokenIsStable?: boolean;
			readsProtected?: boolean;
			readDbs?: KnowledgeDB[];
			writableDbs?: KnowledgeDB[];
			extraAllowedHosts?: string[];
		} = {},
	): void {
		app = createApp({
			db,
			serverStateDb,
			activation,
			consolidation: consolidationStub(),
			adminToken: TOKEN,
			adminTokenIsStable: options.adminTokenIsStable ?? true,
			readsProtected: options.readsProtected ?? false,
			readDbs: options.readDbs ?? [db],
			writableDbs: options.writableDbs ?? [db],
			serverPort: port,
			bindHost: "127.0.0.1",
			extraAllowedHosts: options.extraAllowedHosts ?? [],
		});
	}

	beforeAll(() => {
		server = Bun.serve({
			port: 0,
			hostname: "127.0.0.1",
			fetch: (req) => app.fetch(req),
		});
		port = server.port as number;
		base = `http://127.0.0.1:${port}`;
	});

	afterAll(() => {
		server.stop(true);
	});

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-boundary-test-"));
		db = new KnowledgeDBImpl(
			join(tempDir, "test.db"),
			join(tempDir, "opencode-fake.db"),
		);
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
		activation = new ActivationEngine(db);
		spyOn(activation.embeddings, "embed").mockResolvedValue([0.1, 0.2, 0.3]);
		activateSpy = spyOn(activation, "activate").mockResolvedValue({
			entries: [],
			query: "",
			totalActive: 0,
		});
		buildApp({ adminTokenIsStable: false });
	});

	afterEach(async () => {
		await db.close();
		if (roDb) await roDb.close();
		roDb = undefined;
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── Host header ─────────────────────────────────────────────────────────

	describe("Host header", () => {
		it("refuses a DNS-rebinding Host on every route", async () => {
			const routes: Array<[string, string, string?]> = [
				["GET", "/activate?q=anything"],
				["GET", "/entries"],
				["GET", "/review"],
				["GET", "/status"],
				["GET", "/entries/some-id"],
				["POST", "/hooks/claude-code/user-prompt", '{"prompt":"hello there"}'],
				["POST", "/mcp", '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'],
			];
			for (const [method, path, body] of routes) {
				const res = await fetch(`${base}${path}`, {
					method,
					headers: {
						Host: "attacker.example",
						...(body ? { "Content-Type": "application/json" } : {}),
					},
					body,
				});
				expect({ path, status: res.status }).toEqual({ path, status: 403 });
			}
		});

		it("accepts localhost, 127.0.0.1 and [::1] Host values", async () => {
			for (const host of [
				`localhost:${port}`,
				`127.0.0.1:${port}`,
				`[::1]:${port}`,
			]) {
				const res = await fetch(`${base}/status`, { headers: { Host: host } });
				expect({ host, status: res.status }).toEqual({ host, status: 200 });
			}
		});

		it("refuses Host values that try to smuggle a second authority", async () => {
			for (const host of [
				"evil.example@127.0.0.1",
				"127.0.0.1.evil.example",
				"127.0.0.1/evil",
				`127.0.0.1:${port} evil.example`,
			]) {
				const res = await app.request("/status", { headers: { Host: host } });
				expect({ host, status: res.status }).toEqual({ host, status: 403 });
			}
		});

		it("refuses a request with no Host header", async () => {
			const res = await app.request("/status");
			expect(res.status).toBe(403);
		});
	});

	// ── Origin header ───────────────────────────────────────────────────────

	describe("Origin header", () => {
		it("refuses cross-origin browser requests", async () => {
			for (const origin of [
				"http://evil.example",
				"https://evil.example:3179",
				"null",
				"not a url",
			]) {
				const res = await fetch(`${base}/status`, {
					headers: { Origin: origin },
				});
				expect({ origin, status: res.status }).toEqual({ origin, status: 403 });
			}
		});

		it("accepts an Origin on an allowed host, and requests without Origin", async () => {
			const withOrigin = await fetch(`${base}/status`, {
				headers: { Origin: `http://127.0.0.1:${port}` },
			});
			expect(withOrigin.status).toBe(200);

			const withoutOrigin = await fetch(`${base}/status`);
			expect(withoutOrigin.status).toBe(200);
		});

		it("refuses an Origin with a non-http scheme on an allowed host", async () => {
			const res = await fetch(`${base}/status`, {
				headers: { Origin: `ftp://127.0.0.1:${port}` },
			});
			expect(res.status).toBe(403);
		});
	});

	describe("Fetch metadata", () => {
		it("refuses a cross-site or same-site request, which a no-cors GET can send without Origin", async () => {
			for (const site of ["cross-site", "same-site"]) {
				const res = await fetch(`${base}/status`, {
					headers: { "Sec-Fetch-Site": site },
				});
				expect({ site, status: res.status }).toEqual({ site, status: 403 });
			}
		});

		it("allows same-origin, user-initiated (none) and absent Sec-Fetch-Site", async () => {
			for (const site of ["same-origin", "none"]) {
				const res = await fetch(`${base}/status`, {
					headers: { "Sec-Fetch-Site": site },
				});
				expect({ site, status: res.status }).toEqual({ site, status: 200 });
			}
			expect((await fetch(`${base}/status`)).status).toBe(200);
		});
	});

	describe("admin routes with non-object JSON bodies", () => {
		const adminHeaders = {
			"Content-Type": "application/json",
			Authorization: `Bearer ${TOKEN}`,
		};

		for (const body of ["null", '"text"', "[1,2]", "42"]) {
			it(`answers ${body} with 400 on PATCH, flag and resolve`, async () => {
				await db.insertEntry(entryFixture("json-active", "active entry"));
				await db.insertEntry({
					...entryFixture("json-conflicted", "conflicted entry"),
					status: "conflicted",
				});

				const patch = await fetch(`${base}/entries/json-active`, {
					method: "PATCH",
					headers: adminHeaders,
					body,
				});
				expect({ body, route: "PATCH", status: patch.status }).toEqual({
					body,
					route: "PATCH",
					status: 400,
				});

				const flag = await fetch(`${base}/entries/json-active/flag`, {
					method: "POST",
					headers: adminHeaders,
					body,
				});
				expect({ body, route: "flag", status: flag.status }).toEqual({
					body,
					route: "flag",
					status: 400,
				});

				const resolve = await fetch(`${base}/entries/json-conflicted/resolve`, {
					method: "POST",
					headers: adminHeaders,
					body,
				});
				expect({ body, route: "resolve", status: resolve.status }).toEqual({
					body,
					route: "resolve",
					status: 400,
				});
			});
		}
	});

	describe("KNOWLEDGE_ALLOWED_HOSTS entries", () => {
		it("accepts a configured extra host and refuses lookalikes", async () => {
			buildApp({
				extraAllowedHosts: ["knowledge.example.com"],
				adminTokenIsStable: false,
			});
			const ok = await fetch(`${base}/status`, {
				headers: { Host: "knowledge.example.com" },
			});
			expect(ok.status).toBe(200);

			const lookalike = await fetch(`${base}/status`, {
				headers: { Host: "knowledge.example.com.evil.net" },
			});
			expect(lookalike.status).toBe(403);
		});

		it("fails at startup on a malformed entry", () => {
			expect(() => buildApp({ extraAllowedHosts: ["bad host/value"] })).toThrow(
				/KNOWLEDGE_ALLOWED_HOSTS/,
			);
		});
	});

	// ── /mcp ────────────────────────────────────────────────────────────────

	describe("/mcp", () => {
		const initialize = {
			jsonrpc: "2.0",
			id: 1,
			method: "initialize",
			params: {
				protocolVersion: "2025-03-26",
				capabilities: {},
				clientInfo: { name: "boundary-test", version: "1.0.0" },
			},
		};

		function post(body: unknown, headers: Record<string, string> = {}) {
			return fetch(`${base}/mcp`, {
				method: "POST",
				headers: { ...MCP_HEADERS, ...headers },
				body: JSON.stringify(body),
			});
		}

		it("serves three sequential requests on a stateless transport", async () => {
			const init = await post(initialize);
			expect(init.status).toBe(200);
			expect((await readRpc(init)).result).toBeDefined();

			const list = await post({ jsonrpc: "2.0", id: 2, method: "tools/list" });
			expect(list.status).toBe(200);
			const listed = (await readRpc(list)) as {
				result: { tools: Array<{ name: string }> };
			};
			expect(listed.result.tools.map((t) => t.name)).toContain("activate");

			const call = await post({
				jsonrpc: "2.0",
				id: 3,
				method: "tools/call",
				params: { name: "activate", arguments: { cues: "churn analysis" } },
			});
			expect(call.status).toBe(200);
			const called = (await readRpc(call)) as {
				id: number;
				result: { content: Array<{ text: string }> };
			};
			expect(called.id).toBe(3);
			expect(called.result.content[0].text).toContain("No relevant knowledge");
			expect(activateSpy).toHaveBeenCalledTimes(1);
		});

		it("keeps concurrent replies apart (each carries its own id)", async () => {
			const replies = await Promise.all(
				[10, 11].map((id) =>
					post({
						jsonrpc: "2.0",
						id,
						method: "tools/call",
						params: { name: "activate", arguments: { cues: `cue ${id}` } },
					}).then(readRpc),
				),
			);
			expect(replies.map((r) => r.id)).toEqual([10, 11]);
		});

		it("requires the admin token when one is configured", async () => {
			buildApp({ adminTokenIsStable: true, readsProtected: true });
			const none = await post(initialize);
			expect(none.status).toBe(401);

			const wrong = await post(initialize, {
				Authorization: "Bearer not-the-token-0000",
			});
			expect(wrong.status).toBe(401);

			const right = await post(initialize, {
				Authorization: `Bearer ${TOKEN}`,
			});
			expect(right.status).toBe(200);
			const again = await post(
				{ jsonrpc: "2.0", id: 2, method: "tools/list" },
				{ Authorization: `Bearer ${TOKEN}` },
			);
			expect(again.status).toBe(200);
		});

		it("refuses a rebinding Host on /mcp", async () => {
			const res = await post(initialize, { Host: "attacker.example" });
			expect(res.status).toBe(403);
		});
	});

	// ── Input caps and body limits ──────────────────────────────────────────

	describe("input caps", () => {
		it("/activate: keeps the full query last in the cues the engine receives", async () => {
			const qs = Array.from({ length: 40 }, (_, i) => `q=segment-${i}-text`);
			const res = await fetch(`${base}/activate?${qs.join("&")}&q=FINAL-QUERY`);
			expect(res.status).toBe(200);
			// The engine applies the cue cap (MAX_CUES) and the dedupe; see activation-cues.
			const cues = activateSpy.mock.calls[0][0] as string[];
			expect(cues[cues.length - 1]).toBe("FINAL-QUERY");
		});

		it("/activate: truncates an oversized query to head and tail", async () => {
			const long = `HEAD${"x".repeat(30_000)}TAIL`;
			const res = await app.request(`/activate?q=${encodeURIComponent(long)}`, {
				headers: { Host: `127.0.0.1:${port}` },
			});
			expect(res.status).toBe(200);
			const cue = activateSpy.mock.calls[0][0] as string;
			expect(cue.length).toBeLessThanOrEqual(20_000);
			expect(cue.startsWith("HEAD")).toBe(true);
			expect(cue.endsWith("TAIL")).toBe(true);
		});

		it("/activate: clamps limit to 1..50", async () => {
			await fetch(`${base}/activate?q=hello+there+world&limit=9999`);
			expect(activateSpy.mock.calls[0][1]).toEqual(
				expect.objectContaining({ limit: 50 }),
			);
		});

		it("/activate: the client's budget reaches the engine (codex has less)", async () => {
			await fetch(`${base}/activate?q=hello+there+world&client=codex`);
			expect(activateSpy.mock.calls[0][1]).toEqual(
				expect.objectContaining({ budgetMs: 3_500 }),
			);
			await fetch(`${base}/activate?q=hello+there+world&client=opencode`);
			expect(activateSpy.mock.calls[1][1]).toEqual(
				expect.objectContaining({ budgetMs: 4_500 }),
			);
		});

		it("hook: bounds a very long prompt before splitting into cues", async () => {
			const line = "a reasonably long prompt line of text\n";
			const prompt = line.repeat(1000); // ~38k chars, ~1000 segments
			const res = await fetch(`${base}/hooks/claude-code/user-prompt`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ prompt }),
			});
			expect(res.status).toBe(200);
			const cues = activateSpy.mock.calls[0][0] as string[];
			expect(cues.length).toBeLessThanOrEqual(16);
			for (const cue of cues) expect(cue.length).toBeLessThanOrEqual(20_000);
		});

		it("hook: refuses a body above the size limit", async () => {
			const res = await fetch(`${base}/hooks/claude-code/user-prompt`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ prompt: "x".repeat(1_100_000) }),
				...OVERSIZED_REQUEST,
			});
			expect(await res.json()).toEqual({});
			expect(activateSpy).not.toHaveBeenCalled();
		});

		it("MCP: truncates an oversized cue string to the prompt cap", async () => {
			const cues = `HEAD ${"y".repeat(30_000)} TAIL`;
			const res = await fetch(`${base}/mcp`, {
				method: "POST",
				headers: { ...MCP_HEADERS, Authorization: `Bearer ${TOKEN}` },
				body: JSON.stringify({
					jsonrpc: "2.0",
					id: 1,
					method: "tools/call",
					params: { name: "activate", arguments: { cues } },
				}),
			});
			expect(res.status).toBe(200);
			const passed = activateSpy.mock.calls[0][0] as string;
			expect(passed.length).toBeLessThanOrEqual(20_000);
			expect(passed.endsWith("TAIL")).toBe(true);
		});

		it("PATCH: refuses a body above the size limit before parsing", async () => {
			await db.insertEntry(entryFixture("big-1", "original"));
			const res = await fetch(`${base}/entries/big-1`, {
				method: "PATCH",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${TOKEN}`,
				},
				body: JSON.stringify({ content: "x".repeat(1_100_000) }),
				...OVERSIZED_REQUEST,
			});
			expect(res.status).toBe(413);
			expect((await db.getEntry("big-1"))?.content).toBe("original");
		});
	});

	// ── Read-only stores ────────────────────────────────────────────────────

	describe("read-only stores", () => {
		beforeEach(async () => {
			roDb = new KnowledgeDBImpl(
				join(tempDir, "readonly.db"),
				join(tempDir, "readonly-opencode.db"),
			);
			await roDb.insertEntry(entryFixture("ro-1", "read-only entry"));
			buildApp({
				readDbs: [db, roDb],
				writableDbs: [db],
				adminTokenIsStable: false,
			});
		});

		it("PATCH and DELETE refuse entries that live only in a read-only store", async () => {
			const auth = { Authorization: `Bearer ${TOKEN}` };
			const patched = await fetch(`${base}/entries/ro-1`, {
				method: "PATCH",
				headers: { ...auth, "Content-Type": "application/json" },
				body: JSON.stringify({ content: "changed" }),
			});
			expect(patched.status).toBe(403);

			const deleted = await fetch(`${base}/entries/ro-1`, {
				method: "DELETE",
				headers: auth,
			});
			expect(deleted.status).toBe(403);

			expect((await roDb?.getEntry("ro-1"))?.content).toBe("read-only entry");
		});

		it("resolve refuses read-only entries; unknown ids stay 404", async () => {
			const auth = { Authorization: `Bearer ${TOKEN}` };
			const resolved = await fetch(`${base}/entries/ro-1/resolve`, {
				method: "POST",
				headers: { ...auth, "Content-Type": "application/json" },
				body: JSON.stringify({ resolution: "delete" }),
			});
			expect(resolved.status).toBe(403);

			const missing = await fetch(`${base}/entries/no-such-id`, {
				method: "DELETE",
				headers: auth,
			});
			expect(missing.status).toBe(404);
		});

		it("reads still see read-only entries", async () => {
			const res = await fetch(`${base}/entries/ro-1`);
			expect(res.status).toBe(200);
		});
	});
});
