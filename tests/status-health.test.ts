/**
 * /status health: ok | degraded | down, with reasons, and the existing JSON shape kept.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { type StatusRuntime, createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { IKnowledgeStore } from "../src/db/interface";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import {
	type HealthInput,
	LLM_DEGRADED_FAILING_RUNS,
	LLM_DOWN_AFTER_MS,
	LLM_DOWN_FAILING_RUNS,
	OutcomeTracker,
	componentHealth,
	evaluateHealth,
} from "../src/health";
import { daemonLockPath, daemonStatusPath } from "../src/runtime-files";
import { startLockHolder } from "./support/process-lock-holder";

const TOKEN = "status-health-token-xyz";
const HOST = "127.0.0.1:3179";

const HEALTHY: HealthInput = {
	stateDbReachable: true,
	stores: [{ id: "sqlite", failing: false }],
	providers: [{ name: "embedding", open: false }],
	daemon: { expected: false, running: false, consecutiveFailures: 0 },
	consolidation: {
		consecutiveFailures: 0,
		deadLettered: 0,
		pendingSessions: 0,
	},
	llm: { failingRuns: 0, failingForMs: 0, failureClass: null },
};

describe("evaluateHealth", () => {
	it("is ok when every component is fine", () => {
		expect(evaluateHealth(HEALTHY)).toEqual({ health: "ok", reasons: [] });
	});

	it("is degraded when an expected daemon is not running", () => {
		const r = evaluateHealth({
			...HEALTHY,
			daemon: { expected: true, running: false, consecutiveFailures: 0 },
		});
		expect(r.health).toBe("degraded");
		expect(r.reasons.join(" ")).toContain("knowledge-daemon not running");
	});

	it("is ok when no daemon is expected and none runs", () => {
		expect(evaluateHealth(HEALTHY).health).toBe("ok");
	});

	it("is degraded for a failing daemon upload cycle, consolidation errors, dead letters and backlog", () => {
		const r = evaluateHealth({
			...HEALTHY,
			daemon: { expected: true, running: true, consecutiveFailures: 2 },
			consolidation: {
				consecutiveFailures: 3,
				deadLettered: 4,
				pendingSessions: 900,
			},
		});
		expect(r.health).toBe("degraded");
		const text = r.reasons.join("\n");
		expect(text).toContain("upload cycle failing (2 in a row)");
		expect(text).toContain("consolidation failing (3 consecutive errors)");
		expect(text).toContain("4 episode(s) dead-lettered");
		expect(text).toContain("backlog: 900 pending sessions");
	});

	it("is degraded when one store fails and down only when every store fails", () => {
		const one = evaluateHealth({
			...HEALTHY,
			stores: [
				{ id: "work", failing: true },
				{ id: "sqlite", failing: false },
			],
		});
		expect(one.health).toBe("degraded");
		expect(one.reasons).toEqual(['knowledge store "work" unreachable']);

		const all = evaluateHealth({
			...HEALTHY,
			stores: [
				{ id: "work", failing: true },
				{ id: "sqlite", failing: true },
			],
		});
		expect(all.health).toBe("down");
		expect(all.reasons).toContain("all knowledge stores unreachable");
	});

	it("is down when the state DB is unreachable", () => {
		const r = evaluateHealth({ ...HEALTHY, stateDbReachable: false });
		expect(r.health).toBe("down");
		expect(r.reasons).toContain("state database unreachable");
	});

	it("is degraded while a provider circuit is open", () => {
		const r = evaluateHealth({
			...HEALTHY,
			providers: [{ name: "rerank", open: true }],
		});
		expect(r.health).toBe("degraded");
		expect(r.reasons.join(" ")).toContain("rerank provider circuit open");
	});
});

describe("evaluateHealth: LLM provider outage", () => {
	const llm = (failingRuns: number, failingForMs = 0) => ({
		failingRuns,
		failingForMs,
		failureClass: "rate limited (HTTP 429)",
	});

	it("stays ok below the degraded threshold", () => {
		expect(
			evaluateHealth({ ...HEALTHY, llm: llm(LLM_DEGRADED_FAILING_RUNS - 1) })
				.health,
		).toBe("ok");
	});

	it("is degraded after consecutive failing runs and names the failure class, not a message", () => {
		const r = evaluateHealth({
			...HEALTHY,
			llm: llm(LLM_DEGRADED_FAILING_RUNS, 2 * 60 * 60 * 1000),
		});
		expect(r.health).toBe("degraded");
		expect(r.reasons.join("\n")).toContain("LLM provider failing");
		expect(r.reasons.join("\n")).toContain("rate limited (HTTP 429)");
	});

	it("is down only after many failing runs that have lasted more than an hour", () => {
		const longer = LLM_DOWN_AFTER_MS + 1;
		expect(
			evaluateHealth({ ...HEALTHY, llm: llm(LLM_DOWN_FAILING_RUNS, longer) })
				.health,
		).toBe("down");
		// Many quick retries inside the hour are not an outage of that length.
		expect(
			evaluateHealth({
				...HEALTHY,
				llm: llm(LLM_DOWN_FAILING_RUNS, LLM_DOWN_AFTER_MS),
			}).health,
		).toBe("degraded");
		expect(
			evaluateHealth({
				...HEALTHY,
				llm: llm(LLM_DOWN_FAILING_RUNS - 1, longer),
			}).health,
		).toBe("degraded");
	});
});

describe("OutcomeTracker", () => {
	it("reports a failing last outcome and opens after N consecutive failures within the window", () => {
		let now = 1_000;
		const t = new OutcomeTracker({ openAfter: 3, openMs: 100, now: () => now });
		t.recordFailure("embedding", new Error("boom"));
		t.recordFailure("embedding", new Error("boom"));
		expect(t.snapshot("embedding")).toMatchObject({
			failing: true,
			open: false,
			consecutiveFailures: 2,
			lastError: "boom",
		});
		t.recordFailure("embedding", new Error("boom"));
		expect(t.snapshot("embedding").open).toBe(true);

		now += 100;
		expect(t.snapshot("embedding").open).toBe(false);
	});

	it("a success clears the failure run and the failing flag", () => {
		const t = new OutcomeTracker();
		t.recordFailure("store:a", "x");
		t.recordSuccess("store:a");
		expect(t.snapshot("store:a")).toMatchObject({
			failing: false,
			consecutiveFailures: 0,
			open: false,
		});
	});

	it("an unknown component is healthy", () => {
		expect(new OutcomeTracker().snapshot("nothing")).toMatchObject({
			failing: false,
			open: false,
			lastSuccessAt: null,
		});
	});
});

describe("GET /status health", () => {
	let dir: string;
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let activation: ActivationEngine;
	let outcome: {
		consecutiveFailures: number;
		lastError: string | null;
		lastErrorAt: number | null;
		lastSuccessAt: number | null;
	};

	function consolidationStub(): ConsolidationEngine {
		return {
			consolidationOutcome: () => ({ ...outcome }),
			consolidate: async () => ({}),
			tryLock: () => true,
			unlock: () => {},
			close: () => {},
			get isConsolidating() {
				return false;
			},
		} as unknown as ConsolidationEngine;
	}

	function app(
		readDbs: IKnowledgeStore[] = [db],
		stateDb: ServerStateDB = serverStateDb,
		runtime: StatusRuntime = {},
	) {
		return createApp(
			db,
			stateDb,
			activation,
			consolidationStub(),
			TOKEN,
			false,
			new Set(),
			readDbs,
			[db],
			undefined,
			undefined,
			"127.0.0.1",
			[],
			runtime,
		);
	}

	async function status(
		a: ReturnType<typeof createApp>,
		authorized = false,
	): Promise<Record<string, unknown>> {
		const res = await a.request("/status", {
			headers: {
				Host: HOST,
				...(authorized ? { Authorization: `Bearer ${TOKEN}` } : {}),
			},
		});
		expect(res.status).toBe(200);
		return (await res.json()) as Record<string, unknown>;
	}

	beforeEach(() => {
		componentHealth.clear();
		outcome = {
			consecutiveFailures: 0,
			lastError: null,
			lastErrorAt: null,
			lastSuccessAt: null,
		};
		dir = mkdtempSync(join(tmpdir(), "ks-status-health-"));
		db = new KnowledgeDB(join(dir, "knowledge.db"));
		serverStateDb = new ServerStateDB(join(dir, "state.db"));
		activation = new ActivationEngine(db);
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it("reports ok with the existing fields and a liveness status", async () => {
		const body = await status(app());
		expect(body.status).toBe("ok");
		expect(body.health).toBe("ok");
		expect(body.reasons).toEqual([]);
		expect(body.version).toBeString();
		expect(body.knowledge).toBeDefined();
		expect(body.consolidation).toMatchObject({
			consecutiveFailures: 0,
			deadLettered: 0,
			pendingSessions: 0,
		});
		expect(body.config).toBeUndefined();
		expect(body.diagnostics).toBeUndefined();
	});

	it("degrades when the daemon is expected but not running, and recovers when its lock is held", async () => {
		const runtime = { dataDir: dir, daemonAutoSpawn: true };
		let body = await status(app([db], serverStateDb, runtime));
		expect(body.health).toBe("degraded");
		expect(body.reasons).toEqual([
			"knowledge-daemon not running; sessions are not being collected",
		]);

		const holder = await startLockHolder(daemonLockPath(dir));
		try {
			body = await status(app([db], serverStateDb, runtime));
			expect(body.health).toBe("ok");
			expect(body.daemon).toMatchObject({ running: true });
		} finally {
			await holder.release();
		}
	});

	it("surfaces a failing daemon upload cycle from the status file", async () => {
		mkdirSync(dir, { recursive: true });
		writeFileSync(
			daemonStatusPath(dir),
			JSON.stringify({
				pid: process.pid,
				startedAt: Date.now(),
				intervalMs: 1000,
				lastCycleAt: Date.now(),
				consecutiveFailures: 2,
				lastError: "claude-code: unreadable session",
				lastUploadAt: null,
			}),
		);
		const holder = await startLockHolder(daemonLockPath(dir));
		try {
			const body = await status(
				app([db], serverStateDb, { dataDir: dir, daemonAutoSpawn: true }),
			);
			expect(body.health).toBe("degraded");
			expect(String(body.reasons)).toContain(
				"upload cycle failing (2 in a row)",
			);
			// Public view omits the raw error text; the admin view keeps it.
			expect(JSON.stringify(body)).not.toContain("unreadable session");
			const admin = await status(
				app([db], serverStateDb, { dataDir: dir, daemonAutoSpawn: true }),
				true,
			);
			expect(JSON.stringify(admin)).toContain("unreadable session");
		} finally {
			await holder.release();
		}
	});

	it("degrades when consolidation has failed in a row", async () => {
		outcome = { ...outcome, consecutiveFailures: 3, lastError: "llm timeout" };
		const body = await status(app());
		expect(body.health).toBe("degraded");
		expect(String(body.reasons)).toContain(
			"consolidation failing (3 consecutive errors)",
		);
	});

	it("degrades when one knowledge store fails and is down when all of them do", async () => {
		const broken = Object.assign(Object.create(db) as KnowledgeDB, {
			id: "broken-store-status-test",
			getStats: async () => {
				throw new Error("connection refused");
			},
		});
		const partial = await status(app([db, broken]));
		expect(partial.health).toBe("degraded");
		expect(String(partial.reasons)).toContain(
			'knowledge store "broken-store-status-test" unreachable',
		);

		const alsoBroken = Object.assign(Object.create(broken) as KnowledgeDB, {
			id: "broken-store-status-test-2",
		});
		const down = await status(app([broken, alsoBroken]));
		expect(down.health).toBe("down");
	});

	it("reports down, with HTTP 200 and what is known, when the state DB does not answer", async () => {
		const broken = Object.assign(
			Object.create(serverStateDb) as ServerStateDB,
			{
				getConsolidationState: async () => {
					throw new Error("disk I/O error");
				},
			},
		);
		const body = await status(app([db], broken));
		expect(body.health).toBe("down");
		expect(body.reasons).toContain("state database unreachable");
		expect(body.knowledge).toBeDefined();
	});

	it("degrades while the embedding provider circuit is open", async () => {
		for (let i = 0; i < 3; i++) {
			componentHealth.recordFailure("embedding", new Error("401 unauthorized"));
		}
		const body = await status(app());
		expect(body.health).toBe("degraded");
		expect(String(body.reasons)).toContain("embedding provider circuit open");
		const admin = await status(app(), true);
		expect(admin.diagnostics).toMatchObject({
			providers: expect.arrayContaining([
				expect.objectContaining({ name: "embedding", open: true }),
			]),
		});
	});
});
