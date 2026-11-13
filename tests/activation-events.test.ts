import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ActivationEngine } from "../src/activation/activate";
import {
	ActivationEventRecorder,
	hashSessionId,
} from "../src/activation/events";
import { createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { IKnowledgeStore } from "../src/db/interface";
import { ServerStateDB } from "../src/db/state/index";
import { SERVER_LOCAL_SCHEMA_VERSION } from "../src/db/state/schema";
import { logger } from "../src/logger";
import type { ActivationEventRow, ActivationResult } from "../src/types";
import { makeEntry } from "./fixtures";

const SECRET_PROMPT = "ZEBRA-PROMPT-SENTINEL how do I rotate the admin token";

function row(overrides: Partial<ActivationEventRow> = {}): ActivationEventRow {
	return {
		ts: Date.now(),
		client: "opencode",
		sessionHash: null,
		promptChars: 42,
		cueCount: 2,
		nCandidates: 5,
		nReturned: 1,
		entryIds: ["entry-1"],
		rawSims: [0.5],
		rerankScores: null,
		rerankModel: null,
		gateMode: "off",
		latencyMs: 12,
		clientDeadlineExceeded: false,
		gateDropped: 0,
		budgetDropped: 0,
		deduped: 0,
		injectedTokens: 0,
		...overrides,
	};
}

/** Read-only handle on the same file, to look at what was actually stored. */
function openRaw(path: string): Database {
	return new Database(path, { readonly: true });
}

let dir: string;
let statePath: string;
let state: ServerStateDB;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "ks-activation-events-"));
	statePath = join(dir, "state.db");
	state = new ServerStateDB(statePath);
});

afterEach(async () => {
	await state.close();
	logger.init("");
	rmSync(dir, { recursive: true, force: true });
});

describe("activation_event schema", () => {
	it("is content-free: only ids, counts, scores, latency and tags", () => {
		const raw = openRaw(statePath);
		const cols = (
			raw.prepare("PRAGMA table_info(activation_event)").all() as Array<{
				name: string;
			}>
		)
			.map((c) => c.name)
			.sort();
		raw.close();
		expect(cols).toEqual(
			[
				"budget_dropped",
				"client",
				"client_deadline_exceeded",
				"cue_count",
				"deduped",
				"entry_ids",
				"gate_dropped",
				"gate_mode",
				"id",
				"injected_tokens",
				"latency_ms",
				"n_candidates",
				"n_returned",
				"prompt_chars",
				"raw_sims",
				"rerank_scores",
				"rerank_model",
				"session_hash",
				"ts",
			].sort(),
		);
	});

	it("state DB is at the registered v6 schema", () => {
		expect(SERVER_LOCAL_SCHEMA_VERSION).toBe(6);
	});
});

describe("ServerStateDB activation methods", () => {
	it("counts injections per client inside the window", async () => {
		const now = Date.now();
		await state.insertActivationEvents([
			row({ client: "opencode", ts: now }),
			row({ client: "opencode", ts: now - 1000 }),
			row({ client: "pi", ts: now - 10 * 24 * 3600 * 1000 }),
		]);
		expect(await state.countActivationsByClient(now - 3600 * 1000)).toEqual({
			opencode: 2,
		});
	});

	it("prunes only rows older than the cutoff and returns the count", async () => {
		const now = Date.now();
		await state.insertActivationEvents([
			row({ ts: now - 91 * 24 * 3600 * 1000 }),
			row({ ts: now - 91 * 24 * 3600 * 1000 }),
			row({ ts: now - 1000 }),
		]);
		const deleted = await state.pruneActivationEvents(
			now - 90 * 24 * 3600 * 1000,
		);
		expect(deleted).toBe(2);
		expect(await state.countActivationsByClient(0)).toEqual({ opencode: 1 });
	});

	it("counts captured episodes per source", async () => {
		const raw = new Database(statePath);
		raw.exec(`INSERT INTO consolidated_episode
			(source, session_id, start_message_id, end_message_id, content_type, processed_at, entries_created)
			VALUES ('omp', 's1', 'a', 'b', 'messages', ${Date.now()}, 1),
			       ('omp', 's2', 'a', 'b', 'messages', ${Date.now()}, 0),
			       ('opencode', 's3', 'a', 'b', 'messages', 1, 0)`);
		raw.close();
		expect(await state.countEpisodesBySource(Date.now() - 3600 * 1000)).toEqual(
			{ omp: 2 },
		);
	});
});

describe("ActivationEventRecorder", () => {
	it("prunes rows older than the retention window and keeps recent rows", async () => {
		const now = Date.now();
		const recorder = new ActivationEventRecorder(state, {
			retentionMs: 90 * 24 * 3600 * 1000,
		});
		await state.insertActivationEvents([
			row({ ts: now - 100 * 24 * 3600 * 1000 }),
			row({ ts: now - 1000 }),
		]);
		await recorder.prune(now);
		expect(await state.countActivationsByClient(0)).toEqual({ opencode: 1 });
		await recorder.stop();
	});

	it("batches writes and never throws from record()", async () => {
		const insert = mock(async (_rows: ActivationEventRow[]) => {});
		const recorder = new ActivationEventRecorder(
			{ insertActivationEvents: insert, pruneActivationEvents: async () => 0 },
			{ batchSize: 50, flushDelayMs: 60_000 },
		);
		for (let i = 0; i < 60; i++) recorder.record(row({ latencyMs: i }));
		await recorder.flush();
		expect(insert.mock.calls.map((c) => c[0].length)).toEqual([50, 10]);
		await recorder.stop();
	});

	it("logs a failing write once and keeps accepting rows", async () => {
		const logPath = join(dir, "server.log");
		logger.init(logPath, "info");
		const insert = mock(async () => {
			throw new Error("disk gone");
		});
		const recorder = new ActivationEventRecorder(
			{ insertActivationEvents: insert, pruneActivationEvents: async () => 0 },
			{ flushDelayMs: 60_000 },
		);
		recorder.record(row());
		await recorder.flush();
		recorder.record(row());
		await recorder.flush();
		recorder.record(row());
		await recorder.flush();
		const warnings = readFileSync(logPath, "utf8")
			.split("\n")
			.filter((l) => l.includes("Could not write activation_event"));
		expect(warnings.length).toBe(1);
		expect(recorder.pending).toBe(0);
		expect(insert).toHaveBeenCalledTimes(3);
	});

	it("hashes session ids and never returns the raw id", () => {
		expect(hashSessionId(null)).toBeNull();
		expect(hashSessionId("")).toBeNull();
		const h = hashSessionId("session-abc");
		expect(h).toMatch(/^[0-9a-f]{16}$/);
		expect(h).not.toContain("session");
		expect(hashSessionId("session-abc")).toBe(h);
	});
});

describe("HTTP activation writes telemetry without prompt text", () => {
	const result: ActivationResult = {
		entries: [
			{
				entry: makeEntry({
					id: "11111111-1111-1111-1111-111111111111",
					content: "stored knowledge",
				}),
				rawSimilarity: 0.7312,
				similarity: 0.5,
				staleness: {
					ageDays: 1,
					strength: 0.7,
					lastAccessedDaysAgo: 0,
					mayBeStale: false,
				},
			},
		],
		query: SECRET_PROMPT,
		totalActive: 1,
		telemetry: {
			cueCount: 2,
			nCandidates: 4,
			gateMode: "off",
			gateDropped: 0,
			budgetDropped: 0,
			deduped: 0,
			injectedTokens: 0,
		},
	};

	/** Requests carry a loopback Host, as a real client would send. */
	function withHost(app: {
		request: (path: string, init?: RequestInit) => Response | Promise<Response>;
	}) {
		return {
			request: (path: string, init: RequestInit = {}) =>
				app.request(path, {
					...init,
					headers: {
						Host: "127.0.0.1:8080",
						...(init.headers as Record<string, string>),
					},
				}),
		};
	}

	function appWith(recorder: ActivationEventRecorder) {
		const activation = {
			activate: mock(async () => result),
			commitInjections: mock(() => {}),
			embeddings: {},
		} as unknown as ActivationEngine;
		const consolidation = {
			get isConsolidating() {
				return false;
			},
		} as unknown as ConsolidationEngine;
		return createApp(
			{} as IKnowledgeStore,
			state,
			activation,
			consolidation,
			"test-token-for-activation-events",
			false,
			new Set(),
			[],
			[],
			8080,
			recorder,
		);
	}

	it("GET /activate stores client tag and content-free fields; response has no telemetry", async () => {
		const recorder = new ActivationEventRecorder(state, {
			flushDelayMs: 60_000,
		});
		const app = appWith(recorder);
		const res = await withHost(app).request(
			`/activate?q=${encodeURIComponent(SECRET_PROMPT)}`,
			{
				headers: {
					"x-knowledge-client": "opencode",
					"x-knowledge-session": "sess-1",
				},
			},
		);
		expect(res.status).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body).not.toHaveProperty("telemetry");
		await recorder.flush();

		const raw = openRaw(statePath);
		const rows = raw.prepare("SELECT * FROM activation_event").all() as Array<
			Record<string, unknown>
		>;
		raw.close();
		expect(rows.length).toBe(1);
		expect(rows[0].client).toBe("opencode");
		expect(rows[0].session_hash).toBe(hashSessionId("sess-1"));
		expect(rows[0].prompt_chars).toBe(SECRET_PROMPT.length);
		expect(rows[0].n_returned).toBe(1);
		expect(JSON.parse(rows[0].entry_ids as string)).toEqual([
			"11111111-1111-1111-1111-111111111111",
		]);
		expect(JSON.parse(rows[0].raw_sims as string)).toEqual([0.7312]);
		expect(rows[0].rerank_scores).toBeNull();
		expect(rows[0].gate_mode).toBe("off");
		// No column may hold the prompt text.
		expect(JSON.stringify(rows)).not.toContain("ZEBRA-PROMPT-SENTINEL");
		await recorder.stop();
	});

	it("missing or unknown client tags are recorded as unknown", async () => {
		const recorder = new ActivationEventRecorder(state, {
			flushDelayMs: 60_000,
		});
		const app = appWith(recorder);
		await withHost(app).request("/activate?q=one");
		await withHost(app).request("/activate?q=two", {
			headers: { "x-knowledge-client": "bogus" },
		});
		await withHost(app).request("/activate?q=three&client=pi");
		await recorder.flush();
		expect(await state.countActivationsByClient(0)).toEqual({
			unknown: 2,
			pi: 1,
		});
		await recorder.stop();
	});

	it("claude-code hook records client claude-code and hashes the session id", async () => {
		const recorder = new ActivationEventRecorder(state, {
			flushDelayMs: 60_000,
		});
		const app = appWith(recorder);
		const res = await withHost(app).request("/hooks/claude-code/user-prompt", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({
				prompt: SECRET_PROMPT,
				session_id: "raw-session-id",
			}),
		});
		expect(res.status).toBe(200);
		await recorder.flush();

		const raw = openRaw(statePath);
		const rows = raw
			.prepare("SELECT client, session_hash FROM activation_event")
			.all() as Array<{
			client: string;
			session_hash: string;
		}>;
		const dump = JSON.stringify(
			raw.prepare("SELECT * FROM activation_event").all(),
		);
		raw.close();
		expect(rows).toEqual([
			{
				client: "claude-code",
				session_hash: hashSessionId("raw-session-id") as string,
			},
		]);
		expect(dump).not.toContain("raw-session-id");
		expect(dump).not.toContain("ZEBRA-PROMPT-SENTINEL");
		await recorder.stop();
	});
});
