import { afterEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ActivationEngine } from "../src/activation/activate";
import { ActivationEngine as Engine } from "../src/activation/activate";
import { createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { IKnowledgeStore } from "../src/db/interface";
import { ServerStateDB } from "../src/db/state/index";
import type { ActivationResult, KnowledgeEntry } from "../src/types";
import { makeEntry } from "./fixtures";

const QUERY_VEC = [1, 0, 0, 0, 0, 0, 0, 0];

function entryIn(id: string, cos: number): KnowledgeEntry {
	return makeEntry({
		id,
		content: `${id} knowledge`,
		topics: [],
		embedding: [cos, Math.sqrt(1 - cos * cos), 0, 0, 0, 0, 0, 0],
	});
}

/** A scan-path store holding `entries`. Records injections into `recordInjections`. */
function scanStore(id: string, entries: KnowledgeEntry[]) {
	const recordInjections = mock(() => Promise.resolve());
	const store = {
		id,
		scanEmbeddings: mock(() =>
			Promise.resolve(
				entries.map((e) => ({
					id: e.id,
					embedding: new Float32Array(e.embedding ?? []),
				})),
			),
		),
		getEntriesByIds: mock((ids: string[]) =>
			Promise.resolve(entries.filter((e) => ids.includes(e.id))),
		),
		getSupportSourcesForIds: mock(() => Promise.resolve(new Map())),
		getContradictPairsForIds: mock(() => Promise.resolve(new Map())),
		recordInjections,
	} as unknown as IKnowledgeStore;
	return { store, recordInjections };
}

describe("injection recording goes to the owning writable store", () => {
	it("records each entry in the store that returned it, skipping read-only stores", async () => {
		const a = scanStore("primary", [entryIn("entry-a", 1.0)]);
		const b = scanStore("domain-b", [entryIn("entry-b", 0.9)]);
		const c = scanStore("read-only-c", [entryIn("entry-c", 0.95)]);
		const engine = new Engine(
			a.store,
			[a.store, b.store, c.store],
			[a.store, b.store],
		);
		engine.embeddings.embedBatch = mock(async (qs: string[]) =>
			qs.map(() => QUERY_VEC),
		) as typeof engine.embeddings.embedBatch;

		const result = await engine.activate("a question about storage");
		expect(result.entries.map((e) => e.entry.id).sort()).toEqual([
			"entry-a",
			"entry-b",
			"entry-c",
		]);
		expect(result.injection?.writes.map((w) => w.storeId).sort()).toEqual([
			"domain-b",
			"primary",
		]);

		engine.commitInjections(result.injection);
		expect(a.recordInjections.mock.calls).toEqual([
			[["entry-a"], expect.any(Number)],
		]);
		expect(b.recordInjections.mock.calls).toEqual([
			[["entry-b"], expect.any(Number)],
		]);
		expect(c.recordInjections).not.toHaveBeenCalled();
	});

	it("writes one batch per store, not one per entry", async () => {
		const a = scanStore("primary", [
			entryIn("entry-1", 1.0),
			entryIn("entry-2", 0.95),
			entryIn("entry-3", 0.9),
		]);
		const engine = new Engine(a.store);
		engine.embeddings.embedBatch = mock(async (qs: string[]) =>
			qs.map(() => QUERY_VEC),
		) as typeof engine.embeddings.embedBatch;
		const result = await engine.activate("a question about storage");
		engine.commitInjections(result.injection);
		expect(a.recordInjections).toHaveBeenCalledTimes(1);
		expect(a.recordInjections.mock.calls[0][0]).toHaveLength(3);
	});

	it("a failing store is logged, not thrown to the caller", async () => {
		const a = scanStore("primary", [entryIn("entry-a", 1.0)]);
		a.recordInjections.mockImplementation(() =>
			Promise.reject(new Error("disk full")),
		);
		const engine = new Engine(a.store);
		engine.embeddings.embedBatch = mock(async (qs: string[]) =>
			qs.map(() => QUERY_VEC),
		) as typeof engine.embeddings.embedBatch;
		const result = await engine.activate("a question about storage");
		expect(() => engine.commitInjections(result.injection)).not.toThrow();
		await Promise.resolve();
	});
});

describe("abandoned requests are not recorded as injections", () => {
	let dir: string | null = null;
	afterEach(() => {
		if (dir) rmSync(dir, { recursive: true, force: true });
		dir = null;
	});

	const result: ActivationResult = {
		entries: [
			{
				entry: entryIn("served-entry", 0.9),
				rawSimilarity: 0.9,
				similarity: 0.9,
				staleness: {
					ageDays: 1,
					strength: 1,
					lastAccessedDaysAgo: 0,
					mayBeStale: false,
				},
			},
		],
		query: "a question",
		totalActive: 1,
		telemetry: {
			cueCount: 1,
			nCandidates: 1,
			gateMode: "off",
			gateDropped: 0,
			budgetDropped: 0,
			deduped: 0,
			injectedTokens: 10,
		},
		injection: { writes: [{ storeId: "primary", entryIds: ["served-entry"] }] },
	};

	function appWith() {
		dir = mkdtempSync(join(tmpdir(), "ks-access-"));
		const state = new ServerStateDB(join(dir, "state.db"));
		const commitInjections = mock(() => {});
		const activate = mock(async () => result);
		const activation = {
			activate,
			commitInjections,
			embeddings: {},
		} as unknown as ActivationEngine;
		const consolidation = {
			get isConsolidating() {
				return false;
			},
		} as unknown as ConsolidationEngine;
		const app = createApp({
			db: {} as IKnowledgeStore,
			serverStateDb: state,
			activation,
			consolidation,
			adminToken: "test-token-for-access",
			readDbs: [],
			writableDbs: [],
			serverPort: 8080,
		});
		return { app, commitInjections, activate };
	}

	const hook = (
		app: { request: (p: string, i?: RequestInit) => unknown },
		signal?: AbortSignal,
	) =>
		app.request("/hooks/claude-code/user-prompt", {
			method: "POST",
			headers: { "content-type": "application/json", Host: "127.0.0.1:8080" },
			body: JSON.stringify({
				prompt: "how do we rotate the admin token today",
				session_id: "sess-1",
			}),
			signal,
		});

	it("commits injections after a served hook response", async () => {
		const { app, commitInjections } = appWith();
		const res = (await hook(app)) as Response;
		expect(res.status).toBe(200);
		expect(commitInjections).toHaveBeenCalledTimes(1);
	});

	it("does not commit injections when the client has already gone", async () => {
		const { app, commitInjections } = appWith();
		const res = (await hook(app, AbortSignal.abort())) as Response;
		expect(res.status).toBe(200);
		expect(commitInjections).not.toHaveBeenCalled();
	});

	it("passes a client-scoped session key only when a session id was sent", async () => {
		const { app, activate } = appWith();
		await hook(app);
		expect(activate.mock.calls[0][1]).toEqual(
			expect.objectContaining({ session: "claude-code\u0000sess-1" }),
		);
	});
});
