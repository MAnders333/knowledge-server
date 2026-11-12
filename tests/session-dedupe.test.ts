import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import { SessionDedupe } from "../src/activation/session-dedupe";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import type { KnowledgeEntry } from "../src/types";
import { makeEntry } from "./fixtures";

const opts = { ttlMs: 1_000, maxSessions: 2, rescoreMargin: 0.1 };

describe("SessionDedupe", () => {
	it("suppresses a remembered entry until the TTL passes", () => {
		const d = new SessionDedupe(opts);
		d.remember("s", [{ id: "e1", rawSimilarity: 0.6 }], 0);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 999)).toBe(
			true,
		);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 1_000)).toBe(
			true,
		);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 1_001)).toBe(
			false,
		);
	});

	it("is per session: another session key sees nothing", () => {
		const d = new SessionDedupe(opts);
		d.remember("s1", [{ id: "e1", rawSimilarity: 0.6 }], 0);
		expect(d.isSuppressed("s2", { id: "e1", rawSimilarity: 0.6 }, 1)).toBe(
			false,
		);
	});

	it("re-injects an entry that now scores much higher", () => {
		const d = new SessionDedupe(opts);
		d.remember("s", [{ id: "e1", rawSimilarity: 0.6 }], 0);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.65 }, 1)).toBe(
			true,
		);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.71 }, 1)).toBe(
			false,
		);
	});

	it("keeps the TTL anchored at the injection, not at suppressed prompts", () => {
		const d = new SessionDedupe(opts);
		d.remember("s", [{ id: "e1", rawSimilarity: 0.6 }], 0);
		// A suppressed check does not renew the entry.
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 900)).toBe(
			true,
		);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 1_500)).toBe(
			false,
		);
	});

	it("a TTL of zero disables suppression", () => {
		const d = new SessionDedupe({ ...opts, ttlMs: 0 });
		d.remember("s", [{ id: "e1", rawSimilarity: 0.6 }], 0);
		expect(d.isSuppressed("s", { id: "e1", rawSimilarity: 0.6 }, 0)).toBe(
			false,
		);
		expect(d.size).toBe(0);
	});

	it("evicts the least recently used session beyond the cap", () => {
		const d = new SessionDedupe(opts);
		d.remember("s1", [{ id: "a", rawSimilarity: 0.5 }], 0);
		d.remember("s2", [{ id: "b", rawSimilarity: 0.5 }], 0);
		// Touch s1 so s2 becomes the least recently used.
		expect(d.isSuppressed("s1", { id: "a", rawSimilarity: 0.5 }, 0)).toBe(true);
		d.remember("s3", [{ id: "c", rawSimilarity: 0.5 }], 0);
		expect(d.size).toBe(2);
		expect(d.isSuppressed("s2", { id: "b", rawSimilarity: 0.5 }, 0)).toBe(
			false,
		);
		expect(d.isSuppressed("s1", { id: "a", rawSimilarity: 0.5 }, 0)).toBe(true);
	});

	it("prunes expired entries when a session is written", () => {
		const d = new SessionDedupe(opts);
		d.remember("s", [{ id: "old", rawSimilarity: 0.5 }], 0);
		d.remember("s", [{ id: "new", rawSimilarity: 0.5 }], 5_000);
		// "old" expired at 1000 and is gone; a lookup of it reports not suppressed.
		expect(d.isSuppressed("s", { id: "old", rawSimilarity: 0.5 }, 5_000)).toBe(
			false,
		);
		expect(d.isSuppressed("s", { id: "new", rawSimilarity: 0.5 }, 5_000)).toBe(
			true,
		);
	});
});

describe("session dedupe in the engine", () => {
	const gate = config.activation.gate as { sessionTtlMs: number };
	let savedTtl: number;
	beforeEach(() => {
		savedTtl = gate.sessionTtlMs;
	});
	afterEach(() => {
		gate.sessionTtlMs = savedTtl;
	});

	const QUERY_VEC = [1, 0, 0, 0, 0, 0, 0, 0];
	const entries: KnowledgeEntry[] = [
		makeEntry({
			id: "alpha-entry",
			content: "alpha knowledge",
			topics: [],
			embedding: [1, 0, 0, 0, 0, 0, 0, 0],
		}),
		makeEntry({
			id: "beta-entry",
			content: "beta knowledge",
			topics: [],
			embedding: [0.9, Math.sqrt(1 - 0.81), 0, 0, 0, 0, 0, 0],
		}),
	];

	function engine() {
		const store = {
			id: "primary",
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
			recordInjections: mock(() => Promise.resolve()),
		} as unknown as IKnowledgeStore;
		const e = new ActivationEngine(store);
		e.embeddings.embedBatch = mock(async (qs: string[]) =>
			qs.map(() => QUERY_VEC),
		) as typeof e.embeddings.embedBatch;
		return e;
	}

	it("a second prompt in the same session gets nothing already injected", async () => {
		gate.sessionTtlMs = 60_000;
		const e = engine();
		const first = await e.activate("a question about things", {
			session: "claude-code\u0000s1",
		});
		expect(first.entries.length).toBe(2);
		const second = await e.activate("a follow-up question here", {
			session: "claude-code\u0000s1",
		});
		expect(second.entries).toEqual([]);
		expect(second.telemetry?.deduped).toBe(2);
	});

	it("requests without a session key are never deduplicated", async () => {
		gate.sessionTtlMs = 60_000;
		const e = engine();
		await e.activate("a question about things");
		const again = await e.activate("a question about things");
		expect(again.entries.length).toBe(2);
		expect(again.telemetry?.deduped).toBe(0);
	});

	it("a different session key starts clean", async () => {
		gate.sessionTtlMs = 60_000;
		const e = engine();
		await e.activate("a question about things", {
			session: "claude-code\u0000s1",
		});
		const other = await e.activate("a question about things", {
			session: "claude-code\u0000s2",
		});
		expect(other.entries.length).toBe(2);
	});

	it("dry runs keep their in-memory dedupe state but write nothing", async () => {
		gate.sessionTtlMs = 60_000;
		const e = engine();
		const first = await e.activate("a question about things", {
			session: "k",
			dryRun: true,
		});
		expect(first.injection).toBeUndefined();
		const second = await e.activate("a question about things", {
			session: "k",
			dryRun: true,
		});
		expect(second.entries).toEqual([]);
	});
});
