/**
 * Activation fan-out guards (src/activation/activate.ts):
 *
 *  - Scan rows whose embedding dimension differs from the query are skipped
 *    (never scored, never NaN), with one warning per streak.
 *  - The circuit breaker is keyed by store and operation: a timed-out ANN
 *    search opens its own circuit, and a fast count cannot reset it.
 */
import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import type { IKnowledgeStore } from "../src/db/interface";
import { logger } from "../src/logger";
import { makeEntry } from "./fixtures";

const QUERY_DIMS = 1536;
const QUERY = Array.from({ length: QUERY_DIMS }, (_, i) => (i === 0 ? 1 : 0));
const LEGACY = Array.from({ length: 768 }, (_, i) => (i === 0 ? 1 : 0));

interface ScanRow {
	id: string;
	embedding: number[];
}

/** Scan-only store (no findSimilarEntries): activation takes the full-scan path. */
function scanStore(id: string, state: { rows: ScanRow[] }): IKnowledgeStore {
	return {
		id,
		scanEmbeddings: mock(async () =>
			state.rows.map((r) => ({
				id: r.id,
				embedding: Float32Array.from(r.embedding),
			})),
		),
		getEntriesByIds: mock(async (ids: string[]) =>
			ids.map((i) => makeEntry({ id: i, content: `entry ${i}`, topics: [] })),
		),
		getSupportSourcesForIds: mock(async () => new Map()),
		getContradictPairsForIds: mock(async () => new Map()),
		recordInjections: mock(async () => {}),
	} as unknown as IKnowledgeStore;
}

/** ANN-capable store with a count. `search` and `count` decide each call. */
function annStore(
	id: string,
	search: () => Promise<unknown[]>,
	count: () => Promise<number>,
) {
	const findSimilarEntries = mock(search);
	const getActiveEntryCount = mock(count);
	const store = {
		id,
		findSimilarEntries,
		getActiveEntryCount,
		getEntriesByIds: mock(async () => []),
		getSupportSourcesForIds: mock(async () => new Map()),
		getContradictPairsForIds: mock(async () => new Map()),
		recordInjections: mock(async () => {}),
	} as unknown as IKnowledgeStore;
	return { store, findSimilarEntries, getActiveEntryCount };
}

function engineFor(
	readDbs: IKnowledgeStore[],
	options?: { storeTimeoutMs?: number },
) {
	const engine = new ActivationEngine(
		readDbs[0],
		readDbs,
		undefined,
		null,
		options,
	);
	engine.embeddings.embedBatch = mock(async (qs: string[]) =>
		qs.map(() => QUERY),
	) as typeof engine.embeddings.embedBatch;
	return engine;
}

const hang = () => new Promise<never>(() => {});

function warningsMentioning(
	warn: { mock: { calls: unknown[][] } },
	text: string,
): string[] {
	return warn.mock.calls
		.map((c) => String(c[0]))
		.filter((m) => m.includes(text));
}

afterEach(() => {
	mock.restore();
});

describe("scan stores: embedding dimension guard", () => {
	it("skips rows of a different dimension, keeps matching stores, and warns once", async () => {
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		spyOn(logger, "log").mockImplementation(() => {});
		const legacy = {
			rows: [
				{ id: "stale", embedding: LEGACY },
				// Same ID as a matching row in the other store: must not be dropped.
				{ id: "shared", embedding: LEGACY },
			],
		};
		const current = {
			rows: [
				{ id: "fresh", embedding: QUERY },
				{ id: "shared", embedding: QUERY },
			],
		};
		const engine = engineFor([
			scanStore("legacy", legacy),
			scanStore("current", current),
		]);

		const result = await engine.activate("a cue about the knowledge base");

		expect(result.entries.map((e) => e.entry.id).sort()).toEqual([
			"fresh",
			"shared",
		]);
		for (const e of result.entries) {
			expect(Number.isFinite(e.rawSimilarity)).toBe(true);
			expect(Number.isFinite(e.similarity)).toBe(true);
		}
		const mismatch = warningsMentioning(warn, 'Store "legacy"');
		expect(mismatch).toHaveLength(1);
		expect(mismatch[0]).toContain("768-dim");
		expect(mismatch[0]).toContain("1536-dim");
		expect(mismatch[0]).toContain("re-embedded");
	});

	it("warns again only after a scan of the store shows no mismatched rows", async () => {
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		spyOn(logger, "log").mockImplementation(() => {});
		const legacy = { rows: [{ id: "stale", embedding: LEGACY }] };
		const engine = engineFor([scanStore("legacy", legacy)]);
		const cue = "a cue about the knowledge base";

		await engine.activate(cue);
		await engine.activate(cue);
		expect(warningsMentioning(warn, 'Store "legacy"')).toHaveLength(1);

		// Re-embedded: the scan is clean, which ends the streak.
		legacy.rows = [{ id: "stale", embedding: QUERY }];
		await engine.activate(cue);

		legacy.rows = [{ id: "stale", embedding: LEGACY }];
		await engine.activate(cue);
		expect(warningsMentioning(warn, 'Store "legacy"')).toHaveLength(2);
	});
});

describe("circuit breaker: keyed by store and operation", () => {
	it("a timed-out ANN search opens its circuit; a fast count does not reset it", async () => {
		const STORE_TIMEOUT_MS = 40;
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		spyOn(logger, "log").mockImplementation(() => {});
		const ann = annStore(
			"ann",
			() => hang(),
			async () => 2,
		);
		const engine = engineFor([ann.store], {
			storeTimeoutMs: STORE_TIMEOUT_MS,
		});

		const totals: number[] = [];
		for (let i = 0; i < 3; i++) {
			const r = await engine.activate(`single cue number ${i} for the store`);
			totals.push(r.totalActive);
		}
		expect(ann.findSimilarEntries.mock.calls.length).toBe(3);

		// The circuit is open: the fourth activation skips the ANN search. The call
		// count is the proof; no timing is asserted.
		const fourth = await engine.activate("single cue number 3 for the store");

		expect(ann.findSimilarEntries.mock.calls.length).toBe(3);
		expect(ann.getActiveEntryCount.mock.calls.length).toBe(4);
		expect([...totals, fourth.totalActive]).toEqual([2, 2, 2, 2]);

		const opened = warningsMentioning(warn, 'Store "ann"');
		expect(opened).toHaveLength(1);
		expect(opened[0]).toContain("ANN search");
		expect(opened[0]).toContain("circuit open");
	});

	it("a success of the same operation resets its failures", async () => {
		spyOn(logger, "warn").mockImplementation(() => {});
		spyOn(logger, "log").mockImplementation(() => {});
		// Times out, times out, answers, times out twice, answers.
		const outcomes = ["hang", "hang", "ok", "hang", "hang", "ok"];
		let call = 0;
		const ann = annStore(
			"ann",
			() => (outcomes[call++] === "ok" ? Promise.resolve([]) : hang()),
			async () => 2,
		);
		const engine = engineFor([ann.store], { storeTimeoutMs: 20 });

		for (let i = 0; i < outcomes.length; i++) {
			await engine.activate(`cue number ${i} about the store`);
		}
		// Had the answer not reset the failures, the circuit would have opened
		// on the fifth call and the sixth would never reach the store.
		expect(ann.findSimilarEntries.mock.calls.length).toBe(outcomes.length);
	});
});
