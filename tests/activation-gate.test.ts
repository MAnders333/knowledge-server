import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import { formatKnowledgeLine } from "../src/activation/format";
import {
	RERANK_ABS_FLOOR_BY_MODEL,
	applyTokenBudget,
	denseFloor,
	estimateTokens,
	rerankFloor,
} from "../src/activation/gate";
import type { RerankScorer } from "../src/activation/rerank";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import type { KnowledgeEntry } from "../src/types";
import { makeEntry } from "./fixtures";

const QUERY_VEC = [1, 0, 0, 0, 0, 0, 0, 0];

/** A unit vector whose cosine with QUERY_VEC is `cos`. */
function vecWithCos(cos: number): number[] {
	return [cos, Math.sqrt(1 - cos * cos), 0, 0, 0, 0, 0, 0];
}

const gate = config.activation.gate as {
	enabled: boolean;
	rerankFloor: number | null;
	rerankRelFrac: number;
	denseMargin: number;
	tokenBudget: number;
	sessionTtlMs: number;
	strengthDampening: boolean;
};

let saved: typeof gate;
beforeEach(() => {
	saved = { ...gate };
});
afterEach(() => {
	Object.assign(gate, saved);
});

const UNKNOWN_MODEL = {
	provider: "local" as const,
	model: "Xenova/ms-marco-MiniLM-L-6-v2",
};

/** Reranker that scores each document by the first score key its text contains. */
function rerankerWith(
	identity: RerankScorer["identity"],
	scores: Record<string, number>,
): RerankScorer {
	return {
		identity,
		scoreBatch: mock(async (_q: string, docs: string[]) =>
			docs.map((d) => {
				const key = Object.keys(scores).find((k) => d.includes(k));
				return key === undefined ? 0 : (scores[key] ?? 0);
			}),
		),
	};
}

function failingReranker(): RerankScorer {
	return {
		identity: UNKNOWN_MODEL,
		scoreBatch: mock(async () => {
			throw new Error("rerank unavailable");
		}),
	};
}

interface Fixture {
	entries: KnowledgeEntry[];
	/** Entries returned as support sources, keyed by synthesized parent id. */
	sources?: Map<string, KnowledgeEntry[]>;
	contradictPairs?: Map<string, string>;
}

function storeFor(fx: Fixture): IKnowledgeStore {
	return {
		id: "primary",
		getActiveEntriesWithEmbeddings: mock(() => Promise.resolve(fx.entries)),
		scanEmbeddings: mock(() =>
			Promise.resolve(
				fx.entries.map((e) => ({
					id: e.id,
					embedding: new Float32Array(e.embedding ?? []),
				})),
			),
		),
		getEntriesByIds: mock((ids: string[]) =>
			Promise.resolve(fx.entries.filter((e) => ids.includes(e.id))),
		),
		getSupportSourcesForIds: mock(() =>
			Promise.resolve(fx.sources ?? new Map()),
		),
		getContradictPairsForIds: mock(() =>
			Promise.resolve(fx.contradictPairs ?? new Map()),
		),
		recordInjections: mock(() => Promise.resolve()),
	} as unknown as IKnowledgeStore;
}

function engineFor(fx: Fixture, reranker: RerankScorer | null = null) {
	const engine = new ActivationEngine(
		storeFor(fx),
		undefined,
		undefined,
		reranker,
	);
	engine.embeddings.embedBatch = mock(async (qs: string[]) =>
		qs.map(() => QUERY_VEC),
	) as typeof engine.embeddings.embedBatch;
	return engine;
}

const CUE = "a question about the knowledge";
const ids = (r: { entries: Array<{ entry: { id: string } }> }) =>
	r.entries.map((e) => e.entry.id);

function fact(id: string, content: string, cos: number, extra = {}) {
	return makeEntry({
		id,
		type: "fact",
		content,
		topics: [],
		embedding: vecWithCos(cos),
		...extra,
	});
}

describe("gate arithmetic", () => {
	it("uses the larger of the absolute floor and the relative fraction", () => {
		expect(
			rerankFloor({
				identity: UNKNOWN_MODEL,
				floorOverride: 0.6,
				relFrac: 0.5,
				topScore: 0.9,
			}),
		).toBe(0.6);
		expect(
			rerankFloor({
				identity: UNKNOWN_MODEL,
				floorOverride: 0.1,
				relFrac: 0.5,
				topScore: 0.9,
			}),
		).toBe(0.45);
	});

	it("uses only the relative rule for a model without an absolute floor", () => {
		expect(
			rerankFloor({
				identity: UNKNOWN_MODEL,
				floorOverride: null,
				relFrac: 0.5,
				topScore: 0.8,
			}),
		).toBe(0.4);
	});

	it("dense floor is the threshold plus the margin", () => {
		expect(denseFloor(0.42, 0.03)).toBeCloseTo(0.45);
	});

	it("keeps the top entry even over budget, then a ranking prefix only", () => {
		const kept = applyTokenBudget(
			[{ t: 50 }, { t: 5 }, { t: 1 }],
			10,
			(x) => x.t,
			() => true,
		);
		expect(kept).toEqual([{ t: 50 }]);
		const prefix = applyTokenBudget(
			[{ t: 4 }, { t: 9 }, { t: 1 }],
			10,
			(x) => x.t,
			() => true,
		);
		expect(prefix).toEqual([{ t: 4 }]);
	});

	it("estimates tokens at four characters each, rounded up", () => {
		expect(estimateTokens(0)).toBe(0);
		expect(estimateTokens(1)).toBe(1);
		expect(estimateTokens(8)).toBe(2);
	});

	it("strength dampening is off by default", () => {
		expect(saved.strengthDampening).toBe(false);
	});
});

describe("precision gate in the engine", () => {
	const fx: Fixture = {
		entries: [
			fact("alpha-entry", "alpha knowledge", 1.0),
			fact("beta-entry", "beta knowledge", 0.9),
			fact("gamma-entry", "gamma knowledge", 0.8),
		],
	};

	it("drops candidates below the relative floor and keeps the top", async () => {
		gate.rerankRelFrac = 0.5;
		const engine = engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 0.9,
				"beta knowledge": 0.4,
				"gamma knowledge": 0.05,
			}),
		);
		const result = await engine.activate(CUE);
		expect(ids(result)).toEqual(["alpha-entry"]);
		expect(result.telemetry?.gateMode).toBe("rerank");
		expect(result.telemetry?.gateDropped).toBe(2);
	});

	it("applies the RERANK_FLOOR absolute floor, even to the top entry", async () => {
		gate.rerankFloor = 0.1;
		const engine = engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 0.05,
				"beta knowledge": 0.04,
				"gamma knowledge": 0.03,
			}),
		);
		const result = await engine.activate(CUE);
		expect(result.entries).toEqual([]);
		expect(result.telemetry?.gateDropped).toBe(3);
	});

	it("keeps the top entry when the token budget is smaller than it", async () => {
		gate.tokenBudget = 1;
		const engine = engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 0.9,
				"beta knowledge": 0.8,
				"gamma knowledge": 0.7,
			}),
		);
		const result = await engine.activate(CUE);
		expect(ids(result)).toEqual(["alpha-entry"]);
		expect(result.telemetry?.budgetDropped).toBe(2);
	});

	it("stops the budget at the first entry that does not fit", async () => {
		const alphaLine = formatKnowledgeLine({
			entry: { id: "alpha-entry", type: "fact", content: "alpha knowledge" },
			staleness: { mayBeStale: false, lastAccessedDaysAgo: 0 },
		});
		gate.tokenBudget = estimateTokens(alphaLine.length);
		const engine = engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 0.9,
				"beta knowledge": 0.8,
				"gamma knowledge": 0.7,
			}),
		);
		const result = await engine.activate(CUE);
		expect(ids(result)).toEqual(["alpha-entry"]);
	});

	it("gate disabled leaves the candidates and their order alone", async () => {
		gate.enabled = false;
		const engine = engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 0.05,
				"beta knowledge": 0.04,
				"gamma knowledge": 0.03,
			}),
		);
		const result = await engine.activate(CUE);
		expect(result.entries.length).toBe(3);
		expect(result.telemetry?.gateDropped).toBe(0);
	});

	it("support sources are gated at 0.9x their parent and cannot bypass the floor", async () => {
		gate.rerankRelFrac = 0.5;
		const parent = makeEntry({
			id: "parent-principle",
			type: "principle",
			content: "zulu principle",
			topics: [],
			embedding: vecWithCos(0.9),
		});
		const source = makeEntry({
			id: "source-entry",
			type: "fact",
			content: "yankee source",
			topics: [],
		});
		const engine = engineFor(
			{
				entries: [fx.entries[0], parent],
				sources: new Map([[parent.id, [source]]]),
			},
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 1.0,
				"zulu principle": 0.55,
			}),
		);
		// Floor 0.5; the source's gate score is 0.55 × 0.9 = 0.495, so it is dropped.
		const dropped = await engine.activate(CUE);
		expect(ids(dropped)).toEqual(["alpha-entry", "parent-principle"]);
	});

	it("a support source whose parent clears the floor comfortably is kept", async () => {
		gate.rerankRelFrac = 0.5;
		const parent = makeEntry({
			id: "parent-principle",
			type: "principle",
			content: "zulu principle",
			topics: [],
			embedding: vecWithCos(0.9),
		});
		const source = makeEntry({
			id: "source-entry",
			type: "fact",
			content: "yankee source",
			topics: [],
		});
		const engine = engineFor(
			{
				entries: [fx.entries[0], parent],
				sources: new Map([[parent.id, [source]]]),
			},
			rerankerWith(UNKNOWN_MODEL, {
				"alpha knowledge": 1.0,
				"zulu principle": 0.8,
			}),
		);
		const kept = await engine.activate(CUE);
		expect(ids(kept)).toContain("source-entry");
	});

	it("dense floor applies when rerank fails, and the response fails open", async () => {
		gate.denseMargin = 0.1;
		const engine = engineFor(
			{
				entries: [
					fact("alpha-entry", "alpha knowledge", 1.0),
					fact("beta-entry", "beta knowledge", 0.6),
					fact("gamma-entry", "gamma knowledge", 0.5),
				],
			},
			failingReranker(),
		);
		const result = await engine.activate(CUE);
		// Dense floor = 0.42 + 0.1 = 0.52: gamma (0.5) is dropped, the rest stay.
		expect(result.telemetry?.gateMode).toBe("dense");
		expect(ids(result)).toEqual(["alpha-entry", "beta-entry"]);
	});

	it("flagged conflicted entries with no contradicts partner are never injected", async () => {
		const flagged = fact("flagged-entry", "alpha knowledge", 1.0, {
			status: "conflicted",
		});
		const engine = engineFor({
			entries: [flagged, fact("beta-entry", "beta knowledge", 0.9)],
		});
		const result = await engine.activate(CUE);
		expect(ids(result)).toEqual(["beta-entry"]);
	});

	it("conflicted pairs are still injected and annotated", async () => {
		const a = fact("conflict-a", "alpha knowledge", 1.0, {
			status: "conflicted",
		});
		const b = fact("conflict-b", "beta knowledge", 0.9, {
			status: "conflicted",
		});
		const engine = engineFor({
			entries: [a, b],
			contradictPairs: new Map([
				["conflict-a", "conflict-b"],
				["conflict-b", "conflict-a"],
			]),
		});
		const result = await engine.activate(CUE);
		expect(ids(result).sort()).toEqual(["conflict-a", "conflict-b"]);
		expect(result.entries[0].contradiction?.conflictingEntryId).toBe(
			"conflict-b",
		);
	});
});
