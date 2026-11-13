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
import { RerankSkippedError } from "../src/activation/rerank-remote";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import { componentHealth } from "../src/health";
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

	it("at margin 0 without rerank, support sources are not gated", async () => {
		gate.denseMargin = 0;
		const parent = makeEntry({
			id: "parent-principle",
			type: "principle",
			content: "zulu principle",
			topics: [],
			embedding: vecWithCos(0.45),
		});
		const source = makeEntry({
			id: "source-entry",
			type: "fact",
			content: "yankee source",
			topics: [],
		});
		const engine = engineFor({
			entries: [parent],
			sources: new Map([[parent.id, [source]]]),
		});
		// The parent clears the bare threshold. Gated at 0.9x it would score 0.405 and
		// fail the floor, so the source is kept only because it is not gated.
		const kept = await engine.activate(CUE, { threshold: 0.42 });
		expect(ids(kept)).toEqual(["parent-principle", "source-entry"]);
	});

	it("with a dense margin, support sources are gated at 0.9x their parent", async () => {
		gate.denseMargin = 0.01;
		const parent = makeEntry({
			id: "parent-principle",
			type: "principle",
			content: "zulu principle",
			topics: [],
			embedding: vecWithCos(0.45),
		});
		const source = makeEntry({
			id: "source-entry",
			type: "fact",
			content: "yankee source",
			topics: [],
		});
		const engine = engineFor({
			entries: [parent],
			sources: new Map([[parent.id, [source]]]),
		});
		// Floor 0.43: the parent (0.45) passes; its source (0.405) does not.
		const kept = await engine.activate(CUE, { threshold: 0.42 });
		expect(ids(kept)).toEqual(["parent-principle"]);
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
		const result = await engine.activate(CUE, { threshold: 0.42 });
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

describe("candidate filters run before the cap", () => {
	const flaggedFirst = [
		fact("flagged-0", "alpha knowledge", 1.0, { status: "conflicted" }),
		fact("flagged-1", "alpha knowledge", 1.0, { status: "conflicted" }),
		fact("flagged-2", "alpha knowledge", 1.0, { status: "conflicted" }),
	];
	const passing = Array.from({ length: 9 }, (_, i) =>
		fact(`pass-${i}`, `beta knowledge ${i}`, 0.9),
	);

	it("a flagged entry does not use up the cap: limit 4 returns 4 injectable entries", async () => {
		const engine = engineFor({ entries: [...flaggedFirst, ...passing] });
		const result = await engine.activate(CUE, { limit: 4 });
		expect(ids(result)).toEqual(["pass-0", "pass-1", "pass-2", "pass-3"]);
	});

	it("an entry deduped in the session does not use up the cap", async () => {
		gate.sessionTtlMs = 60_000;
		const engine = engineFor({ entries: passing });
		const first = await engine.activate(CUE, {
			limit: 4,
			session: "claude-code\u0000s1",
		});
		engine.commitInjections(first.injection);
		const second = await engine.activate(CUE, {
			limit: 4,
			session: "claude-code\u0000s1",
		});
		expect(ids(second)).toEqual(["pass-4", "pass-5", "pass-6", "pass-7"]);
		expect(second.telemetry?.deduped).toBe(4);
	});
});

describe("single candidate", () => {
	it("is scored by the reranker and gated on its rerank score, not the dense floor", async () => {
		gate.denseMargin = 0.05;
		const engine = engineFor(
			{ entries: [fact("lone-entry", "lone knowledge", 0.44)] },
			rerankerWith(UNKNOWN_MODEL, { "lone knowledge": 0.9 }),
		);
		const result = await engine.activate(CUE, { threshold: 0.42 });
		expect(result.telemetry?.gateMode).toBe("rerank");
		expect(ids(result)).toEqual(["lone-entry"]);
	});
});

describe("rerank call order, telemetry and health", () => {
	const fx: Fixture = {
		entries: [
			fact("low-entry", "low knowledge", 0.5),
			fact("high-entry", "high knowledge", 1.0),
			fact("mid-entry", "mid knowledge", 0.8),
		],
	};

	beforeEach(() => componentHealth.clear());
	afterEach(() => componentHealth.clear());

	it("sends candidates best dense first, so the first document is the dense best", async () => {
		const seen: string[][] = [];
		const scorer: RerankScorer = {
			identity: UNKNOWN_MODEL,
			scoreBatch: mock(async (_q: string, docs: string[]) => {
				seen.push(docs);
				return docs.map(() => 0.9);
			}),
		};
		await engineFor(fx, scorer).activate(CUE);
		expect(seen).toHaveLength(1);
		const order = seen[0].map((d) => d.match(/(high|mid|low) knowledge/)?.[1]);
		expect(order).toEqual(["high", "mid", "low"]);
	});

	it("reports the configured scorer as provider/model, not a model the endpoint names", async () => {
		const scorer: RerankScorer = {
			identity: { provider: "remote", model: "jev-1.14.0" },
			scoreBatch: mock(async (_q: string, docs: string[]) =>
				docs.map(() => 0.9),
			),
		};
		const result = await engineFor(fx, scorer).activate(CUE);
		expect(result.telemetry?.gateMode).toBe("rerank");
		expect(result.telemetry?.rerankModel).toBe("remote/jev-1.14.0");
	});

	it("reports no rerank model when the rerank stage did not score", async () => {
		const result = await engineFor(fx, failingReranker()).activate(CUE);
		expect(result.telemetry?.gateMode).toBe("dense");
		expect(result.telemetry?.rerankModel).toBeNull();
	});

	it("records rerank health once per activation, so a scorer's own breaker is not counted twice", async () => {
		await engineFor(fx, failingReranker()).activate(CUE);
		expect(componentHealth.snapshot("rerank").consecutiveFailures).toBe(1);
		await engineFor(
			fx,
			rerankerWith(UNKNOWN_MODEL, { "high knowledge": 0.9 }),
		).activate(CUE);
		expect(componentHealth.snapshot("rerank").consecutiveFailures).toBe(0);
	});
});

describe("dense margin and refused rerank calls", () => {
	beforeEach(() => {
		gate.enabled = true;
		componentHealth.clear();
	});
	afterEach(() => componentHealth.clear());

	it("applies the dense margin when rerank is disabled, not only after a failure", async () => {
		gate.denseMargin = 0.1;
		const engine = engineFor({
			entries: [
				fact("alpha-entry", "alpha knowledge", 1.0),
				fact("gamma-entry", "gamma knowledge", 0.5),
			],
		});
		const result = await engine.activate(CUE, { threshold: 0.42 });
		// Dense floor = 0.42 + 0.1 = 0.52: gamma (0.5) is dropped.
		expect(result.telemetry?.gateMode).toBe("off");
		expect(ids(result)).toEqual(["alpha-entry"]);
	});

	it("does not count a refused rerank call as a provider failure", async () => {
		const refused: RerankScorer = {
			identity: UNKNOWN_MODEL,
			scoreBatch: mock(async () => {
				throw new RerankSkippedError("rerank busy: 2 queued");
			}),
		};
		const fixture = { entries: [fact("high-entry", "high knowledge", 1.0)] };
		for (let i = 0; i < 5; i++) {
			await engineFor(fixture, refused).activate(CUE);
		}
		expect(componentHealth.snapshot("rerank").consecutiveFailures).toBe(0);
		expect(componentHealth.snapshot("rerank").open).toBe(false);
	});
});
