import { describe, expect, it } from "bun:test";
import { prepareQuery } from "../src/activation/embeddings";
import { SessionDedupe } from "../src/activation/session-dedupe";
import {
	type Candidate,
	appendSupportSources,
	mergeCandidates,
	scoreScanRows,
} from "../src/activation/stages";
import type { IKnowledgeStore } from "../src/db/index";
import { makeEntry } from "./fixtures";

function fakeStore(id: string): IKnowledgeStore {
	return { id } as unknown as IKnowledgeStore;
}

const NOW = Date.now();

describe("scoreScanRows", () => {
	it("a row that cannot be scored does not claim its ID, so another store's copy still counts", () => {
		const a = fakeStore("a");
		const b = fakeStore("b");
		const score = scoreScanRows(
			[
				{ db: a, rows: [{ id: "x", embedding: new Float32Array([1, 0]) }] },
				{
					db: b,
					rows: [{ id: "x", embedding: new Float32Array([1, 0, 0]) }],
				},
			],
			[prepareQuery([1, 0, 0])],
			0.5,
		);
		expect(score.hits.map((h) => [h.db.id, h.id])).toEqual([["b", "x"]]);
		expect(score.mismatches).toEqual([
			{ storeId: "a", mismatchedDims: 2 },
			{ storeId: "b", mismatchedDims: undefined },
		]);
		expect([...score.seenIds]).toEqual(["x"]);
	});
});

describe("mergeCandidates", () => {
	const ann = fakeStore("ann");
	const scan = fakeStore("scan");
	const entry = makeEntry({ id: "e1", createdAt: NOW, lastAccessedAt: NOW });

	it("keeps the ANN copy on a tie and the higher scan score otherwise", () => {
		const tie = mergeCandidates(
			[[{ db: ann, entry, similarity: 0.8 }]],
			[{ db: scan, id: "e1", rawSimilarity: 0.8 }],
			[entry],
			NOW,
		);
		expect(tie.map((c) => c.owner)).toEqual([ann]);

		const better = mergeCandidates(
			[[{ db: ann, entry, similarity: 0.7 }]],
			[{ db: scan, id: "e1", rawSimilarity: 0.9 }],
			[entry],
			NOW,
		);
		expect(better.map((c) => [c.owner, c.rawSimilarity])).toEqual([
			[scan, 0.9],
		]);
	});

	it("drops a scan hit whose entry was not hydrated", () => {
		expect(
			mergeCandidates(
				[],
				[{ db: scan, id: "gone", rawSimilarity: 0.9 }],
				[],
				NOW,
			),
		).toEqual([]);
	});
});

describe("appendSupportSources", () => {
	const store = fakeStore("s");
	const parent: Candidate = {
		entry: makeEntry({ id: "p", type: "principle" }),
		rawSimilarity: 0.8,
		similarity: 0.8,
		gateScore: 0.8,
		owner: store,
		staleness: {
			ageDays: 0,
			strength: 1,
			lastAccessedDaysAgo: 0,
			mayBeStale: false,
		},
	};
	const noSessions = new SessionDedupe({
		ttlMs: 0,
		maxSessions: 10,
		rescoreMargin: 0,
	});

	it("skips flagged sources and checks the cap before the gate", () => {
		const out = appendSupportSources({
			candidates: [parent],
			sourcesByParent: new Map([
				[
					"p",
					[
						{ source: makeEntry({ id: "s1" }), owner: store },
						{
							source: makeEntry({ id: "s2", status: "conflicted" }),
							owner: store,
						},
						{ source: makeEntry({ id: "s3" }), owner: store },
					],
				],
			]),
			contradictPairs: new Map(),
			sourceFloor: null,
			maxResults: 3,
			sessions: noSessions,
			sessionKey: undefined,
			now: NOW,
		});
		expect(out.candidates.map((c) => c.entry.id)).toEqual(["p", "s1", "s3"]);
		expect(out.gateDropped).toBe(1);
		expect(out.deduped).toBe(0);
	});

	it("does not add a source this session already received", () => {
		const sessions = new SessionDedupe({
			ttlMs: 60_000,
			maxSessions: 10,
			rescoreMargin: 0.1,
		});
		sessions.remember("k", [{ id: "s1", rawSimilarity: 0.8 }], NOW);
		const out = appendSupportSources({
			candidates: [parent],
			sourcesByParent: new Map([
				["p", [{ source: makeEntry({ id: "s1" }), owner: store }]],
			]),
			contradictPairs: new Map(),
			sourceFloor: null,
			maxResults: 5,
			sessions,
			sessionKey: "k",
			now: NOW,
		});
		expect(out.candidates.map((c) => c.entry.id)).toEqual(["p"]);
		expect(out.deduped).toBe(1);
	});
});
