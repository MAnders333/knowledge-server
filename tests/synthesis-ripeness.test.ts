/**
 * Tests for runKBSynthesis prompt capping, token budget, sourceId validation,
 * and ripeness of already-synthesized clusters.
 */
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { cosineSimilarity } from "../src/activation/embeddings";
import type { EmbeddingClient } from "../src/activation/embeddings";
import { config } from "../src/config";
import type {
	ConsolidationLLM,
	SynthesisResult,
} from "../src/consolidation/llm";
import { Reconsolidator } from "../src/consolidation/reconsolidate";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { approxTokens } from "../src/domain/tokens";
import { logger } from "../src/logger";
import type { KnowledgeEntry } from "../src/types";
import { makeEntry } from "./fixtures";

type SentPeer = Parameters<ConsolidationLLM["synthesizePrinciple"]>[0][number];

/** Member embeddings: all share a dominant first axis, so they form one cluster. */
function memberVec(i: number): number[] {
	return [
		1,
		((i * 37) % 101) / 100,
		((i * 53) % 97) / 100,
		(i % 13) / 50,
		0,
		0,
		0,
		0,
	];
}

/** Synthesized-entry embedding: orthogonal to all members, so results insert as novel. */
const SYNTH_EMBEDDING = [0, 0, 0, 0, 0, 0, 0, 1];

function meanVector(vectors: number[][]): number[] {
	const dims = vectors[0].length;
	const out = new Array<number>(dims).fill(0);
	for (const v of vectors) for (let d = 0; d < dims; d++) out[d] += v[d];
	return out.map((x) => x / vectors.length);
}

/** Member ids ordered closest-to-centroid first (centroid = mean of members). */
function rankByCentroid(entries: KnowledgeEntry[]): string[] {
	const centroid = meanVector(entries.map((e) => e.embedding as number[]));
	return entries
		.map((e) => ({
			id: e.id,
			sim: cosineSimilarity(e.embedding as number[], centroid),
		}))
		.sort((a, b) => b.sim - a.sim)
		.map((x) => x.id);
}

describe("runKBSynthesis — peer cap and token budget", () => {
	let db: KnowledgeDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "synthesis-ripeness-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	function makeReconsolidator(results: SynthesisResult[] = []) {
		const synthesizePrinciple = mock(
			async (_peers: SentPeer[]): Promise<SynthesisResult[]> => results,
		);
		const llm = { synthesizePrinciple } as unknown as ConsolidationLLM;
		const embeddings = {
			embed: async () => SYNTH_EMBEDDING,
		} as unknown as EmbeddingClient;
		return {
			rec: new Reconsolidator(db, embeddings, llm),
			synthesizePrinciple,
		};
	}

	it("sends at most SYNTHESIS_MAX_PEERS peers, the ones closest to the centroid", async () => {
		// 99: below the dominant-cluster guard (>= 100 members and > 50% of entries).
		const members = Array.from({ length: 99 }, (_, i) =>
			makeEntry({
				id: `m${i}`,
				content: `Member ${i} says something short.`,
				topics: ["topic"],
				embedding: memberVec(i),
			}),
		);
		for (const m of members) await db.insertEntry(m);

		const { rec, synthesizePrinciple } = makeReconsolidator();
		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		const sent = synthesizePrinciple.mock.calls[0][0];
		const cap = config.consolidation.synthesisMaxPeers;
		expect(sent.length).toBe(Math.min(99, cap));

		const expected = rankByCentroid(members).slice(0, sent.length);
		expect(sent.map((p) => p.id)).toEqual(expected);
	});

	it("drops furthest peers until the approximate token budget fits", async () => {
		// 60 members of ~3000 chars each: over the 50-peer cap AND over the token budget.
		const members = Array.from({ length: 60 }, (_, i) =>
			makeEntry({
				id: `long${i}`,
				content: `Long peer ${i} ${"x".repeat(3000)}`,
				topics: ["topic"],
				embedding: memberVec(i),
			}),
		);
		for (const m of members) await db.insertEntry(m);

		const { rec, synthesizePrinciple } = makeReconsolidator();
		await rec.runKBSynthesis(db);

		const sent = synthesizePrinciple.mock.calls[0][0];
		const budget = config.consolidation.synthesisMaxPromptTokens;
		const tokenOf = (p: { content: string; topics: string[] }) =>
			approxTokens(`${p.content} ${p.topics.join(" ")}`);
		const sentTokens = sent.reduce((sum, p) => sum + tokenOf(p), 0);

		expect(sent.length).toBeLessThan(
			Math.min(members.length, config.consolidation.synthesisMaxPeers),
		);
		expect(sentTokens).toBeLessThanOrEqual(budget);

		// Still a closest-first prefix of the ranking, and the next peer would not have fit.
		const ranking = rankByCentroid(members);
		expect(sent.map((p) => p.id)).toEqual(ranking.slice(0, sent.length));
		const nextId = ranking[sent.length];
		const next = members.find((m) => m.id === nextId);
		expect(next).toBeDefined();
		expect(sentTokens + tokenOf(next as KnowledgeEntry)).toBeGreaterThan(
			budget,
		);
	});

	it("drops sourceIds that were not among the peers sent", async () => {
		const members = Array.from({ length: 60 }, (_, i) =>
			makeEntry({
				id: `s${i}`,
				content: `Source peer ${i}.`,
				topics: ["topic"],
				embedding: memberVec(i),
			}),
		);
		for (const m of members) await db.insertEntry(m);

		const unsentId =
			rankByCentroid(members)[config.consolidation.synthesisMaxPeers];
		const sentId = rankByCentroid(members)[0];

		const { rec } = makeReconsolidator([
			{
				type: "principle",
				content: "Cited principle",
				topics: ["topic"],
				confidence: 0.8,
				sourceIds: [sentId, unsentId],
			},
		]);
		const relationSpy = spyOn(db, "insertRelation");

		await rec.runKBSynthesis(db);

		const targets = relationSpy.mock.calls.map((c) => c[0].targetId);
		expect(targets).toEqual([sentId]);
	});
});

describe("runKBSynthesis — stored principles are scrubbed", () => {
	let db: KnowledgeDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "synthesis-scrub-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("redacts a credential in a synthesized principle before it is stored", async () => {
		const members = Array.from({ length: 60 }, (_, i) =>
			makeEntry({
				id: `scrub${i}`,
				content: `Member ${i} says something short.`,
				topics: ["topic"],
				embedding: memberVec(i),
			}),
		);
		for (const m of members) await db.insertEntry(m);
		const token = ["sk-", "c".repeat(24)].join("");
		const llm = {
			synthesizePrinciple: async (): Promise<SynthesisResult[]> => [
				{
					type: "principle",
					content: `Rotate the key ${token} monthly`,
					topics: ["topic"],
					confidence: 0.8,
					sourceIds: [members[0].id],
				},
			],
		} as unknown as ConsolidationLLM;
		const embeddings = {
			embed: async () => SYNTH_EMBEDDING,
		} as unknown as EmbeddingClient;

		await new Reconsolidator(db, embeddings, llm).runKBSynthesis(db);

		const stored = (await db.getEntries({})).map((e) => e.content).join("\n");
		expect(stored).toContain("Rotate the key");
		expect(stored).not.toContain(token);
		expect(stored).toContain("[REDACTED:openai_key]");
	});
});

describe("runKBSynthesis — ripeness of already-synthesized clusters", () => {
	let db: KnowledgeDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "synthesis-ripeness-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	const flush = () => new Promise((resolve) => setTimeout(resolve, 5));

	function member(i: number, prefix = "r"): KnowledgeEntry {
		return makeEntry({
			id: `${prefix}${i}`,
			content: `Ripeness member ${prefix}${i}.`,
			topics: ["topic"],
			embedding: memberVec(i),
		});
	}

	async function insertMembers(from: number, to: number, prefix = "r") {
		for (let i = from; i < to; i++) await db.insertEntry(member(i, prefix));
	}

	function makeReconsolidator() {
		const synthesizePrinciple = mock(
			async (_peers: SentPeer[]): Promise<SynthesisResult[]> => [],
		);
		const llm = { synthesizePrinciple } as unknown as ConsolidationLLM;
		const embeddings = {
			embed: async () => SYNTH_EMBEDDING,
		} as unknown as EmbeddingClient;
		return {
			rec: new Reconsolidator(db, embeddings, llm),
			synthesizePrinciple,
		};
	}

	/** Synthesize a 20-member cluster once (never-synthesized → ripe), then settle. */
	async function synthesizedCluster(size = 20) {
		await insertMembers(0, size);
		const harness = makeReconsolidator();
		await harness.rec.runKBSynthesis(db);
		expect(harness.synthesizePrinciple).toHaveBeenCalledTimes(1);
		await flush();
		harness.synthesizePrinciple.mockClear();
		return harness;
	}

	it("a never-synthesized cluster at the minimum size is always ripe", async () => {
		await insertMembers(0, 5);
		const { rec, synthesizePrinciple } = makeReconsolidator();
		await rec.runKBSynthesis(db);
		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
	});

	it("does not re-synthesize after a single new member joins a 20-member cluster", async () => {
		const { rec, synthesizePrinciple } = await synthesizedCluster();

		await insertMembers(20, 21, "n");
		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).not.toHaveBeenCalled();
	});

	it("re-synthesizes once three members join a 20-member cluster at once", async () => {
		const { rec, synthesizePrinciple } = await synthesizedCluster();

		await insertMembers(20, 23, "n");
		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
	});

	it("accumulates new members across passes until the bar is crossed", async () => {
		const { rec, synthesizePrinciple } = await synthesizedCluster();

		await insertMembers(20, 21, "n");
		await rec.runKBSynthesis(db);
		expect(synthesizePrinciple).not.toHaveBeenCalled();

		await insertMembers(21, 23, "n");
		await rec.runKBSynthesis(db);
		// 1 joined in the earlier pass + 2 now = 3 new since last synthesis.
		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
	});

	it("uses the fraction bar for a small cluster: 2 new of 10 members is ripe", async () => {
		// 8 synthesized members + 2 new = 10 current; 2 >= 0.2 * 10 (and below the
		// absolute bar of 3), so the fraction condition alone makes it ripe.
		const { rec, synthesizePrinciple } = await synthesizedCluster(8);

		await insertMembers(8, 10, "n");
		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
	});

	it("members that only left do not count as a material change", async () => {
		const { rec, synthesizePrinciple } = await synthesizedCluster();

		for (let i = 0; i < 5; i++) {
			await db.updateEntry(`r${i}`, { status: "archived" });
		}
		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).not.toHaveBeenCalled();
	});
});

describe("runKBSynthesis - call cap, ordering, dominant cluster and centroid mode", () => {
	let db: KnowledgeDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "synthesis-cap-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		mock.restore();
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	/** Unit vector along `axis`: entries on different axes never cluster together. */
	function axisVec(axis: number): number[] {
		const v = new Array<number>(8).fill(0);
		v[axis] = 1;
		return v;
	}

	function insertAxis(axis: number, count: number, prefix: string) {
		return Promise.all(
			Array.from({ length: count }, (_, i) =>
				db.insertEntry(
					makeEntry({
						id: `${prefix}${i}`,
						content: `${prefix} content ${i}`,
						embedding: axisVec(axis),
					}),
				),
			),
		);
	}

	function makeReconsolidator() {
		const synthesizePrinciple = mock(
			async (_peers: SentPeer[]): Promise<SynthesisResult[]> => [],
		);
		const llm = { synthesizePrinciple } as unknown as ConsolidationLLM;
		const embeddings = {
			embed: async () => SYNTH_EMBEDDING,
		} as unknown as EmbeddingClient;
		return {
			rec: new Reconsolidator(db, embeddings, llm),
			synthesizePrinciple,
		};
	}

	/** Run `fn` with the given config.consolidation overrides, restoring them afterwards. */
	async function withClusterConfig(
		overrides: Partial<typeof config.consolidation>,
		fn: () => Promise<void>,
	): Promise<void> {
		const saved = {} as Record<string, unknown>;
		const target = config.consolidation as Record<string, unknown>;
		for (const key of Object.keys(overrides)) {
			saved[key] = target[key];
			target[key] = (overrides as Record<string, unknown>)[key];
		}
		try {
			await fn();
		} finally {
			for (const key of Object.keys(saved)) {
				target[key] = saved[key];
			}
		}
	}

	const flush = () => new Promise((resolve) => setTimeout(resolve, 10));

	it("caps calls per run; a deferred never-synthesized cluster stays ripe and goes next", async () => {
		await insertAxis(0, 6, "x");
		await insertAxis(1, 5, "y");
		const { rec, synthesizePrinciple } = makeReconsolidator();

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		// Larger cluster first among never-synthesized clusters.
		expect(synthesizePrinciple.mock.calls[0][0]).toHaveLength(6);
		const deferred = (await db.getClustersWithMembers()).find(
			(c) => c.memberIds.length === 5,
		);
		expect(deferred?.lastSynthesizedAt).toBeNull();

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(2);
		expect(synthesizePrinciple.mock.calls[1][0]).toHaveLength(5);

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(2);
	});

	it("a synthesized cluster deferred with >=3 new members keeps their joined_at and stays ripe", async () => {
		await insertAxis(0, 5, "x");
		const { rec, synthesizePrinciple } = makeReconsolidator();

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		const synthesizedAt = (await db.getClustersWithMembers())[0]
			.lastSynthesizedAt as number;
		expect(synthesizedAt).not.toBeNull();
		await flush();

		const newIds = ["x-new0", "x-new1", "x-new2"];
		for (const id of newIds) {
			await db.insertEntry(
				makeEntry({ id, content: id, embedding: axisVec(0) }),
			);
		}

		await rec.runKBSynthesis(db, { remaining: 0 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		const [cluster] = await db.getClustersWithMembers();
		for (const id of newIds) {
			expect(cluster.memberJoinedAt.get(id)).toBeGreaterThan(synthesizedAt);
		}

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(2);
		expect(synthesizePrinciple.mock.calls[1][0]).toHaveLength(8);
	});

	it("remaining 0 never calls the LLM but still persists clusters", async () => {
		await insertAxis(4, 5, "z");
		const { rec, synthesizePrinciple } = makeReconsolidator();

		await rec.runKBSynthesis(db, { remaining: 0 });

		expect(synthesizePrinciple).not.toHaveBeenCalled();
		expect(await db.getClustersWithMembers()).toHaveLength(1);
	});

	it("orders previously-synthesized ripe clusters oldest lastSynthesizedAt first", async () => {
		await insertAxis(0, 5, "x");
		await insertAxis(1, 5, "y");
		const { rec, synthesizePrinciple } = makeReconsolidator();

		// Synthesize X first, then Y, one per run.
		await rec.runKBSynthesis(db, { remaining: 1 });
		await flush();
		await rec.runKBSynthesis(db, { remaining: 1 });
		await flush();
		expect(synthesizePrinciple).toHaveBeenCalledTimes(2);

		// One new member each (below the >=3 bar on its own): add 3 to each.
		for (const [axis, prefix] of [
			[0, "x"],
			[1, "y"],
		] as const) {
			for (let i = 0; i < 3; i++) {
				await db.insertEntry(
					makeEntry({
						id: `${prefix}-new${i}`,
						content: `${prefix} new ${i}`,
						embedding: axisVec(axis),
					}),
				);
			}
		}

		await rec.runKBSynthesis(db, { remaining: 1 });
		expect(synthesizePrinciple).toHaveBeenCalledTimes(3);
		const peers = synthesizePrinciple.mock.calls[2][0];
		expect(peers.every((p) => p.id.startsWith("x"))).toBe(true);
	});

	it("skips the dominant cluster: no LLM call, no budget spend, not marked, warns", async () => {
		await insertAxis(0, 100, "big");
		await insertAxis(1, 5, "sm");
		const { rec, synthesizePrinciple } = makeReconsolidator();
		const warnSpy = spyOn(logger, "warn").mockImplementation(() => {});
		const budget = { remaining: 5 };

		await rec.runKBSynthesis(db, budget);

		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		const peers = synthesizePrinciple.mock.calls[0][0];
		expect(peers.every((p) => p.id.startsWith("sm"))).toBe(true);
		expect(budget.remaining).toBe(4);

		const clusters = await db.getClustersWithMembers();
		const big = clusters.find((c) => c.memberIds.length === 100);
		const small = clusters.find((c) => c.memberIds.length === 5);
		expect(big?.lastSynthesizedAt).toBeNull();
		expect(small?.lastSynthesizedAt).not.toBeNull();

		expect(
			warnSpy.mock.calls.some(
				(c) =>
					String(c[0]).includes("Skipping synthesis for dominant cluster") &&
					String(c[0]).includes("calibrate --clusters"),
			),
		).toBe(true);
	});

	it("persists the seed embedding as centroid in seed mode", async () => {
		const seedVec = [1, 0, 0, 0, 0, 0, 0, 0];
		const nearVec = [0.9, 0.1, 0, 0, 0, 0, 0, 0];
		await db.insertEntry(
			makeEntry({
				id: "seed",
				content: "seed content",
				observationCount: 9,
				embedding: seedVec,
			}),
		);
		for (let i = 0; i < 4; i++) {
			await db.insertEntry(
				makeEntry({
					id: `near${i}`,
					content: `near content ${i}`,
					observationCount: 1,
					embedding: nearVec,
				}),
			);
		}
		const { rec } = makeReconsolidator();

		await withClusterConfig({ clusterCentroidMode: "seed" }, () =>
			rec.runKBSynthesis(db).then(() => undefined),
		);

		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		for (let k = 0; k < 8; k++) {
			expect(clusters[0].centroid[k]).toBeCloseTo(seedVec[k], 5);
		}
	});

	it("keeps a seed-mode cluster ID when its seed changes but members overlap", async () => {
		// Run 1: seed `a` (highest observationCount) fixes the persisted centroid.
		const near = (tilt: number) => [1 - tilt, tilt, 0, 0, 0, 0, 0, 0];
		await db.insertEntry(
			makeEntry({
				id: "a",
				content: "a",
				observationCount: 9,
				embedding: near(0),
			}),
		);
		for (let i = 0; i < 5; i++) {
			await db.insertEntry(
				makeEntry({
					id: `m${i}`,
					content: `m${i}`,
					observationCount: 1,
					embedding: near(0.05),
				}),
			);
		}
		const { rec } = makeReconsolidator();

		await withClusterConfig({ clusterCentroidMode: "seed" }, () =>
			rec.runKBSynthesis(db).then(() => undefined),
		);
		const [before] = await db.getClustersWithMembers();

		// Run 2: a more-observed entry `g` becomes the new seed. Its centroid is
		// 0.8 similar to the old seed (< 0.9), so the centroid match fails, but the
		// member overlap is 6/7, so the persisted ID must be kept.
		await db.insertEntry(
			makeEntry({
				id: "g",
				content: "g",
				observationCount: 10,
				embedding: [0.8, 0.6, 0, 0, 0, 0, 0, 0],
			}),
		);
		await withClusterConfig({ clusterCentroidMode: "seed" }, () =>
			rec.runKBSynthesis(db).then(() => undefined),
		);

		const after = await db.getClustersWithMembers();
		expect(after).toHaveLength(1);
		expect(after[0].id).toBe(before.id);
		expect(after[0].memberIds).toHaveLength(7);
	});

	it("completes synthesis when an entry is deleted between the snapshot and persist", async () => {
		await insertAxis(0, 5, "v");
		const { rec, synthesizePrinciple } = makeReconsolidator();
		// Runs after the entry snapshot and clustering, before persistClusters: the
		// yield window in which a DELETE /entries/:id can land.
		const originalGetClusters = db.getClustersWithMembers.bind(db);
		spyOn(db, "getClustersWithMembers").mockImplementation(async () => {
			await db.deleteEntry("v0");
			return originalGetClusters();
		});

		await rec.runKBSynthesis(db);

		expect(synthesizePrinciple).toHaveBeenCalledTimes(1);
		const clusters = await db.getClustersWithMembers();
		expect(clusters).toHaveLength(1);
		expect(clusters[0].memberIds.sort()).toEqual(["v1", "v2", "v3", "v4"]);
	});

	it("yields to the event loop while matching persisted clusters", async () => {
		// Six orthogonal entries: six singleton in-memory clusters. Too little work to
		// yield during clustering, so any yield comes from the identity loop.
		const dim = 1024;
		for (let i = 0; i < 6; i++) {
			await db.insertEntry(
				makeEntry({
					id: `y${i}`,
					content: `y content ${i}`,
					embedding: Array.from({ length: dim }, (_, j) => (j === i ? 1 : 0)),
				}),
			);
		}
		// 2000 persisted clusters with no members and centroids far from every
		// in-memory cluster: each costs (6 + 1) × 1024 multiply-adds, ~14M in total,
		// which is several yields at the 4M work budget.
		await db.persistClusters(
			Array.from({ length: 2000 }, (_, i) => ({
				id: `persisted-${i}`,
				centroid: Array.from({ length: dim }, (_, j) => Math.sin(i * 7 + j)),
				memberIds: [],
				isNew: true,
				membershipChanged: true,
			})),
		);
		const { rec } = makeReconsolidator();
		const immediateSpy = spyOn(globalThis, "setImmediate");

		await rec.runKBSynthesis(db);

		expect(immediateSpy.mock.calls.length).toBeGreaterThan(0);
	});
});
