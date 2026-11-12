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
import { approxTokens } from "../src/daemon/readers/shared";
import { KnowledgeDB } from "../src/db/sqlite/index";
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
		const members = Array.from({ length: 120 }, (_, i) =>
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
		expect(sent.length).toBe(Math.min(120, cap));

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
