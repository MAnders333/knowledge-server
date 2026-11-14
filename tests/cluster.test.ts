/**
 * Pure clustering and synthesis-input helpers (src/consolidation/cluster.ts).
 * No DB, no LLM, no config.
 */
import { describe, expect, it } from "bun:test";
import { cosineSimilarity } from "../src/activation/embeddings";
import {
	type ClusterableEntry,
	cosineWithNorms,
	estimateSynthesisPeerTokens,
	findDominantCluster,
	greedyCluster,
	greedyClusterAsync,
	norm,
} from "../src/consolidation/cluster";
import { approxTokens } from "../src/domain/tokens";

function entry(
	id: string,
	embedding: number[],
	observationCount = 1,
): ClusterableEntry {
	return { id, embedding, observationCount };
}

/**
 * Copy of the pre-refactor loop (reconsolidate.ts, runKBSynthesis step 1). Only
 * the visiting order differs from the historical code: it adds the id tie-break
 * (observationCount desc, then id asc) that greedyClusterSteps now uses, so the
 * equivalence still isolates the arithmetic from the ordering change.
 */
function legacyGreedyCluster(
	entries: ClusterableEntry[],
	threshold: number,
): Array<{ centroid: number[]; memberIds: string[] }> {
	const sorted = [...entries].sort((a, b) =>
		b.observationCount !== a.observationCount
			? b.observationCount - a.observationCount
			: a.id < b.id
				? -1
				: a.id > b.id
					? 1
					: 0,
	);
	const inMemoryClusters: Array<{ centroid: number[]; memberIds: string[] }> =
		[];
	for (const e of sorted) {
		let nearestIdx = -1;
		let nearestSim = -1;
		for (let i = 0; i < inMemoryClusters.length; i++) {
			const sim = cosineSimilarity(e.embedding, inMemoryClusters[i].centroid);
			if (sim > nearestSim) {
				nearestSim = sim;
				nearestIdx = i;
			}
		}
		if (nearestIdx >= 0 && nearestSim >= threshold) {
			const cluster = inMemoryClusters[nearestIdx];
			const n = cluster.memberIds.length;
			cluster.centroid = cluster.centroid.map(
				(v, i) => (v * n + e.embedding[i]) / (n + 1),
			);
			cluster.memberIds.push(e.id);
		} else {
			inMemoryClusters.push({
				centroid: [...e.embedding],
				memberIds: [e.id],
			});
		}
	}
	return inMemoryClusters;
}

describe("greedyCluster", () => {
	it("puts identical embeddings in one cluster", () => {
		const entries = Array.from({ length: 5 }, (_, i) =>
			entry(`e${i}`, [1, 0, 0]),
		);
		const clusters = greedyCluster(entries, {
			threshold: 0.5,
			centroidMode: "running",
		});
		expect(clusters).toHaveLength(1);
		expect(clusters[0].memberIds).toHaveLength(5);
	});

	it("gives orthogonal unit vectors one cluster each", () => {
		const entries = [
			entry("x", [1, 0, 0]),
			entry("y", [0, 1, 0]),
			entry("z", [0, 0, 1]),
		];
		const clusters = greedyCluster(entries, {
			threshold: 0.5,
			centroidMode: "running",
		});
		expect(clusters).toHaveLength(3);
	});

	it("respects the threshold boundary", () => {
		// cos([1,0], [0.6,0.8]) = 0.6
		const entries = [entry("A", [1, 0], 2), entry("B", [0.6, 0.8], 1)];
		expect(
			greedyCluster(entries, { threshold: 0.59, centroidMode: "running" }),
		).toHaveLength(1);
		expect(
			greedyCluster(entries, { threshold: 0.61, centroidMode: "running" }),
		).toHaveLength(2);
	});

	it("seeds clusters with the highest observationCount first; ties go by id asc", () => {
		// Tied entries are given in reverse id order so the id tie-break is what
		// puts tieFirst ahead of tieSecond.
		const entries = [
			entry("low", [1, 0], 1),
			entry("tieSecond", [-1, 0], 3),
			entry("tieFirst", [0, 1], 3),
			entry("top", [1, 0], 9),
		];
		const clusters = greedyCluster(entries, {
			threshold: 0.99,
			centroidMode: "running",
		});
		expect(clusters.map((c) => c.memberIds[0])).toEqual([
			"top",
			"tieFirst",
			"tieSecond",
		]);
		expect(clusters[0].memberIds).toEqual(["top", "low"]);
	});

	it("gives the same clusters for shuffled input with equal observationCount", async () => {
		let seed = 3;
		const rand = () => {
			seed = (seed * 1103515245 + 12345) & 0x7fffffff;
			return seed / 0x7fffffff;
		};
		// All observationCount 1, so only the id tie-break orders them. Vectors are
		// spread so that greedy assignment is order-sensitive at the threshold.
		const entries = Array.from({ length: 60 }, (_, i) =>
			entry(
				`e${String(i).padStart(3, "0")}`,
				Array.from({ length: 6 }, () => rand() * 2 - 1),
				1,
			),
		);
		const options = { threshold: 0.1, centroidMode: "running" as const };
		const reference = greedyCluster(entries, options);

		for (let round = 0; round < 5; round++) {
			const shuffled = [...entries];
			for (let i = shuffled.length - 1; i > 0; i--) {
				const j = Math.floor(rand() * (i + 1));
				[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
			}
			expect(greedyCluster(shuffled, options)).toEqual(reference);
			expect(await greedyClusterAsync(shuffled, options)).toEqual(reference);
		}
	});

	it("drifts in running mode but not in seed mode", () => {
		const entries = [
			entry("A", [1, 0], 3),
			entry("B", [0.8, 0.6], 2),
			entry("C", [0.6, 0.8], 1),
		];
		const running = greedyCluster(entries, {
			threshold: 0.75,
			centroidMode: "running",
		});
		expect(running).toHaveLength(1);
		expect(running[0].memberIds).toEqual(["A", "B", "C"]);

		const seed = greedyCluster(entries, {
			threshold: 0.75,
			centroidMode: "seed",
		});
		expect(seed).toHaveLength(2);
		expect(seed[0].memberIds).toEqual(["A", "B"]);
		expect(seed[1].memberIds).toEqual(["C"]);
		expect(seed[0].centroid).toEqual([1, 0]);
	});

	it("keeps the running centroid equal to the arithmetic mean of members", () => {
		const entries = [
			entry("A", [1, 0], 3),
			entry("B", [0.8, 0.6], 2),
			entry("C", [0.6, 0.8], 1),
		];
		const [cluster] = greedyCluster(entries, {
			threshold: 0.75,
			centroidMode: "running",
		});
		const mean = [(1 + 0.8 + 0.6) / 3, (0 + 0.6 + 0.8) / 3];
		expect(cluster.centroid[0]).toBeCloseTo(mean[0], 10);
		expect(cluster.centroid[1]).toBeCloseTo(mean[1], 10);
	});

	it("throws on a dimension mismatch", () => {
		const entries = [entry("A", [1, 0]), entry("B", [1, 0, 0])];
		expect(() =>
			greedyCluster(entries, { threshold: 0.5, centroidMode: "running" }),
		).toThrow("Vector dimension mismatch");
	});

	it("is bit-identical to the legacy loop in running mode (sync and async)", async () => {
		let seed = 42;
		const rand = () => {
			seed = (seed * 1103515245 + 12345) & 0x7fffffff;
			return seed / 0x7fffffff;
		};
		const dim = 16;
		const base = Array.from({ length: 20 }, () =>
			Array.from({ length: dim }, () => rand() * 2 - 1),
		);
		const entries: ClusterableEntry[] = Array.from({ length: 300 }, (_, i) => {
			// Some vectors are exact duplicates of a base vector.
			const v =
				i % 7 === 0
					? base[i % base.length]
					: Array.from({ length: dim }, () => rand() * 2 - 1);
			return entry(`e${i}`, v, 1 + Math.floor(rand() * 5));
		});

		for (const threshold of [0.2, 0.5, 0.8]) {
			const options = { threshold, centroidMode: "running" as const };
			const expected = legacyGreedyCluster(entries, threshold);
			expect(greedyCluster(entries, options)).toEqual(expected);
			expect(await greedyClusterAsync(entries, options)).toEqual(expected);
		}
	});

	it("async path yields to the event loop before it resolves", async () => {
		let seed = 7;
		const rand = () => {
			seed = (seed * 1103515245 + 12345) & 0x7fffffff;
			return seed / 0x7fffffff;
		};
		// 1000 random 128-dim vectors at a high threshold: mostly singletons, so the
		// pass costs ~64M multiply-adds and must yield several times.
		const entries = Array.from({ length: 1000 }, (_, i) =>
			entry(
				`e${i}`,
				Array.from({ length: 128 }, () => rand() * 2 - 1),
			),
		);
		const options = { threshold: 0.99, centroidMode: "running" as const };

		let timerFired = false;
		setImmediate(() => {
			timerFired = true;
		});
		const pending = greedyClusterAsync(entries, options);
		expect(timerFired).toBe(false);
		const clusters = await pending;
		expect(timerFired).toBe(true);
		expect(clusters).toEqual(greedyCluster(entries, options));
	});
});

describe("findDominantCluster", () => {
	const cluster = (size: number) => ({
		centroid: [1],
		memberIds: Array.from({ length: size }, (_, i) => `m${i}`),
	});

	it("returns null when the largest cluster is under 100 members, even at 100% share", () => {
		expect(findDominantCluster([cluster(99)], 99)).toBeNull();
	});

	it("returns null when the share is exactly 0.5", () => {
		expect(findDominantCluster([cluster(100), cluster(100)], 200)).toBeNull();
	});

	it("returns size and share when a cluster holds more than half", () => {
		const result = findDominantCluster([cluster(101), cluster(49)], 150);
		expect(result).not.toBeNull();
		expect(result?.size).toBe(101);
		expect(result?.share).toBeCloseTo(101 / 150, 10);
	});

	it("returns the index of the largest cluster, not the first one", () => {
		const result = findDominantCluster([cluster(10), cluster(101)], 150);
		expect(result?.index).toBe(1);
		expect(result?.size).toBe(101);
	});
});

describe("estimateSynthesisPeerTokens", () => {
	it("is approxTokens of the peer content plus topics", () => {
		const peer = { content: "abcd".repeat(10), topics: ["x", "y"] };
		expect(estimateSynthesisPeerTokens(peer)).toBe(
			approxTokens(`${"abcd".repeat(10)} x y`),
		);
	});
});

describe("cosineWithNorms", () => {
	it("matches cosineSimilarity exactly when given precomputed norms", () => {
		const a = [0.3, -1.2, 4.5, 0, 2.25];
		const b = [1.1, 0.4, -0.7, 3.3, 0.9];
		expect(cosineWithNorms(a, norm(a), b, norm(b))).toBe(
			cosineSimilarity(a, b),
		);
	});

	it("returns 0 for a zero vector and throws on dimension mismatch", () => {
		expect(cosineWithNorms([0, 0], 0, [1, 2], norm([1, 2]))).toBe(0);
		expect(() => cosineWithNorms([1], 1, [1, 2], norm([1, 2]))).toThrow(
			"Vector dimension mismatch",
		);
	});
});
