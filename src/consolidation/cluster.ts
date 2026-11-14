/**
 * Pure clustering helpers shared by `Reconsolidator.runKBSynthesis` and
 * `knowledge-server calibrate --clusters`. No config import, no I/O.
 */
import { approxTokens } from "../domain/tokens.js";

export type ClusterCentroidMode = "running" | "seed";

export interface ClusterableEntry {
	id: string;
	observationCount: number;
	embedding: number[];
}

export interface GreedyCluster {
	/** Vector compared against during assignment and persisted for identity
	 *  reconciliation: running mean of members ("running") or the seed entry's
	 *  embedding ("seed"). */
	centroid: number[];
	/** memberIds[0] is the seed (first entry assigned). */
	memberIds: string[];
}

export function norm(v: number[]): number {
	let s = 0;
	for (let i = 0; i < v.length; i++) s += v[i] * v[i];
	return Math.sqrt(s);
}

/**
 * Cosine similarity from precomputed norms (see `norm`): one dot product per
 * call. Same arithmetic as cosineSimilarity(), so results are identical.
 */
export function cosineWithNorms(
	a: number[],
	normA: number,
	b: number[],
	normB: number,
): number {
	if (a.length !== b.length) {
		throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
	}
	const denom = normA * normB;
	if (denom === 0) return 0;
	let dot = 0;
	for (let i = 0; i < a.length; i++) dot += a[i] * b[i];
	return dot / denom;
}

/** Async path: yield to the event loop after this many entries... */
export const CLUSTER_YIELD_EVERY_ENTRIES = 200;
/** ...or after this many multiply-adds, whichever comes first. Entry count alone
 *  is not enough: one entry costs clusters x dims, so at 10k entries with thousands
 *  of clusters 200 entries could block for seconds. */
export const CLUSTER_YIELD_WORK_BUDGET = 4_000_000;

/** Resolves after pending I/O and timers have had a turn (lets the HTTP server run). */
export function yieldToEventLoop(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

/** Visiting order for greedy clustering: observationCount desc, then id asc. */
function byVisitOrder(a: ClusterableEntry, b: ClusterableEntry): number {
	if (b.observationCount !== a.observationCount) {
		return b.observationCount - a.observationCount;
	}
	return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/**
 * Single implementation of greedy threshold clustering. Yields (value undefined)
 * every CLUSTER_YIELD_EVERY_ENTRIES entries or CLUSTER_YIELD_WORK_BUDGET
 * multiply-adds, and returns the clusters. Drive it with greedyCluster (sync) or
 * greedyClusterAsync (yields to the event loop between chunks). Output does not
 * depend on where it yields.
 *
 * Entries are visited by observationCount desc, then by id asc. The id tie-break
 * makes the visiting order a function of the entries alone, not of the order the
 * store returns them in: strength decays between runs, so an order keyed on
 * strength alone reshuffles equal-observation entries and moves borderline
 * entries between clusters (churn). Each entry joins the nearest cluster whose
 * centroid cosine similarity is >= threshold, else seeds a new cluster.
 *
 * Arithmetic order matches cosineSimilarity() so "running" mode is bit-identical
 * to the historical in-loop implementation. Norms are cached so each comparison
 * costs one dot product.
 */
function* greedyClusterSteps(
	entries: ReadonlyArray<ClusterableEntry>,
	options: { threshold: number; centroidMode: ClusterCentroidMode },
): Generator<void, GreedyCluster[], void> {
	const sorted = [...entries].sort(byVisitOrder);

	const clusters: GreedyCluster[] = [];
	const centroidNorms: number[] = [];
	let entriesSinceYield = 0;
	let workSinceYield = 0;

	for (const entry of sorted) {
		const eNorm = norm(entry.embedding);
		workSinceYield += Math.max(1, clusters.length) * entry.embedding.length;

		let nearestIdx = -1;
		let nearestSim = -1;
		for (let i = 0; i < clusters.length; i++) {
			const centroid = clusters[i].centroid;
			if (entry.embedding.length !== centroid.length) {
				throw new Error(
					`Vector dimension mismatch: ${entry.embedding.length} vs ${centroid.length}`,
				);
			}
			let dot = 0;
			for (let j = 0; j < entry.embedding.length; j++) {
				dot += entry.embedding[j] * centroid[j];
			}
			const denom = eNorm * centroidNorms[i];
			const sim = denom === 0 ? 0 : dot / denom;
			if (sim > nearestSim) {
				nearestSim = sim;
				nearestIdx = i;
			}
		}

		if (nearestIdx >= 0 && nearestSim >= options.threshold) {
			const cluster = clusters[nearestIdx];
			if (options.centroidMode === "running") {
				const n = cluster.memberIds.length;
				const newCentroid = cluster.centroid.map(
					(v, k) => (v * n + entry.embedding[k]) / (n + 1),
				);
				cluster.centroid = newCentroid;
				centroidNorms[nearestIdx] = norm(newCentroid);
			}
			cluster.memberIds.push(entry.id);
		} else {
			clusters.push({
				centroid: [...entry.embedding],
				memberIds: [entry.id],
			});
			centroidNorms.push(eNorm);
		}

		entriesSinceYield++;
		if (
			entriesSinceYield >= CLUSTER_YIELD_EVERY_ENTRIES ||
			workSinceYield >= CLUSTER_YIELD_WORK_BUDGET
		) {
			entriesSinceYield = 0;
			workSinceYield = 0;
			yield;
		}
	}

	return clusters;
}

/** Synchronous greedy clustering (tests, simulation). Same output as greedyClusterAsync. */
export function greedyCluster(
	entries: ReadonlyArray<ClusterableEntry>,
	options: { threshold: number; centroidMode: ClusterCentroidMode },
): GreedyCluster[] {
	const steps = greedyClusterSteps(entries, options);
	for (;;) {
		const next = steps.next();
		if (next.done) return next.value;
	}
}

/**
 * Greedy clustering for the server and `calibrate --clusters`: awaits
 * setImmediate between chunks so the HTTP/MCP server keeps serving requests while
 * a large pass runs. Output is identical to greedyCluster.
 */
export async function greedyClusterAsync(
	entries: ReadonlyArray<ClusterableEntry>,
	options: { threshold: number; centroidMode: ClusterCentroidMode },
): Promise<GreedyCluster[]> {
	const steps = greedyClusterSteps(entries, options);
	for (;;) {
		const next = steps.next();
		if (next.done) return next.value;
		await yieldToEventLoop();
	}
}

/** A single cluster holding more than this share of all entries is a collapse signal. */
export const DOMINANT_CLUSTER_SHARE = 0.5;
/** Below this size a single broad cluster is plausible and cheap; do not warn. */
export const DOMINANT_CLUSTER_MIN_SIZE = 100;

/**
 * The largest cluster, if it is a collapse: at least DOMINANT_CLUSTER_MIN_SIZE
 * members and more than DOMINANT_CLUSTER_SHARE of all entries. `index` is its
 * position in `clusters`, so the caller can skip it. Ties on size: the first wins.
 */
export function findDominantCluster(
	clusters: ReadonlyArray<GreedyCluster>,
	totalEntries: number,
): { index: number; size: number; share: number } | null {
	let size = 0;
	let index = -1;
	for (let i = 0; i < clusters.length; i++) {
		if (clusters[i].memberIds.length > size) {
			size = clusters[i].memberIds.length;
			index = i;
		}
	}
	if (totalEntries <= 0) return null;
	const share = size / totalEntries;
	if (size >= DOMINANT_CLUSTER_MIN_SIZE && share > DOMINANT_CLUSTER_SHARE) {
		return { index, size, share };
	}
	return null;
}

/** Skew warning limits; see findClusterSkew. */
export const SKEW_TOP_SHARE = 0.5;
export const SKEW_MIN_ENTRIES = 100;
export const SKEW_PEER_MULTIPLE = 10;

/** Which skew limits a clustering pass exceeds. */
export interface ClusterSkew {
	topThreeSize: number;
	largestSize: number;
	topShareExceeded: boolean;
	oversized: boolean;
}

/**
 * Warning-only skew check for a clustering pass. Flags when the three largest clusters
 * together hold more than SKEW_TOP_SHARE of the entries (checked from SKEW_MIN_ENTRIES
 * on), or when the largest eligible cluster (at least minMembers) has more than
 * SKEW_PEER_MULTIPLE x maxPeers members. Returns null when neither holds. Nothing is
 * skipped on this signal; the dominant-cluster guard is separate.
 */
export function findClusterSkew(
	clusters: ReadonlyArray<GreedyCluster>,
	totalEntries: number,
	opts: { minMembers: number; maxPeers: number },
): ClusterSkew | null {
	const sizes = clusters.map((c) => c.memberIds.length).sort((a, b) => b - a);
	const topThreeSize = sizes.slice(0, 3).reduce((sum, n) => sum + n, 0);
	const largestSize = sizes[0] ?? 0;
	const topShareExceeded =
		totalEntries >= SKEW_MIN_ENTRIES &&
		topThreeSize / totalEntries > SKEW_TOP_SHARE;
	const oversized =
		largestSize >= opts.minMembers &&
		largestSize > SKEW_PEER_MULTIPLE * opts.maxPeers;
	if (!topShareExceeded && !oversized) return null;
	return { topThreeSize, largestSize, topShareExceeded, oversized };
}

/**
 * Approximate prompt tokens of one synthesis peer (content plus topics). Shared by
 * selectSynthesisPeers (reconsolidate.ts) and `calibrate --clusters` so both use the
 * same estimate.
 */
export function estimateSynthesisPeerTokens(peer: {
	content: string;
	topics: string[];
}): number {
	return approxTokens(`${peer.content} ${peer.topics.join(" ")}`);
}
