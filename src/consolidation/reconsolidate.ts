import { randomUUID } from "node:crypto";
import type { EmbeddingClient } from "../activation/embeddings.js";
import {
	cosineSimilarity,
	formatEmbeddingText,
} from "../activation/embeddings.js";
import { config } from "../config.js";
import type { IKnowledgeStore } from "../db/index.js";
import { computeStrength } from "../domain/strength.js";
import { logger } from "../logger.js";
import { redactSecrets } from "../redact.js";
import { clampKnowledgeType } from "../types.js";
import type { KnowledgeEntry } from "../types.js";
import {
	CLUSTER_YIELD_WORK_BUDGET,
	cosineWithNorms,
	estimateSynthesisPeerTokens,
	findDominantCluster,
	greedyClusterAsync,
	norm,
	yieldToEventLoop,
} from "./cluster.js";
import type { ConsolidationLLM, ExtractedKnowledge } from "./llm.js";

/**
 * Cosine similarity threshold for matching an in-memory cluster centroid to a
 * persisted cluster row. Above this threshold, the persisted cluster ID is reused;
 * below it, a new cluster ID is created.
 */
const CLUSTER_IDENTITY_THRESHOLD = 0.9;

/**
 * Member-overlap (Jaccard) fallback for identity reconciliation. In seed mode a
 * cluster's persisted centroid is its seed's embedding, which changes whenever the
 * seed changes, so the centroid match can fail for a cluster that is otherwise the
 * same. Persisted and in-memory clusters with Jaccard >= this value are the same
 * cluster and keep the persisted ID.
 */
const CLUSTER_IDENTITY_OVERLAP_THRESHOLD = 0.5;

/**
 * Cosine similarity threshold for assigning an entry to an existing cluster
 * during greedy clustering. Below this threshold a new cluster is started.
 * Model-dependent: must sit above the embedding model's noise floor or every
 * entry joins the first cluster. Calibrate with `knowledge-server calibrate --clusters`.
 * Synthesis quality is instead governed by CLUSTER_MIN_MEMBERS (minimum size
 * before synthesis fires).
 * Configurable via CLUSTER_ASSIGNMENT_THRESHOLD env var (see config.ts).
 */
const CLUSTER_ASSIGNMENT_THRESHOLD =
	config.consolidation.clusterAssignmentThreshold;

/**
 * Minimum number of members a cluster must have to be eligible for synthesis.
 * Requires enough distinct observations that a structural invariant has likely
 * genuinely emerged, rather than being a coincidence of a few related entries.
 * Configurable via CLUSTER_MIN_MEMBERS env var (see config.ts).
 */
const CLUSTER_MIN_MEMBERS = config.consolidation.clusterMinMembers;

/**
 * Shared, mutable synthesis-call budget for one consolidation run. Created by
 * ConsolidationEngine.runSynthesis() and passed to every runKBSynthesis() call
 * so CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN spans all stores.
 */
export interface SynthesisCallBudget {
	remaining: number;
}

/**
 * Choose the cluster members sent to synthesizePrinciple for one call.
 * Members are ranked by cosine similarity to the cluster centroid (closest first),
 * capped at SYNTHESIS_MAX_PEERS, then trimmed furthest-first until the approximate
 * prompt size fits SYNTHESIS_MAX_PROMPT_TOKENS. The closest member is always kept.
 */
function selectSynthesisPeers(
	clusterId: string,
	peers: Array<KnowledgeEntry & { embedding: number[] }>,
	centroid: number[],
): Array<KnowledgeEntry & { embedding: number[] }> {
	const ranked = peers
		.map((peer) => ({ peer, sim: cosineSimilarity(peer.embedding, centroid) }))
		.sort((a, b) => b.sim - a.sim);

	const { synthesisMaxPeers, synthesisMaxPromptTokens } = config.consolidation;
	const selected: Array<KnowledgeEntry & { embedding: number[] }> = [];
	let tokens = 0;
	for (const { peer } of ranked) {
		if (selected.length >= synthesisMaxPeers) break;
		const peerTokens = estimateSynthesisPeerTokens(peer);
		if (selected.length > 0 && tokens + peerTokens > synthesisMaxPromptTokens)
			break;
		selected.push(peer);
		tokens += peerTokens;
	}

	if (selected.length < peers.length) {
		logger.log(
			`[synthesis] cluster ${clusterId}: capped peers ${peers.length}→${selected.length} (~${tokens} tokens)`,
		);
	}
	return selected;
}

/**
 * True when an already-synthesized cluster has changed materially since its last
 * synthesis: at least SYNTHESIS_MIN_NEW_MEMBERS current members joined since, or at
 * least SYNTHESIS_MIN_NEW_FRACTION of its members did. A member counts as new when its
 * persisted join time is absent (joined in this pass) or after lastSynthesizedAt.
 * Members that only left never count.
 */
function hasMaterialChange(
	memberIds: string[],
	joinedAt: Map<string, number>,
	lastSynthesizedAt: number,
): boolean {
	let newSince = 0;
	for (const id of memberIds) {
		const joined = joinedAt.get(id);
		if (joined === undefined || joined > lastSynthesizedAt) newSince++;
	}
	if (newSince === 0) return false;
	const { synthesisMinNewMembers, synthesisMinNewFraction } =
		config.consolidation;
	return (
		newSince >= synthesisMinNewMembers ||
		newSince >= synthesisMinNewFraction * memberIds.length
	);
}

/**
 * Handles the reconsolidation of a single extracted knowledge entry
 * against the current knowledge base.
 *
 * Responsibilities:
 * - Embed the extracted entry
 * - Find the nearest existing entry by cosine similarity
 * - If above reconsolidationThreshold: ask the LLM to decide keep/update/replace/insert
 * - If below threshold: insert directly as a novel entry
 */
export class Reconsolidator {
	private db: IKnowledgeStore;
	private embeddings: EmbeddingClient;
	private llm: ConsolidationLLM;

	constructor(
		db: IKnowledgeStore,
		embeddings: EmbeddingClient,
		llm: ConsolidationLLM,
	) {
		this.db = db;
		this.embeddings = embeddings;
		this.llm = llm;
	}

	/**
	 * Reconsolidate a single extracted entry against existing knowledge.
	 *
	 * Flow:
	 * 1. Embed the extracted entry
	 * 2. Find nearest existing entry by cosine similarity
	 * 3. If similarity > reconsolidationThreshold: focused LLM decision
	 *    - "keep"    → discard extracted entry
	 *    - "update"  → merge into existing entry in place
	 *    - "replace" → update existing entry content entirely
	 *    - "insert"  → both are genuinely distinct, insert new
	 * 4. If similarity < threshold: insert directly (clearly novel)
	 */
	async reconsolidate(
		entry: ExtractedKnowledge,
		sessionIds: string[],
		/**
		 * Live in-memory cache of active entries with embeddings.
		 * Passed as a Map so callers can mutate it in place on insert/update,
		 * avoiding a full DB reload (which was O(n × m) per chunk).
		 *
		 * After an "update"/"replace" decision, the entry is updated in place with
		 * its new content and fresh embedding (computed and written to DB atomically
		 * via mergeEntry). Later extractions in the same chunk deduplicate against
		 * the correct vector; the contradiction scan sees accurate cosine distances.
		 */
		entriesMap: Map<string, KnowledgeEntry & { embedding: number[] }>,
		callbacks: {
			onInsert: (
				inserted: KnowledgeEntry & { embedding?: number[] },
			) => void | Promise<void>;
			onUpdate: (
				id: string,
				updated: Partial<KnowledgeEntry>,
				freshEmbedding: number[],
			) => void | Promise<void>;
			onKeep: () => void | Promise<void>;
		},
		/**
		 * Timestamp of the source session (unix ms, capped at Date.now()).
		 *
		 * Used as `createdAt` / `lastAccessedAt` for newly inserted entries so that
		 * knowledge extracted from old sessions starts with the correct decay already
		 * applied. Without this, an entry from a 6-month-old session would be inserted
		 * with `lastAccessedAt = now` and appear fresh — understating its age.
		 *
		 * Defaults to Date.now() when not supplied (backwards-compatible).
		 */
		sessionTimestamp?: number,
		/**
		 * Pre-computed embedding for the entry content.
		 *
		 * When provided, skips the embed() call for the new entry. Used by
		 * runKBSynthesis, which already embeds the synthesized content to feed
		 * reconsolidate() — passing it here avoids a redundant second API call.
		 *
		 * The caller is responsible for ensuring this embedding was produced with
		 * formatEmbeddingText(type, content, topics) — the same format used internally.
		 */
		precomputedEmbedding?: number[],
		/**
		 * Log prefix used in all logger calls from this invocation.
		 * Defaults to "consolidation". Pass "synthesis" when called from runKBSynthesis
		 * so synthesis-time reconsolidation doesn't appear as consolidation activity in logs.
		 */
		logPrefix: "consolidation" | "synthesis" = "consolidation",
		/**
		 * Target database for NEW entry inserts (domain routing).
		 *
		 * When a chunk resolves to a specific domain store, new entries are inserted
		 * into that store rather than this.db. This applies to insert paths only
		 * (novel entries and "insert distinct" decisions).
		 *
		 * Defaults to this.db when not supplied (backwards-compatible).
		 */
		targetDb?: IKnowledgeStore,
		/**
		 * Target database for merge/reinforce operations on EXISTING entries.
		 *
		 * During normal consolidation this is always this.db because entriesMap is
		 * loaded from this.db — existing entries live there. During synthesis, entries
		 * are loaded from the domain store being synthesised, so mergeEntry and
		 * reinforceObservation must target that store instead.
		 *
		 * Defaults to this.db when not supplied (backwards-compatible).
		 */
		mergeDb?: IKnowledgeStore,
	): Promise<void> {
		// insertDb: where NEW entries land (domain-routed or default writable store).
		const insertDb = targetDb ?? this.db;
		// existingDb: where EXISTING entries live — mergeEntry and reinforceObservation
		// must target the same store entriesMap was loaded from.
		const existingDb = mergeDb ?? this.db;

		// Embed the extracted entry content (skip if pre-computed by caller)
		const entryEmbedding =
			precomputedEmbedding ??
			(await this.embeddings.embed(
				formatEmbeddingText(entry.type, entry.content, entry.topics ?? []),
			));

		// Find nearest existing entry from the in-memory cache
		let nearestEntry: KnowledgeEntry | null = null;
		let nearestSimilarity = 0;

		for (const existing of entriesMap.values()) {
			const sim = cosineSimilarity(entryEmbedding, existing.embedding);
			if (sim > nearestSimilarity) {
				nearestSimilarity = sim;
				nearestEntry = existing;
			}
		}

		// Below threshold → clearly novel, insert directly into domain-routed store
		if (
			!nearestEntry ||
			nearestSimilarity < config.consolidation.reconsolidationThreshold
		) {
			const inserted = await this.insertNewEntry(
				entry,
				sessionIds,
				entryEmbedding,
				sessionTimestamp,
				insertDb,
			);
			await callbacks.onInsert(inserted);
			logger.debug(
				`[${logPrefix}] Insert (novel, sim=${nearestSimilarity.toFixed(3)}): ${JSON.stringify(entry.content)}`,
			);
			return;
		}

		// Above threshold → ask LLM for a focused merge decision
		logger.debug(
			`[${logPrefix}] Reconsolidation candidate (sim=${nearestSimilarity.toFixed(3)}): ${JSON.stringify(entry.content)} vs ${JSON.stringify(nearestEntry.content)}`,
		);

		const mergeStart = Date.now();
		const decision = await this.llm.decideMerge(
			{
				content: nearestEntry.content,
				type: nearestEntry.type,
				topics: nearestEntry.topics,
				confidence: nearestEntry.confidence,
			},
			{
				content: entry.content,
				type: entry.type,
				topics: entry.topics || [],
				confidence: entry.confidence,
			},
		);
		logger.log(
			`[${logPrefix}] decideMerge responded in ${((Date.now() - mergeStart) / 1000).toFixed(1)}s.`,
		);

		switch (decision.action) {
			case "keep": {
				// The same knowledge surfaced again in a new episode — reinforce it as evidence.
				// This increments observation_count (the evidence signal that extends half-life)
				// and resets last_accessed_at so decay restarts from now.
				// We use reinforceObservation rather than recordAccess — this is not a retrieval
				// event; it's confirmation that the knowledge is still true.
				// existingDb — nearestEntry lives in whichever store entriesMap was loaded from.
				await existingDb.reinforceObservation(nearestEntry.id);
				logger.debug(
					`[${logPrefix}] Keep existing (reinforced): ${JSON.stringify(nearestEntry.content)}`,
				);
				await callbacks.onKeep();
				break;
			}

			case "update":
			case "replace": {
				const safeType = clampKnowledgeType(decision.type);
				const mergeUpdates = {
					content: decision.content,
					type: safeType,
					topics: decision.topics,
					confidence: Math.max(0, Math.min(1, decision.confidence)),
					additionalSources: sessionIds,
				};
				// Re-embed immediately so the entry never passes through a NULL-embedding
				// state. The embedding text uses the clamped type (same transform mergeEntry
				// applies internally) so it matches what ensureEmbeddings would produce.
				// Passing the embedding to mergeEntry writes content + embedding in a single
				// atomic UPDATE — no gap where the DB has new content but no vector.
				const freshEmbedding = await this.embeddings.embed(
					formatEmbeddingText(
						safeType,
						decision.content,
						decision.topics ?? [],
					),
				);
				// existingDb — nearestEntry lives in whichever store entriesMap was loaded from.
				const merged = await existingDb.mergeEntry(
					nearestEntry.id,
					mergeUpdates,
					freshEmbedding,
				);
				if (!merged) {
					// The target was deleted after entriesMap was loaded. Keep the extracted
					// knowledge as a new entry; the stale id must not stay in the cache.
					entriesMap.delete(nearestEntry.id);
					const inserted = await this.insertNewEntry(
						entry,
						sessionIds,
						entryEmbedding,
						sessionTimestamp,
						insertDb,
					);
					logger.debug(
						`[${logPrefix}] Insert (merge target deleted): ${JSON.stringify(entry.content)}`,
					);
					await callbacks.onInsert(inserted);
					break;
				}
				logger.debug(
					`[${logPrefix}] ${decision.action === "update" ? "Updated" : "Replaced"}: ${JSON.stringify(nearestEntry.content)} → ${JSON.stringify(decision.content)}`,
				);
				await callbacks.onUpdate(nearestEntry.id, mergeUpdates, freshEmbedding);
				break;
			}

			case "insert": {
				const inserted = await this.insertNewEntry(
					entry,
					sessionIds,
					entryEmbedding,
					sessionTimestamp,
					insertDb,
				);
				logger.debug(
					`[${logPrefix}] Insert (distinct despite similarity): ${JSON.stringify(entry.content)}`,
				);
				await callbacks.onInsert(inserted);
				break;
			}
		}
	}

	/**
	 * KB-wide cluster-first cross-session synthesis pass.
	 *
	 * Called once per consolidation cycle, AFTER ensureEmbeddings() so all entries
	 * have embeddings.
	 *
	 * Algorithm:
	 * 1. CLUSTER FORMATION (in-memory, O(n×k), via greedyClusterAsync in ./cluster.ts):
	 *    - Entries visited by observationCount desc, then id asc (most-evidenced entries
	 *      seed clusters first; the id tie-break keeps the order stable across runs).
	 *    - Greedy assign each entry to nearest existing cluster if centroid similarity
	 *      ≥ CLUSTER_ASSIGNMENT_THRESHOLD (env: CLUSTER_ASSIGNMENT_THRESHOLD), else start a new cluster.
	 *    - CLUSTER_CENTROID_MODE "running": centroid is the running average of members.
	 *      "seed": centroid is the seed entry's embedding and never moves.
	 *    - The compared centroid is what is persisted for identity reconciliation.
	 *
	 * 2. CLUSTER IDENTITY RECONCILIATION (against DB):
	 *    - Match each in-memory cluster to persisted cluster by centroid similarity
	 *      ≥ CLUSTER_IDENTITY_THRESHOLD (0.9) → reuse ID.
	 *    - Unmatched persisted clusters then match an unmatched in-memory cluster by
	 *      member overlap (Jaccard ≥ CLUSTER_IDENTITY_OVERLAP_THRESHOLD, 0.5); this
	 *      covers seed-mode centroid drift (a new seed re-keys the cluster otherwise).
	 *    - No match → new cluster ID.
	 *    - Persisted clusters with no match → deleted (entries dispersed).
	 *
	 * 3. RIPENESS CHECK:
	 *    - Minimum CLUSTER_MIN_MEMBERS (env: CLUSTER_MIN_MEMBERS) members.
	 *    - last_synthesized_at IS NULL, OR a material change since then: at least
	 *      SYNTHESIS_MIN_NEW_MEMBERS members (or SYNTHESIS_MIN_NEW_FRACTION of members)
	 *      joined after last_synthesized_at (see hasMaterialChange).
	 *
	 * 3b. ORDER + CAP:
	 *    - The dominant cluster (findDominantCluster: >= 100 members and > 50% of all
	 *      entries, i.e. CLUSTER_ASSIGNMENT_THRESHOLD is below the noise floor) is
	 *      never ripe: no LLM call, no budget spend, not marked. It is logged on
	 *      every run; recalibrate with `knowledge-server calibrate --clusters`.
	 *    - Ripe clusters: never-synthesized first, then oldest last_synthesized_at;
	 *      ties by member count desc, then seed id asc.
	 *    - Each synthesis call consumes one unit of `budget` (CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN,
	 *      shared across stores in one run). Ripe clusters beyond the budget are
	 *      deferred: not marked synthesized, so they stay ripe for the next run. Their
	 *      members keep their joined_at (persistClusters keeps member rows), so the
	 *      material-change check still sees members that joined after the last synthesis.
	 *      Ordering is starvation-free only across runs in which fewer new clusters
	 *      appear than the budget: a never-synthesized cluster always goes first, so
	 *      if at least CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN new clusters appear every
	 *      run, older ripe clusters are deferred each run.
	 *
	 * 4. SYNTHESIS (per ripe cluster):
	 *    - All members are candidate peers (no anchor/neighbor distinction), but the
	 *      prompt carries at most SYNTHESIS_MAX_PEERS, closest to centroid first, within
	 *      SYNTHESIS_MAX_PROMPT_TOKENS (see selectSynthesisPeers).
	 *    - LLM returns array of zero or more principles.
	 *    - Each result routed through reconsolidate() for deduplication.
	 *    - Write last_synthesized_at on cluster after a synthesis attempt. Any
	 *      synthesizePrinciple error marks the cluster and the loop continues.
	 *
	 * Returns the number of synthesis calls that produced at least one principle.
	 *
	 * Domain routing: when domains are configured each store's knowledge is
	 * synthesised independently. Pass the target store explicitly — the caller
	 * is responsible for iterating over each store that received writes this run.
	 * Defaults to this.db for single-store / backwards-compatible callers.
	 *
	 * `budget` is the synthesis-call budget shared by all stores in one run; it is
	 * decremented once per synthesis call. Defaults to a fresh budget of
	 * CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN.
	 */
	async runKBSynthesis(
		db: IKnowledgeStore = this.db,
		budget: SynthesisCallBudget = {
			remaining: config.consolidation.clusterSynthesisMaxCallsPerRun,
		},
	): Promise<number> {
		// Load all active entries with embeddings (ensureEmbeddings just ran).
		const allEntries = await db.getActiveEntriesWithEmbeddings();
		if (allEntries.length < CLUSTER_MIN_MEMBERS) return 0;

		// ── Step 1: Greedy clustering ──────────────────────────────────────────────

		// Read at call time (not a module constant) so tests and a reloaded config take effect.
		const centroidMode = config.consolidation.clusterCentroidMode;
		// O(n × k × dim). greedyClusterAsync yields to the event loop between chunks so the
		// HTTP/MCP server stays responsive; the duration makes a slow pass visible in the log.
		const clusteringStartedAt = Date.now();
		const inMemoryClusters = await greedyClusterAsync(allEntries, {
			threshold: CLUSTER_ASSIGNMENT_THRESHOLD,
			centroidMode,
		});
		logger.log(
			`[synthesis] Formed ${inMemoryClusters.length} clusters from ${allEntries.length} entries (threshold ${CLUSTER_ASSIGNMENT_THRESHOLD}, centroid mode ${centroidMode}) in ${Date.now() - clusteringStartedAt} ms.`,
		);
		const dominant = findDominantCluster(inMemoryClusters, allEntries.length);

		// ── Step 2: Identity reconciliation ───────────────────────────────────────

		const persistedClusters = await db.getClustersWithMembers();
		// Norms are precomputed so each centroid pair costs one dot product.
		const persistedNorms = persistedClusters.map((p) => norm(p.centroid));
		const inMemoryNorms = inMemoryClusters.map((c) => norm(c.centroid));

		// Build a lookup: persisted cluster ID → matched in-memory cluster index
		const persistedToInMemory = new Map<string, number>();
		// Track which in-memory clusters have been matched to avoid double-matching
		const inMemoryMatched = new Set<number>();

		const identityStartedAt = Date.now();
		let identityWork = 0;
		for (const [pIdx, persisted] of persistedClusters.entries()) {
			// Yield every CLUSTER_YIELD_WORK_BUDGET multiply-adds (same bound as the
			// clustering pass) so a large store does not freeze the server here either.
			identityWork += (inMemoryClusters.length + 1) * persisted.centroid.length;
			if (identityWork >= CLUSTER_YIELD_WORK_BUDGET) {
				identityWork = 0;
				await yieldToEventLoop();
			}
			let bestIdx = -1;
			let bestSim = -1;
			for (let i = 0; i < inMemoryClusters.length; i++) {
				if (inMemoryMatched.has(i)) continue;
				const sim = cosineWithNorms(
					persisted.centroid,
					persistedNorms[pIdx],
					inMemoryClusters[i].centroid,
					inMemoryNorms[i],
				);
				if (sim > bestSim) {
					bestSim = sim;
					bestIdx = i;
				}
			}
			if (bestIdx >= 0 && bestSim >= CLUSTER_IDENTITY_THRESHOLD) {
				persistedToInMemory.set(persisted.id, bestIdx);
				inMemoryMatched.add(bestIdx);
			}
		}

		// Overlap fallback (see CLUSTER_IDENTITY_OVERLAP_THRESHOLD). Uses an inverted
		// index member → unmatched in-memory cluster, so the cost is the total
		// membership size rather than persisted × in-memory.
		const unmatchedMemberToInMemory = new Map<string, number>();
		for (let i = 0; i < inMemoryClusters.length; i++) {
			if (inMemoryMatched.has(i)) continue;
			for (const memberId of inMemoryClusters[i].memberIds) {
				unmatchedMemberToInMemory.set(memberId, i);
			}
		}
		for (const persisted of persistedClusters) {
			if (persistedToInMemory.has(persisted.id)) continue;
			const overlap = new Map<number, number>();
			for (const memberId of persisted.memberIds) {
				const idx = unmatchedMemberToInMemory.get(memberId);
				if (idx !== undefined) overlap.set(idx, (overlap.get(idx) ?? 0) + 1);
			}
			let bestIdx = -1;
			let bestJaccard = 0;
			for (const [idx, shared] of overlap) {
				if (inMemoryMatched.has(idx)) continue;
				const union =
					persisted.memberIds.length +
					inMemoryClusters[idx].memberIds.length -
					shared;
				const jaccard = shared / union;
				if (jaccard > bestJaccard) {
					bestJaccard = jaccard;
					bestIdx = idx;
				}
			}
			if (bestIdx >= 0 && bestJaccard >= CLUSTER_IDENTITY_OVERLAP_THRESHOLD) {
				persistedToInMemory.set(persisted.id, bestIdx);
				inMemoryMatched.add(bestIdx);
			}
		}

		logger.log(
			`[synthesis] Matched ${persistedClusters.length} persisted clusters against ${inMemoryClusters.length} in-memory clusters in ${Date.now() - identityStartedAt} ms.`,
		);

		// O(1) lookups for the resolution below. persistedToInMemory is one-to-one, so
		// its inverse is a plain map (in-memory index → persisted ID).
		const inMemoryToPersisted = new Map<number, string>();
		for (const [persistedId, inMemIdx] of persistedToInMemory) {
			inMemoryToPersisted.set(inMemIdx, persistedId);
		}
		const persistedById = new Map(persistedClusters.map((p) => [p.id, p]));

		// Map in-memory cluster index → resolved ID + membership-changed flag
		const resolvedClusters: Array<{
			id: string;
			centroid: number[];
			memberIds: string[];
			isNew: boolean;
			membershipChanged: boolean;
			lastSynthesizedAt: number | null;
			joinedAt: Map<string, number>;
		}> = inMemoryClusters.map((cluster, idx) => {
			// Find the persisted cluster that matched this in-memory cluster (if any)
			const matchedId = inMemoryToPersisted.get(idx) ?? null;
			const persistedCluster =
				matchedId !== null ? persistedById.get(matchedId) : undefined;
			const previousMemberIds = persistedCluster?.memberIds ?? [];

			const id = matchedId ?? randomUUID();
			const isNew = matchedId === null;

			// Membership changed if set of member IDs differs from persisted
			const previousMemberSet = new Set(previousMemberIds);
			const membershipChanged =
				isNew ||
				cluster.memberIds.length !== previousMemberIds.length ||
				cluster.memberIds.some((mid) => !previousMemberSet.has(mid));

			return {
				id,
				centroid: cluster.centroid,
				memberIds: cluster.memberIds,
				isNew,
				membershipChanged,
				lastSynthesizedAt: isNew
					? null
					: (persistedCluster?.lastSynthesizedAt ?? null),
				joinedAt: persistedCluster?.memberJoinedAt ?? new Map(),
			};
		});

		// Persist new cluster state (upsert clusters, delete stale ones)
		await db.persistClusters(
			resolvedClusters.map((c) => ({
				id: c.id,
				centroid: c.centroid,
				memberIds: c.memberIds,
				isNew: c.isNew,
				membershipChanged: c.membershipChanged,
			})),
		);

		// ── Step 3 & 4: Ripeness check and synthesis ──────────────────────────────

		// Build a shared entriesMap for reconsolidate() calls so synthesized principles
		// see each other within the same pass (dedup between clusters).
		const entriesMap = new Map(allEntries.map((e) => [e.id, e]));

		let synthesized = 0;
		let deferred = 0;

		// Dominant cluster (findDominantCluster: >= 100 members and > 50% of all entries): the
		// assignment threshold is below the embedding model's noise floor. Never synthesized:
		// no LLM call, no budget spend, not marked. resolvedClusters keeps the in-memory order,
		// so the index locates it.
		const dominantClusterId =
			dominant !== null ? resolvedClusters[dominant.index].id : null;
		if (dominant !== null && dominantClusterId !== null) {
			logger.warn(
				`[synthesis] Skipping synthesis for dominant cluster ${dominantClusterId}: ${dominant.size}/${allEntries.length} entries (${Math.round(dominant.share * 100)}%) in one cluster, no LLM call, not marked synthesized. CLUSTER_ASSIGNMENT_THRESHOLD=${CLUSTER_ASSIGNMENT_THRESHOLD} (CLUSTER_CENTROID_MODE=${centroidMode}) is likely below the embedding model's noise floor. Recalibrate with \`knowledge-server calibrate --clusters\`.`,
			);
		}

		// Ripeness: never synthesized, or materially changed since last synthesis, AND meets
		// minimum member count.
		const ripe = resolvedClusters.filter(
			(c) =>
				c.id !== dominantClusterId &&
				c.memberIds.length >= CLUSTER_MIN_MEMBERS &&
				(c.lastSynthesizedAt === null ||
					hasMaterialChange(c.memberIds, c.joinedAt, c.lastSynthesizedAt)),
		);

		// Order under the cap: never-synthesized first, then oldest lastSynthesizedAt; ties by
		// member count desc, then seed id asc.
		ripe.sort((a, b) => {
			const aT = a.lastSynthesizedAt ?? -1;
			const bT = b.lastSynthesizedAt ?? -1;
			if (aT !== bT) return aT - bT;
			if (a.memberIds.length !== b.memberIds.length) {
				return b.memberIds.length - a.memberIds.length;
			}
			const aSeed = a.memberIds[0];
			const bSeed = b.memberIds[0];
			return aSeed < bSeed ? -1 : aSeed > bSeed ? 1 : 0;
		});

		for (const cluster of ripe) {
			// Collect peer entries for this cluster
			const peers = cluster.memberIds
				.map((id) => entriesMap.get(id))
				.filter(
					(e): e is KnowledgeEntry & { embedding: number[] } => e !== undefined,
				);

			if (peers.length < CLUSTER_MIN_MEMBERS) continue;

			// Per-run cap: deferred clusters are not marked, so they stay ripe (never synthesized,
			// or their new members keep joined_at > last_synthesized_at).
			if (budget.remaining <= 0) {
				deferred++;
				continue;
			}
			budget.remaining--;

			logger.log(
				`[synthesis] Attempting synthesis for cluster ${cluster.id} (${peers.length} peers).`,
			);

			const synthStart = Date.now();
			const sentPeers = selectSynthesisPeers(
				cluster.id,
				peers,
				cluster.centroid,
			);
			// Only peers actually sent may be cited: the LLM cannot cite what it never saw.
			const sentIdSet = new Set(sentPeers.map((p) => p.id));

			let results: Awaited<ReturnType<typeof this.llm.synthesizePrinciple>>;
			try {
				// Principles are stored and embedded: scrub the LLM's text before either.
				results = (
					await this.llm.synthesizePrinciple(
						sentPeers.map((p) => ({
							id: p.id,
							content: p.content,
							type: p.type,
							topics: p.topics,
							isSynthesized: p.isSynthesized,
						})),
					)
				).map((r) => ({ ...r, content: redactSecrets(r.content).text }));
			} catch (err) {
				logger.warn(
					`[synthesis] synthesizePrinciple failed for cluster ${cluster.id}: ${err instanceof Error ? err.message : String(err)}`,
				);
				// Mark as synthesized anyway to avoid hammering on a bad cluster next run.
				await db.markClusterSynthesized(cluster.id);
				continue;
			}

			logger.log(
				`[synthesis] synthesizePrinciple responded in ${((Date.now() - synthStart) / 1000).toFixed(1)}s — ${results.length} result(s).`,
			);

			// Mark cluster as synthesized regardless of results (bar was applied, nothing met it)
			await db.markClusterSynthesized(cluster.id);

			if (results.length === 0) {
				logger.log(
					`[synthesis] No synthesis emerged for cluster ${cluster.id} — bar not met.`,
				);
				continue;
			}

			// Process each synthesized principle.
			// markClusterSynthesized was already called unconditionally above —
			// the cluster will not be retried even if all results fail here.
			let clusterReconsolidated = 0;
			for (const result of results) {
				logger.debug(
					`[synthesis] Synthesized ${result.type}: ${JSON.stringify(result.content)}`,
				);

				try {
					// Embed and reconsolidate for deduplication.
					// Both embed() and reconsolidate() are inside the try so a transient
					// API error on either path does not abort the cluster loop.
					const synthEmbedding = await this.embeddings.embed(
						formatEmbeddingText(result.type, result.content, result.topics),
					);

					// Validate sourceIds are from the peers sent (hallucination guard)
					const validatedSourceIds = result.sourceIds.filter((id) =>
						sentIdSet.has(id),
					);
					const droppedCount =
						result.sourceIds.length - validatedSourceIds.length;
					if (droppedCount > 0) {
						logger.warn(
							`[synthesis] Dropped ${droppedCount} sourceId(s) from LLM result for cluster ${cluster.id} (not peers sent to the LLM).`,
						);
					}

					await this.reconsolidate(
						{
							type: result.type,
							content: result.content,
							topics: result.topics,
							confidence: result.confidence,
							source: `synthesis:cluster:${cluster.id}`,
							isSynthesized: true,
						},
						[], // no session IDs — KB-derived provenance
						entriesMap,
						{
							onInsert: async (inserted) => {
								if (!inserted.embedding) {
									throw new Error(
										`[synthesis] Synthesized entry ${inserted.id} has no embedding — this is a bug.`,
									);
								}
								entriesMap.set(
									inserted.id,
									inserted as KnowledgeEntry & { embedding: number[] },
								);
								// Write supports relations from synthesized entry → source entries
								for (const sourceId of validatedSourceIds) {
									await db.insertRelation({
										id: randomUUID(),
										sourceId: inserted.id,
										targetId: sourceId,
										type: "supports",
										createdAt: Date.now(),
									});
								}
								synthesized++;
								clusterReconsolidated++;
								logger.log(
									`[synthesis] Inserted synthesized entry ${inserted.id} with ${validatedSourceIds.length} supports relations.`,
								);
							},
							onUpdate: async (id, _updated, freshEmbedding) => {
								// Mark the updated entry as synthesized — mergeEntry does not set
								// this flag, so we patch it here on the synthesis-specific path.
								await db.updateEntry(id, { isSynthesized: true });
								const existing = entriesMap.get(id);
								if (existing) {
									entriesMap.set(id, {
										...existing,
										isSynthesized: true,
										embedding: freshEmbedding,
									});
								}
								synthesized++;
								clusterReconsolidated++;
								logger.log(
									`[synthesis] Refined existing principle ${id} via synthesis.`,
								);
								// TODO: onUpdate does not write new `supports` relations — only the
								// onInsert handler in this same callback object does. An update means
								// the synthesized content merged into an existing principle, so that
								// principle's provenance already covers it. If cross-synthesis
								// provenance tracking is ever needed, insertRelation calls should be
								// added here analogous to onInsert.
							},
							onKeep: () => {
								clusterReconsolidated++; // Not synthesized++: no change to KB content
								logger.log(
									"[synthesis] Synthesis result matches existing principle — kept (no duplicate inserted).",
								);
							},
						},
						undefined, // no sessionTimestamp — synthesized entries stamped at now
						synthEmbedding,
						"synthesis", // logPrefix — keeps synthesis reconsolidation out of [consolidation] logs
						db, // targetDb — new synthesized entries land in the store being synthesized
						db, // mergeDb — existing entries also live in this store (entriesMap loaded from db)
					);
				} catch (err) {
					// Log and skip this result — do NOT rethrow.
					// A single bad synthesized entry must not abort the remaining results
					// in this cluster.
					logger.error(
						`[synthesis] Failed to reconsolidate synthesized entry for cluster ${cluster.id} — skipping:`,
						err,
					);
				}
			}
			if (clusterReconsolidated === 0) {
				logger.warn(
					`[synthesis] All ${results.length} result(s) for cluster ${cluster.id} failed reconsolidation — cluster marked synthesized but nothing persisted.`,
				);
			}
		}

		if (deferred > 0) {
			logger.log(
				`[synthesis] Deferred ${deferred} ripe cluster(s) in store "${db.id}" to the next run: per-run cap CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN=${config.consolidation.clusterSynthesisMaxCallsPerRun} reached.`,
			);
		}

		if (synthesized > 0) {
			logger.log(
				`[synthesis] KB pass complete: ${synthesized} principle(s) synthesized.`,
			);
		}

		return synthesized;
	}

	/**
	 * Insert a new knowledge entry directly (no similarity check, no LLM call).
	 * Called from consolidateExtractedToStore for entries classified as novel
	 * (similarity below reconsolidationThreshold) after batch embedding.
	 * Also used internally by reconsolidate() and runKBSynthesis().
	 *
	 * Pre-supply embedding to skip re-computing it (same format as
	 * formatEmbeddingText(type, content, topics)). If omitted, embed() is called.
	 * sessionTimestamp: unix ms of source session — entries get the session's
	 * timestamp rather than now() so decay is applied correctly.
	 */
	async insertNewEntry(
		entry: ExtractedKnowledge,
		sessionIds: string[],
		embedding?: number[],
		sessionTimestamp?: number,
		targetDb?: IKnowledgeStore,
	): Promise<KnowledgeEntry & { embedding?: number[] }> {
		const writeDb = targetDb ?? this.db;
		const now = Date.now();
		// Cap at now — sessions cannot be from the future, and a clock skew or
		// corrupt timestamp should not produce a future lastAccessedAt.
		// Use != null rather than a falsy check: sessionTimestamp = 0 is still
		// meaningful (epoch timestamp) and should not be silently replaced with now.
		// However, epoch-0 is almost certainly a corrupt or uninitialised record —
		// log a warning so the operator can investigate, since an entry stamped at
		// 1970-01-01 will appear 55+ years old and likely archive on the first decay pass.
		if (sessionTimestamp === 0) {
			// Entry text is left out of this warning: it is LLM-sourced and logged at debug only.
			logger.warn(
				"[consolidation] sessionTimestamp = 0 (epoch) — likely a corrupt or uninitialised session record. Entry will be stamped at 1970-01-01 and may archive immediately.",
			);
		}
		const entryTime =
			sessionTimestamp != null ? Math.min(sessionTimestamp, now) : now;
		const newEntry: KnowledgeEntry & { embedding?: number[] } = {
			id: randomUUID(),
			type: entry.type,
			content: entry.content,
			topics: entry.topics || [],
			confidence: Math.max(0, Math.min(1, entry.confidence || 0.5)),
			source:
				entry.source ||
				`consolidation ${new Date(entryTime).toISOString().split("T")[0]}`,
			status: "active",
			isSynthesized: entry.isSynthesized ?? false,
			// Compute real initial strength using the session timestamp so decay is
			// pre-applied for old sessions. A new entry (obs=1, access=0) from a
			// session N days ago already has N days of decay factored into its strength.
			strength: 1.0, // placeholder — overwritten below once all fields are set
			createdAt: entryTime,
			updatedAt: entryTime,
			lastAccessedAt: entryTime,
			accessCount: 0,
			injectionCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: sessionIds,
			embedding,
		};
		// Overwrite placeholder with the real computed value now that all entry fields are set.
		// Pass `now` as the reference time for age computation so daysSinceAccess reflects
		// real elapsed time from the session date to the current moment.
		newEntry.strength = computeStrength(newEntry, now);
		await writeDb.insertEntry(newEntry);
		return newEntry;
	}
}
