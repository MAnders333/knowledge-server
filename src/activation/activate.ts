import { config } from "../config.js";
import { computeStrength } from "../consolidation/decay.js";
import type { IKnowledgeStore } from "../db/index.js";
import { logger } from "../logger.js";
import type {
	ActivationResult,
	ContradictionAnnotation,
	KnowledgeEntry,
} from "../types.js";
import {
	EmbeddingClient,
	cosineSimilarity,
	formatEmbeddingText,
} from "./embeddings.js";

/**
 * Split a prompt into activation cues for multi-topic retrieval.
 *
 * Strategy (mirrors the OpenCode plugin):
 *   1. Per-line segments — each newline is a topic boundary (Shift+Enter in most UIs).
 *      Segments shorter than MIN_CUE_CHARS are skipped (greetings, "yes", "ok", etc.)
 *   2. The full trimmed prompt as a holistic cue — captures overall intent that no
 *      individual line may fully express.
 *   3. Deduplication — if the message is a single line, the segment and the full
 *      message are the same string; the Set removes the duplicate.
 *
 * All resulting cues are embedded in a single batched API call downstream.
 *
 * Exported so server.ts (claude-code hook) can reuse the same logic without
 * duplicating it. The plugin (plugin/knowledge.ts) runs as a standalone file
 * and must keep its own copy — kept in sync via tests/format.test.ts parity tests.
 */
export const MIN_CUE_CHARS = 15;

export function splitIntoCues(prompt: string): string[] {
	const trimmed = prompt.trim();
	const segments = trimmed
		.split("\n")
		.map((s) => s.trim())
		.filter((s) => s.length >= MIN_CUE_CHARS);
	// Union: unique segments + full message (Set deduplicates single-line messages)
	return [...new Set([...segments, trimmed])];
}

/**
 * Activation engine — the core retrieval mechanism.
 *
 * Models associative activation from cognitive science:
 * - Input cues are embedded into the same vector space as knowledge entries
 * - Entries activate based on semantic similarity (not keyword match)
 * - Activation strength is modulated by the entry's decay-adjusted strength
 * - The same mechanism serves both passive (plugin-triggered) and active (agent-triggered) retrieval
 */
export class ActivationEngine {
	/**
	 * The primary writable store — used for access records and single-store compat.
	 */
	private db: IKnowledgeStore;
	/**
	 * All writable stores — used for ensureEmbeddings, checkAndReEmbed, and
	 * embedding metadata writes. In multi-store setups this includes every store
	 * that domain routing can write to, not just the primary.
	 */
	private writableDbs: IKnowledgeStore[];
	/**
	 * All stores to read from during activation (includes all writable stores).
	 * Fan-out: each activation query runs against all read stores and results
	 * are merged and re-ranked by similarity score.
	 */
	private readDbs: IKnowledgeStore[];
	readonly embeddings: EmbeddingClient;

	/**
	 * @param writableDb   The primary writable store — receives access records.
	 * @param readDbs      All stores to fan out activation reads across.
	 *                     If omitted, defaults to [writableDb] (single-store mode).
	 * @param writableDbs  All writable stores for embedding/re-embed operations.
	 *                     If omitted, defaults to [writableDb] (single-store mode).
	 */
	constructor(
		writableDb: IKnowledgeStore,
		readDbs?: IKnowledgeStore[],
		writableDbs?: IKnowledgeStore[],
	) {
		this.db = writableDb;
		this.readDbs = readDbs ?? [writableDb];
		this.writableDbs = writableDbs ?? [writableDb];
		this.embeddings = new EmbeddingClient();
	}

	/**
	 * Activate knowledge entries based on one or more queries.
	 *
	 * This is the single retrieval mechanism — used by both:
	 * - The plugin (passive: message segments + full message -> activate -> inject)
	 * - The MCP tool (active: agent sends cues -> activate -> return)
	 *
	 * When multiple queries are provided (e.g. per-line segments of a multi-topic
	 * message plus the full message as a holistic cue), they are embedded in a
	 * single batched API call. Each entry is scored against ALL query vectors and
	 * its best (highest) similarity across queries is kept. Results are then
	 * deduplicated, filtered by threshold, and ranked — so a multi-topic message
	 * retrieves relevant knowledge for every topic, not just the dominant one.
	 *
	 * @param queries - One query string, or an array of query strings (segments + full message)
	 * @returns Ranked knowledge entries above the similarity threshold, with staleness signals
	 */
	async activate(
		queries: string | string[],
		options?: {
			/** Max entries to return. Overrides config.activation.maxResults. */
			limit?: number;
			/** Min similarity threshold. Overrides config.activation.similarityThreshold. */
			threshold?: number;
		},
	): Promise<ActivationResult> {
		const queryList = (Array.isArray(queries) ? queries : [queries]).filter(
			(q) => q.trim().length > 0,
		);
		if (queryList.length === 0) {
			return { entries: [], query: "", totalActive: 0 };
		}

		// Use the last query as the display query — the plugin sends [segments..., fullMessage]
		// so the last element is the most holistic representation of the user's intent.
		// For a single string, queryList[0] === queryList[last], so this is always correct.
		const primaryQuery = queryList[queryList.length - 1];

		const maxResults = options?.limit ?? config.activation.maxResults;
		const similarityThreshold =
			options?.threshold ?? config.activation.similarityThreshold;

		// Embed all queries in a single batched API call
		const queryEmbeddings = await this.embeddings.embedBatch(queryList);

		const now = Date.now();
		const DAY_MS = 1000 * 60 * 60 * 24;

		// ── Candidate retrieval ────────────────────────────────────────────────────
		// Two paths depending on whether the store supports DB-side ANN search:
		//
		// Path A — pgvector (PostgresKnowledgeDB with findSimilarEntries):
		//   For each query vector, run one ANN query against each store and union
		//   results. The HNSW index does the heavy lifting in the DB; we receive
		//   only the top candidates above the threshold. Similarity values are
		//   pre-computed by the DB (1 − cosine distance).
		//
		// Path B — in-process full scan (SQLite or pgvector unavailable):
		//   Load all active+conflicted entries from every store into memory and
		//   compute cosine similarity in JS. Correct at any scale; linear cost.
		//
		// In both paths the result type is the same: an array of
		// { entry, rawSimilarity } deduplicated by ID (first-store-wins).

		// Typed as KnowledgeEntry (not the narrower & { embedding: number[] }) so that
		// source entries fetched via getSupportSourcesForIds() can be pushed later without
		// a type error (those entries may have embedding undefined).
		const scored: Array<{
			entry: KnowledgeEntry;
			rawSimilarity: number;
			similarity: number;
			staleness: {
				ageDays: number;
				strength: number;
				lastAccessedDaysAgo: number;
				mayBeStale: boolean;
			};
		}> = [];

		// Track IDs across stores to deduplicate (first-store-wins). Used only on Path B.
		const seenIds = new Set<string>();
		// Sum of per-store active+conflicted counts from Path A's parallel COUNT queries.
		// Zero on Path B (totalActive comes from seenIds.size there instead).
		// biome-ignore lint/style/useConst: assigned in the Path A branch
		let annTotalActive = 0;

		// Determine whether every read store supports ANN search AND is ready.
		// Two conditions must both be true for a store:
		//   1. findSimilarEntries is present (PostgresKnowledgeDB, not SQLite).
		//   2. isVectorSearchReady() returns true — pgvector extension installed,
		//      embedding_vec column populated, HNSW index ready. This can be false
		//      during a model-change re-embed window; without this check Path A
		//      would silently return zero results instead of falling back to Path B.
		const allStoresSupportAnn = this.readDbs.every(
			(db) =>
				typeof db.findSimilarEntries === "function" &&
				(typeof db.isVectorSearchReady !== "function" || db.isVectorSearchReady()),
		);

		if (allStoresSupportAnn) {
			// ── Path A: DB-side ANN search (pgvector) ─────────────────────────────
			// For each query vector, fan out across all stores and union results.
			// We request maxResults per query vector per store so the union covers
			// relevant entries for every topic in a multi-cue message.
			// The COUNT query runs in parallel with ANN queries so it adds no latency.
			const [annResultsPerQuery, perStoreCounts] = await Promise.all([
				Promise.all(
					queryEmbeddings.map((qEmb) =>
						Promise.all(
							this.readDbs.map((db) =>
								// biome-ignore lint/style/noNonNullAssertion: guarded by allStoresSupportAnn
								db.findSimilarEntries!(qEmb, maxResults, similarityThreshold),
							),
						),
					),
				),
				Promise.all(
					this.readDbs.map((db) =>
						typeof db.getActiveEntryCount === "function"
							? db.getActiveEntryCount()
							: Promise.resolve(0),
					),
				),
			]);
			annTotalActive = perStoreCounts.reduce((sum, n) => sum + n, 0);

			// Build a map: entry ID → best rawSimilarity across all query vectors.
			// This mirrors the in-process "max over cues" logic in Path B.
			const bestSimilarityById = new Map<
				string,
				{ entry: KnowledgeEntry & { embedding: number[] }; rawSimilarity: number }
			>();
			for (const storeResultsPerQuery of annResultsPerQuery) {
				for (const storeResults of storeResultsPerQuery) {
					for (const { entry, similarity } of storeResults) {
						const existing = bestSimilarityById.get(entry.id);
						if (!existing || similarity > existing.rawSimilarity) {
							bestSimilarityById.set(entry.id, { entry, rawSimilarity: similarity });
						}
					}
				}
			}

			// Score each deduplicated candidate (bestSimilarityById already deduplicates
			// by entry ID — no further seenIds check needed here).
			for (const { entry, rawSimilarity } of bestSimilarityById.values()) {
				const ageDays = (now - entry.createdAt) / DAY_MS;
				const lastAccessedDaysAgo = (now - entry.lastAccessedAt) / DAY_MS;
				const halfLife =
					config.decay.typeHalfLife[entry.type] ||
					config.decay.typeHalfLife.fact;
				const liveStrength = computeStrength(entry, now);

				scored.push({
					entry,
					rawSimilarity,
					similarity: rawSimilarity * liveStrength,
					staleness: {
						ageDays: Math.round(ageDays),
						strength: liveStrength,
						lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
						mayBeStale: ageDays > halfLife && entry.accessCount < 3,
					},
				});
			}
		} else {
			// ── Path B: in-process full scan ───────────────────────────────────────
			// Fan out across all read stores and merge results.
			// Entries from different stores may have overlapping IDs if the same entry
			// was synced to multiple stores — deduplicate by ID keeping the first occurrence.
			const allEntriesPerStore = await Promise.all(
				this.readDbs.map((db) => db.getActiveEntriesWithEmbeddings()),
			);
			const entries = allEntriesPerStore.flat().filter((e) => {
				if (seenIds.has(e.id)) return false;
				seenIds.add(e.id);
				return true;
			});

			if (entries.length === 0) {
				return { entries: [], query: primaryQuery, totalActive: 0 };
			}

			for (const entry of entries) {
				const rawSimilarity = Math.max(
					...queryEmbeddings.map((qEmb) =>
						cosineSimilarity(qEmb, entry.embedding),
					),
				);
				if (rawSimilarity < similarityThreshold) continue;

				const ageDays = (now - entry.createdAt) / DAY_MS;
				const lastAccessedDaysAgo = (now - entry.lastAccessedAt) / DAY_MS;
				const halfLife =
					config.decay.typeHalfLife[entry.type] ||
					config.decay.typeHalfLife.fact;
				const liveStrength = computeStrength(entry, now);

				scored.push({
					entry,
					// Threshold filtering uses raw cosine similarity so entry age never
					// prevents a semantically relevant entry from activating.
					// Ranking uses raw similarity weighted by live strength — well-consolidated
					// entries sort higher, but stale entries still appear if they're on-topic.
					// The staleness signals in the response let the LLM reason about reliability.
					rawSimilarity,
					similarity: rawSimilarity * liveStrength,
					staleness: {
						ageDays: Math.round(ageDays),
						strength: liveStrength,
						lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
						mayBeStale: ageDays > halfLife && entry.accessCount < 3,
					},
				});
			}
		}

		// Determine totalActive for the response.
		// Path A: sum of per-store active+conflicted counts (from parallel COUNT queries).
		// Path B: seenIds.size is exact (we loaded every entry).
		const totalActive = allStoresSupportAnn ? annTotalActive : seenIds.size;

		if (scored.length === 0) {
			return { entries: [], query: primaryQuery, totalActive };
		}

		// Sort and cap.
		scored.sort((a, b) => b.similarity - a.similarity);
		scored.splice(maxResults);

		// Record access for activated entries (reinforces their strength)
		for (const { entry } of scored) {
			await this.db.recordAccess(entry.id);
		}

		// ── Relation-aware activation: pull in supports sources ───────────────────
		// When a synthesized principle/pattern activates, surface its source entries
		// alongside it. This lets the agent see both the abstraction and the
		// concrete evidence it was distilled from.
		//
		// Design:
		// - Collect IDs of activated synthesized entries (principle or pattern type)
		// - Batch-fetch their supports source entries from the DB
		// - For each source entry not already in the scored set, compute its score
		//   as the synthesized entry's similarity × 0.9 (slightly lower to rank it
		//   below the principle itself, but still nearby)
		// - Add sources to the result (but do NOT exceed maxResults)
		const synthesizedIds = scored
			.filter(
				({ entry }) => entry.type === "principle" || entry.type === "pattern",
			)
			.map(({ entry }) => entry.id);

		const scoredIdSet = new Set(scored.map(({ entry }) => entry.id));

		if (synthesizedIds.length > 0) {
			// Fan out support sources lookup across all read stores and merge maps.
			const sourceMaps = await Promise.all(
				this.readDbs.map((db) => db.getSupportSourcesForIds(synthesizedIds)),
			);
			const sourcesMap = mergeMaps(sourceMaps);

			// Snapshot the array before iterating — we push source entries into `scored`
			// during the loop and don't want to visit newly-added entries (which could
			// cascade if a source is itself a principle with its own supports relations).
			const snapshot = [...scored];
			for (const {
				entry,
				rawSimilarity,
				similarity: synthSimilarity,
			} of snapshot) {
				if (entry.type !== "principle" && entry.type !== "pattern") continue;
				const sources = sourcesMap.get(entry.id);
				if (!sources) continue;

				for (const source of sources) {
					if (scoredIdSet.has(source.id)) continue; // already activated
					if (scored.length >= maxResults) break;

					const liveStrength = computeStrength(source, now);
					const ageDays = (now - source.createdAt) / DAY_MS;
					const lastAccessedDaysAgo = (now - source.lastAccessedAt) / DAY_MS;
					const halfLife =
						config.decay.typeHalfLife[source.type] ||
						config.decay.typeHalfLife.fact;

					// Use the synthesized entry's rawSimilarity as the source's signal.
					// This ensures source entries rank slightly below their synthesized principle.
					scored.push({
						entry: source,
						rawSimilarity,
						similarity: synthSimilarity * 0.9 * liveStrength,
						staleness: {
							ageDays: Math.round(ageDays),
							strength: liveStrength,
							lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
							mayBeStale: ageDays > halfLife && source.accessCount < 3,
						},
					});
					scoredIdSet.add(source.id);
					await this.db.recordAccess(source.id);
				}

				// Respect maxResults cap for the outer loop too.
				if (scored.length >= maxResults) break;
			}
		}
		// ─────────────────────────────────────────────────────────────────────────

		// Build a map of activated entries for fast lookup during annotation.
		// Used to determine whether a conflicted entry's counterpart also activated
		// (we only annotate when both sides of the conflict are relevant to this query),
		// and to retrieve the counterpart's content without an extra DB round-trip.
		const scoredById = new Map(scored.map(({ entry }) => [entry.id, entry]));

		// Batch-fetch all contradicts relations for conflicted entries in a single query,
		// avoiding N+1 DB calls. Only fetch for entries with status 'conflicted'.
		const conflictedIds = scored
			.filter(({ entry }) => entry.status === "conflicted")
			.map(({ entry }) => entry.id);

		// Fan out contradiction pairs lookup across all read stores and merge.
		const contradictMaps = await Promise.all(
			this.readDbs.map((db) => db.getContradictPairsForIds(conflictedIds)),
		);
		const contradictPairs = mergeMaps(contradictMaps);

		// Build contradiction annotations for conflicted entries whose counterpart
		// also activated in this query (both sides relevant — only then is the caveat useful).
		const contradictionMap = new Map<string, ContradictionAnnotation>();
		for (const { entry } of scored) {
			if (entry.status !== "conflicted") continue;

			const counterpartId = contradictPairs.get(entry.id);
			if (!counterpartId) continue;

			// Only annotate if the counterpart also activated in this same query.
			// The counterpart is already in scoredById if it activated, so no extra DB fetch needed.
			const counterpart = scoredById.get(counterpartId);
			if (!counterpart) continue;

			contradictionMap.set(entry.id, {
				conflictingEntryId: counterpartId,
				conflictingContent: counterpart.content,
				caveat:
					"This knowledge conflicts with another activated entry and has not been resolved. " +
					"Verify before relying on it; the conflicting view is also shown in this response.",
			});
		}

		return {
			entries: scored.map(
				({ entry, rawSimilarity, similarity, staleness }) => ({
					entry: { ...entry, embedding: undefined } as KnowledgeEntry,
					similarity,
					rawSimilarity,
					staleness,
					contradiction: contradictionMap.get(entry.id),
				}),
			),
			query: primaryQuery,
			totalActive,
		};
	}

	/**
	 * Ensure all active and conflicted entries have embeddings.
	 * Called during consolidation (new entries) or on startup (migration).
	 *
	 * Conflicted entries are included because:
	 * - They participate in similarity search (getActiveEntriesWithEmbeddings includes them)
	 * - A merge resolution may clear their embedding (embedding = NULL)
	 * - Without re-embedding, they become permanently invisible to reconsolidation
	 *   and the contradiction scan, preventing automatic re-resolution.
	 *
	 * Uses a single DB query (getEntriesMissingEmbeddings) rather than two separate
	 * status queries to avoid any risk of duplicate entries in the result set.
	 */
	async ensureEmbeddings(): Promise<number> {
		let total = 0;
		for (const store of this.writableDbs) {
			total += await this.ensureEmbeddingsForStore(store);
		}
		return total;
	}

	private async ensureEmbeddingsForStore(db: IKnowledgeStore): Promise<number> {
		const needsEmbedding = await db.getEntriesMissingEmbeddings();

		if (needsEmbedding.length === 0) return 0;

		// Build embedding text using the shared formatter — same format as
		// reconsolidate() so vectors are consistent regardless of which path wrote them.
		const texts = needsEmbedding.map((e) =>
			formatEmbeddingText(e.type, e.content, e.topics),
		);

		const embeddings = await this.embeddings.embedBatch(texts);

		for (let i = 0; i < needsEmbedding.length; i++) {
			await db.updateEntry(needsEmbedding[i].id, {
				embedding: embeddings[i],
			});
		}

		return needsEmbedding.length;
	}

	/**
	 * Check whether the configured embedding model differs from the model that
	 * produced the stored embeddings. If it has changed, re-embed all active and
	 * conflicted entries with the new model.
	 *
	 * Flow:
	 * 1. Read stored embedding metadata (model + dimensions) from DB
	 * 2. If no metadata exists (first run or pre-v8 upgrade):
	 *    - If entries with embeddings exist, probe the first one's dimension count
	 *      and record the current model as the baseline
	 *    - If no entries exist, just record the current model
	 * 3. If stored model matches configured model → no-op
	 * 4. If stored model differs:
	 *    a. Log the model change clearly
	 *    b. Write embedding metadata with the new model + dimensions FIRST
	 *       (see inline comments on crash-recovery trade-offs below)
	 *    c. Overwrite each entry's embedding vector with the new-model vector
	 *
	 * Called at startup before consolidation so all vectors are consistent.
	 * Fans out across all writable stores. Returns true if any store was re-embedded.
	 */
	async checkAndReEmbed(): Promise<boolean> {
		let anyReEmbedded = false;
		for (const store of this.writableDbs) {
			const did = await this.checkAndReEmbedStore(store);
			if (did) anyReEmbedded = true;
		}
		return anyReEmbedded;
	}

	/**
	 * Per-store re-embed logic — factored out so checkAndReEmbed() can fan out
	 * across all writable stores. Returns true if a re-embed was performed.
	 */
	private async checkAndReEmbedStore(db: IKnowledgeStore): Promise<boolean> {
		const configuredModel = config.embedding.model;
		const stored = await db.getEmbeddingMetadata();

		if (!stored) {
			// First run or upgrade from pre-v8 schema — seed metadata.
			// Probe dimensions from an existing entry if available.
			const existing = await db.getActiveEntriesWithEmbeddings();
			if (existing.length > 0) {
				const dims = existing[0].embedding.length;
				logger.log(
					`[embedding] No embedding metadata found. Recording current model: ${configuredModel} (${dims} dimensions, from ${existing.length} existing entries).`,
				);
				await db.setEmbeddingMetadata(configuredModel, dims);
			} else {
				// No entries yet — probe the API for dimensions.
				logger.log(
					"[embedding] No embedding metadata found and no entries exist. Probing model for dimensions...",
				);
				try {
					const probe = await this.embeddings.embed("dimension probe");
					await db.setEmbeddingMetadata(configuredModel, probe.length);
					logger.log(
						`[embedding] Recorded embedding model: ${configuredModel} (${probe.length} dimensions).`,
					);
				} catch (e) {
					// Non-fatal at this stage — metadata will be recorded on first
					// successful consolidation or activation. Log and continue.
					logger.warn(
						`[embedding] Could not probe embedding dimensions: ${e instanceof Error ? e.message : String(e)}. Metadata will be recorded on first successful embedding call.`,
					);
				}
			}
			return false;
		}

		// Model matches — nothing to do.
		if (stored.model === configuredModel) {
			return false;
		}

		// ── Model has changed — re-embed all entries in-place ──

		const entriesWithEmbeddings = await db.getActiveEntriesWithEmbeddings();
		const totalEntries = entriesWithEmbeddings.length;

		logger.log(
			`[embedding] Model change detected: "${stored.model}" → "${configuredModel}".`,
		);

		if (totalEntries === 0) {
			// No entries to re-embed — just update metadata.
			logger.log(
				"[embedding] No entries with embeddings to re-embed — updating metadata only.",
			);
			try {
				const probe = await this.embeddings.embed("dimension probe");
				await db.setEmbeddingMetadata(configuredModel, probe.length);
				logger.log(
					`[embedding] Recorded new embedding model: ${configuredModel} (${probe.length} dimensions).`,
				);
			} catch (e) {
				logger.warn(
					`[embedding] Could not probe new model dimensions: ${e instanceof Error ? e.message : String(e)}. Metadata will be recorded on first successful embedding call.`,
				);
			}
			return false;
		}

		logger.log(
			`[embedding] Re-embedding ${totalEntries} entries with new model "${configuredModel}"...`,
		);

		// Re-embed in-place: compute all new embeddings via a single batch call,
		// then overwrite each entry's vector individually, then stamp the metadata.
		//
		// Failure modes:
		// - embedBatch throws (network error, rate limit): nothing is written →
		//   metadata still points to the old model → next startup detects mismatch
		//   and retries in full. Safe.
		// - Process crashes mid-loop: entries updated so far have new-model
		//   embeddings; remaining entries still carry old-model embeddings (non-NULL).
		//   Because metadata is written AFTER the loop, next startup sees
		//   stored.model !== configuredModel and re-runs the full re-embed. The
		//   already-updated entries will be re-embedded redundantly but correctly.
		const reEmbedStart = Date.now();

		const texts = entriesWithEmbeddings.map((e) =>
			formatEmbeddingText(e.type, e.content, e.topics),
		);
		const newEmbeddings = await this.embeddings.embedBatch(texts);

		const newDimensions =
			newEmbeddings.length > 0 ? newEmbeddings[0].length : 0;

		for (let i = 0; i < entriesWithEmbeddings.length; i++) {
			await db.updateEntry(entriesWithEmbeddings[i].id, {
				embedding: newEmbeddings[i],
			});
		}

		// Write metadata AFTER the loop so a mid-loop crash does not leave the DB
		// with mixed-dimension embeddings that look consistent to the next startup.
		await db.setEmbeddingMetadata(configuredModel, newDimensions);

		const durationSec = ((Date.now() - reEmbedStart) / 1000).toFixed(1);

		logger.log(
			`[embedding] Re-embed complete: ${totalEntries} entries re-embedded in ${durationSec}s ` +
				`(${stored.dimensions} → ${newDimensions} dimensions).`,
		);

		return true;
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Merge an array of Maps into a single Map.
 * Earlier maps take precedence on key collisions (first-store-wins).
 */
function mergeMaps<K, V>(maps: Map<K, V>[]): Map<K, V> {
	const result = new Map<K, V>();
	for (const map of maps) {
		for (const [k, v] of map) {
			if (!result.has(k)) result.set(k, v);
		}
	}
	return result;
}
