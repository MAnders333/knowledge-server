import { randomUUID } from "node:crypto";
import type { EmbeddingClient } from "../activation/embeddings.js";
import {
	cosineSimilarity,
	formatEmbeddingText,
} from "../activation/embeddings.js";
import { config } from "../config.js";
import type { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";
import { RECONSOLIDATION_THRESHOLD, clampKnowledgeType } from "../types.js";
import type { KnowledgeEntry } from "../types.js";
import { computeStrength } from "./decay.js";
import type { ConsolidationLLM, ExtractedKnowledge } from "./llm.js";

/**
 * Maximum number of existing knowledge entries to include as context
 * for each chunk's LLM call. Using embedding similarity to select
 * only the most relevant entries keeps the context focused and fast.
 */
const MAX_RELEVANT_KNOWLEDGE = 50;

/**
 * Number of KB neighbors to include in a synthesis call.
 * Analogous to the hippocampal neighborhood — enough to find structural
 * commonality without diluting the synthesis with distant entries.
 */
const SYNTHESIS_NEIGHBORS = 5;

/**
 * Handles the reconsolidation of a single extracted knowledge entry
 * against the current knowledge base.
 *
 * Responsibilities:
 * - Embed the extracted entry
 * - Find the nearest existing entry by cosine similarity
 * - If above RECONSOLIDATION_THRESHOLD: ask the LLM to decide keep/update/replace/insert
 * - If below threshold: insert directly as a novel entry
 * - Also provides getRelevantKnowledge() to select a focused subset of the KB for the LLM prompt
 */
export class Reconsolidator {
	private db: KnowledgeDB;
	private embeddings: EmbeddingClient;
	private llm: ConsolidationLLM;

	constructor(
		db: KnowledgeDB,
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
	 * 3. If similarity > RECONSOLIDATION_THRESHOLD: focused LLM decision
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
			onInsert: (inserted: KnowledgeEntry & { embedding?: number[] }) => void;
			onUpdate: (
				id: string,
				updated: Partial<KnowledgeEntry>,
				freshEmbedding: number[],
			) => void;
			onKeep: () => void;
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
	): Promise<void> {
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

		// Below threshold → clearly novel, insert directly
		if (!nearestEntry || nearestSimilarity < RECONSOLIDATION_THRESHOLD) {
			const inserted = this.insertNewEntry(
				entry,
				sessionIds,
				entryEmbedding,
				sessionTimestamp,
			);
			callbacks.onInsert(inserted);
			logger.log(
				`[consolidation] Insert (novel, sim=${nearestSimilarity.toFixed(3)}): "${entry.content.slice(0, 60)}..."`,
			);
			return;
		}

		// Above threshold → ask LLM for a focused merge decision
		logger.log(
			`[consolidation] Reconsolidation candidate (sim=${nearestSimilarity.toFixed(3)}): "${entry.content.slice(0, 60)}..." vs "${nearestEntry.content.slice(0, 60)}..."`,
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
			`[consolidation] decideMerge responded in ${((Date.now() - mergeStart) / 1000).toFixed(1)}s.`,
		);

		switch (decision.action) {
			case "keep": {
				// The same knowledge surfaced again in a new episode — reinforce it as evidence.
				// This increments observation_count (the evidence signal that extends half-life)
				// and resets last_accessed_at so decay restarts from now.
				// We use reinforceObservation rather than recordAccess — this is not a retrieval
				// event; it's confirmation that the knowledge is still true.
				this.db.reinforceObservation(nearestEntry.id);
				logger.log(
					`[consolidation] Keep existing (reinforced): "${nearestEntry.content.slice(0, 60)}..."`,
				);
				callbacks.onKeep();
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
				this.db.mergeEntry(nearestEntry.id, mergeUpdates, freshEmbedding);
				logger.log(
					`[consolidation] ${decision.action === "update" ? "Updated" : "Replaced"}: "${nearestEntry.content.slice(0, 60)}..." → "${decision.content.slice(0, 60)}..."`,
				);
				callbacks.onUpdate(nearestEntry.id, mergeUpdates, freshEmbedding);
				break;
			}

			case "insert": {
				const inserted = this.insertNewEntry(
					entry,
					sessionIds,
					entryEmbedding,
					sessionTimestamp,
				);
				logger.log(
					`[consolidation] Insert (distinct despite similarity): "${entry.content.slice(0, 60)}..."`,
				);
				callbacks.onInsert(inserted);
				break;
			}
		}
	}

	/**
	 * KB-wide cross-session synthesis pass.
	 *
	 * Called once per consolidation cycle, AFTER ensureEmbeddings() so all entries
	 * have embeddings. Replaces the previous per-entry trigger that fired on each
	 * keep/update event — clustering the entire KB produces better anchors (the
	 * most-evidenced entry per cluster rather than whichever one happened to be
	 * reinforced last) and avoids duplicate principles from concurrent triggers.
	 *
	 * Algorithm:
	 * 1. Select "ripe" anchors: active entries whose observationCount has crossed a
	 *    new synthesis threshold multiple since last synthesis (same eligibility
	 *    condition as the old per-entry trigger, now evaluated in bulk).
	 * 2. For each ripe anchor (sorted by observationCount desc — most-evidenced first):
	 *    a. Skip if anchor was already claimed by an earlier cluster this pass.
	 *    b. Find the SYNTHESIS_NEIGHBORS nearest neighbors by cosine similarity.
	 *    c. If no neighbors → skip (KB too sparse; retry next cycle).
	 *    d. Mark anchor as synthesized (stamp = current observationCount).
	 *    e. Call synthesizePrinciple. If the LLM returns null → bar not met.
	 *    f. If a result is returned, embed it and route through reconsolidate() so
	 *       existing equivalent principles block duplicate insertion (dedup fix).
	 *    g. Mark neighbors as "claimed" so they don't become anchors in the same pass
	 *       (they're semantically covered by this cluster).
	 *
	 * Returns the number of synthesis LLM calls that produced a non-null result.
	 */
	async runKBSynthesis(): Promise<number> {
		const threshold = config.consolidation.synthesisObservationThreshold;
		if (threshold === 0) return 0;

		// Load all active entries with embeddings (ensureEmbeddings just ran).
		const allEntries = this.db.getActiveEntriesWithEmbeddings();
		if (allEntries.length < 2) return 0; // need at least anchor + 1 neighbor

		// Build a shared entriesMap for the synthesis reconsolidate() calls.
		// This allows synthesized principles to see each other within the same pass —
		// if cluster A produces principle P and cluster B would produce a near-duplicate,
		// P is already in entriesMap and reconsolidate() routes the duplicate to decideMerge.
		const entriesMap = new Map(allEntries.map((e) => [e.id, e]));

		// Select ripe anchors: entries that have accumulated new evidence since last synthesis.
		// Sort descending by observationCount so the most-reinforced entries anchor first.
		const ripeAnchors = allEntries
			.filter((e) => {
				const lastSynth = e.lastSynthesizedObservationCount ?? 0;
				return e.observationCount >= lastSynth + threshold;
			})
			.sort((a, b) => b.observationCount - a.observationCount);

		if (ripeAnchors.length === 0) return 0;

		logger.log(
			`[synthesis] KB pass: ${ripeAnchors.length} ripe anchor(s) from ${allEntries.length} total entries (threshold=${threshold}).`,
		);

		// Track entries claimed as neighbors in this pass so they are not also used as
		// anchors. An entry that is a neighbor in one cluster should not independently
		// trigger its own synthesis in the same pass — it's already covered.
		const claimedAsNeighbor = new Set<string>();

		let synthesized = 0;

		for (const anchor of ripeAnchors) {
			if (claimedAsNeighbor.has(anchor.id)) continue;

			// Find nearest neighbors from the full KB, excluding the anchor itself.
			const scored: Array<{
				entry: KnowledgeEntry & { embedding: number[] };
				sim: number;
			}> = [];
			for (const candidate of entriesMap.values()) {
				if (candidate.id === anchor.id) continue;
				scored.push({
					entry: candidate,
					sim: cosineSimilarity(anchor.embedding, candidate.embedding),
				});
			}
			scored.sort((a, b) => b.sim - a.sim);
			const neighbors = scored.slice(0, SYNTHESIS_NEIGHBORS).map((s) => s.entry);

			if (neighbors.length === 0) {
				logger.log(
					`[synthesis] Skipping anchor ${anchor.id} — no neighbors with embeddings.`,
				);
				continue;
			}

			// Stamp the anchor as synthesized BEFORE the async LLM call to prevent
			// concurrent consolidation runs from triggering duplicate synthesis.
			this.db.markSynthesized(anchor.id, anchor.observationCount);
			// Update entriesMap so later clusters in this pass see the stamp.
			entriesMap.set(anchor.id, {
				...anchor,
				lastSynthesizedObservationCount: anchor.observationCount,
			});

			logger.log(
				`[synthesis] Attempting synthesis for "${anchor.content.slice(0, 60)}..." (obs=${anchor.observationCount}) with ${neighbors.length} neighbors.`,
			);

			const synthStart = Date.now();
			let result: Awaited<
				ReturnType<typeof this.llm.synthesizePrinciple>
			>;
			try {
				result = await this.llm.synthesizePrinciple(
					{
						content: anchor.content,
						type: anchor.type,
						topics: anchor.topics,
						observationCount: anchor.observationCount,
					},
					neighbors.map((n) => ({
						id: n.id,
						content: n.content,
						type: n.type,
						topics: n.topics,
					})),
				);
			} catch (err) {
				logger.warn(
					`[synthesis] synthesizePrinciple failed for ${anchor.id} (stamp persisted at obs=${anchor.observationCount}; next attempt at obs=${anchor.observationCount + threshold}): ${err instanceof Error ? err.message : String(err)}`,
				);
				// Don't claim neighbors on failure — they remain eligible as anchors.
				continue;
			}

			logger.log(
				`[synthesis] synthesizePrinciple responded in ${((Date.now() - synthStart) / 1000).toFixed(1)}s.`,
			);

			if (!result) {
				logger.log(
					`[synthesis] No synthesis emerged for "${anchor.content.slice(0, 60)}..." — bar not met.`,
				);
				// Don't claim neighbors on a null result — the cluster didn't produce a
				// principle, so neighbors remain eligible as anchors in this same pass.
				continue;
			}

			// Principle was produced: claim neighbors as covered to prevent the same
			// cluster from being re-attempted via a neighbor anchor in this pass.
			for (const n of neighbors) {
				claimedAsNeighbor.add(n.id);
			}

			logger.log(
				`[synthesis] Synthesized ${result.type}: "${result.content.slice(0, 80)}..."`,
			);

			// Embed and reconsolidate — see inline comments in the reconsolidate call below.
			const synthEmbedding = await this.embeddings.embed(
				formatEmbeddingText(result.type, result.content, result.topics),
			);

			// Filter sourceIds to only IDs that exist in entriesMap, to avoid dangling FKs.
			const validatedSourceIds = [anchor.id, ...result.sourceIds.filter((id) => entriesMap.has(id))];
			if (validatedSourceIds.length < 1 + result.sourceIds.length) {
				logger.warn(
					`[synthesis] Dropped ${result.sourceIds.length - (validatedSourceIds.length - 1)} unknown sourceId(s) from LLM result for anchor ${anchor.id}.`,
				);
			}

			// Route through reconsolidate() for deduplication: if an equivalent principle
			// already exists in the KB (or was produced earlier in this same pass),
			// decideMerge decides keep/update/replace rather than inserting a duplicate.
			await this.reconsolidate(
				{
					type: result.type,
					content: result.content,
					topics: result.topics,
					confidence: result.confidence,
					scope: anchor.scope,
					source: `synthesis:${anchor.id}`,
				},
				[], // no session IDs — KB-derived provenance
				entriesMap,
				{
					onInsert: (inserted) => {
						// synthEmbedding is always set here since we embed before reconsolidate().
						// If it were ever missing, entriesMap would be stale for subsequent clusters.
						if (!inserted.embedding) {
							throw new Error(
								`[synthesis] Synthesized entry ${inserted.id} has no embedding — this is a bug.`,
							);
						}
						entriesMap.set(
							inserted.id,
							inserted as KnowledgeEntry & { embedding: number[] },
						);
						for (const sourceId of validatedSourceIds) {
							this.db.insertRelation({
								id: randomUUID(),
								sourceId: inserted.id,
								targetId: sourceId,
								type: "supports",
								createdAt: Date.now(),
							});
						}
						synthesized++;
						logger.log(
							`[synthesis] Inserted synthesized entry ${inserted.id} with ${validatedSourceIds.length} supports relations.`,
						);
					},
					onUpdate: (id, _updated, freshEmbedding) => {
						const existing = entriesMap.get(id);
						if (existing) {
							entriesMap.set(id, { ...existing, embedding: freshEmbedding });
						}
						logger.log(
							`[synthesis] Refined existing principle ${id} via synthesis.`,
						);
					},
					onKeep: () => {
						logger.log(
							"[synthesis] Synthesis result matches existing principle — kept (no duplicate inserted).",
						);
					},
				},
				undefined, // no sessionTimestamp — synthesized entries stamped at now
				synthEmbedding,
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
	 * Insert a new knowledge entry into the DB.
	 * Optionally pre-supply the embedding to avoid re-computing it.
	 *
	 * sessionTimestamp: unix ms of the source session (capped at now).
	 * Using the session's time rather than Date.now() means entries extracted from
	 * old sessions start with the correct decay already applied — an entry from a
	 * session 30 days ago will have 30 days of decay, not 0.
	 */
	private insertNewEntry(
		entry: ExtractedKnowledge,
		sessionIds: string[],
		embedding?: number[],
		sessionTimestamp?: number,
	): KnowledgeEntry & { embedding?: number[] } {
		const now = Date.now();
		// Cap at now — sessions cannot be from the future, and a clock skew or
		// corrupt timestamp should not produce a future lastAccessedAt.
		// Use != null rather than a falsy check: sessionTimestamp = 0 is still
		// meaningful (epoch timestamp) and should not be silently replaced with now.
		// However, epoch-0 is almost certainly a corrupt or uninitialised record —
		// log a warning so the operator can investigate, since an entry stamped at
		// 1970-01-01 will appear 55+ years old and likely archive on the first decay pass.
		if (sessionTimestamp === 0) {
			// JSON.stringify escapes control characters and ANSI codes so LLM-sourced
			// content cannot inject structured tokens into the log stream.
			logger.warn(
				`[consolidation] sessionTimestamp = 0 (epoch) — likely a corrupt or uninitialised session record. Entry will be stamped at 1970-01-01 and may archive immediately. Preview: ${JSON.stringify(entry.content.slice(0, 60))}`,
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
			scope: entry.scope || "personal",
			status: "active",
			// Compute real initial strength using the session timestamp so decay is
			// pre-applied for old sessions. A new entry (obs=1, access=0) from a
			// session N days ago already has N days of decay factored into its strength.
			strength: 1.0, // placeholder — overwritten below once all fields are set
			createdAt: entryTime,
			updatedAt: entryTime,
			lastAccessedAt: entryTime,
			accessCount: 0,
			observationCount: 1,
			lastSynthesizedObservationCount: null,
			supersededBy: null,
			derivedFrom: sessionIds,
			embedding,
		};
		// Overwrite placeholder with the real computed value now that all entry fields are set.
		// Pass `now` as the reference time for age computation so daysSinceAccess reflects
		// real elapsed time from the session date to the current moment.
		newEntry.strength = computeStrength(newEntry, now);
		this.db.insertEntry(newEntry);
		return newEntry;
	}

	/**
	 * Retrieve existing knowledge entries that are relevant to a chunk of episodes.
	 *
	 * Instead of sending ALL existing knowledge to the LLM (which grows linearly
	 * and bloats the prompt), we embed the chunk content and use cosine similarity
	 * to find only the most relevant entries. This:
	 * - Keeps the prompt focused (better conflict detection)
	 * - Reduces token cost
	 * - Scales to thousands of entries without degradation
	 */
	async getRelevantKnowledge(
		chunkSummary: string,
		allEntries: Array<KnowledgeEntry & { embedding: number[] }>,
	): Promise<KnowledgeEntry[]> {
		if (allEntries.length === 0) return [];

		// If the knowledge base is small enough, just return everything
		if (allEntries.length <= MAX_RELEVANT_KNOWLEDGE) {
			return allEntries;
		}

		// Embed the chunk content (truncated to a reasonable size for embedding)
		const embeddingText = chunkSummary.slice(0, 8000);
		const chunkEmbedding = await this.embeddings.embed(embeddingText);

		// Score all entries by similarity to the chunk
		const scored = allEntries
			.map((entry) => ({
				entry,
				similarity: cosineSimilarity(chunkEmbedding, entry.embedding),
			}))
			.sort((a, b) => b.similarity - a.similarity)
			.slice(0, MAX_RELEVANT_KNOWLEDGE);

		return scored.map((s) => s.entry);
	}
}
