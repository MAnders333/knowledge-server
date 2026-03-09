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
	): Promise<void> {
		// Embed the extracted entry content
		const entryEmbedding = await this.embeddings.embed(entry.content);

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
			const inserted = this.insertNewEntry(entry, sessionIds, entryEmbedding, sessionTimestamp);
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

				// Cross-session synthesis: when a well-reinforced entry reaches a new
				// threshold multiple, look at its KB neighborhood and attempt to synthesize
				// a higher-order principle across them.
				// Fires at observation_count = threshold, 2×threshold, 3×threshold, ...
				// Each firing level is tracked via last_synthesized_observation_count so
				// we never synthesize twice for the same evidence level.
				const threshold = config.consolidation.synthesisObservationThreshold;
				if (threshold > 0) {
					// observation_count was just incremented — read the fresh value
					const freshEntry = this.db.getEntry(nearestEntry.id);
					if (freshEntry) {
						const obs = freshEntry.observationCount;
						const lastSynth = freshEntry.lastSynthesizedObservationCount ?? 0;
						// Fire when obs has crossed a new threshold multiple since last synthesis
					const nextThreshold = lastSynth + threshold;
						if (obs >= nextThreshold) {
							// Don't await — synthesis is best-effort and should not block
							// the consolidation loop. Errors are logged and swallowed.
							// Note: attemptSynthesis calls markSynthesized before the LLM call.
							// A transient LLM failure will therefore suppress a retry for the
							// current threshold cycle. This is an intentional trade-off — the
							// alternative (marking after) risks duplicate synthesis under concurrent
							// keep events. The next threshold multiple (obs + threshold) will trigger
							// a fresh attempt.
							//
							// Pass the in-memory anchor embedding as a hint so attemptSynthesis
							// avoids a DB round-trip for the embedding lookup.
							const anchorEmbeddingHint = entriesMap.get(nearestEntry.id)?.embedding;
							this.attemptSynthesis(freshEntry, entriesMap, anchorEmbeddingHint).catch((err) => {
								// markSynthesized was called before the LLM attempt, so the stamp
								// is persisted even on failure. The next retry fires at
								// obs = freshEntry.observationCount + threshold.
								logger.warn(
									`[consolidation] Synthesis failed for entry ${freshEntry.id} (stamp persisted at obs=${freshEntry.observationCount}; next attempt at obs=${freshEntry.observationCount + threshold}): ${err instanceof Error ? err.message : String(err)}`,
								);
							});
						}
					}
				}
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

				// mergeEntry increments observation_count — check synthesis threshold.
				// Same logic as the keep branch: read the fresh post-merge value from DB.
				const mergeThreshold = config.consolidation.synthesisObservationThreshold;
				if (mergeThreshold > 0) {
					const mergedEntry = this.db.getEntry(nearestEntry.id);
					if (mergedEntry) {
						const mergeObs = mergedEntry.observationCount;
						const mergeLastSynth = mergedEntry.lastSynthesizedObservationCount ?? 0;
						if (mergeObs >= mergeLastSynth + mergeThreshold) {
							this.attemptSynthesis(mergedEntry, entriesMap, freshEmbedding).catch((err) => {
								logger.warn(
									`[consolidation] Synthesis failed for entry ${mergedEntry.id} (stamp persisted at obs=${mergedEntry.observationCount}; next attempt at obs=${mergedEntry.observationCount + mergeThreshold}): ${err instanceof Error ? err.message : String(err)}`,
								);
							});
						}
					}
				}
				break;
			}

			case "insert": {
				const inserted = this.insertNewEntry(entry, sessionIds, entryEmbedding, sessionTimestamp);
				logger.log(
					`[consolidation] Insert (distinct despite similarity): "${entry.content.slice(0, 60)}..."`,
				);
				callbacks.onInsert(inserted);
				break;
			}
		}
	}

	/**
	 * Attempt cross-session synthesis for a well-reinforced entry.
	 *
	 * Builds the neighborhood from the FULL active KB plus the caller's in-memory
	 * entriesMap. The two sources are merged with entriesMap taking precedence:
	 * entries inserted/updated in the current run may have embedding=NULL in DB
	 * (ensureEmbeddings runs after the consolidation loop) but are available with
	 * fresh embeddings in the map — merging ensures they are not silently excluded
	 * from the neighborhood pool.
	 *
	 * This is intentionally fire-and-forget (called without await) so it
	 * never blocks the consolidation loop. Errors are caught by the caller.
	 */
	private async attemptSynthesis(
		anchor: KnowledgeEntry & { embedding?: number[] },
		entriesMap: Map<string, KnowledgeEntry & { embedding: number[] }>,
		anchorEmbeddingHint?: number[],
	): Promise<void> {
		// Need anchor embedding to find neighbors.
		// - For keep: use the cached embedding from entriesMap (content unchanged).
		// - For update/replace: the caller passes freshEmbedding as hint (the just-
		//   computed embedding for the merged content, same value mergeEntry wrote
		//   to DB). This avoids an immediate DB re-read of the row we just updated.
		// Fall back to a single-row DB query only when neither is available.
		const anchorEmbedding =
			anchorEmbeddingHint ??
			entriesMap.get(anchor.id)?.embedding ??
			anchor.embedding ??
			this.db.getEntryEmbedding(anchor.id);
		if (!anchorEmbedding) {
			logger.warn(
				`[synthesis] Skipping synthesis for ${anchor.id} — no embedding available.`,
			);
			return;
		}

		// Build candidate pool: start with the full DB (entries with committed embeddings)
		// then overlay the in-memory map so that entries inserted/updated in the current
		// run (embedding=NULL in DB until ensureEmbeddings runs) are not excluded.
		// entriesMap entries take precedence — they carry the most current embeddings.
		const dbEntries = this.db.getActiveEntriesWithEmbeddings();
		const candidateMap = new Map<string, KnowledgeEntry & { embedding: number[] }>();
		for (const e of dbEntries) {
			candidateMap.set(e.id, e);
		}
		for (const e of entriesMap.values()) {
			candidateMap.set(e.id, e); // in-memory overrides stale DB row
		}

		const scored: Array<{ entry: KnowledgeEntry & { embedding: number[] }; sim: number }> = [];
		for (const candidate of candidateMap.values()) {
			if (candidate.id === anchor.id) continue;
			scored.push({
				entry: candidate,
				sim: cosineSimilarity(anchorEmbedding, candidate.embedding),
			});
		}
		scored.sort((a, b) => b.sim - a.sim);
		const neighbors = scored.slice(0, SYNTHESIS_NEIGHBORS).map((s) => s.entry);

		if (neighbors.length === 0) {
			logger.log(
				`[synthesis] Skipping synthesis for ${anchor.id} — KB has no other entries with embeddings yet.`,
			);
			// Don't mark synthesized here — the KB may gain entries on future runs.
			// Unlike the null-result path (bar not met), this is a data-availability
			// gap, not a signal that synthesis was attempted and failed. We want to
			// retry when context is richer.
			return;
		}

		// Stamp the anchor as synthesized NOW — before the async LLM call — so
		// concurrent keep events for the same entry (possible with parallel source
		// readers) don't trigger a second synthesis call at the same evidence level.
		// Using the current observationCount as the stamp ensures the next threshold
		// check correctly computes: nextThreshold = observationCount + threshold.
		this.db.markSynthesized(anchor.id, anchor.observationCount);

		logger.log(
			`[synthesis] Attempting synthesis for "${anchor.content.slice(0, 60)}..." (obs=${anchor.observationCount}) with ${neighbors.length}/${SYNTHESIS_NEIGHBORS} neighbors.`,
		);

		const synthStart = Date.now();
		const result = await this.llm.synthesizePrinciple(
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

		logger.log(
			`[synthesis] synthesizePrinciple responded in ${((Date.now() - synthStart) / 1000).toFixed(1)}s.`,
		);

		if (!result) {
			logger.log(
				`[synthesis] No synthesis emerged for "${anchor.content.slice(0, 60)}..." — bar not met.`,
			);
			// markSynthesized was already called above — nothing else to do.
			return;
		}

		logger.log(
			`[synthesis] Synthesized ${result.type}: "${result.content.slice(0, 80)}..."`,
		);

		// Insert the synthesized entry. It inherits no session derivation —
		// its provenance is the source KB entry IDs it was derived from.
		const synthEntry = this.insertNewEntry(
			{
				type: result.type,
				content: result.content,
				topics: result.topics,
				confidence: result.confidence,
				scope: anchor.scope,
				source: `synthesis:${anchor.id}`,
			},
			[], // no session IDs — this is KB-derived, not episode-derived
			undefined, // no pre-computed embedding — ensureEmbeddings picks this up next run
		);

		// The synthesized entry has no embedding yet (computed by ensureEmbeddings on
		// the next server cycle). We therefore cannot add it to entriesMap (which
		// requires a number[] embedding). This means within the current consolidation
		// batch: (a) later extractions cannot dedup against this entry, and (b) the
		// contradiction scan won't see it. Both are acceptable — synthesis produces
		// high-level principles unlikely to be re-extracted in the same batch, and the
		// entry will be fully indexed before the next consolidation run.

		// Write `supports` relations: synthesized entry ← anchor + contributing neighbors.
		// Direction: synthesized (source) → evidence entry (target), type "supports".
		// Reads as: "this synthesis is supported by the evidence in those entries."
		const allSourceIds = [anchor.id, ...result.sourceIds];
		for (const sourceId of allSourceIds) {
			this.db.insertRelation({
				id: randomUUID(),
				sourceId: synthEntry.id,
				targetId: sourceId,
				type: "supports",
				createdAt: Date.now(),
			});
		}

		logger.log(
			`[synthesis] Inserted synthesized entry ${synthEntry.id} with ${allSourceIds.length} supports relations.`,
		);
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
			sessionTimestamp != null
				? Math.min(sessionTimestamp, now)
				: now;
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
