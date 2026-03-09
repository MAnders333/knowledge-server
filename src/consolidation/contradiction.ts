import { cosineSimilarity } from "../activation/embeddings.js";
import { config } from "../config.js";
import type { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";
import { RECONSOLIDATION_THRESHOLD } from "../types.js";
import type { KnowledgeEntry } from "../types.js";
import type { ConsolidationLLM } from "./llm.js";

/**
 * Post-extraction contradiction scanner.
 *
 * Responsibilities:
 * - For each newly inserted/updated entry (changedIds), find topic-overlapping
 *   candidates in the mid-similarity band [contradictionMinSimilarity, RECONSOLIDATION_THRESHOLD).
 *   Entries above the upper bound were already handled by Reconsolidator.decideMerge.
 *   Entries below the lower bound are too dissimilar to plausibly contradict.
 * - Ask the LLM to detect and resolve genuine contradictions.
 * - Apply the resolution to the DB (supersede, merge, or flag as irresolvable).
 * - Track which candidates were already resolved this pass to avoid double-processing.
 */
export class ContradictionScanner {
	private db: KnowledgeDB;
	private llm: ConsolidationLLM;

	constructor(db: KnowledgeDB, llm: ConsolidationLLM) {
		this.db = db;
		this.llm = llm;
	}

	/**
	 * Run the contradiction scan for all changed entries in a chunk.
	 *
	 * Two passes:
	 *
	 * Pass 1 — pre-existing vs new (DB query).
	 *   For each newly changed entry, query the DB for topic-overlapping pre-existing
	 *   entries (changedIds excluded). Pre-existing entries were already checked in a
	 *   prior consolidation run so they're only candidates, not initiators.
	 *
	 * Pass 2 — intra-chunk pairs (entriesMap).
	 *   Entries inserted or updated in the same chunk are excluded from the DB query
	 *   (decideMerge handles the sim ≥ 0.82 paths; entries that fall in the mid-band
	 *   between two chunk-inserted entries are never scanned otherwise). This pass
	 *   checks changedIds entries against each other using the in-memory entriesMap.
	 *   Only pairs where NEITHER has been through decideMerge together are candidates:
	 *   if two entries were compared by decideMerge, they were at sim ≥ 0.82 and the
	 *   result was "insert" (both kept) — decideMerge already handled the merge question
	 *   so only the mid-band [minSim, 0.82) pairs are worth scanning here too.
	 *
	 * Returns counts of detected and resolved contradictions.
	 */
	async scan(
		entriesMap: Map<string, KnowledgeEntry & { embedding: number[] }>,
		changedIds: Set<string>,
	): Promise<{ detected: number; resolved: number }> {
		let detected = 0;
		let resolved = 0;

		if (changedIds.size === 0) return { detected, resolved };

		const minSim = config.consolidation.contradictionMinSimilarity;

		// Track entries superseded during this scan pass to avoid double-processing
		// a candidate that was already resolved by an earlier entry in the same pass.
		const supersededInThisScan = new Set<string>();

		// Iterate changedIds directly — O(k) where k = changed entries, not O(n) map size
		for (const id of changedIds) {
			const entry = entriesMap.get(id);
			if (!entry) continue; // was deleted from map (superseded during reconsolidation)
			// Skip if this entry was itself superseded by a previous scan iteration
			if (supersededInThisScan.has(entry.id)) continue;
			if (!entry.topics.length || !entry.embedding) continue;

			// Pass 1: Find topic-overlapping pre-existing entries from DB.
			// Exclude all changedIds — they were either handled by decideMerge (sim ≥ 0.82)
			// or will be covered in pass 2 (intra-chunk pairs, below).
			const candidates = this.db.getEntriesWithOverlappingTopics(
				entry.topics,
				[...changedIds], // only exclude chunk-changed entries, not all of entriesMap
			);

			if (candidates.length === 0) continue;

			// Filter to the mid-similarity band: low enough to have been missed by
			// decideMerge, high enough to be plausibly related (not just same topic word)
			const entryEmbedding = entry.embedding;
			const midBandCandidates = candidates.filter((c) => {
				if (supersededInThisScan.has(c.id)) return false; // skip already-resolved candidates
				const sim = cosineSimilarity(entryEmbedding, c.embedding);
				return sim >= minSim && sim < RECONSOLIDATION_THRESHOLD;
			});

			if (midBandCandidates.length === 0) continue;

			logger.log(
				`[contradiction] Checking ${midBandCandidates.length} candidates for "${entry.content.slice(0, 60)}..."`,
			);

			const validCandidateIds = new Set(midBandCandidates.map((c) => c.id));

			const llmStart = Date.now();
			const results = await this.llm.detectAndResolveContradiction(
				{
					id: entry.id,
					content: entry.content,
					type: entry.type,
					topics: entry.topics,
					confidence: entry.confidence,
					createdAt: entry.createdAt,
				},
				midBandCandidates.map((c) => ({
					id: c.id,
					content: c.content,
					type: c.type,
					topics: c.topics,
					confidence: c.confidence,
					createdAt: c.createdAt,
				})),
			);
			logger.log(
				`[contradiction] LLM responded in ${((Date.now() - llmStart) / 1000).toFixed(1)}s (${midBandCandidates.length} candidates).`,
			);

			let entrySuperseded = false;
			for (const result of results) {
				// Guard: reject any candidateId the LLM returned that was not in the
				// input candidate list. A hallucinated ID would silently no-op (UPDATE
				// WHERE id = <non-existent>) or — worse — self-supersede the newEntry
				// if the LLM echoed entry.id back as the candidateId.
				if (!validCandidateIds.has(result.candidateId)) {
					logger.warn(
						`[contradiction] LLM returned candidateId "${result.candidateId}" not in candidate list — skipping`,
					);
					continue;
				}
				detected++;
				logger.log(
					`[contradiction] ${result.resolution}: "${entry.content.slice(0, 50)}..." vs candidate ${result.candidateId.slice(0, 8)}... — ${result.reason}`,
				);

				const mergedData =
					result.resolution === "merge" &&
					result.mergedContent &&
					result.mergedType &&
					result.mergedTopics &&
					result.mergedConfidence !== undefined
						? {
								content: result.mergedContent,
								type: result.mergedType,
								topics: result.mergedTopics,
								confidence: result.mergedConfidence,
							}
						: undefined;

				this.db.applyContradictionResolution(
					result.resolution,
					entry.id,
					result.candidateId,
					mergedData,
				);

				// supersede_old, supersede_new, and merge are all "resolved" — irresolvable needs human
				if (result.resolution !== "irresolvable") {
					resolved++;
				}

				if (
					result.resolution === "supersede_old" ||
					result.resolution === "merge"
				) {
					// Candidate is now superseded — don't let later entries re-process it
					supersededInThisScan.add(result.candidateId);
				}

				if (result.resolution === "supersede_new") {
					// This entry lost — remove from map and stop checking its other candidates
					supersededInThisScan.add(entry.id);
					entriesMap.delete(entry.id);
					entrySuperseded = true;
					break;
				}
			}

			if (entrySuperseded) continue;
		}

		// Pass 2 — intra-chunk pairs.
		// Scan each changedId against all later changedIds (upper triangle only — each
		// unordered pair is checked exactly once). Uses the in-memory entriesMap so
		// entries that exist only in memory (not yet committed via ensureEmbeddings)
		// are still visible. The DB query in pass 1 excluded all changedIds, so any
		// mid-band contradiction between two chunk-inserted entries was missed above.
		const changedArr = [...changedIds];
		for (let i = 0; i < changedArr.length; i++) {
			const entryA = entriesMap.get(changedArr[i]);
			if (!entryA) continue; // deleted (superseded in pass 1 or earlier in this pass)
			if (supersededInThisScan.has(entryA.id)) continue;
			if (!entryA.topics.length || !entryA.embedding) continue;

			const intraChunkCandidates: Array<
				KnowledgeEntry & { embedding: number[] }
			> = [];
			for (let j = i + 1; j < changedArr.length; j++) {
				const entryB = entriesMap.get(changedArr[j]);
				if (!entryB) continue;
				if (supersededInThisScan.has(entryB.id)) continue;
				if (!entryB.embedding) continue;

				// Only topic-overlapping pairs are worth scanning
				const hasTopicOverlap = entryA.topics.some((t) =>
					entryB.topics.includes(t),
				);
				if (!hasTopicOverlap) continue;

				const sim = cosineSimilarity(entryA.embedding, entryB.embedding);
				// Only mid-band: sim ≥ 0.82 was already handled by decideMerge (the
				// "insert" decision means decideMerge considered them distinct; contradiction
				// scan is the right follow-up for mid-band pairs that weren't compared at all).
				if (sim >= minSim && sim < RECONSOLIDATION_THRESHOLD) {
					intraChunkCandidates.push(entryB);
				}
			}

			if (intraChunkCandidates.length === 0) continue;

			logger.log(
				`[contradiction] Intra-chunk: checking ${intraChunkCandidates.length} same-chunk candidates for "${entryA.content.slice(0, 60)}..."`,
			);

			const validCandidateIds = new Set(intraChunkCandidates.map((c) => c.id));
			const llmStart = Date.now();
			const results = await this.llm.detectAndResolveContradiction(
				{
					id: entryA.id,
					content: entryA.content,
					type: entryA.type,
					topics: entryA.topics,
					confidence: entryA.confidence,
					createdAt: entryA.createdAt,
				},
				intraChunkCandidates.map((c) => ({
					id: c.id,
					content: c.content,
					type: c.type,
					topics: c.topics,
					confidence: c.confidence,
					createdAt: c.createdAt,
				})),
			);
			logger.log(
				`[contradiction] Intra-chunk LLM responded in ${((Date.now() - llmStart) / 1000).toFixed(1)}s (${intraChunkCandidates.length} candidates).`,
			);

			let entryASuperseded = false;
			for (const result of results) {
				if (!validCandidateIds.has(result.candidateId)) {
					logger.warn(
						`[contradiction] Intra-chunk: LLM returned candidateId "${result.candidateId}" not in candidate list — skipping`,
					);
					continue;
				}
				detected++;
				logger.log(
					`[contradiction] Intra-chunk ${result.resolution}: "${entryA.content.slice(0, 50)}..." vs candidate ${result.candidateId.slice(0, 8)}... — ${result.reason}`,
				);

				const mergedData =
					result.resolution === "merge" &&
					result.mergedContent &&
					result.mergedType &&
					result.mergedTopics &&
					result.mergedConfidence !== undefined
						? {
								content: result.mergedContent,
								type: result.mergedType,
								topics: result.mergedTopics,
								confidence: result.mergedConfidence,
							}
						: undefined;

				this.db.applyContradictionResolution(
					result.resolution,
					entryA.id,
					result.candidateId,
					mergedData,
				);

				if (result.resolution !== "irresolvable") {
					resolved++;
				}

				if (
					result.resolution === "supersede_old" ||
					result.resolution === "merge"
				) {
					supersededInThisScan.add(result.candidateId);
					entriesMap.delete(result.candidateId);
				}

				if (result.resolution === "supersede_new") {
					supersededInThisScan.add(entryA.id);
					entriesMap.delete(entryA.id);
					entryASuperseded = true;
					break;
				}
			}

			if (entryASuperseded) continue;
		}

		if (detected > 0) {
			logger.log(
				`[contradiction] Scan complete: ${detected} contradictions found, ${resolved} resolved, ${detected - resolved} flagged for review.`,
			);
		}

		return { detected, resolved };
	}
}
