import { randomUUID } from "node:crypto";
import type { EmbeddingClient } from "../activation/embeddings.js";
import {
	cosineSimilarity,
	formatEmbeddingText,
} from "../activation/embeddings.js";
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
			case "keep":
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
		const entryTime = sessionTimestamp
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
