import type { KnowledgeDB } from "../db/database.js";
import { EmbeddingClient, cosineSimilarity } from "./embeddings.js";
import { config } from "../config.js";
import type { ActivationResult, ContradictionAnnotation, KnowledgeEntry } from "../types.js";

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
  private db: KnowledgeDB;
  private embeddings: EmbeddingClient;

  constructor(db: KnowledgeDB) {
    this.db = db;
    this.embeddings = new EmbeddingClient();
  }

  /**
   * Activate knowledge entries based on a query or set of cues.
   *
   * This is the single retrieval mechanism — used by both:
   * - The plugin (passive: user query -> activate -> inject)
   * - The MCP tool (active: agent sends cues -> activate -> return)
   *
   * @param query - The activation cue (user message, agent-generated cues, etc.)
   * @returns Ranked knowledge entries above the similarity threshold, with staleness signals
   */
  async activate(query: string): Promise<ActivationResult> {
    const entries = this.db.getActiveEntriesWithEmbeddings();

    if (entries.length === 0) {
      return { entries: [], query, totalActive: 0 };
    }

    // Embed the query
    const queryEmbedding = await this.embeddings.embed(query);

    const now = Date.now();
    const DAY_MS = 1000 * 60 * 60 * 24;

    // Compute similarity for all active entries
    const scored = entries
      .map((entry) => {
        const rawSimilarity = cosineSimilarity(queryEmbedding, entry.embedding);
        const ageDays = (now - entry.createdAt) / DAY_MS;
        const lastAccessedDaysAgo = (now - entry.lastAccessedAt) / DAY_MS;

        // Determine staleness: facts older than their half-life with low access are suspect
        const halfLife = config.decay.typeHalfLife[entry.type] || config.decay.typeHalfLife.fact;
        const mayBeStale = ageDays > halfLife && entry.accessCount < 3;

        return {
          entry,
          // Raw similarity modulated by entry strength
          // (stronger entries activate more easily, like well-consolidated memories)
          similarity: rawSimilarity * entry.strength,
          staleness: {
            ageDays: Math.round(ageDays),
            strength: entry.strength,
            lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
            mayBeStale,
          },
        };
      })
      .filter((s) => s.similarity >= config.activation.similarityThreshold)
      .sort((a, b) => b.similarity - a.similarity)
      .slice(0, config.activation.maxResults);

    // Record access for activated entries (reinforces their strength)
    for (const { entry } of scored) {
      this.db.recordAccess(entry.id);
    }

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

    const contradictPairs = this.db.getContradictPairsForIds(conflictedIds);

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
      entries: scored.map(({ entry, similarity, staleness }) => ({
        entry: { ...entry, embedding: undefined } as KnowledgeEntry,
        similarity,
        staleness,
        contradiction: contradictionMap.get(entry.id),
      })),
      query,
      totalActive: entries.length,
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
   */
  async ensureEmbeddings(): Promise<number> {
    const active = this.db.getActiveEntries();
    const conflicted = this.db.getEntriesByStatus("conflicted");
    const entries = [...active, ...conflicted];
    const needsEmbedding = entries.filter((e) => !e.embedding);

    if (needsEmbedding.length === 0) return 0;

    // Build embedding text: content + topics for richer representation
    const texts = needsEmbedding.map(
      (e) => `[${e.type}] ${e.content} (topics: ${e.topics.join(", ")})`
    );

    const embeddings = await this.embeddings.embedBatch(texts);

    for (let i = 0; i < needsEmbedding.length; i++) {
      this.db.updateEntry(needsEmbedding[i].id, {
        embedding: embeddings[i],
      });
    }

    return needsEmbedding.length;
  }
}
