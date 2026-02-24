import type { KnowledgeDB } from "../db/database.js";
import { EmbeddingClient, cosineSimilarity } from "./embeddings.js";
import { config } from "../config.js";
import type { ActivationResult, KnowledgeEntry } from "../types.js";

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

    return {
      entries: scored.map(({ entry, similarity, staleness }) => ({
        entry: { ...entry, embedding: undefined } as KnowledgeEntry,
        similarity,
        staleness,
      })),
      query,
      totalActive: entries.length,
    };
  }

  /**
   * Ensure all active entries have embeddings.
   * Called during consolidation (new entries) or on startup (migration).
   */
  async ensureEmbeddings(): Promise<number> {
    const entries = this.db.getActiveEntries();
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
