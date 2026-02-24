import { randomUUID } from "node:crypto";
import type { KnowledgeDB } from "../db/database.js";
import type { ActivationEngine } from "../activation/activate.js";
import { EmbeddingClient, cosineSimilarity } from "../activation/embeddings.js";
import { EpisodeReader } from "./episodes.js";
import { ConsolidationLLM } from "./llm.js";
import { computeStrength } from "./decay.js";
import { config } from "../config.js";
import type { Episode, ConsolidationResult, KnowledgeEntry } from "../types.js";
import type { ExtractedKnowledge } from "./llm.js";

/**
 * Maximum number of existing knowledge entries to include as context
 * for each chunk's LLM call. Using embedding similarity to select
 * only the most relevant entries keeps the context focused and fast.
 */
const MAX_RELEVANT_KNOWLEDGE = 50;

/**
 * Cosine similarity threshold above which a newly extracted entry is
 * considered a candidate for reconsolidation with an existing entry.
 * At 0.82+, the content is topically very close — worth asking the LLM
 * whether to merge, update, replace, or keep separate.
 * Below this threshold, the entry is novel enough to insert directly.
 */
const RECONSOLIDATION_THRESHOLD = 0.82;

/**
 * The consolidation engine — the heart of the knowledge system.
 *
 * Models the human brain's sleep consolidation process:
 * 1. Read NEW episodes (since last consolidation cursor)
 * 2. Load EXISTING knowledge (the current mental model)
 * 3. Extract new knowledge from episodes (what's worth remembering?)
 * 4. Detect and resolve conflicts with existing knowledge
 * 5. Apply decay to all entries (forgetting curve)
 * 6. Generate embeddings for new entries
 * 7. Advance the cursor
 */
export class ConsolidationEngine {
  private db: KnowledgeDB;
  private activation: ActivationEngine;
  private embeddings: EmbeddingClient;
  private episodes: EpisodeReader;
  private llm: ConsolidationLLM;

  constructor(db: KnowledgeDB, activation: ActivationEngine) {
    this.db = db;
    this.activation = activation;
    this.embeddings = new EmbeddingClient();
    this.episodes = new EpisodeReader();
    this.llm = new ConsolidationLLM();
  }

  /**
   * Check how many sessions are pending consolidation without running it.
   * Used at startup to decide whether to kick off a background consolidation.
   */
  checkPending(): { pendingSessions: number; lastConsolidatedAt: number } {
    const state = this.db.getConsolidationState();
    const pending = this.episodes.countNewSessions(state.lastSessionTimeCreated);
    return { pendingSessions: pending, lastConsolidatedAt: state.lastConsolidatedAt };
  }

  /**
   * Run a consolidation cycle.
   *
   * This is the main entry point — called by HTTP API or CLI.
   *
   * Incremental within-session consolidation (Option D):
   * 1. Load already-processed episode ranges from consolidated_episode
   * 2. Fetch sessions since the session-level cursor, then filter out
   *    already-processed (startMessageId, endMessageId) ranges per session
   * 3. Process only the new episodes
   * 4. Record each episode to consolidated_episode immediately after processing
   * 5. Advance the session-level cursor to cover fully-processed sessions
   */
  async consolidate(): Promise<ConsolidationResult> {
    const startTime = Date.now();
    const state = this.db.getConsolidationState();

    console.log(
      `[consolidation] Starting. Last run: ${state.lastConsolidatedAt ? new Date(state.lastConsolidatedAt).toISOString() : "never"}`
    );

    // 1. Fetch candidate sessions (id + time_created) up to the batch limit.
    //    We need time_created for ALL fetched sessions — not just ones that produce
    //    episodes — so the cursor always advances past the full batch, even when
    //    sessions are skipped due to minSessionMessages filtering.
    const candidateSessions = this.episodes.getCandidateSessions(
      state.lastSessionTimeCreated,
      config.consolidation.maxSessionsPerRun
    );

    if (candidateSessions.length === 0) {
      console.log("[consolidation] No new sessions to process.");
      return {
        sessionsProcessed: 0,
        segmentsProcessed: 0,
        entriesCreated: 0,
        entriesUpdated: 0,
        entriesArchived: 0,
        conflictsDetected: 0,
        conflictsResolved: 0,
        duration: Date.now() - startTime,
      };
    }

    // The cursor will advance to the max time_created of ALL fetched sessions —
    // this ensures sessions with too few messages don't stall the cursor.
    const maxCandidateTime = candidateSessions.reduce((max, s) => s.timeCreated > max ? s.timeCreated : max, 0);
    const candidateIds = candidateSessions.map((s) => s.id);

    // 2. Load already-processed episode ranges for this batch of sessions.
    const processedRanges = this.db.getProcessedEpisodeRanges(candidateIds);

    // 3. Segment sessions into episodes, skipping already-processed ranges.
    const episodes = this.episodes.getNewEpisodes(candidateIds, processedRanges);

    // Count unique sessions that produced at least one new episode
    const uniqueSessionIds = new Set(episodes.map((e) => e.sessionId));

    // Directly classify each skipped session rather than computing by subtraction
    // (subtraction can go negative for partially-processed sessions that still have new episodes).
    let alreadyDone = 0; // had prior episodes recorded, nothing new
    let tooFew = 0;      // no prior episodes, didn't pass minSessionMessages filter
    for (const id of candidateIds) {
      if (uniqueSessionIds.has(id)) continue; // produced new episodes — not skipped
      if (processedRanges.has(id)) {
        alreadyDone++;  // some episodes were previously recorded; no new tail
      } else {
        tooFew++;       // never produced episodes — below minSessionMessages
      }
    }

    console.log(
      `[consolidation] Found ${episodes.length} episodes from ${uniqueSessionIds.size} sessions to process` +
      ` (${tooFew} skipped — too few messages, ${alreadyDone} skipped — already processed).`
    );

    // 4. Process episodes in chunks
    let totalCreated = 0;
    let totalUpdated = 0;

    const chunkSize = config.consolidation.chunkSize;

    for (let i = 0; i < episodes.length; i += chunkSize) {
      const chunk = episodes.slice(i, i + chunkSize);
      const chunkSummary = this.formatEpisodes(chunk);

      console.log(
        `[consolidation] Processing chunk ${Math.floor(i / chunkSize) + 1}/${Math.ceil(episodes.length / chunkSize)} (${chunk.length} episodes)`
      );

      // Retrieve only RELEVANT existing knowledge for this chunk
      // (instead of dumping the entire knowledge base into the prompt)
      const relevantKnowledge = await this.getRelevantKnowledge(chunkSummary);
      const existingKnowledgeSummary = this.formatExistingKnowledge(relevantKnowledge);

      console.log(
        `[consolidation] Using ${relevantKnowledge.length} relevant existing entries as context.`
      );

      // Extract knowledge via LLM
      const extracted = await this.llm.extractKnowledge(
        chunkSummary,
        existingKnowledgeSummary
      );

      console.log(
        `[consolidation] Extracted ${extracted.length} knowledge entries from chunk.`
      );

      // Reconsolidate each extracted entry against existing knowledge.
      // For each extracted entry:
      //   1. Embed it
      //   2. Find the nearest existing entry by cosine similarity
      //   3. If similarity > RECONSOLIDATION_THRESHOLD: ask LLM to decide
      //      keep / update / replace / insert
      //   4. Act on the decision
      //
      // Performance: load all entries with embeddings ONCE per chunk (not per entry).
      // Refresh the cache after each insert/update so later entries in the same
      // chunk see the newly inserted/updated entries.
      const sessionIds = [...new Set(chunk.map((e) => e.sessionId))];
      let cachedEntries = this.db.getActiveEntriesWithEmbeddings();
      let chunkCreated = 0;
      let chunkUpdated = 0;

      for (const entry of extracted) {
        await this.reconsolidate(entry, sessionIds, cachedEntries, {
          onInsert: () => {
            totalCreated++;
            chunkCreated++;
            cachedEntries = this.db.getActiveEntriesWithEmbeddings();
          },
          onUpdate: () => {
            totalUpdated++;
            chunkUpdated++;
            cachedEntries = this.db.getActiveEntriesWithEmbeddings();
          },
          onKeep: () => {},
        });
      }

      // 4. Record each episode in this chunk as processed.
      //    This happens after the LLM call and DB writes succeed, making
      //    consolidation idempotent on crash/retry at the episode level.
      //    entriesCreated is split evenly across episodes in the chunk as an approximation
      //    (we don't track which entries came from which specific episode).
      const entriesPerEp = chunk.length > 0 ? Math.round((chunkCreated + chunkUpdated) / chunk.length) : 0;
      for (const ep of chunk) {
        this.db.recordEpisode(
          ep.sessionId,
          ep.startMessageId,
          ep.endMessageId,
          ep.contentType,
          entriesPerEp
        );
      }
    }

    // 5. Apply decay to ALL active entries
    const archived = this.applyDecay();

    // 6. Generate embeddings for new entries
    const embeddedCount = await this.activation.ensureEmbeddings();
    console.log(
      `[consolidation] Generated embeddings for ${embeddedCount} entries.`
    );

    // 7. Advance cursor past ALL fetched candidate sessions (not just episode-producing ones).
    //    This prevents sessions with too few messages from stalling the cursor.
    this.db.updateConsolidationState({
      lastConsolidatedAt: Date.now(),
      lastSessionTimeCreated: maxCandidateTime,
      totalSessionsProcessed: state.totalSessionsProcessed + candidateSessions.length,
      totalEntriesCreated: state.totalEntriesCreated + totalCreated,
      totalEntriesUpdated: state.totalEntriesUpdated + totalUpdated,
    });

    const result: ConsolidationResult = {
      sessionsProcessed: candidateSessions.length,
      segmentsProcessed: episodes.length,
      entriesCreated: totalCreated,
      entriesUpdated: totalUpdated,
      entriesArchived: archived,
      conflictsDetected: 0,
      conflictsResolved: 0,
      duration: Date.now() - startTime,
    };

    console.log(
      `[consolidation] Complete. ${result.sessionsProcessed} sessions (${result.segmentsProcessed} segments) -> ${result.entriesCreated} entries (${result.entriesArchived} archived, ${result.conflictsDetected} conflicts, ${result.conflictsResolved} resolved) in ${result.duration}ms`
    );

    return result;
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
  private async reconsolidate(
    entry: ExtractedKnowledge,
    sessionIds: string[],
    /**
     * Pre-loaded active entries with embeddings — callers must pass this in
     * to avoid an N+1 DB query per extracted entry. Callers should refresh
     * this cache after each insert/update so later entries in the same chunk
     * see newly added entries.
     *
     * Note: after an "update"/"replace" decision, the matched entry's embedding
     * is cleared (mergeEntry sets embedding = NULL). If a later extracted entry
     * in the same chunk is similar to the same existing entry, it won't find it
     * via embedding lookup and will be inserted as novel. This is acceptable —
     * the re-embedded version will be available for the next consolidation run.
     */
    existingEntries: Array<KnowledgeEntry & { embedding: number[] }>,
    callbacks: {
      onInsert: () => void;
      onUpdate: () => void;
      onKeep: () => void;
    }
  ): Promise<void> {
    // Embed the extracted entry content
    const entryEmbedding = await this.embeddings.embed(entry.content);

    // Find nearest existing entry from the pre-loaded cache
    let nearestEntry: KnowledgeEntry | null = null;
    let nearestSimilarity = 0;

    for (const existing of existingEntries) {
      const sim = cosineSimilarity(entryEmbedding, existing.embedding);
      if (sim > nearestSimilarity) {
        nearestSimilarity = sim;
        nearestEntry = existing;
      }
    }

    // Below threshold → clearly novel, insert directly
    if (!nearestEntry || nearestSimilarity < RECONSOLIDATION_THRESHOLD) {
      this.insertNewEntry(entry, sessionIds, entryEmbedding);
      callbacks.onInsert();
      console.log(
        `[consolidation] Insert (novel, sim=${nearestSimilarity.toFixed(3)}): "${entry.content.slice(0, 60)}..."`
      );
      return;
    }

    // Above threshold → ask LLM for a focused merge decision
    console.log(
      `[consolidation] Reconsolidation candidate (sim=${nearestSimilarity.toFixed(3)}): "${entry.content.slice(0, 60)}..." vs "${nearestEntry.content.slice(0, 60)}..."`
    );

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
      }
    );

    switch (decision.action) {
      case "keep":
        console.log(`[consolidation] Keep existing (no new info): "${nearestEntry.content.slice(0, 60)}..."`);
        callbacks.onKeep();
        break;

      case "update":
      case "replace":
        this.db.mergeEntry(nearestEntry.id, {
          content: decision.content,
          type: decision.type,
          topics: decision.topics,
          confidence: Math.max(0, Math.min(1, decision.confidence)),
          additionalSources: sessionIds,
        });
        // mergeEntry clears the embedding — ensureEmbeddings will regenerate it
        console.log(`[consolidation] ${decision.action === "update" ? "Updated" : "Replaced"}: "${nearestEntry.content.slice(0, 60)}..." → "${decision.content.slice(0, 60)}..."`);
        callbacks.onUpdate();
        break;

      case "insert":
        this.insertNewEntry(entry, sessionIds, entryEmbedding);
        console.log(`[consolidation] Insert (distinct despite similarity): "${entry.content.slice(0, 60)}..."`);
        callbacks.onInsert();
        break;
    }
  }

  /**
   * Insert a new knowledge entry into the DB.
   * Optionally pre-supply the embedding to avoid re-computing it.
   */
  private insertNewEntry(
    entry: ExtractedKnowledge,
    sessionIds: string[],
    embedding?: number[]
  ): void {
    const now = Date.now();
    const newEntry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] } = {
      id: randomUUID(),
      type: entry.type,
      content: entry.content,
      topics: entry.topics || [],
      confidence: Math.max(0, Math.min(1, entry.confidence || 0.5)),
      source: entry.source || `consolidation ${new Date().toISOString().split("T")[0]}`,
      scope: entry.scope || "personal",
      status: "active",
      strength: 1.0,
      createdAt: now,
      updatedAt: now,
      lastAccessedAt: now,
      accessCount: 0,
      supersededBy: null,
      derivedFrom: sessionIds,
      embedding,
    };
    this.db.insertEntry(newEntry);
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
  private async getRelevantKnowledge(
    chunkSummary: string
  ): Promise<KnowledgeEntry[]> {
    const allEntries = this.db.getActiveEntriesWithEmbeddings();

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

  /**
   * Apply decay to all active entries.
   * Returns the number of entries that were archived.
   */
  private applyDecay(): number {
    const entries = this.db.getActiveEntries();
    let archived = 0;

    for (const entry of entries) {
      const newStrength = computeStrength(entry);

      if (newStrength < config.decay.archiveThreshold) {
        this.db.updateEntry(entry.id, {
          status: "archived",
          strength: newStrength,
        });
        archived++;
        console.log(
          `[decay] Archived: "${entry.content.slice(0, 60)}..." (strength: ${newStrength.toFixed(3)})`
        );
      } else if (Math.abs(newStrength - entry.strength) > 0.01) {
        // Only update if strength changed meaningfully
        this.db.updateStrength(entry.id, newStrength);
      }
    }

    // Tombstone long-archived entries
    const archivedEntries = this.db.getEntriesByStatus("archived");
    const tombstoneThreshold =
      Date.now() - config.decay.tombstoneAfterDays * 24 * 60 * 60 * 1000;

    for (const entry of archivedEntries) {
      if (entry.updatedAt < tombstoneThreshold) {
        this.db.updateEntry(entry.id, { status: "tombstoned" });
        console.log(
          `[decay] Tombstoned: "${entry.content.slice(0, 60)}..." (archived for ${config.decay.tombstoneAfterDays}+ days)`
        );
      }
    }

    return archived;
  }

  /**
   * Format existing knowledge entries for the LLM context.
   * This is the "existing mental model" that new episodes are consolidated against.
   */
  private formatExistingKnowledge(entries: KnowledgeEntry[]): string {
    if (entries.length === 0) return "";

    return entries
      .map(
        (e) =>
          `- [${e.type}] ${e.content} (topics: ${e.topics.join(", ")}; confidence: ${e.confidence}; scope: ${e.scope})`
      )
      .join("\n");
  }

  /**
   * Format a batch of episodes into a text summary for the LLM.
   * Episodes can be either compaction summaries (already condensed)
   * or raw message sequences.
   */
  private formatEpisodes(episodes: Episode[]): string {
    return episodes
      .map((ep) => {
        const typeLabel = ep.contentType === "compaction_summary"
          ? " (compaction summary)"
          : "";

        return `### Session: "${ep.sessionTitle}"${typeLabel} (${new Date(ep.timeCreated).toISOString().split("T")[0]}, project: ${ep.projectName})
${ep.content}`;
      })
      .join("\n\n---\n\n");
  }

  close(): void {
    this.episodes.close();
  }
}
