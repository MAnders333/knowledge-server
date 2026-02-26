import { randomUUID } from "node:crypto";
import type { KnowledgeDB } from "../db/database.js";
import type { ActivationEngine } from "../activation/activate.js";
import { EmbeddingClient, cosineSimilarity, formatEmbeddingText } from "../activation/embeddings.js";
import { EpisodeReader } from "./episodes.js";
import { ConsolidationLLM } from "./llm.js";
import { computeStrength } from "./decay.js";
import { config } from "../config.js";
import { clampKnowledgeType } from "../types.js";
import type { Episode, ConsolidationResult, KnowledgeEntry } from "../types.js";
import type { ExtractedKnowledge, ContradictionResult } from "./llm.js";

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

  /**
   * Concurrency guard — only one consolidation run at a time, regardless of
   * whether it was triggered by the startup background loop or an API call.
   * Lives here (not in the API closure) so both paths share the same flag.
   *
   * Use tryLock() / unlock() instead of mutating directly — this keeps external
   * callers from accidentally bypassing or prematurely clearing the guard.
   */
  private _isConsolidating = false;

  get isConsolidating(): boolean {
    return this._isConsolidating;
  }

  /**
   * Atomically claim the consolidation lock.
   * Returns true if the lock was acquired (caller should proceed),
   * false if another run is already in progress (caller should abort).
   *
   * Must be called synchronously (no await before this call) to be race-free
   * under Node/Bun's single-threaded event loop.
   */
  tryLock(): boolean {
    if (this._isConsolidating) return false;
    this._isConsolidating = true;
    return true;
  }

  unlock(): void {
    this._isConsolidating = false;
  }

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
    const pending = this.episodes.countNewSessions(state.lastMessageTimeCreated);
    return { pendingSessions: pending, lastConsolidatedAt: state.lastConsolidatedAt };
  }

  /**
   * Run a consolidation cycle.
   *
   * This is the main entry point — called by HTTP API or CLI.
   *
   * Per-run steps:
   * 1. Fetch candidate sessions since the cursor
   * 2. Load already-processed episode ranges to determine new work
   * 3. Segment sessions into episodes, skipping already-processed ranges
   * 4. For each chunk: extract → reconsolidate → contradiction scan → record episodes
   * 5. Apply decay to all active entries
   * 6. Generate embeddings for new/updated entries
   * 7. Advance the session cursor past all fetched candidates
   */
  async consolidate(): Promise<ConsolidationResult> {
    const startTime = Date.now();
    const state = this.db.getConsolidationState();

    console.log(
      `[consolidation] Starting. Last run: ${state.lastConsolidatedAt ? new Date(state.lastConsolidatedAt).toISOString() : "never"}`
    );

    // 1. Fetch candidate sessions: those with messages newer than the cursor.
    //    Returns session IDs plus the max message timestamp per session,
    //    ordered by max message time ASC for deterministic batching.
    const candidateSessions = this.episodes.getCandidateSessions(
      state.lastMessageTimeCreated,
      config.consolidation.maxSessionsPerRun
    );

    if (candidateSessions.length === 0) {
      console.log("[consolidation] No new sessions to process.");
      // Still run decay — entries must age even during quiet periods where no
      // new sessions arrive. Without this, the forgetting curve stops ticking.
      const archived = this.applyDecay();
      await this.activation.ensureEmbeddings();
      return {
        sessionsProcessed: 0,
        segmentsProcessed: 0,
        entriesCreated: 0,
        entriesUpdated: 0,
        entriesArchived: archived,
        conflictsDetected: 0,
        conflictsResolved: 0,
        duration: Date.now() - startTime,
      };
    }

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
    let totalConflictsDetected = 0;
    let totalConflictsResolved = 0;

    const chunkSize = config.consolidation.chunkSize;

    for (let i = 0; i < episodes.length; i += chunkSize) {
      const chunk = episodes.slice(i, i + chunkSize);
      const chunkSummary = this.formatEpisodes(chunk);

      console.log(
        `[consolidation] Processing chunk ${Math.floor(i / chunkSize) + 1}/${Math.ceil(episodes.length / chunkSize)} (${chunk.length} episodes)`
      );

      // Load entries once for this chunk — used both for relevance selection
      // (prompt context) and reconsolidation (dedup). Loaded here so getRelevantKnowledge
      // doesn't make a second DB call when we immediately need the same data below.
      const allEntriesForChunk = this.db.getActiveEntriesWithEmbeddings();

      // Retrieve only RELEVANT existing knowledge for this chunk
      // (instead of dumping the entire knowledge base into the prompt)
      const relevantKnowledge = await this.getRelevantKnowledge(chunkSummary, allEntriesForChunk);
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
      // Performance: load all entries with embeddings ONCE per chunk into an in-memory
      // Map. On insert/update, mutate the Map in place rather than reloading from DB.
      // This avoids an O(n × m) reload — previously ~9.7 MB × m reads per chunk.
      const sessionIds = [...new Set(chunk.map((e) => e.sessionId))];
      // Reuse the entries already loaded for getRelevantKnowledge — no second DB read.
      const entriesMap = new Map(allEntriesForChunk.map((e) => [e.id, e]));
      let chunkCreated = 0;
      let chunkUpdated = 0;

      // Track IDs that were inserted or updated this chunk — only these are
      // eligible for the contradiction scan (pre-existing entries were already
      // checked in a previous consolidation run).
      const changedIds = new Set<string>();

      for (const entry of extracted) {
        try {
          await this.reconsolidate(entry, sessionIds, entriesMap, {
            onInsert: (inserted) => {
              totalCreated++;
              chunkCreated++;
              changedIds.add(inserted.id);
              // Add to cache so subsequent entries in this chunk can deduplicate against it.
              // Embedding is available immediately since insertNewEntry stores it.
              if (inserted.embedding) {
                entriesMap.set(inserted.id, inserted as KnowledgeEntry & { embedding: number[] });
              }
            },
            onUpdate: (id, updated, freshEmbedding) => {
              totalUpdated++;
              chunkUpdated++;
              changedIds.add(id);
              // Update the cache with the new content and fresh embedding.
              // The fresh embedding was computed immediately after mergeEntry() and
              // written to the DB, so the in-memory map and DB are now in sync.
              // Later extractions in this chunk can deduplicate against the correct
              // vector, and the contradiction scan sees accurate cosine distances.
              const existing = entriesMap.get(id);
              if (existing) {
                entriesMap.set(id, {
                  ...existing,
                  content: updated.content ?? existing.content,
                  type: (updated.type as KnowledgeEntry["type"]) ?? existing.type,
                  topics: updated.topics ?? existing.topics,
                  confidence: updated.confidence ?? existing.confidence,
                  embedding: freshEmbedding,
                });
              }
            },
            onKeep: () => {},
          });
        } catch (err) {
          // Log and skip this extracted entry — do NOT rethrow.
          // Rethrowing would skip recordEpisode for the whole chunk, causing all
          // entries in this chunk to be re-processed on the next run and producing
          // duplicates for the entries that were already successfully inserted.
          console.error(
            `[consolidation] Failed to reconsolidate entry "${String(entry.content ?? "").slice(0, 60)}..." — skipping:`,
            err
          );
        }
      }

      // 5. Post-extraction contradiction scan.
      //    For each newly inserted/updated entry, find topic-overlapping entries
      //    in the mid-similarity band (contradictionMinSimilarity–RECONSOLIDATION_THRESHOLD).
      //    Entries above the upper bound were already handled by decideMerge.
      //    Entries below the lower bound are too dissimilar to plausibly contradict.
      const chunkContradictions = await this.runContradictionScan(entriesMap, changedIds);
      totalConflictsDetected += chunkContradictions.detected;
      totalConflictsResolved += chunkContradictions.resolved;

      // 6. Record each episode in this chunk as processed.
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

    // 7. Apply decay to ALL active entries
    const archived = this.applyDecay();

    // 8. Generate embeddings for new entries
    const embeddedCount = await this.activation.ensureEmbeddings();
    console.log(
      `[consolidation] Generated embeddings for ${embeddedCount} entries.`
    );

    // 9. Advance cursor to the max message timestamp across all processed episodes.
    //    This is the true high-water mark: any message with time_created > this value
    //    is genuinely unprocessed.
    const maxEpisodeMessageTime = episodes.length > 0
      ? episodes.reduce((max, ep) => ep.maxMessageTime > max ? ep.maxMessageTime : max, 0)
      : 0;

    // Start with the episode high-water mark; we'll decide below whether to also
    // advance past sessions that produced no episodes.
    let newCursor = Math.max(maxEpisodeMessageTime, state.lastMessageTimeCreated);

    const lastSession = candidateSessions[candidateSessions.length - 1];
    const hitBatchLimit = candidateSessions.length === config.consolidation.maxSessionsPerRun;

    // Boundary-timestamp safety: if the batch is full, there may be additional
    // unprocessed sessions beyond the batch boundary that share the exact same
    // maxMessageTime as the last session in this batch. The next query uses `>`,
    // so those sessions would be excluded forever if we advance the cursor to
    // lastSession.maxMessageTime.
    //
    // Guard: keep the cursor just below the boundary so those sessions are
    // re-fetched next run. Apply this cap whenever the batch is full, regardless
    // of whether the last session produced an episode — a session without episodes
    // (e.g. below minSessionMessages) could still share a timestamp with other
    // sessions that DO have episodes and are waiting beyond the boundary.
    //
    // We only apply the cap when it is strictly above the current cursor
    // (otherwise the safety floor below would undo it on the next line).
    if (hitBatchLimit) {
      // Cap below boundary; re-fetch same-timestamp sessions next run.
      const cap = lastSession.maxMessageTime - 1;
      if (cap > state.lastMessageTimeCreated) {
        newCursor = Math.min(newCursor, cap);
      }
    } else {
      // Batch is not full — no boundary risk. Advance past all candidates so
      // sessions that produced no episodes don't re-appear as candidates.
      newCursor = Math.max(newCursor, lastSession.maxMessageTime);
    }

    // Safety floor: never move the cursor backwards.
    newCursor = Math.max(newCursor, state.lastMessageTimeCreated);

    this.db.updateConsolidationState({
      lastConsolidatedAt: Date.now(),
      lastMessageTimeCreated: newCursor,
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
      conflictsDetected: totalConflictsDetected,
      conflictsResolved: totalConflictsResolved,
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
      onUpdate: (id: string, updated: Partial<KnowledgeEntry>, freshEmbedding: number[]) => void;
      onKeep: () => void;
    }
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
      const inserted = this.insertNewEntry(entry, sessionIds, entryEmbedding);
      callbacks.onInsert(inserted);
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
        // The same knowledge surfaced again in a new episode — reinforce it as evidence.
        // This increments observation_count (the evidence signal that extends half-life)
        // and resets last_accessed_at so decay restarts from now.
        // We use reinforceObservation rather than recordAccess — this is not a retrieval
        // event; it's confirmation that the knowledge is still true.
        this.db.reinforceObservation(nearestEntry.id);
        console.log(`[consolidation] Keep existing (reinforced): "${nearestEntry.content.slice(0, 60)}..."`);
        callbacks.onKeep();
        break;

      case "update":
      case "replace": {
        const mergeUpdates = {
          content: decision.content,
          type: decision.type,
          topics: decision.topics,
          confidence: Math.max(0, Math.min(1, decision.confidence)),
          additionalSources: sessionIds,
        };
        // Re-embed immediately so the entry never passes through a NULL-embedding
        // state. The embedding text uses the clamped type (same transform mergeEntry
        // applies internally) so it matches what ensureEmbeddings would produce.
        // Passing the embedding to mergeEntry writes content + embedding in a single
        // atomic UPDATE — no gap where the DB has new content but no vector.
        const safeType = clampKnowledgeType(decision.type);
        const freshEmbedding = await this.embeddings.embed(
          formatEmbeddingText(safeType, decision.content, decision.topics ?? [])
        );
        this.db.mergeEntry(nearestEntry.id, mergeUpdates, freshEmbedding);
        console.log(`[consolidation] ${decision.action === "update" ? "Updated" : "Replaced"}: "${nearestEntry.content.slice(0, 60)}..." → "${decision.content.slice(0, 60)}..."`);
        callbacks.onUpdate(nearestEntry.id, mergeUpdates, freshEmbedding);
        break;
      }

      case "insert": {
        const inserted = this.insertNewEntry(entry, sessionIds, entryEmbedding);
        console.log(`[consolidation] Insert (distinct despite similarity): "${entry.content.slice(0, 60)}..."`);
        callbacks.onInsert(inserted);
        break;
      }
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
  ): KnowledgeEntry & { embedding?: number[] } {
    const now = Date.now();
    const newEntry: KnowledgeEntry & { embedding?: number[] } = {
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
      observationCount: 1,
      supersededBy: null,
      derivedFrom: sessionIds,
      embedding,
    };
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
  private async getRelevantKnowledge(
    chunkSummary: string,
    allEntries: Array<KnowledgeEntry & { embedding: number[] }>
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

  /**
   * Post-extraction contradiction scan.
   *
   * Only scans entries that were inserted or updated during this chunk (changedIds).
   * Pre-existing entries were already checked in a prior consolidation run.
   *
   * For each changed entry, finds topic-overlapping candidates in the mid-similarity
   * band (contradictionMinSimilarity–RECONSOLIDATION_THRESHOLD) and asks the LLM
   * to detect and resolve any genuine contradictions.
   *
   * Returns counts of detected and resolved contradictions.
   */
  private async runContradictionScan(
    entriesMap: Map<string, KnowledgeEntry & { embedding: number[] }>,
    changedIds: Set<string>
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

      // Find topic-overlapping active entries, excluding only entries changed this chunk.
      // Pre-existing entries NOT changed this chunk are valid contradiction candidates.
      // Changed entries are excluded because decideMerge already handled them (sim ≥ 0.82
      // paths) or they're the entry we're checking right now.
      const candidates = this.db.getEntriesWithOverlappingTopics(
        entry.topics,
        [...changedIds] // only exclude chunk-changed entries, not all of entriesMap
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

      console.log(
        `[contradiction] Checking ${midBandCandidates.length} candidates for "${entry.content.slice(0, 60)}..."`
      );

      const validCandidateIds = new Set(midBandCandidates.map((c) => c.id));

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
        }))
      );

      let entrySuperseded = false;
      for (const result of results) {
        // Guard: reject any candidateId the LLM returned that was not in the
        // input candidate list. A hallucinated ID would silently no-op (UPDATE
        // WHERE id = <non-existent>) or — worse — self-supersede the newEntry
        // if the LLM echoed entry.id back as the candidateId.
        if (!validCandidateIds.has(result.candidateId)) {
          console.warn(
            `[contradiction] LLM returned candidateId "${result.candidateId}" not in candidate list — skipping`
          );
          continue;
        }
        detected++;
        console.log(
          `[contradiction] ${result.resolution}: "${entry.content.slice(0, 50)}..." vs candidate ${result.candidateId.slice(0, 8)}... — ${result.reason}`
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
          mergedData
        );

        // supersede_old, supersede_new, and merge are all "resolved" — irresolvable needs human
        if (result.resolution !== "irresolvable") {
          resolved++;
        }

        if (result.resolution === "supersede_old" || result.resolution === "merge") {
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

    if (detected > 0) {
      console.log(
        `[contradiction] Scan complete: ${detected} contradictions found, ${resolved} resolved, ${detected - resolved} flagged for review.`
      );
    }

    return { detected, resolved };
  }

  /**
   * Apply decay to all active entries.
   * Returns the number of entries that were archived.
   */
  private applyDecay(): number {
    // Include conflicted entries — their strength must continue aging.
    // A conflicted entry whose strength falls to zero is effectively forgotten
    // regardless of the conflict, and should still be archived.
    // Single query avoids the TOCTOU window of two separate status queries.
    const entries = this.db.getActiveAndConflictedEntries();
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
