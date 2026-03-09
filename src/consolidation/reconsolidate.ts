import { randomUUID } from "node:crypto";
import type { EmbeddingClient } from "../activation/embeddings.js";
import {
  cosineSimilarity,
  formatEmbeddingText,
} from "../activation/embeddings.js";
import { config } from "../config.js";
import type { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";
import { clampKnowledgeType } from "../types.js";
import type { KnowledgeEntry } from "../types.js";
import { computeStrength } from "./decay.js";
import type { ConsolidationLLM, ExtractedKnowledge } from "./llm.js";

/**
 * Cosine similarity threshold for matching an in-memory cluster centroid to a
 * persisted cluster row. Above this threshold, the persisted cluster ID is reused;
 * below it, a new cluster ID is created.
 */
const CLUSTER_IDENTITY_THRESHOLD = 0.9;

/**
 * Cosine similarity threshold for assigning an entry to an existing cluster
 * during greedy clustering. Below this threshold a new cluster is started.
 */
const CLUSTER_ASSIGNMENT_THRESHOLD = 0.5;

/**
 * Minimum number of members a cluster must have to be eligible for synthesis.
 */
const CLUSTER_MIN_MEMBERS = 3;

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
    /**
     * Log prefix used in all logger calls from this invocation.
     * Defaults to "consolidation". Pass "synthesis" when called from runKBSynthesis
     * so synthesis-time reconsolidation doesn't appear as consolidation activity in logs.
     */
    logPrefix: "consolidation" | "synthesis" = "consolidation",
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
    if (
      !nearestEntry ||
      nearestSimilarity < config.consolidation.reconsolidationThreshold
    ) {
      const inserted = this.insertNewEntry(
        entry,
        sessionIds,
        entryEmbedding,
        sessionTimestamp,
      );
      callbacks.onInsert(inserted);
      logger.log(
        `[${logPrefix}] Insert (novel, sim=${nearestSimilarity.toFixed(3)}): ${JSON.stringify(entry.content)}`,
      );
      return;
    }

    // Above threshold → ask LLM for a focused merge decision
    logger.log(
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
        this.db.reinforceObservation(nearestEntry.id);
        logger.log(
          `[${logPrefix}] Keep existing (reinforced): ${JSON.stringify(nearestEntry.content)}`,
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
          `[${logPrefix}] ${decision.action === "update" ? "Updated" : "Replaced"}: ${JSON.stringify(nearestEntry.content)} → ${JSON.stringify(decision.content)}`,
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
          `[${logPrefix}] Insert (distinct despite similarity): ${JSON.stringify(entry.content)}`,
        );
        callbacks.onInsert(inserted);
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
   * 1. CLUSTER FORMATION (in-memory, O(n×k)):
   *    - Sort all active entries by observationCount desc (most-evidenced entries seed clusters first).
   *    - Greedy assign each entry to nearest existing cluster if centroid similarity
   *      ≥ CLUSTER_ASSIGNMENT_THRESHOLD (0.5), else start a new cluster.
   *    - Update centroid as running average after each assignment.
   *
   * 2. CLUSTER IDENTITY RECONCILIATION (against DB):
   *    - Match each in-memory cluster to persisted cluster by centroid similarity
   *      ≥ CLUSTER_IDENTITY_THRESHOLD (0.9) → reuse ID.
   *    - No match → new cluster ID.
   *    - Persisted clusters with no match → deleted (entries dispersed).
   *
   * 3. RIPENESS CHECK:
   *    - last_membership_changed_at > last_synthesized_at OR last_synthesized_at IS NULL.
   *    - Minimum CLUSTER_MIN_MEMBERS (3) members.
   *
   * 4. SYNTHESIS (per ripe cluster):
   *    - All members are peers (no anchor/neighbor distinction).
   *    - LLM returns array of zero or more principles.
   *    - Each result routed through reconsolidate() for deduplication.
   *    - Write last_synthesized_at on cluster after successful synthesis attempt.
   *
   * Returns the number of synthesis calls that produced at least one principle.
   */
  async runKBSynthesis(): Promise<number> {
    // Load all active entries with embeddings (ensureEmbeddings just ran).
    const allEntries = this.db.getActiveEntriesWithEmbeddings();
    if (allEntries.length < CLUSTER_MIN_MEMBERS) return 0;

    // ── Step 1: Greedy clustering ──────────────────────────────────────────────

    // Sort descending by observationCount so well-evidenced entries form cluster
    // seeds first.
    const sorted = [...allEntries].sort(
      (a, b) => b.observationCount - a.observationCount,
    );

    // In-memory clusters: { centroid, memberIds }
    const inMemoryClusters: Array<{
      centroid: number[];
      memberIds: string[];
    }> = [];

    for (const entry of sorted) {
      // Find nearest cluster
      let nearestIdx = -1;
      let nearestSim = -1;
      for (let i = 0; i < inMemoryClusters.length; i++) {
        const sim = cosineSimilarity(
          entry.embedding,
          inMemoryClusters[i].centroid,
        );
        if (sim > nearestSim) {
          nearestSim = sim;
          nearestIdx = i;
        }
      }

      if (nearestIdx >= 0 && nearestSim >= CLUSTER_ASSIGNMENT_THRESHOLD) {
        // Assign to existing cluster, update centroid as running average
        const cluster = inMemoryClusters[nearestIdx];
        const n = cluster.memberIds.length;
        cluster.centroid = cluster.centroid.map(
          (v, i) => (v * n + entry.embedding[i]) / (n + 1),
        );
        cluster.memberIds.push(entry.id);
      } else {
        // Start a new cluster seeded by this entry
        inMemoryClusters.push({
          centroid: [...entry.embedding],
          memberIds: [entry.id],
        });
      }
    }

    logger.log(
      `[synthesis] Formed ${inMemoryClusters.length} clusters from ${allEntries.length} entries.`,
    );

    // ── Step 2: Identity reconciliation ───────────────────────────────────────

    const persistedClusters = this.db.getClustersWithMembers();

    // Build a lookup: persisted cluster ID → matched in-memory cluster index
    const persistedToInMemory = new Map<string, number>();
    // Track which in-memory clusters have been matched to avoid double-matching
    const inMemoryMatched = new Set<number>();

    for (const persisted of persistedClusters) {
      let bestIdx = -1;
      let bestSim = -1;
      for (let i = 0; i < inMemoryClusters.length; i++) {
        if (inMemoryMatched.has(i)) continue;
        const sim = cosineSimilarity(
          persisted.centroid,
          inMemoryClusters[i].centroid,
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

    // Map in-memory cluster index → resolved ID + membership-changed flag
    const resolvedClusters: Array<{
      id: string;
      centroid: number[];
      memberIds: string[];
      isNew: boolean;
      membershipChanged: boolean;
      lastSynthesizedAt: number | null;
    }> = inMemoryClusters.map((cluster, idx) => {
      // Find the persisted cluster that matched this in-memory cluster (if any)
      let matchedId: string | null = null;
      let lastSynthesizedAt: number | null = null;
      let previousMemberIds: string[] = [];

      for (const [persistedId, inMemIdx] of persistedToInMemory) {
        if (inMemIdx === idx) {
          matchedId = persistedId;
          const persistedCluster = persistedClusters.find(
            (p) => p.id === persistedId,
          );
          if (persistedCluster) {
            lastSynthesizedAt = persistedCluster.lastSynthesizedAt;
            previousMemberIds = persistedCluster.memberIds;
          }
          break;
        }
      }

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
        lastSynthesizedAt: isNew ? null : lastSynthesizedAt,
      };
    });

    // Persist new cluster state (upsert clusters, delete stale ones)
    this.db.persistClusters(
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

    for (const cluster of resolvedClusters) {
      // Ripeness: must have changed membership since last synthesis (or never synthesized)
      // AND meet minimum member count.
      const isRipe =
        cluster.memberIds.length >= CLUSTER_MIN_MEMBERS &&
        (cluster.lastSynthesizedAt === null || cluster.membershipChanged);

      if (!isRipe) continue;

      // Collect peer entries for this cluster
      const peers = cluster.memberIds
        .map((id) => entriesMap.get(id))
        .filter(
          (e): e is KnowledgeEntry & { embedding: number[] } => e !== undefined,
        );

      if (peers.length < CLUSTER_MIN_MEMBERS) continue;

      logger.log(
        `[synthesis] Attempting synthesis for cluster ${cluster.id} (${peers.length} peers).`,
      );

      const synthStart = Date.now();
      let results: Awaited<ReturnType<typeof this.llm.synthesizePrinciple>>;
      try {
        results = await this.llm.synthesizePrinciple(
          peers.map((p) => ({
            id: p.id,
            content: p.content,
            type: p.type,
            topics: p.topics,
          })),
        );
      } catch (err) {
        logger.warn(
          `[synthesis] synthesizePrinciple failed for cluster ${cluster.id}: ${err instanceof Error ? err.message : String(err)}`,
        );
        // Mark as synthesized anyway to avoid hammering on a bad cluster next run.
        this.db.markClusterSynthesized(cluster.id);
        continue;
      }

      logger.log(
        `[synthesis] synthesizePrinciple responded in ${((Date.now() - synthStart) / 1000).toFixed(1)}s — ${results.length} result(s).`,
      );

      // Mark cluster as synthesized regardless of results (bar was applied, nothing met it)
      this.db.markClusterSynthesized(cluster.id);

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
        logger.log(
          `[synthesis] Synthesized ${result.type}: ${JSON.stringify(result.content)}`,
        );

        try {
          // Embed and reconsolidate for deduplication.
          // Both embed() and reconsolidate() are inside the try so a transient
          // API error on either path does not abort the cluster loop.
          const synthEmbedding = await this.embeddings.embed(
            formatEmbeddingText(result.type, result.content, result.topics),
          );

          // Validate sourceIds are from the cluster member set (hallucination guard)
          const memberIdSet = new Set(cluster.memberIds);
          const validatedSourceIds = result.sourceIds.filter((id) =>
            memberIdSet.has(id),
          );
          const droppedCount =
            result.sourceIds.length - validatedSourceIds.length;
          if (droppedCount > 0) {
            logger.warn(
              `[synthesis] Dropped ${droppedCount} sourceId(s) from LLM result for cluster ${cluster.id} (not cluster members).`,
            );
          }

          // Pick a representative peer for scope (highest observationCount)
          const repPeer = peers.reduce((best, p) =>
            p.observationCount > best.observationCount ? p : best,
          );

          await this.reconsolidate(
            {
              type: result.type,
              content: result.content,
              topics: result.topics,
              confidence: result.confidence,
              scope: repPeer.scope,
              source: `synthesis:cluster:${cluster.id}`,
            },
            [], // no session IDs — KB-derived provenance
            entriesMap,
            {
              onInsert: (inserted) => {
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
                  this.db.insertRelation({
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
              onUpdate: (id, _updated, freshEmbedding) => {
                const existing = entriesMap.get(id);
                if (existing) {
                  entriesMap.set(id, {
                    ...existing,
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
        `[consolidation] sessionTimestamp = 0 (epoch) — likely a corrupt or uninitialised session record. Entry will be stamped at 1970-01-01 and may archive immediately. Entry: ${JSON.stringify(entry.content)}`,
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
}
