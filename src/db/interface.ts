import type {
	ConsolidationState,
	DaemonCursor,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
	PendingEpisode,
	ProcessedRange,
} from "../types.js";

/**
 * Server-local database interface — staging and bookkeeping tables.
 *
 * Always backed by a local SQLite file (state.db) on the machine where
 * knowledge-server runs. Never lives in a remote Postgres instance.
 *
 * Holds:
 *   - pending_episodes  — daemon writes here; server consolidation drains it
 *   - consolidated_episode — idempotency log for consolidation
 *   - consolidation_state — global counters and last-run timestamp
 *   - daemon_cursor — daemon upload progress (also written by the daemon)
 *
 * The knowledge stores (IKnowledgeStore) are separate — they hold the actual
 * extracted knowledge and can live anywhere (local SQLite or remote Postgres).
 */
export interface IServerStateDB {
	// ── Pending Episodes (daemon → server staging) ────────────────────────────

	/**
	 * Insert a pending episode uploaded by the daemon.
	 * Idempotent — silently ignores duplicate IDs (ON CONFLICT DO NOTHING).
	 */
	insertPendingEpisode(episode: PendingEpisode): Promise<void>;

	/**
	 * Fetch pending episodes from all sources and users, ordered by max_message_time ASC.
	 * Drains everything in the staging table — source and user_id are provenance only.
	 */
	getPendingEpisodes(
		afterMaxMessageTime: number,
		limit?: number,
	): Promise<PendingEpisode[]>;

	/**
	 * Delete pending episodes by their IDs after successful consolidation.
	 */
	deletePendingEpisodes(ids: string[]): Promise<void>;

	/**
	 * Count distinct pending session IDs without loading episode content.
	 * Efficient O(1)-memory alternative to fetching all rows for counting.
	 */
	countPendingSessions(): Promise<number>;

	// ── Episode Tracking (idempotency) ────────────────────────────────────────

	/**
	 * Record a processed episode range for idempotency tracking.
	 * source is the original episode source (e.g. "opencode"), not the reader name.
	 */
	recordEpisode(
		source: string,
		sessionId: string,
		startMessageId: string,
		endMessageId: string,
		contentType: "compaction_summary" | "messages" | "document",
		entriesCreated: number,
	): Promise<void>;

	/**
	 * Return already-processed episode ranges for the given session IDs across all sources.
	 * Returns a Map keyed by sessionId. Each ProcessedRange includes the source so
	 * idempotency is maintained per (source, session, start, end).
	 */
	getProcessedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>>;

	// ── Consolidation State ───────────────────────────────────────────────────

	getConsolidationState(): Promise<ConsolidationState>;

	updateConsolidationState(state: Partial<ConsolidationState>): Promise<void>;

	// ── Consolidation Lock ────────────────────────────────────────────────────

	/**
	 * Try to acquire an exclusive consolidation lock.
	 * Returns true if acquired, false if another process holds it.
	 * SQLite: in-process boolean flag.
	 */
	tryAcquireConsolidationLock(): Promise<boolean>;

	/**
	 * Release the consolidation lock.
	 */
	releaseConsolidationLock(): Promise<void>;

	// ── Daemon Cursor ─────────────────────────────────────────────────────────

	/**
	 * Get the daemon's upload cursor for a source.
	 * Returns a zero-state cursor if none exists yet.
	 */
	getDaemonCursor(source: string): Promise<DaemonCursor>;

	/**
	 * Advance the daemon's upload cursor for a source.
	 */
	updateDaemonCursor(
		source: string,
		cursor: Partial<Omit<DaemonCursor, "source">>,
	): Promise<void>;

	/**
	 * Reset the daemon upload cursors for all sources back to zero.
	 *
	 * This causes the daemon to re-upload all historical episodes on its next
	 * tick. Use this when connecting a machine to a new or existing store for
	 * the first time, or when retroactively rerouting knowledge under a new
	 * domain configuration.
	 *
	 * Safe for shared stores: daemon_cursor is per-machine (local state.db).
	 * Other users' episodes are not affected.
	 */
	resetDaemonCursors(): Promise<void>;

	/**
	 * Wipe staging data: pending_episodes, consolidated_episode, and reset
	 * consolidation_state counters.
	 *
	 * daemon_cursor is intentionally NOT touched here — call resetDaemonCursors()
	 * explicitly if re-upload is also needed.
	 *
	 * Use for re-consolidation with updated domain routing context. On shared
	 * stores this is safe because session_ids are per-user by nature.
	 */
	reinitialize(): Promise<void>;

	close(): Promise<void>;
}

/**
 * Knowledge store interface — the extracted knowledge graph.
 *
 * Implemented by SQLiteKnowledgeStore and PostgresKnowledgeStore.
 * A knowledge server can have multiple stores (e.g. one SQLite for "work",
 * one Postgres for "personal"), each holding their own knowledge_entry rows.
 *
 * Does NOT hold staging or bookkeeping tables — those live in IServerStateDB.
 */
export interface IKnowledgeStore {
	// ── Entry CRUD ──

	insertEntry(
		entry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] },
	): Promise<void>;

	/**
	 * Low-level field update for non-semantic fields: status, strength, confidence,
	 * scope, isSynthesized — and `embedding` **only when supplying a freshly computed
	 * vector for the current content/topics** (e.g. in ensureEmbeddings / checkAndReEmbed).
	 *
	 * **Never call this with `content` or `topics` changes.**
	 * Use `KnowledgeService.updateEntry` instead — it automatically re-embeds when
	 * semantic fields change, keeping the stored vector in sync.
	 */
	updateEntry(id: string, updates: Partial<KnowledgeEntry>): Promise<void>;

	getEntry(id: string): Promise<KnowledgeEntry | null>;

	getActiveEntries(): Promise<KnowledgeEntry[]>;

	getActiveEntriesWithEmbeddings(): Promise<
		Array<KnowledgeEntry & { embedding: number[] }>
	>;

	getOneEntryWithEmbedding(): Promise<
		(KnowledgeEntry & { embedding: number[] }) | null
	>;

	getActiveAndConflictedEntries(): Promise<KnowledgeEntry[]>;

	getEntriesMissingEmbeddings(): Promise<KnowledgeEntry[]>;

	getEntriesByStatus(status: KnowledgeStatus): Promise<KnowledgeEntry[]>;

	getEntries(filters: {
		status?: string;
		type?: string;
		scope?: string;
	}): Promise<KnowledgeEntry[]>;

	recordAccess(id: string): Promise<void>;

	reinforceObservation(id: string): Promise<void>;

	updateStrength(id: string, strength: number): Promise<void>;

	getStats(): Promise<Record<string, number>>;

	// ── Contradiction detection ──

	getEntriesWithOverlappingTopics(
		topics: string[],
		excludeIds: string[],
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>>;

	applyContradictionResolution(
		resolution: "supersede_old" | "supersede_new" | "merge" | "irresolvable",
		newEntryId: string,
		existingEntryId: string,
		mergedData?: {
			content: string;
			type: string;
			topics: string[];
			confidence: number;
		},
	): Promise<void>;

	deleteEntry(id: string): Promise<boolean>;

	// ── Relations ──

	insertRelation(relation: KnowledgeRelation): Promise<void>;

	getRelationsFor(entryId: string): Promise<KnowledgeRelation[]>;

	getSupportSourcesForIds(
		synthesizedIds: string[],
	): Promise<Map<string, KnowledgeEntry[]>>;

	getContradictPairsForIds(entryIds: string[]): Promise<Map<string, string>>;

	// ── Entry Merge ──

	mergeEntry(
		id: string,
		updates: {
			content: string;
			type: string;
			topics: string[];
			confidence: number;
			additionalSources: string[];
		},
		embedding?: number[],
	): Promise<void>;

	/**
	 * Wipe all knowledge entries, relations, clusters, and embeddings.
	 * Does NOT touch staging/bookkeeping tables (those are in IServerStateDB).
	 */
	reinitialize(): Promise<void>;

	// ── Embedding Metadata ──

	getEmbeddingMetadata(): Promise<{
		model: string;
		dimensions: number;
		recordedAt: number;
	} | null>;

	setEmbeddingMetadata(model: string, dimensions: number): Promise<void>;

	// ── Cluster Management ──

	getClustersWithMembers(): Promise<
		Array<{
			id: string;
			centroid: number[];
			memberCount: number;
			lastSynthesizedAt: number | null;
			lastMembershipChangedAt: number;
			createdAt: number;
			memberIds: string[];
		}>
	>;

	persistClusters(
		clusters: Array<{
			id: string;
			centroid: number[];
			memberIds: string[];
			isNew: boolean;
			membershipChanged: boolean;
		}>,
	): Promise<void>;

	markClusterSynthesized(clusterId: string): Promise<void>;

	clearAllEmbeddings(): Promise<number>;

	close(): Promise<void>;
}
