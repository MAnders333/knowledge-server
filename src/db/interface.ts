import type {
	ConsolidationState,
	DaemonCursor,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
	PendingEpisode,
	ProcessedRange,
	SourceCursor,
} from "../types.js";

/**
 * Database interface for the knowledge graph.
 *
 * Both SQLite (KnowledgeDB) and PostgreSQL (PostgresKnowledgeDB) implement
 * this interface, providing identical semantics with engine-specific SQL.
 *
 * All methods are async (return Promises) so the interface works with both
 * synchronous engines (SQLite — wrapped in Promise.resolve) and asynchronous
 * engines (PostgreSQL — native async/await over the network).
 *
 * All timestamps are unix milliseconds. Embeddings are float32 arrays.
 */
export interface IKnowledgeDB {
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
	 * Bypassing the service will silently leave the embedding stale, causing wrong
	 * similarity scores in activation and reconsolidation.
	 */
	updateEntry(id: string, updates: Partial<KnowledgeEntry>): Promise<void>;

	getEntry(id: string): Promise<KnowledgeEntry | null>;

	getActiveEntries(): Promise<KnowledgeEntry[]>;

	/**
	 * Get all active and conflicted entries that have embeddings (for similarity search).
	 */
	getActiveEntriesWithEmbeddings(): Promise<
		Array<KnowledgeEntry & { embedding: number[] }>
	>;

	/**
	 * Get a single active or conflicted entry that has an embedding.
	 * Used to probe embedding dimensions without loading all entries into memory.
	 */
	getOneEntryWithEmbedding(): Promise<
		(KnowledgeEntry & { embedding: number[] }) | null
	>;

	/**
	 * Get all active and conflicted entries in a single query.
	 */
	getActiveAndConflictedEntries(): Promise<KnowledgeEntry[]>;

	/**
	 * Get entries missing an embedding — used by ensureEmbeddings.
	 */
	getEntriesMissingEmbeddings(): Promise<KnowledgeEntry[]>;

	/**
	 * Get entries by status.
	 */
	getEntriesByStatus(status: KnowledgeStatus): Promise<KnowledgeEntry[]>;

	/**
	 * Get entries with optional server-side filtering.
	 */
	getEntries(filters: {
		status?: string;
		type?: string;
		scope?: string;
	}): Promise<KnowledgeEntry[]>;

	/**
	 * Record an access (bump access_count and last_accessed_at).
	 */
	recordAccess(id: string): Promise<void>;

	/**
	 * Reinforce an observation (bump observation_count and reset last_accessed_at).
	 */
	reinforceObservation(id: string): Promise<void>;

	/**
	 * Batch update strength scores (used during decay).
	 */
	updateStrength(id: string, strength: number): Promise<void>;

	/**
	 * Count entries by status.
	 */
	getStats(): Promise<Record<string, number>>;

	// ── Contradiction detection ──

	/**
	 * Find active and conflicted entries that share at least one topic with the
	 * given topics list, excluding a set of IDs.
	 */
	getEntriesWithOverlappingTopics(
		topics: string[],
		excludeIds: string[],
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>>;

	/**
	 * Record the outcome of a contradiction resolution between two entries.
	 */
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

	/**
	 * Hard-delete an entry and all its relations.
	 */
	deleteEntry(id: string): Promise<boolean>;

	// ── Relations ──

	insertRelation(relation: KnowledgeRelation): Promise<void>;

	getRelationsFor(entryId: string): Promise<KnowledgeRelation[]>;

	/**
	 * Batch fetch all entries that are sources for the given synthesized entry IDs.
	 */
	getSupportSourcesForIds(
		synthesizedIds: string[],
	): Promise<Map<string, KnowledgeEntry[]>>;

	/**
	 * Batch fetch all 'contradicts' relations that involve any of the given entry IDs.
	 */
	getContradictPairsForIds(entryIds: string[]): Promise<Map<string, string>>;

	// ── Episode Tracking ──

	/**
	 * Record a processed episode range for idempotency tracking.
	 * userId scopes the record per user so multi-user shared DBs don't
	 * suppress other users' episodes.
	 */
	recordEpisode(
		source: string,
		userId: string,
		sessionId: string,
		startMessageId: string,
		endMessageId: string,
		contentType: "compaction_summary" | "messages" | "document",
		entriesCreated: number,
	): Promise<void>;

	/**
	 * Return already-processed episode ranges for the given sessions.
	 * userId scopes the lookup so each user sees only their own records.
	 */
	getProcessedEpisodeRanges(
		source: string,
		userId: string,
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>>;

	// ── Source Cursor ──

	/**
	 * Get the per-source per-user high-water mark cursor.
	 * Returns a zeroed cursor if none exists yet.
	 */
	getSourceCursor(source: string, userId: string): Promise<SourceCursor>;

	/**
	 * Advance the cursor for a source+user pair.
	 */
	updateSourceCursor(
		source: string,
		userId: string,
		cursor: Partial<Omit<SourceCursor, "source" | "userId">>,
	): Promise<void>;

	// ── Consolidation State ──

	getConsolidationState(): Promise<ConsolidationState>;

	updateConsolidationState(state: Partial<ConsolidationState>): Promise<void>;

	// ── Entry Merge ──

	/**
	 * Merge new content into an existing entry (reconsolidation).
	 */
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
	 * Wipe all knowledge entries, relations, episode records, and reset all cursors.
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

	/**
	 * NULL out all embeddings on active and conflicted entries.
	 * @internal — exposed for tests and manual recovery only.
	 */
	clearAllEmbeddings(): Promise<number>;

	// ── Consolidation lock ────────────────────────────────────────────────────

	/**
	 * Try to acquire an exclusive consolidation lock.
	 *
	 * Returns true if the lock was acquired (caller may proceed with consolidation).
	 * Returns false if another process/instance already holds the lock (caller
	 * should skip this consolidation run rather than waiting).
	 *
	 * SQLite: in-process boolean flag — prevents re-entrant consolidation on the
	 * same DB instance. Multiple processes sharing the same SQLite file are rare
	 * and not the primary use case (SQLite is single-machine).
	 *
	 * Postgres: pg_try_advisory_lock(3179) — session-scoped, prevents concurrent
	 * consolidation across multiple processes sharing the same Postgres DB.
	 * The lock key 3179 is the default knowledge-server port — stable, memorable,
	 * and unique enough for this application.
	 */
	tryAcquireConsolidationLock(): Promise<boolean>;

	/**
	 * Release the consolidation lock acquired by tryAcquireConsolidationLock().
	 * No-op if the lock is not currently held by this instance.
	 */
	releaseConsolidationLock(): Promise<void>;

	// ── Pending Episodes (daemon ↔ server staging table) ──────────────────────

	/**
	 * Insert a pending episode uploaded by the daemon.
	 * Idempotent — silently ignores duplicate IDs (ON CONFLICT DO NOTHING).
	 */
	insertPendingEpisode(episode: PendingEpisode): Promise<void>;

	/**
	 * Fetch pending episodes for a given source and user, ordered by max_message_time ASC.
	 * Used by PendingEpisodesReader to get new episodes for consolidation.
	 */
	getPendingEpisodes(
		source: string,
		userId: string,
		afterMaxMessageTime: number,
		limit?: number,
	): Promise<PendingEpisode[]>;

	/**
	 * Delete pending episodes by their IDs after successful consolidation.
	 */
	deletePendingEpisodes(ids: string[]): Promise<void>;

	// ── Daemon Cursor (local SQLite only, not in shared Postgres) ─────────────

	/**
	 * Get the daemon's upload cursor for a source.
	 * Returns a zero-state cursor if none exists yet.
	 * Only meaningful on local SQLite — shared Postgres instances return zero.
	 */
	getDaemonCursor(source: string): Promise<DaemonCursor>;

	/**
	 * Advance the daemon's upload cursor for a source.
	 * Only meaningful on local SQLite — no-op on shared Postgres.
	 */
	updateDaemonCursor(
		source: string,
		cursor: Partial<Omit<DaemonCursor, "source">>,
	): Promise<void>;

	close(): Promise<void>;
}
