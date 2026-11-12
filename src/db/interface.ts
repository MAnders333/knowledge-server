import type {
	ActivationEventRow,
	ConsolidationState,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
	PendingEpisode,
	ProcessedRange,
} from "../types.js";

/**
 * Number of failed consolidation attempts after which a pending episode is
 * dead-lettered: its row is kept for inspection but excluded from
 * getPendingEpisodes() and countPendingSessions(), so it cannot stall newer
 * sessions. Dead-lettered rows are removed only by reinitialize.
 */
export const PENDING_EPISODE_MAX_ATTEMPTS = 5;

/**
 * Server state database interface — staging and bookkeeping tables.
 *
 * Can be backed by local SQLite (state.db, default) or remote Postgres.
 * Postgres backing enables a fully remote/cloud consolidation server: the
 * daemon writes pending_episodes to the shared Postgres, and the server
 * (running anywhere) drains it from there.
 *
 * Holds:
 *   - pending_episodes  — daemon writes here; server consolidation drains it
 *   - consolidated_episode — idempotency log for consolidation
 *   - consolidation_state — global counters and last-run timestamp
 *
 * Does NOT hold daemon_cursor — that lives in DaemonDB (always local SQLite,
 * per-machine). Separating cursor from staging allows the daemon to write
 * episodes to a remote IServerStateDB while keeping its cursor local.
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
	 * Rows dead-lettered (failed_attempts >= PENDING_EPISODE_MAX_ATTEMPTS) are excluded.
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
	 * Dead-lettered rows are excluded.
	 */
	countPendingSessions(): Promise<number>;

	/**
	 * Record a failed consolidation attempt for the given pending rows: increments
	 * failed_attempts and stores the error message. Rows are kept for retry.
	 */
	recordPendingEpisodeFailures(ids: string[], error: string): Promise<void>;
	/** Number of pending episodes that reached PENDING_EPISODE_MAX_ATTEMPTS. */
	countDeadLetteredEpisodes(): Promise<number>;

	/**
	 * Requeue dead-lettered episodes by clearing their attempt counts.
	 * Returns the number of episodes requeued.
	 */
	retryDeadLetteredEpisodes(): Promise<number>;

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
	 * Return already-consolidated episode ranges for the given session IDs.
	 * Queries consolidated_episode only. Used by the consolidation engine to
	 * skip already-processed episodes.
	 */
	getProcessedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>>;

	/**
	 * Return episode ranges that have already been staged OR consolidated,
	 * for the given session IDs. Unions consolidated_episode with pending_episodes.
	 * Used by the daemon uploader to avoid re-uploading episodes already in flight.
	 */
	getUploadedEpisodeRanges(
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>>;

	// ── Consolidation State ───────────────────────────────────────────────────

	getConsolidationState(): Promise<ConsolidationState>;

	updateConsolidationState(state: Partial<ConsolidationState>): Promise<void>;

	/**
	 * Wipe staging data: consolidated_episode and the consolidation_state counters,
	 * plus pending_episodes.
	 *
	 * When `userId` is given, only that user's pending episodes are deleted; other
	 * users' unconsolidated episodes on a shared state DB are kept. Omit it for a
	 * full reset of every user's pending episodes.
	 *
	 * daemon_cursor lives in DaemonDB (src/db/daemon/index.ts), not here.
	 * Call DaemonDB.resetDaemonCursors() separately if re-upload is also needed.
	 */
	reinitialize(userId?: string): Promise<void>;

	// ── Activation events (measurement, content-free) ─────────────────────────

	/** Append content-free activation_event rows. Batched by the caller. */
	insertActivationEvents(rows: ActivationEventRow[]): Promise<void>;

	/** Delete activation_event rows with ts < cutoffMs. Returns the number deleted. */
	pruneActivationEvents(cutoffMs: number): Promise<number>;

	/** Injected activations per client since sinceMs (ts >= sinceMs). */
	countActivationsByClient(sinceMs: number): Promise<Record<string, number>>;

	/** Captured episodes per source since sinceMs (consolidated_episode.processed_at >= sinceMs). */
	countEpisodesBySource(sinceMs: number): Promise<Record<string, number>>;

	close(): Promise<void>;
}

/**
 * Minimum gap between two last_accessed_at bumps from injection. Decay reads
 * last_accessed_at, so retrieval keeps an entry fresh at most once a day.
 */
export const LAST_ACCESS_TOUCH_INTERVAL_MS = 24 * 60 * 60 * 1000;

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
	/**
	 * Store identifier from config.jsonc (e.g. "work", "personal").
	 * Set by StoreRegistry after construction. Defaults to the store kind
	 * ("sqlite" / "postgres") when not set through the registry (e.g. in tests).
	 * Used for logging in fan-out error paths.
	 */
	id: string;

	// ── Entry CRUD ──

	insertEntry(
		entry: Omit<KnowledgeEntry, "embedding"> & { embedding?: number[] },
	): Promise<void>;

	/**
	 * Low-level field update for non-semantic fields: status, strength, confidence,
	 * isSynthesized — and `embedding` **only when supplying a freshly computed
	 * vector for the current content/topics** (e.g. in ensureEmbeddings / checkAndReEmbed).
	 *
	 * **Never call this with `content` or `topics` changes.**
	 * Use `KnowledgeService.updateEntry` instead — it automatically re-embeds when
	 * semantic fields change, keeping the stored vector in sync.
	 */
	updateEntry(id: string, updates: Partial<KnowledgeEntry>): Promise<void>;

	/** Plain entry without its vector; use getEntryWithEmbedding when the vector is needed. */
	getEntry(id: string): Promise<KnowledgeEntry | null>;

	/** Entry including its stored vector, or `embedding` undefined when none is stored. */
	getEntryWithEmbedding(id: string): Promise<KnowledgeEntry | null>;

	getActiveEntries(): Promise<KnowledgeEntry[]>;

	getActiveEntriesWithEmbeddings(): Promise<
		Array<KnowledgeEntry & { embedding: number[] }>
	>;

	getOneEntryWithEmbedding(): Promise<
		(KnowledgeEntry & { embedding: number[] }) | null
	>;

	/**
	 * id + embedding of every active or conflicted entry, for the in-process
	 * full-scan retrieval path. Loads no other columns, so scoring does not
	 * materialise whole entries.
	 */
	scanEmbeddings(): Promise<Array<{ id: string; embedding: Float32Array }>>;

	/**
	 * Full entries (no embedding) for the given ids that are active or conflicted.
	 * Hydrates the few scan hits that clear the similarity threshold.
	 */
	getEntriesByIds(ids: string[]): Promise<KnowledgeEntry[]>;

	getActiveAndConflictedEntries(): Promise<KnowledgeEntry[]>;

	getEntriesMissingEmbeddings(): Promise<KnowledgeEntry[]>;

	/**
	 * One keyset-paginated page of entries to re-embed on an embedding model
	 * change: every non-tombstoned entry that currently has an embedding, in any
	 * status. Ordered by id; pass the last id of the previous page (null for the
	 * first page). Carries only the fields the embedding text is built from.
	 */
	getEntriesForReEmbed(
		afterId: string | null,
		limit: number,
	): Promise<Array<Pick<KnowledgeEntry, "id" | "type" | "content" | "topics">>>;

	getEntriesByStatus(status: KnowledgeStatus): Promise<KnowledgeEntry[]>;

	getEntries(filters: {
		status?: string;
		type?: string;
	}): Promise<KnowledgeEntry[]>;

	/**
	 * Record one activation's injections as a batch: injection_count +1 for each id,
	 * and last_accessed_at moved to `now` at most once per entry per
	 * LAST_ACCESS_TOUCH_INTERVAL_MS. access_count is not touched (frozen, see
	 * plan-review §3.1(d)); updated_at is not touched (content did not change).
	 * Retrieval-only: never called during consolidation.
	 */
	recordInjections(ids: string[], now: number): Promise<void>;

	reinforceObservation(id: string): Promise<void>;

	getStats(): Promise<Record<string, number>>;

	// ── Contradiction detection ──

	getEntriesWithOverlappingTopics(
		topics: string[],
		excludeIds: string[],
	): Promise<Array<KnowledgeEntry & { embedding: number[] }>>;

	/**
	 * Find entries that share at least one topic with `topics` AND whose cosine
	 * similarity to `queryVector` falls in the band [minSimilarity, maxSimilarity).
	 * IDs in `excludeIds` are always excluded.
	 *
	 * Optional — implemented by PostgresKnowledgeDB when pgvector is available.
	 * When absent, callers fall back to getEntriesWithOverlappingTopics() + in-process
	 * similarity filtering (existing behaviour).
	 *
	 * The combined topic + similarity filter in a single DB query avoids loading
	 * all topic-overlapping candidates into memory just to discard most of them.
	 */
	findContradictionCandidates?(
		queryVector: number[],
		topics: string[],
		excludeIds: string[],
		minSimilarity: number,
		maxSimilarity: number,
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

	/**
	 * Merge new content into an existing entry. Resolves true when the row was
	 * merged, false when the target no longer exists (deleted after it was chosen).
	 * On false nothing is written; the caller keeps the knowledge as a new entry.
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
	): Promise<boolean>;

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

	/**
	 * Start an embedding model change (called before any re-embed write, after
	 * the new vector size is known). Discards vector-search structures sized for
	 * the old model so new-size vectors never meet an old-size column. Does not
	 * touch the recorded metadata: that is stamped by setEmbeddingMetadata() once
	 * every entry holds a new-model vector.
	 */
	beginEmbeddingModelChange(dimensions: number): Promise<void>;

	/**
	 * NULL both embedding columns on tombstoned entries. Tombstoned entries are
	 * not re-embedded on a model change; clearing them means a later revival
	 * regenerates the vector (ensureEmbeddings) instead of reviving a stale one.
	 * Returns the number of rows changed.
	 */
	clearTombstonedEmbeddings(): Promise<number>;

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
			/** entry id → ms when the entry first joined this cluster (kept across passes). */
			memberJoinedAt: Map<string, number>;
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

	// ── Vector Search (optional — Postgres + pgvector only) ─────────────────
	//
	// When present, callers use DB-side approximate nearest-neighbour (ANN) search
	// via an HNSW index instead of loading all entries into memory and computing
	// cosine similarity in JS.  SQLite stores do not implement this method; all
	// callers must fall back to getActiveEntriesWithEmbeddings() + in-process
	// cosineSimilarity when findSimilarEntries is absent.
	//
	// Return type includes the pre-computed similarity so callers avoid
	// re-computing it (the DB already did the work).  similarity is a cosine
	// similarity value in [0, 1] (NOT a distance; 1 = identical).
	//
	// statuses defaults to ['active', 'conflicted'] when omitted, mirroring
	// the behaviour of getActiveEntriesWithEmbeddings.  Pass an explicit list
	// when a caller needs a different filter (e.g. contradiction scan).

	/**
	 * Returns true when DB-side ANN search is fully operational — pgvector
	 * extension installed, embedding_vec column present, HNSW index built, and
	 * embedding dimensions known.
	 *
	 * Callers MUST check this before relying on findSimilarEntries returning
	 * meaningful results.  findSimilarEntries returns [] when this is false.
	 *
	 * Optional — only implemented by PostgresKnowledgeDB.  Absent on SQLite stores.
	 */
	isVectorSearchReady?(): boolean;

	/**
	 * Return the count of active+conflicted entries in this store.
	 *
	 * Used by ActivationEngine on Path A (ANN search) to report an accurate
	 * `totalActive` without loading all entries into memory.  On Path B the count
	 * is derived from the full-scan result set without an extra query.
	 *
	 * Optional - when absent, ActivationEngine counts the store as 0 toward
	 * `totalActive`.
	 */
	getActiveEntryCount?(): Promise<number>;

	/**
	 * Find the `limit` most similar entries to `queryVector` above `threshold`
	 * using DB-side ANN search.  Optional — only implemented by
	 * PostgresKnowledgeDB when the pgvector extension is available.
	 *
	 * Always check isVectorSearchReady() first — returns [] when not ready.
	 */
	findSimilarEntries?(
		queryVector: number[],
		limit: number,
		threshold: number,
		statuses?: KnowledgeStatus[],
	): Promise<
		Array<{
			entry: KnowledgeEntry;
			similarity: number;
		}>
	>;

	// ── Consolidation Lock ──────────────────────────────────────────────────
	// Per-store advisory lock that prevents concurrent consolidation runs across
	// different processes targeting the same physical database.
	//
	// The lock key is derived from the database's own OID (Postgres) or is a
	// no-op (SQLite — single-process by design). This makes the lock config-
	// name independent: two users who assign different local names to the same
	// Postgres database will still share the same lock automatically.
	//
	// Owned by consolidateExtractedToStore() in the ConsolidationEngine. Each
	// store acquires its own lock independently, allowing stores backed by
	// different physical databases to consolidate in parallel.

	/**
	 * Try to acquire an exclusive per-database consolidation lock.
	 * Returns true if acquired, false if another process holds it.
	 *
	 * Postgres: uses pg_try_advisory_lock(db_oid) on a reserved connection.
	 * SQLite: always returns true (single-process, no cross-process risk).
	 */
	tryAcquireConsolidationLock(): Promise<boolean>;

	/**
	 * Release the consolidation lock acquired by tryAcquireConsolidationLock().
	 * No-op if the lock is not held.
	 */
	releaseConsolidationLock(): Promise<void>;

	close(): Promise<void>;
}
