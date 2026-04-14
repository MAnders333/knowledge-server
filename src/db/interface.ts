import type {
	ConsolidationState,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
	PendingEpisode,
	ProcessedRange,
} from "../types.js";

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
	 * Wipe staging data: pending_episodes, consolidated_episode, and reset
	 * consolidation_state counters.
	 *
	 * daemon_cursor lives in DaemonDB (src/db/daemon/index.ts), not here.
	 * Call DaemonDB.resetDaemonCursors() separately if re-upload is also needed.
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
	 * isSynthesized — and `embedding` **only when supplying a freshly computed
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
	 * Optional — falls back to returning undefined when absent, in which case
	 * callers should use an alternative (e.g. the length of the full-scan result).
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
		Array<{ entry: KnowledgeEntry & { embedding: number[] }; similarity: number }>
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
