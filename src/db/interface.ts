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
