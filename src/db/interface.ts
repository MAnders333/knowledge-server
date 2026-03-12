import type {
	ConsolidationState,
	KnowledgeEntry,
	KnowledgeRelation,
	KnowledgeStatus,
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
	getContradictPairsForIds(
		entryIds: string[],
	): Promise<Map<string, string>>;

	// ── Episode Tracking ──

	recordEpisode(
		source: string,
		sessionId: string,
		startMessageId: string,
		endMessageId: string,
		contentType: "compaction_summary" | "messages" | "document",
		entriesCreated: number,
	): Promise<void>;

	getProcessedEpisodeRanges(
		source: string,
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>>;

	// ── Source Cursor ──

	getSourceCursor(source: string): Promise<SourceCursor>;

	updateSourceCursor(
		source: string,
		cursor: Partial<Omit<SourceCursor, "source">>,
	): Promise<void>;

	// ── Consolidation State ──

	getConsolidationState(): Promise<ConsolidationState>;

	updateConsolidationState(
		state: Partial<ConsolidationState>,
	): Promise<void>;

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

	setEmbeddingMetadata(
		model: string,
		dimensions: number,
	): Promise<void>;

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

	close(): Promise<void>;
}
