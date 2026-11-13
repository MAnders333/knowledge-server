/**
 * Knowledge entry types — what kind of knowledge this represents.
 *
 * Modeled after how the human brain categorizes learned information:
 * - fact: A specific, verifiable piece of information ("churn is 4.2%")
 * - principle: A general rule derived from multiple observations ("joins on table A×B time out above 10M rows")
 * - pattern: A recurring behavior or tendency ("stakeholders prefer visual dashboards over data tables")
 * - decision: A choice made with rationale ("we chose BigQuery over Snowflake because...")
 * - procedure: A learned workflow or process ("to deploy to prod, first run X then Y")
 */
/** Single source of truth for valid entry types — mirrors the SQLite CHECK constraint in schema.ts. */
const KNOWLEDGE_TYPES = [
	"fact",
	"principle",
	"pattern",
	"decision",
	"procedure",
] as const;

export type KnowledgeType = (typeof KNOWLEDGE_TYPES)[number];

/**
 * Clamp a raw LLM-returned type string to the nearest valid KnowledgeType.
 * LLMs sometimes return compound values like "fact/principle" or "Fact".
 * Falls back to "fact" if no valid type is found.
 */
export function clampKnowledgeType(type: string): KnowledgeType {
	const lower = type.toLowerCase();
	return (KNOWLEDGE_TYPES.find((t) => lower.includes(t)) ??
		"fact") as KnowledgeType;
}

/**
 * Lifecycle status of a knowledge entry.
 *
 * Models the human forgetting curve:
 * - active: Readily available for activation (like knowledge you use regularly)
 * - archived: Faded but recoverable (like knowledge you haven't used in months)
 * - superseded: Replaced by newer knowledge (like outdated facts)
 * - conflicted: Two entries contradict each other (needs human resolution)
 * - tombstoned: Effectively forgotten (kept only for audit trail)
 */
export type KnowledgeStatus =
	| "active"
	| "archived"
	| "superseded"
	| "conflicted"
	| "tombstoned";

/**
 * A single knowledge entry in the graph.
 */
export interface KnowledgeEntry {
	id: string;
	type: KnowledgeType;
	content: string;
	topics: string[];
	confidence: number; // 0-1
	source: string; // human-readable provenance

	// Lifecycle
	status: KnowledgeStatus;
	strength: number; // computed decay score
	createdAt: number; // unix timestamp ms
	updatedAt: number;
	lastAccessedAt: number;
	accessCount: number; // retrieval-only: incremented on activate, never during consolidation
	injectionCount: number; // incremented per response that showed the entry (recordInjections); the live usage signal now that access_count is frozen
	observationCount: number; // evidence-only: incremented on keep/update/insert, never on activate

	// Provenance
	supersededBy: string | null;
	derivedFrom: string[]; // session IDs or entry IDs this was distilled from
	isSynthesized: boolean; // true when produced by the synthesis pass (source starts with "synthesis:")

	// Embedding (stored as binary blob in DB, represented as float array in memory)
	embedding?: number[];
}

/**
 * A relationship between two knowledge entries.
 */
export interface KnowledgeRelation {
	id: string;
	sourceId: string;
	targetId: string;
	type: "supports" | "contradicts" | "supersedes";
	createdAt: number;
}

/**
 * Consolidation state — global counters and last-run timestamp.
 */
export interface ConsolidationState {
	lastConsolidatedAt: number; // unix timestamp ms
	totalSessionsProcessed: number;
	totalEntriesCreated: number;
	totalEntriesUpdated: number;
}

/**
 * An episode is a segment of a session, bounded by compaction points or token limits.
 *
 * For sessions WITH compactions:
 *   - Each compaction summary becomes one episode (rich, pre-condensed)
 *   - Messages AFTER the last compaction become a final episode (raw messages)
 *
 * For sessions WITHOUT compactions:
 *   - The whole session is one episode if it fits the token budget
 *   - Otherwise chunked by message boundaries
 *
 * Episodes are keyed by (sessionId, startMessageId, endMessageId) — stable message IDs
 * from the OpenCode DB that don't shift when new messages are appended to the session.
 * This allows incremental within-session consolidation: a second consolidation run
 * on the same session only processes messages after the last recorded endMessageId.
 */
export interface Episode {
	/** Original source name, e.g. "opencode", "claude-code". Used for idempotency keying. */
	source: string;
	sessionId: string;
	startMessageId: string; // first message ID in this episode (stable OpenCode UUID)
	endMessageId: string; // last message ID in this episode (stable OpenCode UUID)
	sessionTitle: string;
	projectName: string;
	directory: string;
	timeCreated: number;
	maxMessageTime: number; // max time_created of messages in this episode — used for cursor advance
	content: string; // pre-formatted text (either compaction summary or formatted messages)
	contentType: "compaction_summary" | "messages" | "document"; // what kind of content this is
	approxTokens: number; // rough token estimate for budget enforcement
}

/**
 * A pending episode row written by the daemon and read by the server's
 * PendingEpisodesReader. Structurally identical to Episode plus daemon metadata.
 */
export interface PendingEpisode extends Episode {
	/** Row UUID assigned by the daemon. */
	id: string;
	/** User identifier from the daemon's machine (KNOWLEDGE_USER_ID / hostname). */
	userId: string;
	/** Source name, e.g. "opencode", "claude-code". */
	source: string;
	/** Unix ms timestamp when the daemon uploaded this row. */
	uploadedAt: number;
	/**
	 * Consolidation attempts that failed for this row. Populated by
	 * getPendingEpisodes(); absent when the row is being inserted by the daemon.
	 */
	failedAttempts?: number;
}

/**
 * Daemon cursor — local high-water mark per source, tracking what has been uploaded.
 * Always stored in local SQLite on the user's machine, never in shared Postgres.
 */
export interface DaemonCursor {
	source: string;
	lastMessageTimeCreated: number;
	lastUploadedAt: number;
}

/**
 * An already-processed episode range, keyed by stable message IDs.
 * Loaded from consolidated_episode to skip re-processing on subsequent runs.
 * source is included so episodes from different tools with the same session ID
 * don't suppress each other's idempotency check.
 */
export interface ProcessedRange {
	source: string;
	startMessageId: string;
	endMessageId: string;
}

/** Identifies one episode by its consolidation key (source, session, message range). */
export interface EpisodeKey {
	source: string;
	sessionId: string;
	startMessageId: string;
	endMessageId: string;
}

/**
 * Per-batch outcome handed to IEpisodeReader.afterConsolidated().
 *
 * - completed: episodes that are fully handled (recorded as consolidated, or
 *   already processed in an earlier run). Their pending rows are deleted.
 * - failed: episodes whose extraction or store write failed. Their pending rows
 *   are kept and the failed-attempt count is incremented.
 *
 * Episodes in neither list (store lock held by another process, store
 * unavailable) are deferred: nothing is written and nothing is counted.
 */
export interface EpisodeOutcome {
	completed: EpisodeKey[];
	failed: Array<{ episode: EpisodeKey; error: string }>;
}

/**
 * Common interface for episode readers from different sources (OpenCode, Claude Code, etc.).
 *
 * Each source implements this interface so ConsolidationEngine can process multiple
 * sources uniformly without knowing their internal storage formats.
 *
 * The source name (e.g. "opencode", "claude-code") is used as the key for:
 * - The source_cursor table (per-source high-water mark)
 * - The consolidated_episode.source column (idempotency tracking per source)
 */
export interface IEpisodeReader {
	/** Stable identifier for this source (e.g. "opencode", "claude-code"). */
	readonly source: string;

	/**
	 * Return candidate sessions that have messages newer than the cursor.
	 * Used to decide whether to start background consolidation and to drive
	 * the consolidation loop.
	 */
	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit?: number,
	): Array<{ id: string; maxMessageTime: number }>;

	/**
	 * Count sessions with messages newer than the cursor.
	 * Cheap check used at startup.
	 */
	countNewSessions(afterMessageTimeCreated: number): number;

	/**
	 * Segment candidate sessions into new episodes, excluding already-processed ranges.
	 */
	getNewEpisodes(
		candidateSessionIds: string[],
		processedRanges: Map<string, ProcessedRange[]>,
	): Episode[];

	/**
	 * Optional async preparation step called before getCandidateSessions.
	 * Used by readers that need async DB access to pre-load their candidate list
	 * (e.g. PendingEpisodesReader fetches from the pending_episodes table).
	 * Not called for readers that don't implement it (backwards-compatible).
	 */
	prepare?(
		afterMessageTimeCreated: number,
		isBlocked?: (episode: EpisodeKey & { directory: string }) => boolean,
	): Promise<void>;

	/**
	 * Optional post-consolidation hook called once per consolidation batch.
	 * Used by PendingEpisodesReader to delete rows whose episodes were fully
	 * handled and to count failed attempts on rows that were not.
	 * Rows that are neither completed nor failed (lock held, store unavailable)
	 * are left untouched for retry.
	 */
	afterConsolidated?(outcome: EpisodeOutcome): Promise<void>;

	/** Release any held resources (DB connections, file handles, etc.). */
	close(): void;
}

/**
 * Raw message extracted from OpenCode DB before formatting.
 */
export interface EpisodeMessage {
	messageId: string; // stable UUID from OpenCode DB — used to key episode ranges
	/** "tool" marks tool output: third-party text, never a user turn. */
	role: "user" | "assistant" | "tool";
	content: string;
	timestamp: number;
}

/**
 * When an activated entry has status 'conflicted', this annotation is attached
 * to surface the counterpart entry that it contradicts — but only when that
 * counterpart also activates in the same query (i.e. both sides are relevant).
 */
export interface ContradictionAnnotation {
	conflictingEntryId: string;
	conflictingContent: string;
	caveat: string; // human-readable warning for the consuming agent
}

/**
 * Result of an activation query — knowledge entries ranked by relevance.
 */
export interface ActivationResult {
	entries: Array<{
		entry: KnowledgeEntry;
		/** Pure cosine similarity between query and entry embedding. Reflects semantic match quality. */
		rawSimilarity: number;
		/** Decay-weighted ranking score: rawSimilarity × strength (dense-only) or rerankScore × strength (when the local reranker is active). Used for sorting. */
		similarity: number;
		/**
		 * Cross-encoder relevance score in [0, 1]. Present only when the local
		 * reranker scored this entry; absent on rerank failure fallback and for
		 * relation-pulled support sources (which inherit their principle's rank).
		 */
		rerankScore?: number;
		staleness: {
			ageDays: number;
			strength: number;
			lastAccessedDaysAgo: number;
			mayBeStale: boolean;
		};
		/**
		 * Present only when this entry is conflicted AND its contradicting
		 * counterpart also activated in the same query. The agent should
		 * treat this knowledge with caution and not act on it unilaterally.
		 */
		contradiction?: ContradictionAnnotation;
	}>;
	query: string;
	totalActive: number;
	/**
	 * Facts about how this result was produced, for the activation_event table.
	 * Never sent to clients (the HTTP handler strips it) and carries no content.
	 */
	telemetry?: ActivationTelemetry;
	/**
	 * Present unless the activation is a dryRun (then there are no writes, and the
	 * plan is absent unless a session key was given, see `session`). What to record
	 * as injected, and what to remember for session dedupe, once the response is
	 * committed (see ActivationEngine.commitInjections). Never sent to clients.
	 */
	injection?: InjectionPlan;
}

/** How the candidate ordering was produced: cross-encoder, dense-only, or no reranker configured. */
export type GateMode = "rerank" | "dense" | "off";

export interface ActivationTelemetry {
	cueCount: number;
	nCandidates: number;
	gateMode: GateMode;
	gateDropped: number;
	budgetDropped: number;
	deduped: number;
	injectedTokens: number;
	/** Scorer `provider/model` when the rerank stage scored the candidates; undefined or null otherwise. */
	rerankModel?: string | null;
}

/**
 * What a response showed, grouped by the writable store that owns each entry.
 * `writes` is empty for dryRun. `session` is the dedupe record of the entries
 * the response carries (only when the request had a session key).
 */
export interface InjectionPlan {
	writes: Array<{ storeId: string; entryIds: string[] }>;
	session?: {
		key: string;
		at: number;
		items: Array<{ id: string; rawSimilarity: number }>;
	};
}

/** One content-free row of the activation_event table (see src/activation/events.ts). */
export interface ActivationEventRow {
	/** Epoch milliseconds. */
	ts: number;
	client: string;
	/** Hash of the client session id; null when the client did not send one. */
	sessionHash: string | null;
	promptChars: number;
	cueCount: number;
	nCandidates: number;
	nReturned: number;
	entryIds: string[];
	rawSims: number[];
	/** Cross-encoder score per returned entry (null for support sources); null when no rerank ran. */
	rerankScores: Array<number | null> | null;
	/** Scorer that produced rerank_scores, as `provider/model` (the resolved model for remote); null when no rerank ran. */
	rerankModel: string | null;
	gateMode: GateMode;
	latencyMs: number;
	clientDeadlineExceeded: boolean;
	/** Candidates the precision gate excluded: floor, support-source gate, and flagged conflicted entries. */
	gateDropped: number;
	/** Candidates the token budget excluded. */
	budgetDropped: number;
	/** Candidates suppressed because this session already received them. */
	deduped: number;
	/** Estimated tokens of the injected entry lines (chars / 4). */
	injectedTokens: number;
}

/**
 * Result of a consolidation run.
 */
export interface ConsolidationResult {
	sessionsProcessed: number;
	segmentsProcessed: number;
	entriesCreated: number;
	entriesUpdated: number;
	conflictsDetected: number;
	conflictsResolved: number;
	duration: number; // ms
}

/** Stable identity of an episode, matching the consolidation key. */
export function episodeKeyId(key: EpisodeKey): string {
	return JSON.stringify([
		key.source,
		key.sessionId,
		key.startMessageId,
		key.endMessageId,
	]);
}
