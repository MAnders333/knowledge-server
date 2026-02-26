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
export const KNOWLEDGE_TYPES = ["fact", "principle", "pattern", "decision", "procedure"] as const;
export type KnowledgeType = typeof KNOWLEDGE_TYPES[number];

/**
 * Clamp a raw LLM-returned type string to the nearest valid KnowledgeType.
 * LLMs sometimes return compound values like "fact/principle" or "Fact".
 * Falls back to "fact" if no valid type is found.
 */
export function clampKnowledgeType(type: string): KnowledgeType {
  const lower = type.toLowerCase();
  return (KNOWLEDGE_TYPES.find((t) => lower.includes(t)) ?? "fact") as KnowledgeType;
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
 * Whether this knowledge is relevant only to the individual or to the whole team.
 */
export type KnowledgeScope = "personal" | "team";

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
  scope: KnowledgeScope;

  // Lifecycle
  status: KnowledgeStatus;
  strength: number; // computed decay score
  createdAt: number; // unix timestamp ms
  updatedAt: number;
  lastAccessedAt: number;
  accessCount: number;

  // Provenance
  supersededBy: string | null;
  derivedFrom: string[]; // session IDs or entry IDs this was distilled from

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
  type: "supports" | "contradicts" | "refines" | "depends_on" | "supersedes";
  createdAt: number;
}

/**
 * Consolidation state — tracks the high-water mark of what's been processed.
 */
export interface ConsolidationState {
  lastConsolidatedAt: number; // unix timestamp ms
  lastMessageTimeCreated: number; // max time_created of messages processed in last run
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
  sessionId: string;
  startMessageId: string; // first message ID in this episode (stable OpenCode UUID)
  endMessageId: string;   // last message ID in this episode (stable OpenCode UUID)
  sessionTitle: string;
  projectName: string;
  directory: string;
  timeCreated: number;
  maxMessageTime: number; // max time_created of messages in this episode — used for cursor advance
  content: string; // pre-formatted text (either compaction summary or formatted messages)
  contentType: "compaction_summary" | "messages"; // what kind of content this is
  approxTokens: number; // rough token estimate for budget enforcement
}

/**
 * An already-processed episode range, keyed by stable message IDs.
 * Loaded from consolidated_episode to skip re-processing on subsequent runs.
 */
export interface ProcessedRange {
  startMessageId: string;
  endMessageId: string;
}

/**
 * Raw message extracted from OpenCode DB before formatting.
 */
export interface EpisodeMessage {
  messageId: string; // stable UUID from OpenCode DB — used to key episode ranges
  role: "user" | "assistant";
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
    similarity: number;
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
}

/**
 * Result of a consolidation run.
 */
export interface ConsolidationResult {
  sessionsProcessed: number;
  segmentsProcessed: number;
  entriesCreated: number;
  entriesUpdated: number;
  entriesArchived: number;
  conflictsDetected: number;
  conflictsResolved: number;
  duration: number; // ms
}
