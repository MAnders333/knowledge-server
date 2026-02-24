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
export type KnowledgeType =
  | "fact"
  | "principle"
  | "pattern"
  | "decision"
  | "procedure";

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
  lastSessionTimeCreated: number; // time_created of last processed session
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
 */
export interface Episode {
  sessionId: string;
  segmentIndex: number; // 0-based segment within the session
  sessionTitle: string;
  projectName: string;
  directory: string;
  timeCreated: number;
  content: string; // pre-formatted text (either compaction summary or formatted messages)
  contentType: "compaction_summary" | "messages"; // what kind of content this is
  approxTokens: number; // rough token estimate for budget enforcement
}

/**
 * Raw message extracted from OpenCode DB before formatting.
 */
export interface EpisodeMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
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
