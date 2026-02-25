import { Database } from "bun:sqlite";
import { config } from "../config.js";
import type { Episode, EpisodeMessage, ProcessedRange } from "../types.js";

/**
 * Maximum tokens per episode segment (soft limit — see chunkByTokenBudget).
 * The LLM sees: system prompt + existing knowledge + episode batch.
 * Keeping each episode under 50K tokens means a chunk of 10 episodes
 * stays well within context limits even with a large existing knowledge base.
 */
const MAX_TOKENS_PER_EPISODE = 50_000;

/**
 * Approximate token count from character count (1 token ~ 4 chars).
 */
function approxTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

/**
 * Reads episodes (raw session data) from OpenCode's SQLite database.
 *
 * This is a READ-ONLY connection to the OpenCode DB.
 * We never write to it — we only extract episodes for consolidation.
 *
 * Episode segmentation strategy:
 * - For sessions WITH compactions: each compaction summary = 1 episode,
 *   plus messages after the last compaction = 1 final episode (if any).
 * - For sessions WITHOUT compactions: the whole session is 1 episode,
 *   chunked by message boundaries if it exceeds the token budget.
 *
 * Incremental within-session consolidation (Option D):
 * - Episodes are keyed by (sessionId, startMessageId, endMessageId).
 * - Message IDs are stable UUIDs from OpenCode — they never shift when new
 *   messages are appended, unlike a segment_index approach.
 * - On each consolidation run, already-processed (start, end) ranges are
 *   excluded, so only the new tail of a session is re-processed.
 */

export class EpisodeReader {
  private db: Database;

  constructor(dbPath?: string) {
    this.db = new Database(dbPath || config.opencodeDbPath, { readonly: true });
  }

  /**
   * Return candidate sessions that have messages newer than the cursor.
   *
   * Queries the message table directly — session_id is a FK on every message row,
   * so no join to session is needed. Groups by session_id and orders by the max
   * message timestamp ASC so batches advance the cursor in chronological order.
   *
   * This replaces the old session time_created cursor, which failed for:
   *   - Sessions that are consolidated once then continued (time_created never changes)
   *   - Sessions reopened arbitrarily far in the past (time_created behind cursor)
   *
   * The episode-level idempotency (startMessageId/endMessageId) handles skipping
   * already-processed parts — this query just identifies which sessions need a look.
   *
   * Note: the old implementation excluded sessions from the knowledge-server's own
   * directory to avoid a feedback loop. That filter is intentionally omitted here.
   * The knowledge-server does not create OpenCode sessions, so there is nothing to
   * exclude. If that changes, add AND s.directory NOT LIKE '<knowledge_db_dir>%'.
   */
  getCandidateSessions(
    afterMessageTimeCreated: number,
    limit: number = config.consolidation.maxSessionsPerRun
  ): Array<{ id: string; maxMessageTime: number }> {
    const rows = this.db
      .prepare(
        `SELECT m.session_id as id, MAX(m.time_created) as max_message_time
         FROM message m
         JOIN session s ON s.id = m.session_id
         WHERE m.time_created > ?
           AND s.parent_id IS NULL
         GROUP BY m.session_id
         ORDER BY max_message_time ASC
         LIMIT ?`
      )
      .all(afterMessageTimeCreated, limit) as Array<{ id: string; max_message_time: number }>;
    return rows.map((r) => ({ id: r.id, maxMessageTime: r.max_message_time }));
  }

  /**
   * Count sessions with messages newer than the cursor.
   * Cheap check used at startup to decide whether to start background consolidation.
   */
  countNewSessions(afterMessageTimeCreated: number): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(DISTINCT m.session_id) as n
         FROM message m
         JOIN session s ON s.id = m.session_id
         WHERE m.time_created > ?
           AND s.parent_id IS NULL`
      )
      .get(afterMessageTimeCreated) as { n: number };
    return row.n;
  }

  /**
   * Segment a list of candidate sessions into episodes, excluding already-processed ranges.
   *
   * Caller is responsible for fetching the candidate session IDs (via getCandidateSessions)
   * and the processed ranges (via KnowledgeDB.getProcessedEpisodeRanges). This avoids
   * a redundant DB query and lets the caller use the full session list for cursor advancement.
   *
   * @param candidateSessionIds - session IDs to segment (already fetched by caller)
   * @param processedRanges     - per-session map of already-processed message ID ranges
   */
  getNewEpisodes(
    candidateSessionIds: string[],
    processedRanges: Map<string, ProcessedRange[]>
  ): Episode[] {
    if (candidateSessionIds.length === 0) return [];

    // Use json_each() to avoid SQLite's SQLITE_MAX_VARIABLE_NUMBER (999) limit.
    const sessions = this.db
      .prepare(
        `SELECT s.id, s.title, s.directory, s.time_created,
                COALESCE(p.name, 'unknown') as project_name
         FROM session s
         LEFT JOIN project p ON s.project_id = p.id
         WHERE s.id IN (SELECT value FROM json_each(?))
         ORDER BY s.time_created ASC`
      )
      .all(JSON.stringify(candidateSessionIds)) as Array<{
      id: string;
      title: string;
      directory: string;
      time_created: number;
      project_name: string;
    }>;

    const episodes: Episode[] = [];

    for (const session of sessions) {
      const sessionProcessed = processedRanges.get(session.id) ?? [];
      const sessionEpisodes = this.segmentSession(session, sessionProcessed);
      episodes.push(...sessionEpisodes);
    }

    return episodes;
  }

  /**
   * Segment a single session into episodes, skipping already-processed ranges.
   *
   * Strategy:
   * 1. Find all compaction points in the session
   * 2. If compactions exist:
   *    - Each compaction summary becomes one episode (already condensed)
   *    - Messages after the last compaction become the final episode
   * 3. If no compactions:
   *    - Extract all messages, chunk if they exceed token budget
   * 4. Filter out any episodes whose (startMessageId, endMessageId) range
   *    is already recorded in consolidated_episode.
   */
  private segmentSession(
    session: {
      id: string;
      title: string;
      directory: string;
      time_created: number;
      project_name: string;
    },
    processedRanges: ProcessedRange[]
  ): Episode[] {
    const compactionPoints = this.getCompactionPoints(session.id);

    let episodes: Episode[];
    if (compactionPoints.length > 0) {
      episodes = this.segmentWithCompactions(session, compactionPoints);
    } else {
      episodes = this.segmentWithoutCompactions(session);
    }

    // Filter out already-processed episodes by (startMessageId, endMessageId)
    if (processedRanges.length === 0) return episodes;

    const processedSet = new Set(
      processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`)
    );

    return episodes.filter(
      (ep) => !processedSet.has(`${ep.startMessageId}::${ep.endMessageId}`)
    );
  }

  /**
   * Get compaction points in a session: the timestamp of the compaction marker
   * and the continuation summary that follows it.
   */
  private getCompactionPoints(
    sessionId: string
  ): Array<{ compactionTime: number; summaryText: string; summaryMessageId: string; summaryMessageTime: number }> {
    // Find all compaction part timestamps
    const compactionTimes = this.db
      .prepare(
        `SELECT m.time_created
         FROM part p
         JOIN message m ON m.id = p.message_id
         WHERE json_extract(p.data, '$.type') = 'compaction'
           AND m.session_id = ?
         ORDER BY m.time_created ASC`
      )
      .all(sessionId) as Array<{ time_created: number }>;

    const points: Array<{ compactionTime: number; summaryText: string; summaryMessageId: string; summaryMessageTime: number }> = [];

    for (const ct of compactionTimes) {
      // The continuation summary is the first assistant text part AFTER the compaction.
      // We fetch time_created alongside id — the summary message's timestamp is what
      // the cursor must advance to, not the compaction marker's time (which is earlier).
      const summary = this.db
        .prepare(
          `SELECT m.id as message_id, m.time_created as message_time, json_extract(p.data, '$.text') as text
           FROM message m
           JOIN part p ON p.message_id = m.id
           WHERE m.session_id = ?
             AND m.time_created > ?
             AND json_extract(m.data, '$.role') = 'assistant'
             AND json_extract(p.data, '$.type') = 'text'
           ORDER BY m.time_created ASC, p.time_created ASC
           LIMIT 1`
        )
        .get(sessionId, ct.time_created) as { message_id: string; message_time: number; text: string } | null;

      if (summary?.text) {
        points.push({
          compactionTime: ct.time_created,
          summaryText: summary.text,
          summaryMessageId: summary.message_id,
          summaryMessageTime: summary.message_time,
        });
      }
    }

    return points;
  }

  /**
   * Segment a session that has compactions.
   *
   * Each compaction summary is a pre-condensed episode with a stable
   * (startMessageId = endMessageId = summaryMessageId) key.
   * Messages after the last compaction become the final episode.
   */
  private segmentWithCompactions(
    session: {
      id: string;
      title: string;
      directory: string;
      time_created: number;
      project_name: string;
    },
    compactionPoints: Array<{ compactionTime: number; summaryText: string; summaryMessageId: string; summaryMessageTime: number }>
  ): Episode[] {
    const episodes: Episode[] = [];

    // Each compaction summary is one episode keyed by its own message ID.
    // maxMessageTime uses the summary message's time_created (not the compaction marker's)
    // so the cursor advances to the actual message that was processed.
    for (const point of compactionPoints) {
      const tokens = approxTokens(point.summaryText);
      episodes.push({
        sessionId: session.id,
        startMessageId: point.summaryMessageId,
        endMessageId: point.summaryMessageId,
        sessionTitle: session.title || "Untitled",
        projectName: session.project_name,
        directory: session.directory,
        timeCreated: session.time_created,
        maxMessageTime: point.summaryMessageTime,
        content: point.summaryText,
        contentType: "compaction_summary",
        approxTokens: tokens,
      });
    }

    // Messages after the last compaction become the final episode(s)
    const lastCompactionTime =
      compactionPoints[compactionPoints.length - 1].compactionTime;

    const tailMessages = this.getMessagesAfterCompaction(
      session.id,
      lastCompactionTime
    );

    if (tailMessages.length >= config.consolidation.minSessionMessages) {
      const chunks = this.chunkByTokenBudget(tailMessages, MAX_TOKENS_PER_EPISODE);
      for (const chunk of chunks) {
        const content = this.formatMessages(chunk);
        if (content.trim()) {
          episodes.push({
            sessionId: session.id,
            startMessageId: chunk[0].messageId,
            endMessageId: chunk[chunk.length - 1].messageId,
            sessionTitle: session.title || "Untitled",
            projectName: session.project_name,
            directory: session.directory,
            timeCreated: session.time_created,
            maxMessageTime: chunk[chunk.length - 1].timestamp,
            content,
            contentType: "messages",
            approxTokens: approxTokens(content),
          });
        }
      }
    }

    return episodes;
  }

  /**
   * Get messages after the last compaction, skipping the continuation summary itself.
   * The continuation summary is the first assistant message after the compaction —
   * we already captured it as an episode, so we start from the message after that.
   */
  private getMessagesAfterCompaction(
    sessionId: string,
    compactionTime: number
  ): EpisodeMessage[] {
    // Find the continuation summary message (first assistant message after compaction).
    // We fetch its ID so we can exclude it by ID rather than by timestamp — excluding
    // by timestamp (m.time_created > summaryMsg.time_created) would silently drop any
    // subsequent message that shares the exact same millisecond timestamp.
    const summaryMsg = this.db
      .prepare(
        `SELECT m.id, m.time_created
         FROM message m
         WHERE m.session_id = ?
           AND m.time_created > ?
           AND json_extract(m.data, '$.role') = 'assistant'
         ORDER BY m.time_created ASC
         LIMIT 1`
      )
      .get(sessionId, compactionTime) as { id: string; time_created: number } | null;

    // Fetch all messages after the compaction point, then exclude the summary itself by ID.
    const afterTime = summaryMsg ? summaryMsg.time_created - 1 : compactionTime;
    const allAfter = this.getMessagesInRange(sessionId, afterTime, Number.MAX_SAFE_INTEGER);
    return summaryMsg
      ? allAfter.filter((m) => m.messageId !== summaryMsg.id)
      : allAfter;
  }

  /**
   * Segment a session without compactions.
   * The whole session is one or more episodes, chunked by token budget.
   */
  private segmentWithoutCompactions(session: {
    id: string;
    title: string;
    directory: string;
    time_created: number;
    project_name: string;
  }): Episode[] {
    const messages = this.getSessionMessages(session.id);

    // Skip sessions with too few messages
    if (messages.length < config.consolidation.minSessionMessages) {
      return [];
    }

    const chunks = this.chunkByTokenBudget(messages, MAX_TOKENS_PER_EPISODE);
    const episodes: Episode[] = [];

    for (const chunk of chunks) {
      const content = this.formatMessages(chunk);
      if (content.trim()) {
        episodes.push({
          sessionId: session.id,
          startMessageId: chunk[0].messageId,
          endMessageId: chunk[chunk.length - 1].messageId,
          sessionTitle: session.title || "Untitled",
          projectName: session.project_name,
          directory: session.directory,
          timeCreated: session.time_created,
          maxMessageTime: chunk[chunk.length - 1].timestamp,
          content,
          contentType: "messages",
          approxTokens: approxTokens(content),
        });
      }
    }

    return episodes;
  }

  /**
   * Chunk messages into groups that fit within a token budget.
   *
   * Note: `maxTokens` is a soft limit. A single message that individually exceeds
   * the budget is never split — it is placed alone in its own chunk. This means
   * an individual chunk may exceed `maxTokens` when a single message is very large.
   */
  private chunkByTokenBudget(
    messages: EpisodeMessage[],
    maxTokens: number
  ): EpisodeMessage[][] {
    const chunks: EpisodeMessage[][] = [];
    let currentChunk: EpisodeMessage[] = [];
    let currentTokens = 0;

    for (const msg of messages) {
      const msgTokens = approxTokens(msg.content);

      if (currentTokens + msgTokens > maxTokens && currentChunk.length > 0) {
        chunks.push(currentChunk);
        currentChunk = [];
        currentTokens = 0;
      }

      currentChunk.push(msg);
      currentTokens += msgTokens;
    }

    if (currentChunk.length > 0) {
      chunks.push(currentChunk);
    }

    return chunks;
  }

  /**
   * Extract text content from a session's messages.
   * Filters to user and assistant text parts only.
   */
  private getSessionMessages(sessionId: string): EpisodeMessage[] {
    return this.getMessagesInRange(sessionId, 0, Number.MAX_SAFE_INTEGER);
  }

  /**
   * Get messages in a time range within a session.
   * Returns messages with their stable message IDs for episode keying.
   */
  private getMessagesInRange(
    sessionId: string,
    afterTime: number,
    beforeTime: number
  ): EpisodeMessage[] {
    const messages = this.db
      .prepare(
        `SELECT m.id, json_extract(m.data, '$.role') as role, m.time_created
         FROM message m
         WHERE m.session_id = ?
           AND m.time_created > ?
           AND m.time_created < ?
         ORDER BY m.time_created ASC`
      )
      .all(sessionId, afterTime, beforeTime) as Array<{
      id: string;
      role: string;
      time_created: number;
    }>;

    const result: EpisodeMessage[] = [];

    for (const msg of messages) {
      if (msg.role !== "user" && msg.role !== "assistant") continue;

      // Get text parts for this message
      const parts = this.db
        .prepare(
          `SELECT json_extract(data, '$.text') as text
           FROM part
           WHERE message_id = ?
             AND json_extract(data, '$.type') = 'text'
           ORDER BY time_created ASC`
        )
        .all(msg.id) as Array<{ text: string }>;

      const content = parts
        .map((p) => p.text)
        .filter(Boolean)
        .join("\n");

      if (content.trim()) {
        result.push({
          messageId: msg.id,
          role: msg.role as "user" | "assistant",
          content: content.trim(),
          timestamp: msg.time_created,
        });
      }
    }

    return result;
  }

  /**
   * Format messages into a text block for the LLM.
   * Truncates very long individual messages to stay manageable.
   */
  private formatMessages(messages: EpisodeMessage[]): string {
    return messages
      .map((m) => {
        const content =
          m.content.length > 2000
            ? `${m.content.slice(0, 2000)}\n[...truncated]`
            : m.content;
        return `  ${m.role}: ${content}`;
      })
      .join("\n");
  }

  close(): void {
    this.db.close();
  }
}
