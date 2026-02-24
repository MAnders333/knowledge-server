import { Database } from "bun:sqlite";
import { dirname } from "node:path";
import { config } from "../config.js";
import type { Episode, EpisodeMessage } from "../types.js";

/**
 * Maximum tokens per episode segment.
 * Gemini 3.1 Pro supports 1M tokens, but we want consolidation prompts
 * to be manageable. The LLM sees: system prompt + existing knowledge + episode batch.
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
 */
/**
 * Directory of the knowledge DB — used to exclude knowledge-server's own sessions
 * from consolidation. Using the actual config path is robust to the project being
 * cloned under any directory name (avoids fragile %knowledge-server% string match).
 */
const KNOWLEDGE_DB_DIR = dirname(config.dbPath);

export class EpisodeReader {
  private db: Database;

  constructor(dbPath?: string) {
    this.db = new Database(dbPath || config.opencodeDbPath, { readonly: true });
  }

  /**
   * Count sessions pending consolidation without loading their content.
   * Cheap check used at startup.
   */
  countNewSessions(afterTimeCreated: number): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) as n FROM session
         WHERE time_created > ?
           AND parent_id IS NULL
           AND directory NOT LIKE ?`
      )
      .get(afterTimeCreated, `${KNOWLEDGE_DB_DIR}%`) as { n: number };
    return row.n;
  }

  /**
   * Get episodes from sessions created after the given timestamp.
   * A single session may produce multiple episodes (segments).
   */
  getNewEpisodes(
    afterTimeCreated: number,
    limit: number = config.consolidation.maxSessionsPerRun
  ): Episode[] {
    const sessions = this.db
      .prepare(
        `SELECT s.id, s.title, s.directory, s.time_created,
                COALESCE(p.name, 'unknown') as project_name
         FROM session s
         LEFT JOIN project p ON s.project_id = p.id
         WHERE s.time_created > ?
            AND s.parent_id IS NULL
            AND s.directory NOT LIKE ?
         ORDER BY s.time_created ASC
         LIMIT ?`
      )
      .all(afterTimeCreated, `${KNOWLEDGE_DB_DIR}%`, limit) as Array<{
      id: string;
      title: string;
      directory: string;
      time_created: number;
      project_name: string;
    }>;

    const episodes: Episode[] = [];

    for (const session of sessions) {
      const sessionEpisodes = this.segmentSession(session);
      episodes.push(...sessionEpisodes);
    }

    return episodes;
  }

  /**
   * Segment a single session into episodes.
   *
   * Strategy:
   * 1. Find all compaction points in the session
   * 2. If compactions exist:
   *    - Each compaction summary becomes one episode (already condensed)
   *    - Messages after the last compaction become the final episode
   * 3. If no compactions:
   *    - Extract all messages, chunk if they exceed token budget
   */
  private segmentSession(session: {
    id: string;
    title: string;
    directory: string;
    time_created: number;
    project_name: string;
  }): Episode[] {
    const compactionPoints = this.getCompactionPoints(session.id);

    if (compactionPoints.length > 0) {
      return this.segmentWithCompactions(session, compactionPoints);
    }

    return this.segmentWithoutCompactions(session);
  }

  /**
   * Get compaction points in a session: the timestamp of the compaction marker
   * and the continuation summary that follows it.
   */
  private getCompactionPoints(
    sessionId: string
  ): Array<{ compactionTime: number; summaryText: string }> {
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

    const points: Array<{ compactionTime: number; summaryText: string }> = [];

    for (const ct of compactionTimes) {
      // The continuation summary is the first assistant text part AFTER the compaction
      const summary = this.db
        .prepare(
          `SELECT json_extract(p.data, '$.text') as text
           FROM message m
           JOIN part p ON p.message_id = m.id
           WHERE m.session_id = ?
             AND m.time_created > ?
             AND json_extract(m.data, '$.role') = 'assistant'
             AND json_extract(p.data, '$.type') = 'text'
           ORDER BY m.time_created ASC, p.time_created ASC
           LIMIT 1`
        )
        .get(sessionId, ct.time_created) as { text: string } | null;

      if (summary?.text) {
        points.push({
          compactionTime: ct.time_created,
          summaryText: summary.text,
        });
      }
    }

    return points;
  }

  /**
   * Segment a session that has compactions.
   *
   * Each compaction summary is a pre-condensed episode.
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
    compactionPoints: Array<{ compactionTime: number; summaryText: string }>
  ): Episode[] {
    const episodes: Episode[] = [];
    let segmentIndex = 0;

    // Each compaction summary is one episode
    for (const point of compactionPoints) {
      const tokens = approxTokens(point.summaryText);
      episodes.push({
        sessionId: session.id,
        segmentIndex: segmentIndex++,
        sessionTitle: session.title || "Untitled",
        projectName: session.project_name,
        directory: session.directory,
        timeCreated: session.time_created,
        content: point.summaryText,
        contentType: "compaction_summary",
        approxTokens: tokens,
      });
    }

    // Messages after the last compaction become the final episode
    const lastCompactionTime =
      compactionPoints[compactionPoints.length - 1].compactionTime;

    // We need to skip the continuation summary message itself —
    // get messages from the second assistant message after the compaction onward,
    // plus any user messages after the compaction
    const tailMessages = this.getMessagesAfterCompaction(
      session.id,
      lastCompactionTime
    );

    if (tailMessages.length >= config.consolidation.minSessionMessages) {
      const formatted = this.formatMessages(tailMessages);
      if (formatted.trim()) {
        // Chunk if needed
        const chunks = this.chunkByTokenBudget(
          tailMessages,
          MAX_TOKENS_PER_EPISODE
        );
        for (const chunk of chunks) {
          const content = this.formatMessages(chunk);
          episodes.push({
            sessionId: session.id,
            segmentIndex: segmentIndex++,
            sessionTitle: session.title || "Untitled",
            projectName: session.project_name,
            directory: session.directory,
            timeCreated: session.time_created,
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
    // Find the continuation summary message (first assistant message after compaction)
    const summaryMsg = this.db
      .prepare(
        `SELECT m.time_created
         FROM message m
         WHERE m.session_id = ?
           AND m.time_created > ?
           AND json_extract(m.data, '$.role') = 'assistant'
         ORDER BY m.time_created ASC
         LIMIT 1`
      )
      .get(sessionId, compactionTime) as { time_created: number } | null;

    // Start from after the summary message
    const startTime = summaryMsg
      ? summaryMsg.time_created
      : compactionTime;

    return this.getMessagesInRange(sessionId, startTime, Number.MAX_SAFE_INTEGER);
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

    for (let i = 0; i < chunks.length; i++) {
      const content = this.formatMessages(chunks[i]);
      if (content.trim()) {
        episodes.push({
          sessionId: session.id,
          segmentIndex: i,
          sessionTitle: session.title || "Untitled",
          projectName: session.project_name,
          directory: session.directory,
          timeCreated: session.time_created,
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

  /**
   * Get total session count and time range for status reporting.
   */
  getSessionStats(): {
    totalSessions: number;
    earliest: number;
    latest: number;
  } {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) as total, 
                MIN(time_created) as earliest, 
                MAX(time_created) as latest
         FROM session 
         WHERE parent_id IS NULL`
      )
      .get() as { total: number; earliest: number; latest: number };

    return {
      totalSessions: row.total,
      earliest: row.earliest,
      latest: row.latest,
    };
  }

  close(): void {
    this.db.close();
  }
}
