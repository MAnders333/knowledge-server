import { Database } from "bun:sqlite";
import type { Statement } from "bun:sqlite";
import { config } from "../../config.js";
import type {
	Episode,
	EpisodeMessage,
	IEpisodeReader,
	ProcessedRange,
} from "../../types.js";
import {
	MAX_MESSAGE_CHARS,
	MAX_TOKENS_PER_EPISODE,
	approxTokens,
	chunkByTokenBudget,
	formatMessages,
} from "./shared.js";

/**
 * Extract readable text from a tool output value.
 *
 * Tool outputs are stored as JSON strings by OpenCode. The structure varies by
 * tool — Confluence pages return `{metadata, content: {value: "..."}}`, search
 * results return an array of `{content: {value: "..."}, ...}` objects. We try
 * to extract the human-readable `.content.value` field(s) when present; if the
 * structure is unrecognised we fall back to the raw string.
 *
 * NOTE: The JSON extraction logic is purpose-built for the Atlassian Confluence
 * MCP tool schemas. Tools with different output shapes that are added to the
 * CONSOLIDATION_INCLUDE_TOOL_OUTPUTS allowlist will fall back to the raw JSON
 * string, which may need updating here for best extraction quality.
 *
 * No per-output size cap is applied here — the assembled message cap
 * (MAX_MESSAGE_CHARS) and the chunker handle oversized content.
 */
function extractToolText(raw: string): string {
	let text = raw;

	try {
		const parsed: unknown = JSON.parse(raw);

		if (Array.isArray(parsed)) {
			// e.g. atlassian_confluence_search — array of result objects
			const parts: string[] = [];
			for (const item of parsed) {
				if (item && typeof item === "object") {
					const entry = item as Record<string, unknown>;
					const title = typeof entry.title === "string" ? entry.title : "";
					const content = entry.content as Record<string, unknown> | undefined;
					const value = typeof content?.value === "string" ? content.value : "";
					if (title || value) {
						parts.push(title ? `${title}\n${value}` : value);
					}
				}
			}
			if (parts.length > 0) text = parts.join("\n\n");
		} else if (parsed && typeof parsed === "object") {
			// e.g. atlassian_confluence_get_page — single page object
			const obj = parsed as Record<string, unknown>;
			const meta = obj.metadata as Record<string, unknown> | undefined;
			const title = typeof meta?.title === "string" ? meta.title : "";
			const content = obj.content as Record<string, unknown> | undefined;
			const value = typeof content?.value === "string" ? content.value : "";
			if (title || value) {
				text = title ? `${title}\n\n${value}` : value;
			}
		}
	} catch {
		// Not JSON — use raw string as-is
	}

	return text;
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

export class OpenCodeEpisodeReader implements IEpisodeReader {
	readonly source = "opencode";
	private _db: Database | null = null;
	private readonly dbPath: string;

	// Cached prepared statements — compiled once, reused across all getMessagesInRange calls.
	private _textPartsStmt: Statement | null = null;
	private _toolPartsStmt: Statement | null = null;

	constructor(dbPath?: string) {
		this.dbPath = dbPath ?? config.opencodeDbPath;
	}

	/**
	 * Lazily open the OpenCode DB on first use.
	 * Deferred so that ConsolidationEngine can be constructed (and tested)
	 * without requiring the OpenCode DB to exist on disk at construction time.
	 */
	private get db(): Database {
		if (!this._db) {
			this._db = new Database(this.dbPath, { readonly: true });
		}
		return this._db;
	}

	private get textPartsStmt(): Statement {
		if (this._textPartsStmt) return this._textPartsStmt;
		this._textPartsStmt = this.db.prepare(
			`SELECT json_extract(data, '$.text') as text
       FROM part
       WHERE message_id = ?
         AND json_extract(data, '$.type') = 'text'
         AND (json_extract(data, '$.synthetic') IS NULL
              OR json_extract(data, '$.synthetic') = 0)
       ORDER BY time_created ASC`,
		);
		return this._textPartsStmt;
	}

	private get toolPartsStmt(): Statement | null {
		// Config check before cache check: if tool output extraction is disabled,
		// always return null regardless of any previously compiled statement.
		if (config.consolidation.includeToolOutputs.length === 0) return null;
		if (this._toolPartsStmt) return this._toolPartsStmt;
		this._toolPartsStmt = this.db.prepare(
			`SELECT json_extract(data, '$.tool') as tool,
              json_extract(data, '$.state.output') as output
       FROM part
       WHERE message_id = ?
         AND json_extract(data, '$.type') = 'tool'
         AND json_extract(data, '$.state.status') = 'completed'
       ORDER BY time_created ASC`,
		);
		return this._toolPartsStmt;
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
		limit: number = config.consolidation.maxSessionsPerRun,
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
         LIMIT ?`,
			)
			.all(afterMessageTimeCreated, limit) as Array<{
			id: string;
			max_message_time: number;
		}>;
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
           AND s.parent_id IS NULL`,
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
		processedRanges: Map<string, ProcessedRange[]>,
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
         ORDER BY s.time_created ASC`,
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
		processedRanges: ProcessedRange[],
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
			processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`),
		);

		return episodes.filter(
			(ep) => !processedSet.has(`${ep.startMessageId}::${ep.endMessageId}`),
		);
	}

	/**
	 * Get compaction points in a session: the timestamp of the compaction marker
	 * and the continuation summary that follows it.
	 */
	private getCompactionPoints(sessionId: string): Array<{
		compactionTime: number;
		summaryText: string;
		summaryMessageId: string;
		summaryMessageTime: number;
	}> {
		// Find all compaction part timestamps
		const compactionTimes = this.db
			.prepare(
				`SELECT m.time_created
         FROM part p
         JOIN message m ON m.id = p.message_id
         WHERE json_extract(p.data, '$.type') = 'compaction'
           AND m.session_id = ?
         ORDER BY m.time_created ASC`,
			)
			.all(sessionId) as Array<{ time_created: number }>;

		const points: Array<{
			compactionTime: number;
			summaryText: string;
			summaryMessageId: string;
			summaryMessageTime: number;
		}> = [];

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
           LIMIT 1`,
				)
				.get(sessionId, ct.time_created) as {
				message_id: string;
				message_time: number;
				text: string;
			} | null;

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
		compactionPoints: Array<{
			compactionTime: number;
			summaryText: string;
			summaryMessageId: string;
			summaryMessageTime: number;
		}>,
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
			lastCompactionTime,
		);

		if (tailMessages.length >= config.consolidation.minSessionMessages) {
			const chunks = chunkByTokenBudget(tailMessages, MAX_TOKENS_PER_EPISODE);
			for (const chunk of chunks) {
				const content = formatMessages(chunk);
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
		compactionTime: number,
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
         LIMIT 1`,
			)
			.get(sessionId, compactionTime) as {
			id: string;
			time_created: number;
		} | null;

		// Fetch all messages after the compaction point, then exclude the summary itself by ID.
		const afterTime = summaryMsg ? summaryMsg.time_created - 1 : compactionTime;
		const allAfter = this.getMessagesInRange(
			sessionId,
			afterTime,
			Number.MAX_SAFE_INTEGER,
		);
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

		const chunks = chunkByTokenBudget(messages, MAX_TOKENS_PER_EPISODE);
		const episodes: Episode[] = [];

		for (const chunk of chunks) {
			const content = formatMessages(chunk);
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
		beforeTime: number,
	): EpisodeMessage[] {
		const messages = this.db
			.prepare(
				`SELECT m.id, json_extract(m.data, '$.role') as role, m.time_created
         FROM message m
         WHERE m.session_id = ?
           AND m.time_created > ?
           AND m.time_created < ?
         ORDER BY m.time_created ASC`,
			)
			.all(sessionId, afterTime, beforeTime) as Array<{
			id: string;
			role: string;
			time_created: number;
		}>;

		const result: EpisodeMessage[] = [];

		const includeToolOutputs = config.consolidation.includeToolOutputs;

		for (const msg of messages) {
			if (msg.role !== "user" && msg.role !== "assistant") continue;

			// Get text parts for this message (uses cached prepared statement)
			const textParts = this.textPartsStmt.all(msg.id) as Array<{
				text: string;
			}>;

			const textContent = textParts
				.map((p) => p.text)
				.filter(Boolean)
				.join("\n");

			// Get tool outputs for allowlisted tools (assistant messages only)
			let toolContent = "";
			const toolStmt = this.toolPartsStmt;
			if (msg.role === "assistant" && toolStmt) {
				const toolParts = toolStmt.all(msg.id) as Array<{
					tool: string;
					output: string;
				}>;

				const relevantTools = toolParts.filter(
					(p) => p.tool && p.output && includeToolOutputs.includes(p.tool),
				);

				if (relevantTools.length > 0) {
					toolContent = relevantTools
						.map((p) => `[tool: ${p.tool}]\n${extractToolText(p.output)}`)
						.join("\n\n");
				}
			}

			// Cap final assembled content. Applied unconditionally — protects both the
			// tool-output path (multiple stacked outputs) and the plain-text path (a
			// single very long user/assistant message). The chunker's soft token limit
			// only protects across messages, not within a single oversized one.
			let content = [textContent.trim(), toolContent.trim()]
				.filter(Boolean)
				.join("\n\n");
			if (content.length > MAX_MESSAGE_CHARS) {
				content = `${content.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`;
			}

			if (content) {
				result.push({
					messageId: msg.id,
					role: msg.role as "user" | "assistant",
					content,
					timestamp: msg.time_created,
				});
			}
		}

		return result;
	}

	close(): void {
		this._textPartsStmt?.finalize();
		this._textPartsStmt = null;
		this._toolPartsStmt?.finalize();
		this._toolPartsStmt = null;
		this._db?.close();
		this._db = null;
	}
}
