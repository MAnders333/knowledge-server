import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { config } from "../../config.js";
import type {
	Episode,
	EpisodeMessage,
	IEpisodeReader,
	ProcessedRange,
} from "../../types.js";

/**
 * Maximum tokens per episode segment (same soft limit as OpenCodeEpisodeReader).
 */
const MAX_TOKENS_PER_EPISODE = 50_000;

/**
 * Maximum characters to include from a single tool result output.
 * ~20K chars ≈ 5K tokens.
 */
const MAX_TOOL_OUTPUT_CHARS = 20_000;

/**
 * Maximum characters for a fully assembled message.
 * ~60K chars ≈ 15K tokens.
 */
const MAX_MESSAGE_CHARS = 60_000;

/**
 * Approximate token count from character count (1 token ~ 4 chars).
 */
function approxTokens(text: string): number {
	return Math.ceil(text.length / 4);
}

// ── Claude Code JSONL record types ────────────────────────────────────────────

/**
 * A single content part inside a Claude Code message.
 * The `type` field discriminates between text, thinking, tool_use, tool_result.
 */
interface ContentPart {
	type: string;
	text?: string; // type === "text" | "thinking"
	name?: string; // type === "tool_use" — tool name
	id?: string; // type === "tool_use" — stable ID matched by tool_result
	tool_use_id?: string; // type === "tool_result" — references tool_use.id
	content?: string | ContentPart[]; // type === "tool_result" — result content
	// Other fields (input, etc.) ignored — we only need text and tool results
}

/**
 * A single record in a Claude Code JSONL session file.
 * Claude Code stores sessions as newline-delimited JSON records.
 */
interface CCRecord {
	type: "user" | "assistant" | "system" | string;
	subtype?: string; // system records: "compact_boundary", "init", etc.
	uuid?: string;
	parentUuid?: string | null;
	timestamp?: string; // ISO string, e.g. "2026-03-04T10:00:00.000Z"
	sessionId?: string;
	cwd?: string;
	message?: {
		role: "user" | "assistant";
		content: string | ContentPart[];
	};
	// Compaction metadata on system records with subtype === "compact_boundary"
	compactMetadata?: {
		trigger: string;
		preTokens: number;
	};
	// slug is set on most records; used as session title fallback
	slug?: string;
}

/**
 * A parsed session file ready for segmentation.
 */
interface ParsedSession {
	sessionId: string; // UUID from filename
	slug: string; // human-readable slug (title)
	projectName: string; // last component of cwd
	directory: string; // cwd value
	filePath: string; // absolute path of the JSONL file
	mtimeMs: number; // file mtime — used as cheap pre-filter
	records: CCRecord[];
	minTimestampMs: number; // earliest message timestamp in this file
	maxTimestampMs: number; // latest message timestamp in this file
}

/**
 * Reads episodes from Claude Code's JSONL session files.
 *
 * Storage layout (confirmed by direct inspection):
 *   ~/.claude/projects/<path-encoded-cwd>/<session-uuid>.jsonl
 *
 * Each JSONL file contains a sequence of records. We process:
 * - `system` records with subtype === "compact_boundary": mark a compaction point
 * - The next `user` record after a compaction: the compaction summary (plain text string)
 * - `user` / `assistant` records: conversation messages
 *
 * Content extraction:
 * - Text parts from `message.content[]` items with type === "text"
 * - Tool results from `user` message content parts with type === "tool_result",
 *   filtered by the CONSOLIDATION_INCLUDE_TOOL_OUTPUTS allowlist (same as OpenCode)
 *
 * Cursor semantics:
 * - `lastMessageTimeCreated` is stored as unix ms (same column as OpenCode).
 * - Timestamps come from `record.timestamp` (ISO string → parsed to ms).
 *
 * Incremental processing:
 * - Files are pre-filtered by mtime: only files modified after the cursor are fully parsed.
 * - Episode idempotency uses the same (startMessageId, endMessageId) key scheme,
 *   where message IDs are the record's `uuid` field.
 */
export class ClaudeCodeEpisodeReader implements IEpisodeReader {
	readonly source = "claude-code";

	private readonly claudeDir: string;

	constructor(claudeDir?: string) {
		this.claudeDir = claudeDir ?? config.claudeDbPath;
	}

	// ── IEpisodeReader implementation ─────────────────────────────────────────

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit: number = config.consolidation.maxSessionsPerRun,
	): Array<{ id: string; maxMessageTime: number }> {
		const sessions = this.loadModifiedSessions(afterMessageTimeCreated);

		// Return sessions that have messages newer than the cursor, ordered by
		// max message time ASC so the consolidation loop advances the cursor in
		// chronological order (matching OpenCodeEpisodeReader behaviour).
		return sessions
			.filter((s) => s.maxTimestampMs > afterMessageTimeCreated)
			.sort((a, b) => a.maxTimestampMs - b.maxTimestampMs)
			.slice(0, limit)
			.map((s) => ({ id: s.sessionId, maxMessageTime: s.maxTimestampMs }));
	}

	countNewSessions(afterMessageTimeCreated: number): number {
		return this.loadModifiedSessions(afterMessageTimeCreated).filter(
			(s) => s.maxTimestampMs > afterMessageTimeCreated,
		).length;
	}

	getNewEpisodes(
		candidateSessionIds: string[],
		processedRanges: Map<string, ProcessedRange[]>,
	): Episode[] {
		if (candidateSessionIds.length === 0) return [];

		// Build a lookup map so we can find session files by UUID efficiently.
		const sessionMap = this.buildSessionMap();
		const episodes: Episode[] = [];

		for (const sessionId of candidateSessionIds) {
			const session = sessionMap.get(sessionId);
			if (!session) continue;

			const sessionProcessed = processedRanges.get(sessionId) ?? [];
			const sessionEpisodes = this.segmentSession(session, sessionProcessed);
			episodes.push(...sessionEpisodes);
		}

		return episodes;
	}

	close(): void {
		// No persistent resources to release (stateless file reader)
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	/**
	 * Enumerate all JSONL session files under ~/.claude/projects/ and parse only
	 * those whose file mtime is after the cursor (cheap pre-filter).
	 *
	 * Returns parsed sessions sorted by maxTimestampMs ASC.
	 */
	private loadModifiedSessions(
		afterMessageTimeCreated: number,
	): ParsedSession[] {
		const projectsDir = join(this.claudeDir, "projects");
		let projectDirs: string[];
		try {
			// withFileTypes: true lets us filter directories without a separate stat call,
			// avoiding readdirSync being called on stray files (e.g. .DS_Store) at this level.
			projectDirs = readdirSync(projectsDir, { withFileTypes: true })
				.filter((d) => d.isDirectory())
				.map((d) => d.name);
		} catch {
			// ~/.claude/projects doesn't exist yet (Claude Code never opened a session)
			return [];
		}

		const sessions: ParsedSession[] = [];

		for (const projectDir of projectDirs) {
			const projectPath = join(projectsDir, projectDir);
			let files: string[];
			try {
				files = readdirSync(projectPath).filter((f) => f.endsWith(".jsonl"));
			} catch {
				continue;
			}

			for (const file of files) {
				const filePath = join(projectPath, file);
				let mtimeMs: number;
				try {
					mtimeMs = statSync(filePath).mtimeMs;
				} catch {
					continue;
				}

				// Cheap pre-filter: skip files not modified since the cursor.
				// mtime granularity is ~1ms on most OSes, so this is a safe lower bound.
				// False positives (files modified but with no new messages) are handled
				// by the timestamp check in getCandidateSessions.
				if (mtimeMs <= afterMessageTimeCreated) continue;

				const sessionId = file.replace(/\.jsonl$/, "");
				const parsed = this.parseSessionFile(filePath, sessionId);
				if (parsed) sessions.push(parsed);
			}
		}

		return sessions;
	}

	/**
	 * Build a full map of all sessions (no mtime pre-filter) for getNewEpisodes.
	 * Called once per getNewEpisodes invocation; candidate IDs are already known.
	 */
	private buildSessionMap(): Map<string, ParsedSession> {
		const map = new Map<string, ParsedSession>();
		const projectsDir = join(this.claudeDir, "projects");
		let projectDirs: string[];
		try {
			projectDirs = readdirSync(projectsDir, { withFileTypes: true })
				.filter((d) => d.isDirectory())
				.map((d) => d.name);
		} catch {
			return map;
		}

		for (const projectDir of projectDirs) {
			const projectPath = join(projectsDir, projectDir);
			let files: string[];
			try {
				files = readdirSync(projectPath).filter((f) => f.endsWith(".jsonl"));
			} catch {
				continue;
			}

			for (const file of files) {
				const filePath = join(projectPath, file);
				const sessionId = file.replace(/\.jsonl$/, "");
				const parsed = this.parseSessionFile(filePath, sessionId);
				if (parsed) map.set(sessionId, parsed);
			}
		}

		return map;
	}

	/**
	 * Parse a single JSONL session file.
	 * Returns null if the file is empty, unreadable, or has no message records.
	 */
	private parseSessionFile(
		filePath: string,
		sessionId: string,
	): ParsedSession | null {
		let content: string;
		try {
			content = readFileSync(filePath, "utf8");
		} catch {
			return null;
		}

		const lines = content.split("\n").filter((l) => l.trim());
		if (lines.length === 0) return null;

		const records: CCRecord[] = [];
		let slug = sessionId;
		let cwd = "";
		let minTimestampMs = Number.MAX_SAFE_INTEGER;
		let maxTimestampMs = 0;

		for (const line of lines) {
			let record: CCRecord;
			try {
				record = JSON.parse(line) as CCRecord;
			} catch {
				continue; // skip malformed lines
			}

			records.push(record);

			// Extract slug and cwd from any record that has them
			if (record.slug) slug = record.slug;
			if (record.cwd) cwd = record.cwd;

			// Track timestamp range (only message records have timestamps)
			if (
				record.timestamp &&
				(record.type === "user" || record.type === "assistant")
			) {
				const ts = Date.parse(record.timestamp);
				if (!Number.isNaN(ts)) {
					if (ts < minTimestampMs) minTimestampMs = ts;
					if (ts > maxTimestampMs) maxTimestampMs = ts;
				}
			}
		}

		if (records.length === 0 || maxTimestampMs === 0) return null;

		// Derive project name from the last component of the working directory.
		// Claude Code encodes the cwd in the project dir name (URL-encoded path),
		// but the raw `cwd` field on records is more reliable.
		const projectName = cwd
			? (cwd.split("/").filter(Boolean).pop() ?? "unknown")
			: "unknown";

		let mtimeMs: number;
		try {
			mtimeMs = statSync(filePath).mtimeMs;
		} catch {
			mtimeMs = maxTimestampMs;
		}

		return {
			sessionId,
			slug,
			projectName,
			directory: cwd,
			filePath,
			mtimeMs,
			records,
			minTimestampMs:
				minTimestampMs === Number.MAX_SAFE_INTEGER ? 0 : minTimestampMs,
			maxTimestampMs,
		};
	}

	/**
	 * Segment a single parsed session into episodes, skipping already-processed ranges.
	 *
	 * Strategy (mirrors OpenCodeEpisodeReader):
	 * 1. Find compaction points (system records with subtype === "compact_boundary")
	 * 2. If compactions exist:
	 *    - The next user record after each compaction is the summary → one episode
	 *    - Messages after the last compaction → final episode(s)
	 * 3. If no compactions:
	 *    - All messages → one or more episodes chunked by token budget
	 * 4. Filter out already-processed (startMessageId, endMessageId) ranges.
	 */
	private segmentSession(
		session: ParsedSession,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const compactionPoints = this.getCompactionPoints(session);

		let episodes: Episode[];
		if (compactionPoints.length > 0) {
			episodes = this.segmentWithCompactions(session, compactionPoints);
		} else {
			episodes = this.segmentWithoutCompactions(session);
		}

		if (processedRanges.length === 0) return episodes;

		const processedSet = new Set(
			processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`),
		);
		return episodes.filter(
			(ep) => !processedSet.has(`${ep.startMessageId}::${ep.endMessageId}`),
		);
	}

	/**
	 * Find compaction points: system records with subtype === "compact_boundary",
	 * paired with the compaction summary (next user record after the marker).
	 *
	 * The compaction summary is the plain text string in the first `user` record
	 * after the compact_boundary marker. Claude Code stores it as a plain text
	 * string in `message.content` (not an array), or as a single text part.
	 */
	private getCompactionPoints(session: ParsedSession): Array<{
		compactionTimestamp: number;
		summaryText: string;
		summaryUuid: string;
		summaryTimestampMs: number;
	}> {
		const points: Array<{
			compactionTimestamp: number;
			summaryText: string;
			summaryUuid: string;
			summaryTimestampMs: number;
		}> = [];

		for (let i = 0; i < session.records.length; i++) {
			const rec = session.records[i];
			if (rec.type !== "system" || rec.subtype !== "compact_boundary") continue;

			const compactionTimestamp = rec.timestamp ? Date.parse(rec.timestamp) : 0;

			// The summary is the next user record after the compact_boundary.
			for (let j = i + 1; j < session.records.length; j++) {
				const next = session.records[j];
				if (next.type !== "user") continue;

				const summaryTimestampMs = next.timestamp
					? Date.parse(next.timestamp)
					: 0;
				const summaryUuid = next.uuid ?? `${session.sessionId}-compact-${i}`;

				// Extract summary text — stored as plain string or single-part array
				let summaryText = "";
				if (typeof next.message?.content === "string") {
					summaryText = next.message.content;
				} else if (Array.isArray(next.message?.content)) {
					const textParts = (next.message.content as ContentPart[])
						.filter((p) => p.type === "text" && p.text)
						.map((p) => p.text ?? "");
					summaryText = textParts.join("\n");
				}

				if (summaryText.trim()) {
					points.push({
						compactionTimestamp,
						summaryText,
						summaryUuid,
						summaryTimestampMs,
					});
				}
				break; // only the first user record after the marker is the summary
			}
		}

		return points;
	}

	/**
	 * Segment a session with compactions.
	 * Each compaction summary → one episode; messages after the last compaction → final episode(s).
	 */
	private segmentWithCompactions(
		session: ParsedSession,
		compactionPoints: Array<{
			compactionTimestamp: number;
			summaryText: string;
			summaryUuid: string;
			summaryTimestampMs: number;
		}>,
	): Episode[] {
		// Build the tool name map once for the whole session rather than
		// re-scanning all records inside getMessagesAfterCompaction.
		const toolNameMap = this.buildToolNameMap(session.records);
		const episodes: Episode[] = [];

		for (const point of compactionPoints) {
			const tokens = approxTokens(point.summaryText);
			episodes.push({
				sessionId: session.sessionId,
				startMessageId: point.summaryUuid,
				endMessageId: point.summaryUuid,
				sessionTitle: session.slug,
				projectName: session.projectName,
				directory: session.directory,
				timeCreated: session.minTimestampMs,
				maxMessageTime: point.summaryTimestampMs,
				content: point.summaryText,
				contentType: "compaction_summary",
				approxTokens: tokens,
			});
		}

		// Messages after the last compaction → final episode(s)
		const lastCompaction = compactionPoints[compactionPoints.length - 1];
		const tailMessages = this.getMessagesAfterCompaction(
			session,
			lastCompaction.summaryUuid,
			toolNameMap,
		);

		if (tailMessages.length >= config.consolidation.minSessionMessages) {
			const chunks = this.chunkByTokenBudget(
				tailMessages,
				MAX_TOKENS_PER_EPISODE,
			);
			for (const chunk of chunks) {
				const content = this.formatMessages(chunk);
				if (content.trim()) {
					episodes.push({
						sessionId: session.sessionId,
						startMessageId: chunk[0].messageId,
						endMessageId: chunk[chunk.length - 1].messageId,
						sessionTitle: session.slug,
						projectName: session.projectName,
						directory: session.directory,
						timeCreated: session.minTimestampMs,
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
	 * Get messages after the compaction summary, skipping the summary itself.
	 *
	 * @param toolNameMap - pre-built tool_use_id → name map (caller owns, avoids re-scan).
	 */
	private getMessagesAfterCompaction(
		session: ParsedSession,
		summaryUuid: string,
		toolNameMap: Map<string, string>,
	): EpisodeMessage[] {
		let pastSummary = false;
		const messages: EpisodeMessage[] = [];

		for (const rec of session.records) {
			if (rec.uuid === summaryUuid) {
				pastSummary = true;
				continue; // skip the summary itself
			}
			if (!pastSummary) continue;
			if (rec.type !== "user" && rec.type !== "assistant") continue;

			const msg = this.extractMessage(rec, toolNameMap);
			if (msg) messages.push(msg);
		}

		return messages;
	}

	/**
	 * Segment a session without compactions.
	 * All messages → one or more episodes chunked by token budget.
	 */
	private segmentWithoutCompactions(session: ParsedSession): Episode[] {
		const messages = this.extractAllMessages(session);

		if (messages.length < config.consolidation.minSessionMessages) return [];

		const chunks = this.chunkByTokenBudget(messages, MAX_TOKENS_PER_EPISODE);
		const episodes: Episode[] = [];

		for (const chunk of chunks) {
			const content = this.formatMessages(chunk);
			if (content.trim()) {
				episodes.push({
					sessionId: session.sessionId,
					startMessageId: chunk[0].messageId,
					endMessageId: chunk[chunk.length - 1].messageId,
					sessionTitle: session.slug,
					projectName: session.projectName,
					directory: session.directory,
					timeCreated: session.minTimestampMs,
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
	 * Build a map of tool_use_id → tool name by scanning all assistant records in a session.
	 *
	 * Claude Code stores tool calls in assistant records as `tool_use` content parts
	 * (with `id` and `name` fields). The corresponding results land in the next user
	 * record as `tool_result` parts that reference the call via `tool_use_id` — but
	 * carry no `name` themselves. This map bridges the two sides so the allowlist
	 * filter in extractMessage can work correctly.
	 */
	private buildToolNameMap(records: CCRecord[]): Map<string, string> {
		const map = new Map<string, string>();
		for (const rec of records) {
			if (rec.type !== "assistant" || !Array.isArray(rec.message?.content))
				continue;
			for (const part of rec.message.content as ContentPart[]) {
				if (part.type === "tool_use" && part.id && part.name) {
					map.set(part.id, part.name);
				}
			}
		}
		return map;
	}

	/**
	 * Extract all user/assistant messages from a session's records.
	 */
	private extractAllMessages(session: ParsedSession): EpisodeMessage[] {
		const toolNameMap = this.buildToolNameMap(session.records);
		const messages: EpisodeMessage[] = [];
		for (const rec of session.records) {
			if (rec.type !== "user" && rec.type !== "assistant") continue;
			const msg = this.extractMessage(rec, toolNameMap);
			if (msg) messages.push(msg);
		}
		return messages;
	}

	/**
	 * Extract a single EpisodeMessage from a JSONL record.
	 * Returns null if the record has no extractable text content.
	 *
	 * @param toolNameMap - maps tool_use_id → tool name (built from assistant records).
	 *   Required for allowlist filtering of tool_result parts, since tool_result parts
	 *   carry no name themselves — only a tool_use_id reference back to the tool_use part.
	 */
	private extractMessage(
		rec: CCRecord,
		toolNameMap: Map<string, string>,
	): EpisodeMessage | null {
		if (!rec.message || !rec.uuid) return null;
		if (rec.type !== "user" && rec.type !== "assistant") return null;

		const timestampMs = rec.timestamp ? Date.parse(rec.timestamp) : 0;
		const includeToolOutputs = config.consolidation.includeToolOutputs;

		let textContent = "";
		let toolContent = "";

		if (typeof rec.message.content === "string") {
			// Plain string content (compaction summaries, simple messages)
			textContent = rec.message.content;
		} else if (Array.isArray(rec.message.content)) {
			const parts = rec.message.content as ContentPart[];

			// Text and thinking parts
			textContent = parts
				.filter((p) => (p.type === "text" || p.type === "thinking") && p.text)
				.map((p) => p.text ?? "")
				.join("\n");

			// Tool result parts (user messages only) — filtered by allowlist.
			// tool_result parts carry no tool name — resolve via tool_use_id → toolNameMap.
			if (rec.type === "user" && includeToolOutputs.length > 0) {
				const toolResults = parts.filter((p) => p.type === "tool_result");
				const relevant = toolResults
					.map((p) => {
						const toolName = p.tool_use_id
							? (toolNameMap.get(p.tool_use_id) ?? "")
							: "";
						// tool_result content can be a string or an array of parts
						let resultText = "";
						if (typeof p.content === "string") {
							resultText = p.content;
						} else if (Array.isArray(p.content)) {
							resultText = (p.content as ContentPart[])
								.filter((sub) => sub.type === "text" && sub.text)
								.map((sub) => sub.text ?? "")
								.join("\n");
						}
						return { name: toolName, text: resultText };
					})
					.filter(
						(r) => r.name && r.text && includeToolOutputs.includes(r.name),
					);

				if (relevant.length > 0) {
					toolContent = relevant
						.map((r) => {
							const truncated =
								r.text.length > MAX_TOOL_OUTPUT_CHARS
									? `${r.text.slice(0, MAX_TOOL_OUTPUT_CHARS)}\n[...truncated]`
									: r.text;
							return `[tool: ${r.name}]\n${truncated}`;
						})
						.join("\n\n");
				}
			}
		}

		let content = [textContent.trim(), toolContent.trim()]
			.filter(Boolean)
			.join("\n\n");
		if (!content) return null;

		if (content.length > MAX_MESSAGE_CHARS) {
			content = `${content.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`;
		}

		return {
			messageId: rec.uuid,
			role: rec.type as "user" | "assistant",
			content,
			timestamp: timestampMs,
		};
	}

	/**
	 * Chunk messages into groups that fit within a token budget.
	 * A single oversized message is placed alone in its own chunk (soft limit).
	 */
	private chunkByTokenBudget(
		messages: EpisodeMessage[],
		maxTokens: number,
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

		if (currentChunk.length > 0) chunks.push(currentChunk);
		return chunks;
	}

	/**
	 * Format messages into a text block for the LLM.
	 */
	private formatMessages(messages: EpisodeMessage[]): string {
		return messages.map((m) => `  ${m.role}: ${m.content}`).join("\n");
	}
}
