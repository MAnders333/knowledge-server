import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { config } from "../../config.js";
import { stripInjectedKnowledge } from "../../consolidation/untrusted.js";
import type {
	Episode,
	EpisodeMessage,
	IEpisodeReader,
	ProcessedRange,
} from "../../types.js";
import {
	type EpisodeBase,
	MAX_MESSAGE_CHARS,
	dropProcessed,
	episodeFrom,
	episodesFromMessages,
	isJsonObject,
	recordSkip,
} from "./shared.js";

// ── pi session JSONL entry types ──────────────────────────────────────────────
// Format reference: pi docs/session-format.md (v3). Entries form a tree via
// id/parentId; we process them in file order — the same simplification the
// Claude Code reader makes with parentUuid. Branch interleaving is accepted
// as noise for knowledge-extraction purposes.

/**
 * Content part inside a pi message. `type` discriminates:
 * text | image | thinking | toolCall.
 */
interface PiContentPart {
	type: string;
	text?: string; // type === "text"
	thinking?: string; // type === "thinking"
	name?: string; // type === "toolCall" — tool name (arguments ignored)
}

/** The `message` payload of a pi `message` entry (AgentMessage union). */
interface PiMessage {
	role: "user" | "assistant" | "toolResult" | "bashExecution" | string;
	// user: string | parts; assistant: parts; toolResult: parts
	content?: string | PiContentPart[];
	// toolResult fields:
	toolName?: string;
	isError?: boolean;
	// bashExecution fields (interactive `!` commands):
	command?: string;
	output?: string;
	excludeFromContext?: boolean; // true for `!!` — model never saw it
}

/**
 * A single record (line) in a pi session JSONL file.
 */
interface PiRecord {
	type:
		| "session"
		| "message"
		| "compaction"
		| "branch_summary"
		| "model_change"
		| "thinking_level_change"
		| "label"
		| "custom"
		| "custom_message"
		| "session_info"
		| string;
	id?: string; // 8-char hex entry id (absent on the session header)
	parentId?: string | null;
	timestamp?: string; // ISO string
	// session header:
	version?: number;
	cwd?: string;
	// message entries:
	message?: PiMessage;
	// compaction / branch_summary entries (summary inlined):
	summary?: string;
	tokensBefore?: number;
	// session_info entries:
	name?: string;
}

/**
 * A parsed session file ready for segmentation.
 */
interface ParsedSession {
	sessionId: string; // UUID from the session header (fallback: filename)
	title: string; // session_info name ?? first user message preview ?? sessionId
	projectName: string; // last component of cwd
	directory: string; // cwd from the session header
	filePath: string; // absolute path of the JSONL file
	records: PiRecord[];
	minTimestampMs: number; // earliest entry timestamp
	maxTimestampMs: number; // latest entry timestamp
}

/** Unix ms of an ISO timestamp field; NaN when absent or not a parseable string. */
function parseTimestamp(value: unknown): number {
	return typeof value === "string" ? Date.parse(value) : Number.NaN;
}

function messageOf(err: unknown): string {
	return err instanceof Error ? err.message : String(err);
}

/**
 * Reads episodes from pi's JSONL session files.
 *
 * Storage layout (https://github.com/earendil-works/pi-mono, docs/session-format.md):
 *   <piAgentDir>/sessions/--<cwd-encoded>--/<timestamp>_<uuid>.jsonl
 *
 * pi's agent dir is `~/.pi/agent` by default but can be redirected per-invocation
 * via PI_CODING_AGENT_DIR (a SWAP, not a merge — e.g. separate work/personal
 * layers). Since the daemon runs without that env var, this reader scans ALL
 * layer dirs found under the configured root:
 *   <root>/<asterisk>/sessions/<projectDir>/<file>.jsonl
 * With the default root ~/.pi this covers `agent`, and any custom layers such
 * as `work` / `personal` automatically, including ones created later.
 *
 * Content extraction:
 * - user messages: text parts (images skipped)
 * - assistant messages: text + thinking parts (thinking mirrors Claude reader)
 * - toolResult messages: text parts, filtered by the
 *   CONSOLIDATION_INCLUDE_TOOL_OUTPUTS allowlist. pi carries `toolName` directly
 *   on the result, so no tool-call/tool-result ID bridging is needed.
 * - bashExecution messages (`!` prefix commands): included as `[tool: bash]`
 *   output when "bash" is allowlisted; skipped entirely when excludeFromContext
 *   is set (`!!` prefix — the model never saw them).
 * - compaction / branch_summary entries: the inlined `summary` becomes a
 *   compaction_summary episode (pi stores the summary on the entry itself —
 *   no "next user record" lookup like Claude Code needs).
 *
 * Deliberately skipped:
 * - model_change / thinking_level_change / label / session_info (metadata;
 *   session_info.name is used as the episode title)
 * - custom entries (extension state, never in LLM context)
 * - custom_message entries (extension-injected context — skipping avoids
 *   re-ingesting content that was itself injected, e.g. by a knowledge
 *   integration → feedback-loop safe)
 *
 * Cursor semantics:
 * - `lastMessageTimeCreated` is unix ms, same column as the other readers.
 * - Timestamps come from `record.timestamp` (ISO string → ms).
 *
 * Incremental processing:
 * - Files are pre-filtered by mtime: only files modified after the cursor are parsed.
 * - Message IDs are the pi entry `id` (8-char hex, unique within a session).
 *   Messages up to the session's processed watermark are dropped before
 *   chunking (episodesFromMessages), so only the new tail is segmented.
 * - Compaction / branch summaries are keyed by their entry id and deduplicated
 *   by exact (startMessageId, endMessageId) match (dropProcessed).
 */
export class PiEpisodeReader implements IEpisodeReader {
	readonly source: string;

	private readonly sessionsDirs: string[];

	/**
	 * Cache of the last `loadModifiedSessions` result, keyed by session UUID.
	 * Same rationale as ClaudeCodeEpisodeReader: getCandidateSessions populates
	 * it so the immediately-following getNewEpisodes call reuses parsed data
	 * without a filesystem rescan.
	 */
	private _sessionCache = new Map<string, ParsedSession>();

	/**
	 * @param sessionsDirs Explicit list of sessions directories to scan
	 *   (each containing <projectDir>/<file>.jsonl subdirectories).
	 *   Defaults to resolvePiSessionsDirs(config.piSessionsRoot).
	 * @param source Source id stamped on produced episodes (idempotency keys).
	 *   Overridden by the oh-my-pi reader, which shares this parser.
	 */
	constructor(sessionsDirs?: string[], source = "pi") {
		this.sessionsDirs = sessionsDirs ?? resolvePiSessionsDirs();
		this.source = source;
	}

	// ── IEpisodeReader implementation ─────────────────────────────────────────

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit: number = config.consolidation.maxSessionsPerRun,
	): Array<{ id: string; maxMessageTime: number }> {
		const sessions = this.loadModifiedSessions(afterMessageTimeCreated);

		this._sessionCache = new Map(sessions.map((s) => [s.sessionId, s]));

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

		// Fallback for direct getNewEpisodes calls without a prior
		// getCandidateSessions (tests) — same pattern as ClaudeCodeEpisodeReader.
		const missingIds = candidateSessionIds.filter(
			(id) => !this._sessionCache.has(id),
		);
		if (missingIds.length > 0) {
			const fallback = this.loadSessionsByIds(missingIds);
			for (const [id, session] of fallback) {
				this._sessionCache.set(id, session);
			}
		}

		const episodes: Episode[] = [];

		for (const sessionId of candidateSessionIds) {
			const session = this._sessionCache.get(sessionId);
			if (!session) continue;

			try {
				episodes.push(
					...this.segmentSession(session, processedRanges.get(sessionId) ?? []),
				);
			} catch (err) {
				recordSkip(
					this.source,
					sessionId,
					`segmentation failed: ${messageOf(err)}`,
				);
			}
		}

		return episodes;
	}

	close(): void {
		// No persistent resources to release (stateless file reader)
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	/**
	 * Scan all project dirs under all sessions dirs for specific session UUIDs.
	 * Fallback for getNewEpisodes when the cache wasn't populated first.
	 */
	private loadSessionsByIds(sessionIds: string[]): Map<string, ParsedSession> {
		const needed = new Set(sessionIds);
		const result = new Map<string, ParsedSession>();

		for (const sessionsDir of this.sessionsDirs) {
			for (const projectDir of this.listProjectDirs(sessionsDir)) {
				if (result.size === needed.size) return result;
				const projectPath = join(sessionsDir, projectDir);
				for (const file of this.listJsonlFiles(projectPath)) {
					if (!this.fileMayBeSession(file, needed)) continue;
					const parsed = this.parseSessionFile(join(projectPath, file));
					if (parsed && needed.has(parsed.sessionId)) {
						result.set(parsed.sessionId, parsed);
					}
				}
			}
		}

		return result;
	}

	/**
	 * Enumerate JSONL session files across all sessions dirs and parse only
	 * those whose file mtime is after the cursor (cheap pre-filter).
	 * Returns parsed sessions sorted by maxTimestampMs ASC.
	 */
	private loadModifiedSessions(
		afterMessageTimeCreated: number,
	): ParsedSession[] {
		const sessions: ParsedSession[] = [];

		for (const sessionsDir of this.sessionsDirs) {
			for (const projectDir of this.listProjectDirs(sessionsDir)) {
				const projectPath = join(sessionsDir, projectDir);
				for (const file of this.listJsonlFiles(projectPath)) {
					const filePath = join(projectPath, file);
					let mtimeMs: number;
					try {
						mtimeMs = statSync(filePath).mtimeMs;
					} catch (err) {
						recordSkip(this.source, filePath, `unreadable: ${messageOf(err)}`);
						continue;
					}
					if (mtimeMs <= afterMessageTimeCreated) continue;

					const parsed = this.parseSessionFile(filePath);
					if (parsed) sessions.push(parsed);
				}
			}
		}

		return sessions.sort((a, b) => a.maxTimestampMs - b.maxTimestampMs);
	}

	private listProjectDirs(sessionsDir: string): string[] {
		try {
			return readdirSync(sessionsDir, { withFileTypes: true })
				.filter((d) => d.isDirectory())
				.map((d) => d.name);
		} catch (err) {
			recordSkip(this.source, sessionsDir, `unreadable: ${messageOf(err)}`);
			return [];
		}
	}

	private listJsonlFiles(projectPath: string): string[] {
		try {
			return readdirSync(projectPath).filter((f) => f.endsWith(".jsonl"));
		} catch (err) {
			recordSkip(this.source, projectPath, `unreadable: ${messageOf(err)}`);
			return [];
		}
	}

	/**
	 * Cheap filename pre-filter for loadSessionsByIds: pi filenames are
	 * `<timestamp>_<uuid>.jsonl`, so a needed UUID must appear in the name.
	 */
	private fileMayBeSession(file: string, needed: Set<string>): boolean {
		for (const id of needed) {
			if (file.includes(id)) return true;
		}
		return false;
	}

	/**
	 * Parse a single JSONL session file.
	 * Returns null if the file is empty, unreadable, has no session header,
	 * or contains no timestamped entries. Unreadable files and malformed lines
	 * are counted as skips; malformed or non-object lines are dropped individually.
	 */
	private parseSessionFile(filePath: string): ParsedSession | null {
		let content: string;
		try {
			content = readFileSync(filePath, "utf8");
		} catch (err) {
			recordSkip(this.source, filePath, `unreadable: ${messageOf(err)}`);
			return null;
		}

		const lines = content.split("\n").filter((l) => l.trim());
		if (lines.length === 0) return null;

		const records: PiRecord[] = [];
		let sessionId = "";
		let cwd = "";
		let infoName = "";
		let minTimestampMs = Number.MAX_SAFE_INTEGER;
		let maxTimestampMs = 0;
		let malformedLines = 0;

		for (const line of lines) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(line);
			} catch {
				malformedLines += 1;
				continue;
			}
			// `null`, scalars and arrays are not entries; skip without dereferencing.
			if (!isJsonObject(parsed)) {
				malformedLines += 1;
				continue;
			}
			const record = parsed as PiRecord;

			records.push(record);

			if (record.type === "session") {
				if (typeof record.id === "string" && record.id) sessionId = record.id;
				if (typeof record.cwd === "string" && record.cwd) cwd = record.cwd;
			} else if (
				record.type === "session_info" &&
				typeof record.name === "string" &&
				record.name
			) {
				infoName = record.name;
			}

			const ts = parseTimestamp(record.timestamp);
			if (!Number.isNaN(ts)) {
				if (ts < minTimestampMs) minTimestampMs = ts;
				if (ts > maxTimestampMs) maxTimestampMs = ts;
			}
		}

		// Fallback: derive session UUID from the `<timestamp>_<uuid>.jsonl` filename.
		if (!sessionId) {
			const base = filePath.split("/").pop() ?? "";
			const underscore = base.indexOf("_");
			if (underscore >= 0) {
				sessionId = base.slice(underscore + 1).replace(/\.jsonl$/, "");
			}
		}

		if (malformedLines > 0) {
			recordSkip(
				this.source,
				sessionId || filePath,
				"malformed JSONL line(s)",
				malformedLines,
			);
		}

		if (records.length === 0 || !sessionId || maxTimestampMs === 0) return null;

		// Title fallback chain: /name → first user message preview → sessionId.
		let firstUserText = "";
		for (const rec of records) {
			if (rec.type !== "message" || rec.message?.role !== "user") continue;
			const text = this.extractTextParts(rec.message.content);
			if (text.trim()) {
				firstUserText =
					text.trim().length > 60
						? `${text.trim().slice(0, 60)}…`
						: text.trim();
				break;
			}
		}

		const projectName = cwd
			? (cwd.split("/").filter(Boolean).pop() ?? "unknown")
			: "unknown";

		return {
			sessionId,
			title: infoName || firstUserText || sessionId,
			projectName,
			directory: cwd,
			filePath,
			records,
			minTimestampMs:
				minTimestampMs === Number.MAX_SAFE_INTEGER ? 0 : minTimestampMs,
			maxTimestampMs,
		};
	}

	/**
	 * Segment a single parsed session into episodes, skipping already-processed ranges.
	 *
	 * Strategy (mirrors ClaudeCodeEpisodeReader, adapted for pi's inline summaries):
	 * 1. Find compaction + branch_summary entries (both carry an inline `summary`)
	 * 2. If any exist:
	 *    - Each summary → one episode (rich, pre-condensed)
	 *    - Messages after the last summary entry → tail episode(s)
	 * 3. If none exist:
	 *    - All messages → tail episode(s), chunked by token budget
	 *    Both message paths go through episodesFromMessages, which drops the
	 *    already-uploaded prefix before chunking.
	 * 4. Drop exact (startMessageId, endMessageId) matches already processed.
	 */
	private segmentSession(
		session: ParsedSession,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const summaryPoints = this.getSummaryPoints(session);
		const base: EpisodeBase = {
			source: this.source,
			sessionId: session.sessionId,
			sessionTitle: session.title,
			projectName: session.projectName,
			directory: session.directory,
			timeCreated: session.minTimestampMs,
		};

		const episodes =
			summaryPoints.length > 0
				? this.segmentWithSummaries(
						base,
						session,
						summaryPoints,
						processedRanges,
					)
				: this.segmentWithoutSummaries(base, session, processedRanges);

		return dropProcessed(episodes, processedRanges);
	}

	/**
	 * Find compaction and branch_summary entries. Both carry their summary
	 * inline on the entry (`summary` field) — unlike Claude Code, where the
	 * summary lives in the following user record.
	 */
	private getSummaryPoints(session: ParsedSession): Array<{
		entryId: string;
		summaryText: string;
		timestampMs: number;
	}> {
		const points: Array<{
			entryId: string;
			summaryText: string;
			timestampMs: number;
		}> = [];

		for (const rec of session.records) {
			if (rec.type !== "compaction" && rec.type !== "branch_summary") continue;
			if (typeof rec.id !== "string" || !rec.id) continue;
			if (typeof rec.summary !== "string" || !rec.summary.trim()) continue;
			const ts = parseTimestamp(rec.timestamp);
			points.push({
				entryId: rec.id,
				summaryText: rec.summary,
				timestampMs: Number.isNaN(ts) ? 0 : ts,
			});
		}

		return points;
	}

	private segmentWithSummaries(
		base: EpisodeBase,
		session: ParsedSession,
		summaryPoints: Array<{
			entryId: string;
			summaryText: string;
			timestampMs: number;
		}>,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const episodes: Episode[] = [];

		for (const point of summaryPoints) {
			episodes.push(
				episodeFrom(base, {
					startMessageId: point.entryId,
					endMessageId: point.entryId,
					maxMessageTime: point.timestampMs,
					content: point.summaryText,
					contentType: "compaction_summary",
				}),
			);
		}

		// Messages after the last summary entry (in file order) → tail episode(s).
		// Pre-summary messages are covered by the summary itself.
		const lastSummaryId = summaryPoints[summaryPoints.length - 1].entryId;
		const tailMessages = this.getMessagesAfterEntry(session, lastSummaryId);
		episodes.push(
			...episodesFromMessages(base, tailMessages, { processedRanges }),
		);

		return episodes;
	}

	/**
	 * Extract message entries appearing after `entryId` in file order.
	 */
	private getMessagesAfterEntry(
		session: ParsedSession,
		entryId: string,
	): EpisodeMessage[] {
		let pastEntry = false;
		const messages: EpisodeMessage[] = [];

		for (const rec of session.records) {
			if (rec.id === entryId) {
				pastEntry = true;
				continue;
			}
			if (!pastEntry) continue;

			const msg = this.extractMessage(rec, session.sessionId);
			if (msg) messages.push(msg);
		}

		return messages;
	}

	private segmentWithoutSummaries(
		base: EpisodeBase,
		session: ParsedSession,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const messages: EpisodeMessage[] = [];
		for (const rec of session.records) {
			const msg = this.extractMessage(rec, session.sessionId);
			if (msg) messages.push(msg);
		}

		return episodesFromMessages(base, messages, {
			processedRanges,
		});
	}

	/**
	 * Extract a single EpisodeMessage from a `message` entry.
	 * Returns null for non-message entries and messages with no extractable content.
	 * Malformed message payloads are counted as skips and dropped individually.
	 *
	 * Role mapping (EpisodeMessage allows "user" | "assistant" | "tool"):
	 * - user → user
	 * - assistant → assistant (text + thinking; toolCall arguments skipped,
	 *   same as the Claude reader skipping tool_use input)
	 * - toolResult → tool, prefixed `[tool: <name>]`, allowlist-filtered
	 * - bashExecution → tool, as `[tool: bash]` ($ command + output),
	 *   allowlist-filtered; skipped when excludeFromContext (`!!`)
	 */
	private extractMessage(
		rec: PiRecord,
		sessionId: string,
	): EpisodeMessage | null {
		if (rec.type !== "message" || typeof rec.id !== "string" || !rec.id) {
			return null;
		}

		const msg = rec.message;
		if (!isJsonObject(msg)) {
			recordSkip(
				this.source,
				sessionId,
				"message entry without an object payload",
			);
			return null;
		}
		if (
			msg.content !== undefined &&
			typeof msg.content !== "string" &&
			!Array.isArray(msg.content)
		) {
			recordSkip(this.source, sessionId, "non-array message content");
			return null;
		}

		let timestampMs = 0;
		if (rec.timestamp) {
			timestampMs = parseTimestamp(rec.timestamp);
			if (Number.isNaN(timestampMs)) {
				recordSkip(this.source, sessionId, "unparseable message timestamp");
				return null;
			}
		}
		const includeToolOutputs = config.consolidation.includeToolOutputs;

		let role: EpisodeMessage["role"];
		let content = "";

		if (msg.role === "user") {
			role = "user";
			content = this.extractTextParts(msg.content).trim();
		} else if (msg.role === "assistant") {
			role = "assistant";
			content = this.extractAssistantText(msg.content).trim();
		} else if (msg.role === "toolResult") {
			role = "tool";
			const toolName = typeof msg.toolName === "string" ? msg.toolName : "";
			if (!toolName || !includeToolOutputs.includes(toolName)) return null;
			const text = this.extractTextParts(msg.content).trim();
			if (!text) return null;
			content = `[tool: ${toolName}]\n${text}`;
		} else if (msg.role === "bashExecution") {
			role = "tool";
			if (msg.excludeFromContext) return null; // `!!` — model never saw it
			if (!includeToolOutputs.includes("bash")) return null;
			const command = typeof msg.command === "string" ? msg.command.trim() : "";
			const output = typeof msg.output === "string" ? msg.output.trim() : "";
			if (!command && !output) return null;
			content = `[tool: bash]\n$ ${command}\n${output}`.trim();
		} else {
			// compactionSummary / branchSummary message roles (legacy), custom
			// messages, and anything unknown: skip — summaries are handled via
			// their entry types, custom_message is skipped deliberately.
			return null;
		}

		if (!content) return null;

		// Remove our injected context (any role) before truncation; drop the message only if nothing else remains.
		content = stripInjectedKnowledge(content);
		if (!content.trim()) return null;

		if (content.length > MAX_MESSAGE_CHARS) {
			content = `${content.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`;
		}

		return { messageId: rec.id, role, content, timestamp: timestampMs };
	}

	/** Text parts of a user/toolResult message (string or part array; images skipped). */
	private extractTextParts(content: PiMessage["content"]): string {
		if (typeof content === "string") return content;
		if (!Array.isArray(content)) return "";
		return content
			.filter((p) => isJsonObject(p) && p.type === "text" && p.text)
			.map((p) => p.text ?? "")
			.join("\n");
	}

	/** Text + thinking parts of an assistant message (toolCall parts skipped). */
	private extractAssistantText(content: PiMessage["content"]): string {
		if (typeof content === "string") return content;
		if (!Array.isArray(content)) return "";
		return content
			.filter(
				(p) =>
					isJsonObject(p) &&
					((p.type === "text" && p.text) ||
						(p.type === "thinking" && p.thinking)),
			)
			.map((p) => (p.type === "thinking" ? (p.thinking ?? "") : (p.text ?? "")))
			.join("\n");
	}
}

/**
 * Discover pi sessions directories under the configured root.
 *
 * Layout: pi stores sessions at <agentDir>/sessions/, where the agent dir is
 * `~/.pi/agent` by default or whatever PI_CODING_AGENT_DIR pointed at for a
 * given invocation (the var is a swap, not a merge — e.g. separate work and
 * personal layers). The daemon runs without that per-invocation context, so we
 * scan one level down from the root and include every `<root>/<dir>/sessions`
 * that exists. If `<root>/sessions` itself exists (root pointed directly at an
 * agent dir), it is included as well.
 *
 * Resolution:
 * 1. PI_SESSIONS_ROOT env var (via config.piSessionsRoot)
 * 2. ~/.pi
 */
export function resolvePiSessionsDirs(root?: string): string[] {
	const piRoot = root ?? config.piSessionsRoot;
	const dirs: string[] = [];

	try {
		if (statSync(join(piRoot, "sessions")).isDirectory()) {
			dirs.push(join(piRoot, "sessions"));
		}
	} catch {
		// no direct sessions dir — fine
	}

	let subdirs: string[];
	try {
		subdirs = readdirSync(piRoot, { withFileTypes: true })
			.filter((d) => d.isDirectory())
			.map((d) => d.name);
	} catch {
		return dirs; // root doesn't exist at all
	}

	for (const sub of subdirs) {
		const candidate = join(piRoot, sub, "sessions");
		try {
			if (statSync(candidate).isDirectory()) dirs.push(candidate);
		} catch {
			// no sessions dir in this layer — skip
		}
	}

	return dirs;
}
