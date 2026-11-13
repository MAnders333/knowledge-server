import { readFileSync, readdirSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import { containsInjectedKnowledge } from "../../activation/format.js";
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

// ── Codex CLI JSONL record types ──────────────────────────────────────────────

/**
 * Top-level record in a Codex CLI rollout JSONL file.
 *
 * Each line is one of:
 *   - session_meta  — first record; contains session id, cwd, git info
 *   - response_item — conversation content (messages, tool calls, tool outputs)
 *   - event_msg     — user input echoes (not useful for extraction)
 *
 * The `type` field on response_item records is in the `payload.type` sub-field.
 */
interface CodexRecord {
	timestamp: string; // ISO string, e.g. "2025-09-18T14:12:27.640Z"
	type: "session_meta" | "response_item" | "event_msg" | string;
	payload: CodexSessionMeta | CodexResponseItem | CodexEventMsg | unknown;
}

interface CodexSessionMeta {
	id: string; // session UUID
	timestamp: string; // ISO — session start time
	cwd: string; // working directory
	originator?: string; // "codex_cli_rs"
	cli_version?: string;
	instructions?: string; // AGENTS.md content
	git?: {
		commit_hash?: string;
		branch?: string;
		repository_url?: string;
	};
}

/**
 * A response_item record. The `type` field inside payload discriminates content:
 *   - "message"              — user or assistant turn (role field present)
 *   - "function_call"        — tool invocation (name, arguments, call_id)
 *   - "function_call_output" — tool result (call_id, output)
 */
interface CodexResponseItem {
	type: "message" | "function_call" | "function_call_output" | string;
	// message fields
	role?: "user" | "assistant";
	content?: CodexContentPart[];
	// function_call fields
	name?: string;
	arguments?: string;
	call_id?: string;
	// function_call_output fields
	output?: string;
}

interface CodexContentPart {
	type: "input_text" | "output_text" | string;
	text?: string;
}

interface CodexEventMsg {
	type: string;
	message?: string;
}

// ── Parsed session ─────────────────────────────────────────────────────────────

/**
 * A parsed Codex session file ready for segmentation.
 */
interface ParsedSession {
	sessionId: string; // UUID from session_meta or filename
	sessionTitle: string; // last component of cwd
	projectName: string; // same as sessionTitle (Codex has no separate project concept)
	directory: string; // cwd value
	filePath: string; // absolute path of the JSONL file
	mtimeMs: number; // file mtime — cheap pre-filter
	messages: EpisodeMessage[]; // extracted user/assistant messages in order
	minTimestampMs: number; // earliest message timestamp
	maxTimestampMs: number; // latest message timestamp
}

// ── Path resolution ────────────────────────────────────────────────────────────

/**
 * Resolve the Codex sessions directory path.
 *
 * Resolution order:
 * 1. CODEX_SESSIONS_DIR env var (already baked into config.codexSessionsDir)
 * 2. $CODEX_HOME/sessions (Codex's own home dir env var)
 * 3. ~/.codex/sessions (default)
 *
 * Always returns a string (the resolved path). The caller is responsible for
 * checking whether the directory actually exists via existsSync().
 */
export function resolveCodexSessionsDir(): string {
	// Explicit override from knowledge-server config
	if (config.codexSessionsDir) return config.codexSessionsDir;

	// CODEX_HOME is used by Codex itself (equivalent to ~/.codex by default)
	const codexHome = process.env.CODEX_HOME
		? process.env.CODEX_HOME
		: join(homedir(), ".codex");

	return join(codexHome, "sessions");
}

// ── Reader ─────────────────────────────────────────────────────────────────────

/**
 * Reads episodes from Codex CLI's JSONL rollout files.
 *
 * Storage layout (confirmed by direct inspection on macOS):
 *   ~/.codex/sessions/YYYY/MM/DD/rollout-<ISO-timestamp>-<uuid>.jsonl
 *
 * Each JSONL file contains a sequence of records. We process:
 *   - `session_meta` records: extract session id, cwd, start time
 *   - `response_item` records where payload.type === "message":
 *     - role "user": plain text from content[].text (input_text parts)
 *     - role "assistant": plain text from content[].text (output_text parts)
 *   - `function_call` and `function_call_output` records are skipped
 *     (tool I/O is not useful for knowledge extraction — no allowlist mechanism)
 *
 * Message IDs:
 *   Codex has no per-message stable UUID. We use "<sessionId>:<lineIndex>" as
 *   a stable synthetic ID. This is safe as long as Codex only appends to files
 *   (which it does — rollout files are written sequentially and never edited).
 *   Line indices are 0-based and count all non-empty lines in the file.
 *
 * Cursor semantics:
 *   maxTimestampMs = max(record.timestamp) across all response_item records.
 *   This is the value stored in the source_cursor table and advanced per cycle.
 *
 * Incremental processing:
 *   Files are pre-filtered by mtime (cheap). Within a file the
 *   (startMessageId, endMessageId) pair provides episode-level idempotency.
 */
export class CodexEpisodeReader implements IEpisodeReader {
	readonly source = "codex";

	private readonly sessionsDir: string;

	/**
	 * Cache of parsed sessions from the last getCandidateSessions call.
	 * Rebuilt each cycle — same bounded-cache pattern as CursorEpisodeReader.
	 */
	private _sessionCache = new Map<string, ParsedSession>();

	constructor(sessionsDir?: string) {
		this.sessionsDir = sessionsDir ?? resolveCodexSessionsDir();
	}

	// ── IEpisodeReader ──────────────────────────────────────────────────────────

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit: number = config.consolidation.maxSessionsPerRun,
	): Array<{ id: string; maxMessageTime: number }> {
		const sessions = this.loadModifiedSessions(afterMessageTimeCreated);

		const selected = sessions
			.filter((s) => s.maxTimestampMs > afterMessageTimeCreated)
			.sort((a, b) => a.maxTimestampMs - b.maxTimestampMs)
			.slice(0, limit);

		// Rebuild cache from exactly the selected sessions (bounded to `limit`).
		this._sessionCache = new Map(selected.map((s) => [s.sessionId, s]));

		return selected.map((s) => ({
			id: s.sessionId,
			maxMessageTime: s.maxTimestampMs,
		}));
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

		// Fallback: load any IDs not already in the cache (e.g. direct test calls).
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

			const sessionProcessed = processedRanges.get(sessionId) ?? [];
			const sessionEpisodes = this.segmentSession(session, sessionProcessed);
			episodes.push(...sessionEpisodes);
		}

		return episodes;
	}

	close(): void {
		// No persistent resources (stateless file reader)
	}

	// ── Private helpers ─────────────────────────────────────────────────────────

	/**
	 * Walk ~/.codex/sessions/YYYY/MM/DD/ and parse files modified after the cursor.
	 */
	private loadModifiedSessions(
		afterMessageTimeCreated: number,
	): ParsedSession[] {
		const sessions: ParsedSession[] = [];

		// Year-level dirs
		let yearDirs: string[];
		try {
			yearDirs = readdirSync(this.sessionsDir, { withFileTypes: true })
				.filter((d) => d.isDirectory())
				.map((d) => d.name);
		} catch {
			return [];
		}

		for (const year of yearDirs) {
			const yearPath = join(this.sessionsDir, year);
			let monthDirs: string[];
			try {
				monthDirs = readdirSync(yearPath, { withFileTypes: true })
					.filter((d) => d.isDirectory())
					.map((d) => d.name);
			} catch {
				continue;
			}

			for (const month of monthDirs) {
				const monthPath = join(yearPath, month);
				let dayDirs: string[];
				try {
					dayDirs = readdirSync(monthPath, { withFileTypes: true })
						.filter((d) => d.isDirectory())
						.map((d) => d.name);
				} catch {
					continue;
				}

				for (const day of dayDirs) {
					const dayPath = join(monthPath, day);
					let files: string[];
					try {
						files = readdirSync(dayPath).filter(
							(f) => f.startsWith("rollout-") && f.endsWith(".jsonl"),
						);
					} catch {
						continue;
					}

					for (const file of files) {
						const filePath = join(dayPath, file);
						let mtimeMs: number;
						try {
							mtimeMs = statSync(filePath).mtimeMs;
						} catch {
							continue;
						}
						if (mtimeMs <= afterMessageTimeCreated) continue;

						const parsed = this.parseSessionFile(filePath);
						if (parsed) sessions.push(parsed);
					}
				}
			}
		}

		return sessions;
	}

	/**
	 * Load specific session IDs by scanning all date directories.
	 * Used as a fallback when the cache doesn't contain a requested ID.
	 */
	private loadSessionsByIds(sessionIds: string[]): Map<string, ParsedSession> {
		const needed = new Set(sessionIds);
		const result = new Map<string, ParsedSession>();

		let yearDirs: string[];
		try {
			yearDirs = readdirSync(this.sessionsDir, { withFileTypes: true })
				.filter((d) => d.isDirectory())
				.map((d) => d.name);
		} catch {
			return result;
		}

		for (const year of yearDirs) {
			if (result.size === needed.size) break;
			const yearPath = join(this.sessionsDir, year);
			let monthDirs: string[];
			try {
				monthDirs = readdirSync(yearPath, { withFileTypes: true })
					.filter((d) => d.isDirectory())
					.map((d) => d.name);
			} catch {
				continue;
			}

			for (const month of monthDirs) {
				if (result.size === needed.size) break;
				const monthPath = join(yearPath, month);
				let dayDirs: string[];
				try {
					dayDirs = readdirSync(monthPath, { withFileTypes: true })
						.filter((d) => d.isDirectory())
						.map((d) => d.name);
				} catch {
					continue;
				}

				for (const day of dayDirs) {
					if (result.size === needed.size) break;
					const dayPath = join(monthPath, day);
					let files: string[];
					try {
						files = readdirSync(dayPath).filter(
							(f) => f.startsWith("rollout-") && f.endsWith(".jsonl"),
						);
					} catch {
						continue;
					}

					for (const file of files) {
						if (result.size === needed.size) break;
						const filePath = join(dayPath, file);
						const parsed = this.parseSessionFile(filePath);
						if (parsed && needed.has(parsed.sessionId)) {
							result.set(parsed.sessionId, parsed);
						}
					}
				}
			}
		}

		return result;
	}

	/**
	 * Parse a single Codex JSONL rollout file.
	 *
	 * Two-pass approach:
	 * 1. Pass 1 — resolve sessionId (from session_meta or filename fallback), cwd,
	 *    and sessionStartMs. session_meta is always the first record in practice, but
	 *    we do a dedicated pass to guarantee the final sessionId is known before any
	 *    message IDs are generated.
	 * 2. Pass 2 — extract messages using the resolved sessionId for stable IDs.
	 *
	 * Returns null if the file is unreadable or contains no extractable messages.
	 */
	private parseSessionFile(filePath: string): ParsedSession | null {
		// Stat the file once up-front so we have mtimeMs available throughout
		// (timestamp fallback and ParsedSession field) without a second syscall
		// that could fail independently if the file is removed mid-parse.
		let mtimeMs: number;
		try {
			mtimeMs = statSync(filePath).mtimeMs;
		} catch {
			return null;
		}

		let content: string;
		try {
			content = readFileSync(filePath, "utf8");
		} catch {
			return null;
		}

		const lines = content.split("\n").filter((l) => l.trim());
		if (lines.length === 0) return null;

		// Extract session ID from filename as initial fallback.
		// Filename format: rollout-<ISO-ts>-<uuid>.jsonl
		// The UUID is the last UUID-shaped segment before .jsonl.
		const fileBasename = basename(filePath);
		const uuidMatch = fileBasename.match(
			/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i,
		);
		let sessionId = uuidMatch
			? uuidMatch[1]
			: fileBasename.replace(/\.jsonl$/, "");

		let cwd = "";
		let sessionStartMs = 0;

		// ── Pass 1: resolve sessionId, cwd, sessionStartMs ─────────────────────
		for (const line of lines) {
			let record: CodexRecord;
			try {
				record = JSON.parse(line) as CodexRecord;
			} catch {
				continue;
			}
			if (record.type !== "session_meta") continue;
			const meta = record.payload as CodexSessionMeta;
			if (meta.id) sessionId = meta.id;
			if (meta.cwd) cwd = meta.cwd;
			if (meta.timestamp) {
				const t = Date.parse(meta.timestamp);
				if (!Number.isNaN(t)) sessionStartMs = t;
			}
			break; // session_meta appears once at the top — stop scanning
		}

		// ── Pass 2: extract messages with the final sessionId ──────────────────
		const messages: EpisodeMessage[] = [];
		let minTimestampMs = Number.MAX_SAFE_INTEGER;
		let maxTimestampMs = 0;

		for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
			let record: CodexRecord;
			try {
				record = JSON.parse(lines[lineIndex]) as CodexRecord;
			} catch {
				continue;
			}

			if (record.type !== "response_item") continue;

			const item = record.payload as CodexResponseItem;
			if (item.type !== "message") continue;
			if (item.role !== "user" && item.role !== "assistant") continue;
			if (!Array.isArray(item.content) || item.content.length === 0) continue;

			// Extract text from content parts.
			// User turns use "input_text", assistant turns use "output_text".
			// We accept both to be robust against format variations.
			const text = item.content
				.filter(
					(p) =>
						(p.type === "input_text" || p.type === "output_text") && p.text,
				)
				.map((p) => p.text ?? "")
				.join("\n")
				.trim();

			if (!text) continue;

			// Skip injected context blocks (AGENTS.md / environment_context injections
			// that Codex prepends to the first user turn as XML-wrapped system info).
			// These are large, static, and contain no user-generated knowledge.
			if (
				item.role === "user" &&
				(text.startsWith("<user_instructions>") ||
					text.startsWith("<environment_context>"))
			) {
				continue;
			}

			// Drop messages carrying our injected context, checked before truncation.
			if (containsInjectedKnowledge(text)) continue;

			let truncated = text;
			if (truncated.length > MAX_MESSAGE_CHARS) {
				truncated = `${truncated.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`;
			}

			const recordTs = Date.parse(record.timestamp);
			const ts = Number.isNaN(recordTs) ? 0 : recordTs;

			// Synthetic message ID: "<sessionId>:<lineIndex>" — stable because
			// Codex rollout files are append-only (lines never shift).
			const messageId = `${sessionId}:${lineIndex}`;

			messages.push({
				messageId,
				role: item.role,
				content: truncated,
				timestamp: ts,
			});

			if (ts > 0) {
				if (ts < minTimestampMs) minTimestampMs = ts;
				if (ts > maxTimestampMs) maxTimestampMs = ts;
			}
		}

		if (messages.length === 0) return null;

		// Fall back to sessionStartMs or file mtime when no valid message timestamps
		// exist (e.g. all records had malformed timestamp fields). This prevents
		// silently discarding sessions with otherwise valid content.
		if (maxTimestampMs === 0) {
			maxTimestampMs = sessionStartMs || mtimeMs;
			minTimestampMs = maxTimestampMs;
		}

		if (maxTimestampMs === 0) return null; // truly unresolvable

		const projectName = cwd ? basename(cwd) || "unknown" : "unknown";

		return {
			sessionId,
			sessionTitle: projectName,
			projectName,
			directory: cwd,
			filePath,
			mtimeMs,
			messages,
			minTimestampMs:
				minTimestampMs === Number.MAX_SAFE_INTEGER
					? sessionStartMs || 0
					: minTimestampMs,
			maxTimestampMs,
		};
	}

	/**
	 * Segment a parsed session into episodes, skipping already-processed ranges.
	 *
	 * Codex has no compaction mechanism — all messages are treated as a flat
	 * sequence and chunked by token budget.
	 */
	private segmentSession(
		session: ParsedSession,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const { messages } = session;

		if (messages.length < config.consolidation.minSessionMessages) return [];

		const chunks = chunkByTokenBudget(messages, MAX_TOKENS_PER_EPISODE);
		const episodes: Episode[] = [];

		const processedSet = new Set(
			processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`),
		);

		for (const chunk of chunks) {
			const startMessageId = chunk[0].messageId;
			const endMessageId = chunk[chunk.length - 1].messageId;

			if (processedSet.has(`${startMessageId}::${endMessageId}`)) continue;

			const content = formatMessages(chunk);
			if (!content.trim()) continue;

			episodes.push({
				source: this.source,
				sessionId: session.sessionId,
				startMessageId,
				endMessageId,
				sessionTitle: session.sessionTitle,
				projectName: session.projectName,
				directory: session.directory,
				timeCreated: session.minTimestampMs,
				maxMessageTime: chunk[chunk.length - 1].timestamp,
				content,
				contentType: "messages",
				approxTokens: approxTokens(content),
			});
		}

		return episodes;
	}
}
