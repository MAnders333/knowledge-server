import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { homedir, platform } from "node:os";
import { basename, dirname, join } from "node:path";
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

// ── Cursor storage schema ──────────────────────────────────────────────────────
//
// Cursor stores all session data in a single SQLite database.
//
// Platform-specific default locations (probed in order):
//   macOS:  ~/Library/Application Support/Cursor/User/globalStorage/state.vscdb
//   Linux:  ~/.config/Cursor/User/globalStorage/state.vscdb
//           $XDG_CONFIG_HOME/Cursor/User/globalStorage/state.vscdb (if set)
//   Windows: %APPDATA%\Cursor\User\globalStorage\state.vscdb  (not yet supported)
//
// The `cursorDiskKV` table holds key-value pairs. Relevant key patterns:
//
//   composerData:<uuid>
//     Session metadata blob. Contains name, createdAt, lastUpdatedAt, isAgentic,
//     plus the conversation data in one of two formats:
//
//     Format A (older sessions): conversation[] array of turn objects — each turn
//       has type (1=user, 2=assistant), bubbleId, text, etc.
//
//     Format B (newer sessions): fullConversationHeadersOnly[] — an ordered list
//       of {bubbleId, type} stubs. The actual turn content is stored separately
//       under bubbleId:<composerId>:<bubbleId> keys. conversationMap is present
//       but empty in this format.
//
//   bubbleId:<composerId>:<bubbleId>
//     Individual turn content for Format B sessions. Contains type, bubbleId,
//     text (plain string), toolResults, capabilitiesRan, etc.
//
// Turn type: 1 = user, 2 = assistant
//
// Timestamps:
//   composerData.createdAt and lastUpdatedAt are unix ms.
//   Individual turns have no timestamps. We synthesise monotonically increasing
//   approximate timestamps by interpolating between createdAt and lastUpdatedAt.
//
// Incremental processing:
//   lastUpdatedAt from composerData is used as the session's maxMessageTime.
//   The source cursor advances to max(lastUpdatedAt) of processed sessions.

// ── Types ─────────────────────────────────────────────────────────────────────

/** A turn as it appears in the inline conversation[] array (Format A). */
interface CursorTurn {
	type: 1 | 2;
	bubbleId?: string;
	text?: string;
	isAgentic?: boolean;
}

/** A header stub in fullConversationHeadersOnly[] (Format B). */
interface CursorConversationHeader {
	bubbleId: string;
	type: 1 | 2;
	serverBubbleId?: string;
}

/** A full bubble value stored in bubbleId:<composerId>:<bubbleId> (Format B). */
interface CursorBubble {
	type?: 1 | 2;
	bubbleId?: string;
	text?: string;
	isAgentic?: boolean;
	toolResults?: unknown[];
	capabilitiesRan?: Record<string, unknown>;
}

/** Shape of a composerData JSON value. */
interface CursorComposerData {
	composerId?: string;
	name?: string;
	createdAt?: number;
	lastUpdatedAt?: number;
	isAgentic?: boolean;
	tokenCount?: number;
	// Format A: full inline turns
	conversation?: CursorTurn[];
	// Format B: ordered stubs; full content in separate bubbleId: KV entries
	fullConversationHeadersOnly?: CursorConversationHeader[];
	latestConversationSummary?: { title?: string; tldr?: string };
	unifiedMode?: string;
	forceMode?: string;
}

interface ParsedSession {
	sessionId: string;
	title: string;
	createdAt: number;
	lastUpdatedAt: number;
	/** Format A only — turns with inline text. Empty for Format B. */
	inlineTurns: CursorTurn[];
	/** Format B only — ordered bubble stubs. Empty for Format A. */
	headers: CursorConversationHeader[];
	isAgentic: boolean;
	/** Resolved workspace root directory (e.g. "/Users/x/Documents/projectX"). Empty if unresolvable. */
	directory: string;
	/** Human-readable project name — last segment of directory (e.g. "projectX"). Empty if unresolvable. */
	projectName: string;
}

// ── CursorEpisodeReader ───────────────────────────────────────────────────────

/**
 * Reads episodes from Cursor's SQLite state database.
 *
 * Two storage formats are transparently supported:
 *
 *   Format A (older sessions): conversation[] is present in composerData.
 *     Turn text is read directly from the inline array.
 *
 *   Format B (newer sessions): fullConversationHeadersOnly[] provides the
 *     ordered bubble ID list; per-turn content is loaded from separate
 *     bubbleId:<composerId>:<bubbleId> KV entries.
 *
 * In both formats, turns with empty text are skipped (assistant turns often
 * carry no `text` and rely on richText which we intentionally ignore).
 *
 * Timestamps are synthesised by interpolating between session.createdAt and
 * session.lastUpdatedAt, since Cursor does not store per-turn timestamps.
 */
export class CursorEpisodeReader implements IEpisodeReader {
	readonly source = "cursor";

	private readonly dbPath: string;
	private _db: Database | null = null;

	/** Session cache populated by getCandidateSessions, consumed by getNewEpisodes. */
	private _sessionCache = new Map<string, ParsedSession>();

	constructor(dbPath: string) {
		this.dbPath = dbPath;
	}

	// ── IEpisodeReader implementation ─────────────────────────────────────────

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit: number = config.consolidation.maxSessionsPerRun,
	): Array<{ id: string; maxMessageTime: number }> {
		const sessions = this.loadSessions(afterMessageTimeCreated);

		// Rebuild the cache from exactly the sessions being returned this cycle.
		// This bounds cache size to `limit` entries — the consolidation engine
		// always calls getCandidateSessions immediately before getNewEpisodes in
		// the same cycle, so stale entries from prior cycles are never needed.
		const selected = sessions
			.sort((a, b) => a.lastUpdatedAt - b.lastUpdatedAt)
			.slice(0, limit);
		this._sessionCache = new Map(selected.map((s) => [s.sessionId, s]));

		return selected.map((s) => ({
			id: s.sessionId,
			maxMessageTime: s.lastUpdatedAt,
		}));
	}

	countNewSessions(afterMessageTimeCreated: number): number {
		// loadSessions already filters to lastUpdatedAt > afterMessageTimeCreated,
		// so .length is the count without a redundant re-filter.
		return this.loadSessions(afterMessageTimeCreated).length;
	}

	getNewEpisodes(
		candidateSessionIds: string[],
		processedRanges: Map<string, ProcessedRange[]>,
	): Episode[] {
		if (candidateSessionIds.length === 0) return [];

		// Populate cache for any IDs not already present (tests / unusual call order)
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
			episodes.push(...this.segmentSession(session, sessionProcessed));
		}
		return episodes;
	}

	close(): void {
		if (this._db) {
			try {
				this._db.close();
			} catch {
				// ignore
			}
			this._db = null;
		}
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	private getDb(): Database {
		if (!this._db) {
			this._db = new Database(this.dbPath, { readonly: true });
		}
		return this._db;
	}

	/**
	 * Load all composerData sessions whose lastUpdatedAt > afterMessageTimeCreated.
	 */
	private loadSessions(afterMessageTimeCreated: number): ParsedSession[] {
		let db: Database;
		try {
			db = this.getDb();
		} catch {
			return [];
		}

		let rows: Array<{ key: string; value: string }>;
		try {
			// All composerData entries — we filter in JS since lastUpdatedAt is inside JSON
			rows = db
				.query<{ key: string; value: string }, []>(
					"SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'",
				)
				.all();
		} catch {
			return [];
		}

		const sessions: ParsedSession[] = [];
		for (const row of rows) {
			const parsed = this.parseComposerData(row.key, row.value);
			if (parsed && parsed.lastUpdatedAt > afterMessageTimeCreated) {
				sessions.push(parsed);
			}
		}
		return sessions;
	}

	/** Load specific sessions by composerId (fallback path for getNewEpisodes). */
	private loadSessionsByIds(sessionIds: string[]): Map<string, ParsedSession> {
		const result = new Map<string, ParsedSession>();
		let db: Database;
		try {
			db = this.getDb();
		} catch {
			return result;
		}

		for (const id of sessionIds) {
			let row: { value: string } | null = null;
			try {
				row = db
					.query<{ value: string }, [string]>(
						"SELECT value FROM cursorDiskKV WHERE key = ?",
					)
					.get(`composerData:${id}`);
			} catch {
				continue;
			}
			if (!row) continue;
			const parsed = this.parseComposerData(`composerData:${id}`, row.value);
			if (parsed) result.set(id, parsed);
		}
		return result;
	}

	/**
	 * Parse a raw composerData JSON blob.
	 * Detects Format A (inline conversation[]) vs Format B (fullConversationHeadersOnly).
	 * Returns null for empty, unreadable, or stub sessions.
	 */
	private parseComposerData(key: string, raw: string): ParsedSession | null {
		const sessionId = key.slice("composerData:".length);
		let data: CursorComposerData;
		try {
			const parsed: unknown = JSON.parse(raw);
			if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
				return null;
			data = parsed as CursorComposerData;
		} catch {
			return null;
		}

		const createdAt = data.createdAt ?? 0;
		const lastUpdatedAt = data.lastUpdatedAt ?? createdAt;
		if (lastUpdatedAt === 0) return null;

		const title =
			data.name || data.latestConversationSummary?.title || sessionId;

		// Format A: inline conversation array
		const inlineTurns = data.conversation ?? [];

		// Format B: ordered bubble header stubs
		const headers = data.fullConversationHeadersOnly ?? [];

		// Skip sessions with no conversation data at all
		if (inlineTurns.length === 0 && headers.length === 0) return null;

		// Resolve workspace root from file:// URIs scattered across DB entries
		const directory = this.resolveWorkspaceRoot(sessionId, raw);
		const projectName = directory ? basename(directory) : "";

		return {
			sessionId,
			title,
			createdAt,
			lastUpdatedAt,
			inlineTurns,
			headers,
			isAgentic: data.isAgentic ?? false,
			directory,
			projectName,
		};
	}

	/**
	 * Segment a session into episodes, skipping already-processed ranges.
	 */
	private segmentSession(
		session: ParsedSession,
		processedRanges: ProcessedRange[],
	): Episode[] {
		const messages = this.extractMessages(session);
		if (messages.length < config.consolidation.minSessionMessages) return [];

		const chunks = chunkByTokenBudget(messages, MAX_TOKENS_PER_EPISODE);
		const processedSet = new Set(
			processedRanges.map((r) => `${r.startMessageId}::${r.endMessageId}`),
		);
		const episodes: Episode[] = [];

		for (const chunk of chunks) {
			const startId = chunk[0].messageId;
			const endId = chunk[chunk.length - 1].messageId;
			if (processedSet.has(`${startId}::${endId}`)) continue;

			const content = formatMessages(chunk);
			if (!content.trim()) continue;

			episodes.push({
				source: this.source,
				sessionId: session.sessionId,
				startMessageId: startId,
				endMessageId: endId,
				sessionTitle: session.title || "Untitled",
				projectName: session.projectName,
				directory: session.directory,
				timeCreated: session.createdAt,
				maxMessageTime: chunk[chunk.length - 1].timestamp,
				content,
				contentType: "messages",
				approxTokens: approxTokens(content),
			});
		}

		return episodes;
	}

	/**
	 * Extract EpisodeMessages from a session, dispatching to the appropriate
	 * format handler (Format A: inline turns, Format B: separate bubble KV entries).
	 *
	 * Timestamps are synthesised by linear interpolation across the turn list
	 * since Cursor does not store per-turn timestamps.
	 */
	private extractMessages(session: ParsedSession): EpisodeMessage[] {
		if (session.inlineTurns.length > 0) {
			return this.extractFromInlineTurns(session);
		}
		if (session.headers.length > 0) {
			return this.extractFromBubbleHeaders(session);
		}
		return [];
	}

	/**
	 * Format A: text is embedded in the conversation[] array.
	 */
	private extractFromInlineTurns(session: ParsedSession): EpisodeMessage[] {
		const { inlineTurns: turns, createdAt, lastUpdatedAt, sessionId } = session;
		const messages: EpisodeMessage[] = [];
		const n = turns.length;
		const timeSpan = lastUpdatedAt - createdAt;

		for (let i = 0; i < n; i++) {
			const turn = turns[i];
			if (turn.type !== 1 && turn.type !== 2) continue;
			const text = (turn.text ?? "").trim();
			if (!text) continue;

			const messageId = turn.bubbleId ?? `${sessionId}-turn-${i}`;
			const timestamp =
				n <= 1
					? lastUpdatedAt
					: Math.round(createdAt + (timeSpan * i) / (n - 1));

			messages.push({
				messageId,
				role: turn.type === 1 ? "user" : "assistant",
				content:
					text.length > MAX_MESSAGE_CHARS
						? `${text.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`
						: text,
				timestamp,
			});
		}

		return messages;
	}

	/**
	 * Format B: bubble stubs in fullConversationHeadersOnly[], full content in
	 * separate bubbleId:<composerId>:<bubbleId> KV entries.
	 *
	 * We batch-fetch all bubble keys for the session in a single SQL IN query
	 * to avoid N round-trips to the DB.
	 */
	private extractFromBubbleHeaders(session: ParsedSession): EpisodeMessage[] {
		const { headers, sessionId, createdAt, lastUpdatedAt } = session;
		if (headers.length === 0) return [];

		let db: Database;
		try {
			db = this.getDb();
		} catch {
			return [];
		}

		// Build the full set of DB keys we need.
		const bubbleKeys = headers.map(
			(h) => `bubbleId:${sessionId}:${h.bubbleId}`,
		);

		// Batch fetch in chunks of SQLITE_BATCH_SIZE to stay below SQLite's
		// SQLITE_LIMIT_VARIABLE_NUMBER (default 999). Very long agentic sessions
		// can have hundreds of bubbles; without batching, sessions with ≥1000
		// turns would silently produce no messages (the query throws and we return []).
		const SQLITE_BATCH_SIZE = 500;
		const allRows: Array<{ key: string; value: string }> = [];
		try {
			for (let i = 0; i < bubbleKeys.length; i += SQLITE_BATCH_SIZE) {
				const batch = bubbleKeys.slice(i, i + SQLITE_BATCH_SIZE);
				const placeholders = batch.map(() => "?").join(",");
				const batchRows = db
					.query<{ key: string; value: string }, string[]>(
						`SELECT key, value FROM cursorDiskKV WHERE key IN (${placeholders})`,
					)
					.all(...batch);
				allRows.push(...batchRows);
			}
		} catch {
			return [];
		}

		// Build a map of bubbleId → bubble data.
		// Key format: bubbleId:<composerId>:<bubbleId>
		// Use slice(2).join(":") rather than parts[2] so that bubble IDs which
		// themselves contain colons (unlikely but possible) are reconstructed correctly.
		const PREFIX = `bubbleId:${sessionId}:`;
		const bubbleMap = new Map<string, CursorBubble>();
		for (const row of allRows) {
			const bId = row.key.startsWith(PREFIX)
				? row.key.slice(PREFIX.length)
				: null;
			if (!bId) continue;
			try {
				const parsed: unknown = JSON.parse(row.value);
				if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
					bubbleMap.set(bId, parsed as CursorBubble);
				}
			} catch {
				// skip malformed entries
			}
		}

		// Reconstruct messages in header order
		const messages: EpisodeMessage[] = [];
		const n = headers.length;
		const timeSpan = lastUpdatedAt - createdAt;

		for (let i = 0; i < n; i++) {
			const header = headers[i];
			const bubble = bubbleMap.get(header.bubbleId);
			// Use the type from the header (always present) as ground truth
			const type = header.type;
			if (type !== 1 && type !== 2) continue;

			// Text may live on the bubble or be absent (e.g. tool-only turns)
			const text = (bubble?.text ?? "").trim();
			if (!text) continue;

			const timestamp =
				n <= 1
					? lastUpdatedAt
					: Math.round(createdAt + (timeSpan * i) / (n - 1));

			messages.push({
				messageId: header.bubbleId,
				role: type === 1 ? "user" : "assistant",
				content:
					text.length > MAX_MESSAGE_CHARS
						? `${text.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`
						: text,
				timestamp,
			});
		}

		return messages;
	}

	// ── Workspace root resolution ────────────────────────────────────────────

	/**
	 * Resolve the workspace root directory for a Cursor session by collecting
	 * absolute file:// URIs from all available sources in the DB, then computing
	 * the longest common directory prefix.
	 *
	 * Sources probed (in order, stops early once paths are found):
	 * 1. composerData itself — context.fileSelections, allAttachedFileCodeChunksUris
	 * 2. checkpointId:<id>:* — edit diffs with external file:// URIs
	 * 3. bubbleId:<id>:* — codebase context chunks, relevant files
	 * 4. messageRequestContext:<id>:* — currentFileLocationData, git status
	 *
	 * Returns "" if no file paths can be found (pure chat session).
	 */
	private resolveWorkspaceRoot(
		sessionId: string,
		composerDataRaw: string,
	): string {
		let db: Database;
		try {
			db = this.getDb();
		} catch {
			return "";
		}

		const filePaths: string[] = [];

		// Source 1: composerData itself (context.fileSelections, attached files, etc.)
		collectFilePathsFromString(composerDataRaw, filePaths);

		// Source 2: checkpointId entries (edit diffs with absolute paths)
		if (filePaths.length === 0) {
			try {
				const rows = db
					.query<{ value: string }, [string]>(
						"SELECT value FROM cursorDiskKV WHERE key LIKE ? LIMIT 10",
					)
					.all(`checkpointId:${sessionId}:%`);
				for (const row of rows) {
					collectFilePathsFromString(row.value, filePaths);
					if (filePaths.length >= 2) break; // enough for a meaningful prefix
				}
			} catch {
				// ignore — try next source
			}
		}

		// Source 3: bubbleId entries (only those containing file:// URIs)
		if (filePaths.length === 0) {
			try {
				const rows = db
					.query<{ value: string }, [string]>(
						"SELECT value FROM cursorDiskKV WHERE key LIKE ? AND value LIKE '%file:///%' LIMIT 10",
					)
					.all(`bubbleId:${sessionId}:%`);
				for (const row of rows) {
					collectFilePathsFromString(row.value, filePaths);
					if (filePaths.length >= 2) break;
				}
			} catch {
				// ignore — try next source
			}
		}

		// Source 4: messageRequestContext entries
		if (filePaths.length === 0) {
			try {
				const rows = db
					.query<{ value: string }, [string]>(
						"SELECT value FROM cursorDiskKV WHERE key LIKE ? LIMIT 5",
					)
					.all(`messageRequestContext:${sessionId}:%`);
				for (const row of rows) {
					collectFilePathsFromString(row.value, filePaths);
					if (filePaths.length >= 2) break;
				}
			} catch {
				// ignore
			}
		}

		if (filePaths.length === 0) return "";
		if (filePaths.length === 1) return dirname(filePaths[0]);

		return longestCommonDirectoryPrefix(filePaths);
	}
}

// ── Workspace root helpers ────────────────────────────────────────────────────

/**
 * Extract absolute file paths from file:// URIs found anywhere in a string.
 * Paths are decoded and pushed into the output array.
 */
function collectFilePathsFromString(str: string, out: string[]): void {
	// Use a fresh regex per call to avoid shared lastIndex state
	const re = /file:\/\/\/([\w/._@+%-]+)/g;
	for (const match of str.matchAll(re)) {
		// Reconstruct the absolute path (the URI strips the leading /)
		const decoded = decodeURIComponent(match[1]);
		out.push(`/${decoded}`);
	}
}

/**
 * Compute the longest common directory prefix across a list of absolute file paths.
 *
 * Example:
 *   ["/Users/x/Documents/projectX/src/a.ts", "/Users/x/Documents/projectX/lib/b.ts"]
 *   → "/Users/x/Documents/projectX"
 *
 * The result is always a directory (never a partial filename match):
 *   ["/Users/x/foo-bar/a.ts", "/Users/x/foo-baz/b.ts"]
 *   → "/Users/x"  (not "/Users/x/foo-")
 *
 * Returns "" if no meaningful common prefix exists (less than 2 path segments).
 */
function longestCommonDirectoryPrefix(paths: string[]): string {
	if (paths.length === 0) return "";
	if (paths.length === 1) return dirname(paths[0]);

	const split = paths.map((p) => p.split("/"));
	const minLen = Math.min(...split.map((s) => s.length));

	let commonDepth = 0;
	for (let i = 0; i < minLen; i++) {
		const segment = split[0][i];
		if (split.every((s) => s[i] === segment)) {
			commonDepth = i + 1;
		} else {
			break;
		}
	}

	// Join the common segments back into a path. Require at least 2 non-empty
	// segments (e.g. "/Users") to avoid returning just "/".
	const commonSegments = split[0].slice(0, commonDepth);
	const result = commonSegments.join("/");

	// If the result points to a file (last common segment has an extension and
	// matches a specific file path), return its parent directory instead.
	if (paths.some((p) => p === result)) {
		return dirname(result);
	}

	return result || "";
}

// ── Path resolution ───────────────────────────────────────────────────────────

/**
 * Probe list of candidate paths for Cursor's state.vscdb, ordered by likelihood.
 *
 * Platform layout:
 *   macOS:   ~/Library/Application Support/Cursor/User/globalStorage/state.vscdb
 *   Linux:   $XDG_CONFIG_HOME/Cursor/User/globalStorage/state.vscdb
 *            ~/.config/Cursor/User/globalStorage/state.vscdb  (XDG default)
 *
 * Windows is not yet supported (knowledge-server is a Unix-first tool).
 *
 * Override all of this with CURSOR_DB_PATH env var.
 */
function getCursorDbProbePaths(): string[] {
	const home = homedir();
	const os = platform();
	const suffix = join("Cursor", "User", "globalStorage", "state.vscdb");

	if (os === "darwin") {
		return [join(home, "Library", "Application Support", suffix)];
	}

	if (os === "linux") {
		const paths: string[] = [];
		// Honour XDG_CONFIG_HOME if set
		if (process.env.XDG_CONFIG_HOME) {
			paths.push(join(process.env.XDG_CONFIG_HOME, suffix));
		}
		paths.push(join(home, ".config", suffix));
		return paths;
	}

	// Unknown platform — return empty list; user must set CURSOR_DB_PATH
	return [];
}

/**
 * Resolve the Cursor state.vscdb path.
 *
 * Resolution order:
 * 1. CURSOR_DB_PATH env var — explicit override; used as-is (returns null if missing,
 *    so a typo surfaces as a warning rather than silently falling through to auto-detect).
 * 2. Platform-specific probe list (macOS → ~/Library/Application Support/…;
 *    Linux → $XDG_CONFIG_HOME/… then ~/.config/…).
 *
 * Returns null if no existing path can be found (Cursor not installed / wrong platform).
 * The caller (createEpisodeReaders) logs a warning in that case and skips the source.
 */
export function resolveCursorDbPath(): string | null {
	// Explicit override — trust it, don't fall through to probe list on miss
	if (process.env.CURSOR_DB_PATH) {
		const p = process.env.CURSOR_DB_PATH;
		return existsSync(p) ? p : null;
	}

	// Auto-detect via platform probe list
	for (const candidate of getCursorDbProbePaths()) {
		if (existsSync(candidate)) return candidate;
	}

	return null;
}
