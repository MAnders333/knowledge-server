import { existsSync, readFileSync, readdirSync } from "node:fs";
import { homedir, platform } from "node:os";
import { join } from "node:path";
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

// ── VSCode / GitHub Copilot Chat storage schema ────────────────────────────────
//
// VSCode (with GitHub Copilot Chat) stores conversations as individual JSON files
// within per-workspace directories:
//
//   <vscodeDataDir>/User/workspaceStorage/<workspace-hash>/chatSessions/<session-uuid>.json
//
// Platform-specific default locations:
//   macOS:  ~/Library/Application Support/Code/User/workspaceStorage/
//   Linux:  ~/.config/Code/User/workspaceStorage/
//           $XDG_CONFIG_HOME/Code/User/workspaceStorage/ (if set)
//
// Each session JSON file (version 3) has the following structure:
//
//   sessionId:        string (UUID)
//   creationDate:     number (unix ms)
//   lastMessageDate:  number (unix ms)
//   customTitle:      string | undefined (user-visible session title)
//   requests:         Array<{
//     requestId:      string
//     timestamp:      number (unix ms)
//     message: {
//       text:         string (the user's message, with #file: references inline)
//     }
//     response:       Array<ResponsePart> — mixed types; text is kind=undefined with value:string
//     modelId:        string (e.g. "copilot/claude-sonnet-4.5")
//   }>
//
// Response parts with no `kind` property (or kind omitted) carry the assistant's text
// in `value`. Other kinds (toolInvocationSerialized, codeblockUri, textEditGroup,
// inlineReference, etc.) are metadata and can be skipped for knowledge extraction.
//
// Each workspace directory also contains a workspace.json with { folder: "file:///..." }
// which maps the workspace hash to a real filesystem path.
//
// Incremental processing:
//   lastMessageDate from the session JSON is used as maxMessageTime.
//   The source cursor advances to max(lastMessageDate) of processed sessions.

// ── Types ─────────────────────────────────────────────────────────────────────

/** A single response part from a VSCode chat session. */
interface VSCodeResponsePart {
	kind?: string;
	value?: string;
}

/** A single request (user message + assistant response) in a VSCode chat session. */
interface VSCodeRequest {
	requestId: string;
	message?: {
		// VSCode also stores a `parts` array for richer messages, but file references
		// (#file:, images, etc.) are inlined into `text` as plain strings — so reading
		// `text` is sufficient for knowledge extraction and `parts` is intentionally omitted.
		text?: string;
	};
	response?: VSCodeResponsePart[];
	timestamp?: number;
	modelId?: string;
}

/** Top-level structure of a VSCode chat session JSON file. */
interface VSCodeSessionFile {
	version?: number;
	sessionId?: string;
	creationDate?: number;
	lastMessageDate?: number;
	customTitle?: string;
	requests?: VSCodeRequest[];
	isEmpty?: boolean;
}

/** Parsed session ready for episode segmentation. */
interface ParsedSession {
	sessionId: string;
	title: string;
	creationDate: number;
	lastMessageDate: number;
	/** Filesystem path to the session JSON file. */
	filePath: string;
	/** The workspace directory this session belongs to (from workspace.json). */
	workspaceDir: string;
	requests: VSCodeRequest[];
}

// ── VSCodeEpisodeReader ───────────────────────────────────────────────────────

/**
 * Reads episodes from VSCode / GitHub Copilot Chat session files.
 *
 * VSCode stores each chat session as an individual JSON file in
 * per-workspace directories under the VSCode data directory. This reader
 * scans all workspace directories for chatSessions/ subdirectories and
 * reads the session JSON files.
 *
 * The reader supports incremental processing — only sessions with
 * lastMessageDate newer than the source cursor are returned as candidates.
 */
export class VSCodeEpisodeReader implements IEpisodeReader {
	readonly source = "vscode";

	private readonly dataDir: string;

	/** Session cache populated by getCandidateSessions, consumed by getNewEpisodes. */
	private _sessionCache = new Map<string, ParsedSession>();

	constructor(dataDir: string) {
		this.dataDir = dataDir;
	}

	// ── IEpisodeReader implementation ─────────────────────────────────────────

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit: number = config.consolidation.maxSessionsPerRun,
	): Array<{ id: string; maxMessageTime: number }> {
		const sessions = this.loadSessions(afterMessageTimeCreated);

		const selected = sessions
			.sort((a, b) => a.lastMessageDate - b.lastMessageDate)
			.slice(0, limit);

		// Rebuild cache to bound memory to `limit` entries.
		this._sessionCache = new Map(selected.map((s) => [s.sessionId, s]));

		return selected.map((s) => ({
			id: s.sessionId,
			maxMessageTime: s.lastMessageDate,
		}));
	}

	countNewSessions(afterMessageTimeCreated: number): number {
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
			// Re-scan to find the missing sessions — expensive but rare
			const allSessions = this.loadSessions(0);
			for (const session of allSessions) {
				if (missingIds.includes(session.sessionId)) {
					this._sessionCache.set(session.sessionId, session);
				}
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
		// No persistent resources to release (file-based, not DB).
		this._sessionCache.clear();
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	/**
	 * Scan all workspace directories for chat session files newer than the cursor.
	 */
	private loadSessions(afterMessageTimeCreated: number): ParsedSession[] {
		const workspaceStorageDir = join(this.dataDir, "User", "workspaceStorage");
		if (!existsSync(workspaceStorageDir)) return [];

		let workspaceDirs: string[];
		try {
			workspaceDirs = readdirSync(workspaceStorageDir);
		} catch {
			return [];
		}

		const sessions: ParsedSession[] = [];

		for (const wsHash of workspaceDirs) {
			const wsDir = join(workspaceStorageDir, wsHash);
			const chatDir = join(wsDir, "chatSessions");
			if (!existsSync(chatDir)) continue;

			// Read workspace.json to get the project directory
			let workspaceDir = "";
			try {
				const wsJsonPath = join(wsDir, "workspace.json");
				if (existsSync(wsJsonPath)) {
					const wsJson = JSON.parse(readFileSync(wsJsonPath, "utf8")) as {
						folder?: string;
					};
					if (wsJson.folder) {
						workspaceDir = resolveWorkspaceFolder(wsJson.folder);
					}
				}
			} catch {
				// Non-fatal — we can still read sessions without workspace info
			}

			// Read all session JSON files in this workspace
			let sessionFiles: string[];
			try {
				sessionFiles = readdirSync(chatDir).filter((f) => f.endsWith(".json"));
			} catch {
				continue;
			}

			for (const file of sessionFiles) {
				const filePath = join(chatDir, file);
				const parsed = this.parseSessionFile(filePath, workspaceDir);
				if (parsed && parsed.lastMessageDate > afterMessageTimeCreated) {
					sessions.push(parsed);
				}
			}
		}

		return sessions;
	}

	/**
	 * Parse a single VSCode chat session JSON file.
	 * Returns null for empty, unreadable, or stub sessions.
	 */
	private parseSessionFile(
		filePath: string,
		workspaceDir: string,
	): ParsedSession | null {
		let raw: string;
		try {
			raw = readFileSync(filePath, "utf8");
		} catch {
			return null;
		}

		let data: VSCodeSessionFile;
		try {
			const parsed: unknown = JSON.parse(raw);
			if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
				return null;
			data = parsed as VSCodeSessionFile;
		} catch {
			return null;
		}

		// Skip empty sessions
		if (data.isEmpty) return null;

		const sessionId = data.sessionId;
		if (!sessionId) return null;

		const creationDate = data.creationDate ?? 0;
		const lastMessageDate = data.lastMessageDate ?? creationDate;
		if (lastMessageDate === 0) return null;

		const requests = data.requests ?? [];
		if (requests.length === 0) return null;

		// Check if any request has actual text content
		const hasContent = requests.some(
			(r) => (r.message?.text ?? "").trim().length > 0,
		);
		if (!hasContent) return null;

		const title = data.customTitle || `VSCode Chat ${sessionId.slice(0, 8)}`;

		return {
			sessionId,
			title,
			creationDate,
			lastMessageDate,
			filePath,
			workspaceDir,
			requests,
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

			// Derive project name from workspace directory
			const projectName = session.workspaceDir
				? (session.workspaceDir.split("/").pop() ?? "")
				: "";

			episodes.push({
				source: this.source,
				sessionId: session.sessionId,
				startMessageId: startId,
				endMessageId: endId,
				sessionTitle: session.title || "Untitled",
				projectName,
				directory: session.workspaceDir,
				timeCreated: session.creationDate,
				maxMessageTime: chunk[chunk.length - 1].timestamp,
				content,
				contentType: "messages",
				approxTokens: approxTokens(content),
			});
		}

		return episodes;
	}

	/**
	 * Extract EpisodeMessages from a VSCode chat session.
	 *
	 * Each request in the session produces up to two messages:
	 * 1. A user message from request.message.text
	 * 2. An assistant message from the concatenated response value parts
	 *
	 * Response parts with kind values like "toolInvocationSerialized",
	 * "codeblockUri", "textEditGroup", "inlineReference", etc. are metadata
	 * and are skipped — only plain text parts (no kind or kind omitted) are
	 * included in the assistant response.
	 */
	private extractMessages(session: ParsedSession): EpisodeMessage[] {
		const messages: EpisodeMessage[] = [];

		for (let i = 0; i < session.requests.length; i++) {
			const request = session.requests[i];
			const timestamp = request.timestamp ?? session.lastMessageDate;

			// User message
			const userText = (request.message?.text ?? "").trim();
			if (userText) {
				const messageId =
					request.requestId ?? `${session.sessionId}-req-${i}-user`;
				messages.push({
					messageId,
					role: "user",
					content:
						userText.length > MAX_MESSAGE_CHARS
							? `${userText.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`
							: userText,
					timestamp,
				});
			}

			// Assistant response — concatenate all text parts
			const responseParts = request.response ?? [];
			const textParts: string[] = [];

			for (const part of responseParts) {
				// Only include parts that carry text content.
				// Parts with explicit kind values like "toolInvocationSerialized",
				// "prepareToolInvocation", "codeblockUri", "textEditGroup",
				// "inlineReference", "mcpServersStarting", "undoStop" are metadata.
				// Text parts either have no kind property or have kind omitted.
				//
				// This is an allowlist: only kind=undefined, "markdownContent", and
				// "markdownVuln" are treated as extractable text. If VSCode introduces
				// new text-bearing part kinds in future versions, add them here.
				if (
					part.value &&
					typeof part.value === "string" &&
					(!part.kind ||
						part.kind === "markdownContent" ||
						part.kind === "markdownVuln")
				) {
					const trimmed = part.value.trim();
					if (trimmed) textParts.push(trimmed);
				}
			}

			if (textParts.length > 0) {
				const assistantText = textParts.join("\n\n");
				const responseId = `${request.requestId ?? `${session.sessionId}-req-${i}`}-response`;
				messages.push({
					messageId: responseId,
					role: "assistant",
					content:
						assistantText.length > MAX_MESSAGE_CHARS
							? `${assistantText.slice(0, MAX_MESSAGE_CHARS)}\n[...truncated]`
							: assistantText,
					timestamp,
				});
			}
		}

		return messages;
	}
}

// ── Workspace folder resolution ───────────────────────────────────────────────

/**
 * Resolve a workspace folder URI from workspace.json into a usable filesystem path.
 *
 * VSCode stores workspace folders as URIs in three forms:
 *
 * 1. Local:         file:///Users/x/Documents/project
 *    → /Users/x/Documents/project
 *
 *    Note: on Windows this would yield /C:/Users/... after stripping the scheme,
 *    but Windows is not a supported platform for knowledge-server.
 *
 * 2. SSH Remote:    vscode-remote://ssh-remote+<host>/<remote-path>
 *    (stored percent-encoded as vscode-remote://ssh-remote%2B<host>/<remote-path>)
 *    → /<remote-path>  (the remote filesystem path, e.g. /home/user/project)
 *
 * 3. Dev Container: vscode-remote://dev-container+<hex-encoded-json>/<container-path>
 *    The authority contains hex-encoded JSON with a `hostPath` field pointing to
 *    the local project directory that was mounted into the container. We decode
 *    and extract that host path, which is the real project directory on the local
 *    machine.
 *    → /Users/x/Documents/project  (the host path from the JSON payload)
 *    If decoding fails, returns "" rather than falling back to the container-internal
 *    path (e.g. /opt/app) which would be useless for domain routing on the host.
 *
 * Returns "" if the URI cannot be parsed, uses an unknown scheme, or if a
 * Dev Container URI cannot be decoded. An empty string propagates cleanly
 * through the domain router (no match) rather than silently routing incorrectly.
 */
export function resolveWorkspaceFolder(folderUri: string): string {
	// Case 1: file:// URI — strip scheme, decode percent-encoding
	if (folderUri.startsWith("file://")) {
		return decodeURIComponent(folderUri.replace(/^file:\/\//, ""));
	}

	// Case 2 & 3: vscode-remote:// URI
	if (folderUri.startsWith("vscode-remote://")) {
		let url: URL;
		try {
			url = new URL(folderUri);
		} catch {
			return "";
		}

		const authority = decodeURIComponent(url.hostname);

		// Case 3: Dev Container — authority is "dev-container+<hex-encoded-json>"
		// The JSON payload contains a `hostPath` field with the local project directory.
		// On decode failure we return "" rather than falling through to pathname —
		// the container-internal path (e.g. /opt/app) is not meaningful on the host.
		if (authority.startsWith("dev-container+")) {
			try {
				const hexPayload = authority.slice("dev-container+".length);
				const jsonStr = Buffer.from(hexPayload, "hex").toString("utf-8");
				const containerConfig: unknown = JSON.parse(jsonStr);
				if (
					containerConfig &&
					typeof containerConfig === "object" &&
					"hostPath" in containerConfig &&
					typeof (containerConfig as Record<string, unknown>).hostPath ===
						"string"
				) {
					return (containerConfig as Record<string, string>).hostPath;
				}
			} catch {
				// Hex decode or JSON parse failed — return "" (container-internal
				// path would be useless for domain routing on the host machine)
			}
			return "";
		}

		// Case 2: SSH Remote — use the pathname which is the remote filesystem path.
		const remotePath = decodeURIComponent(url.pathname);
		return remotePath || "";
	}

	// Unknown scheme — return "" rather than the raw URI, which would fail
	// silently in domain routing just like an empty string but is harder to
	// reason about.
	return "";
}

// ── Path resolution ───────────────────────────────────────────────────────────

/**
 * Probe list of candidate paths for VSCode's data directory.
 *
 * Platform layout:
 *   macOS:   ~/Library/Application Support/Code
 *   Linux:   $XDG_CONFIG_HOME/Code
 *            ~/.config/Code (XDG default)
 *
 * Windows is not yet supported (knowledge-server is a Unix-first tool).
 *
 * Override all of this with VSCODE_DATA_DIR env var.
 */
function getVSCodeDataDirProbePaths(): string[] {
	const home = homedir();
	const os = platform();

	if (os === "darwin") {
		return [join(home, "Library", "Application Support", "Code")];
	}

	if (os === "linux") {
		const paths: string[] = [];
		if (process.env.XDG_CONFIG_HOME) {
			paths.push(join(process.env.XDG_CONFIG_HOME, "Code"));
		}
		paths.push(join(home, ".config", "Code"));
		return paths;
	}

	return [];
}

/**
 * Resolve the VSCode data directory.
 *
 * Resolution order:
 * 1. VSCODE_DATA_DIR env var — explicit override; used as-is.
 * 2. Platform-specific probe list (macOS → ~/Library/Application Support/Code;
 *    Linux → $XDG_CONFIG_HOME/Code then ~/.config/Code).
 *
 * The resolved directory must contain User/workspaceStorage/ to be considered valid.
 *
 * Returns null if no valid path can be found (VSCode not installed / wrong platform).
 */
export function resolveVSCodeDataDir(): string | null {
	// Explicit override
	if (process.env.VSCODE_DATA_DIR) {
		const p = process.env.VSCODE_DATA_DIR;
		const wsDir = join(p, "User", "workspaceStorage");
		return existsSync(wsDir) ? p : null;
	}

	// Auto-detect via platform probe list
	for (const candidate of getVSCodeDataDirProbePaths()) {
		const wsDir = join(candidate, "User", "workspaceStorage");
		if (existsSync(wsDir)) return candidate;
	}

	return null;
}
