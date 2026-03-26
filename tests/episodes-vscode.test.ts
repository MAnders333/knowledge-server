/**
 * Tests for VSCodeEpisodeReader.
 *
 * VSCode stores each chat session as an individual JSON file under
 * <dataDir>/User/workspaceStorage/<hash>/chatSessions/*.json
 * These tests create a minimal fake VSCode data directory in a temp dir,
 * seed it with session JSON files, and exercise the reader's segmentation
 * + extraction logic without any filesystem access beyond the temp dir.
 *
 * Covered:
 *   - getCandidateSessions / countNewSessions — lastMessageDate cursor filtering
 *   - getNewEpisodes — basic user + assistant message extraction
 *   - getNewEpisodes — response part kind filtering (metadata parts skipped)
 *   - getNewEpisodes — markdownContent and markdownVuln kinds included
 *   - getNewEpisodes — sessions with isEmpty=true skipped
 *   - getNewEpisodes — sessions with missing sessionId skipped
 *   - getNewEpisodes — sessions with lastMessageDate=0 skipped
 *   - getNewEpisodes — sessions with no text content skipped
 *   - getNewEpisodes — processedRanges exclusion
 *   - getNewEpisodes — minSessionMessages filter
 *   - getNewEpisodes — projectName derived from workspace.json folder URI
 *   - getNewEpisodes — ordering by lastMessageDate ASC
 *   - getNewEpisodes — limit parameter
 *   - Malformed JSON session files skipped gracefully
 *   - resolveVSCodeDataDir() — env var branch
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import {
	VSCodeEpisodeReader,
	resolveVSCodeDataDir,
	resolveWorkspaceFolder,
} from "../src/daemon/readers/vscode";

// ── Helpers ────────────────────────────────────────────────────────────────────

const BASE = 1_700_000_000_000; // fixed unix ms base for all timestamps

/** Build the standard VSCode directory structure under a fresh temp dir. */
function setupDataDir(): {
	tmpDir: string;
	dataDir: string;
	wsDir: string;
	chatDir: string;
} {
	const tmpDir = mkdtempSync(join(tmpdir(), "ks-vscode-test-"));
	const dataDir = join(tmpDir, "vscode-data");
	const wsDir = join(dataDir, "User", "workspaceStorage", "abc123hash");
	const chatDir = join(wsDir, "chatSessions");
	mkdirSync(chatDir, { recursive: true });
	return { tmpDir, dataDir, wsDir, chatDir };
}

/** Write workspace.json mapping the workspace hash to a fake project path. */
function writeWorkspaceJson(dir: string, folderPath: string): void {
	writeFileSync(
		join(dir, "workspace.json"),
		JSON.stringify({ folder: `file://${folderPath}` }),
	);
}

/** Build a minimal valid VSCode session JSON object. */
function makeSession(opts: {
	sessionId?: string;
	customTitle?: string;
	creationDate?: number;
	lastMessageDate?: number;
	isEmpty?: boolean;
	requests?: Array<{
		requestId?: string;
		messageText?: string;
		timestamp?: number;
		response?: Array<{ kind?: string; value?: string }>;
	}>;
}): object {
	return {
		version: 1,
		sessionId: opts.sessionId ?? "session-uuid-1234",
		customTitle: opts.customTitle,
		creationDate: opts.creationDate ?? BASE,
		lastMessageDate: opts.lastMessageDate ?? BASE + 10_000,
		isEmpty: opts.isEmpty,
		requests: (opts.requests ?? []).map((r, i) => ({
			requestId: r.requestId ?? `req-${i}`,
			timestamp: r.timestamp ?? BASE + i * 1000,
			message:
				r.messageText !== undefined ? { text: r.messageText } : undefined,
			response: r.response,
		})),
	};
}

/** Write a session JSON file into the given chatDir. */
function writeSession(
	chatDir: string,
	filename: string,
	session: object,
): string {
	const path = join(chatDir, filename);
	writeFileSync(path, JSON.stringify(session));
	return path;
}

// ── getCandidateSessions / countNewSessions ────────────────────────────────────

describe("VSCodeEpisodeReader.getCandidateSessions", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("returns a session newer than the cursor", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				lastMessageDate: BASE + 5000,
				requests: [
					{ messageText: "hello", response: [{ value: "hi" }] },
					{ messageText: "bye", response: [{ value: "ok" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(BASE);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe("session-1");
		expect(candidates[0].maxMessageTime).toBe(BASE + 5000);
		reader.close();
	});

	it("excludes sessions at or before the cursor", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				lastMessageDate: BASE,
				requests: [{ messageText: "old", response: [{ value: "reply" }] }],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		// cursor is exactly at lastMessageDate — must be excluded (strictly greater than)
		expect(reader.getCandidateSessions(BASE)).toHaveLength(0);
		reader.close();
	});

	it("returns sessions ordered by lastMessageDate ASC", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-earlier",
				lastMessageDate: BASE + 2000,
				requests: [
					{ messageText: "a", response: [{ value: "b" }] },
					{ messageText: "c", response: [{ value: "d" }] },
				],
			}),
		);
		writeSession(
			chatDir,
			"s2.json",
			makeSession({
				sessionId: "session-later",
				lastMessageDate: BASE + 10_000,
				requests: [
					{ messageText: "e", response: [{ value: "f" }] },
					{ messageText: "g", response: [{ value: "h" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(2);
		expect(candidates[0].id).toBe("session-earlier");
		expect(candidates[1].id).toBe("session-later");
		reader.close();
	});

	it("respects the limit parameter", () => {
		for (let i = 1; i <= 3; i++) {
			writeSession(
				chatDir,
				`s${i}.json`,
				makeSession({
					sessionId: `session-${i}`,
					lastMessageDate: BASE + i * 1000,
					requests: [
						{ messageText: "hi", response: [{ value: "hey" }] },
						{ messageText: "bye", response: [{ value: "ok" }] },
					],
				}),
			);
		}

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(BASE - 1, 2);
		expect(candidates).toHaveLength(2);
		reader.close();
	});
});

describe("VSCodeEpisodeReader.countNewSessions", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("returns the correct count", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				lastMessageDate: BASE + 5000,
				requests: [{ messageText: "hello", response: [{ value: "hi" }] }],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.countNewSessions(BASE)).toBe(1);
		// lastMessageDate (BASE+5000) is not strictly greater than BASE+5000 — excluded
		expect(reader.countNewSessions(BASE + 5000)).toBe(0);
		reader.close();
	});
});

// ── getNewEpisodes — basic extraction ─────────────────────────────────────────

describe("VSCodeEpisodeReader.getNewEpisodes — basic extraction", () => {
	let tmpDir: string;
	let dataDir: string;
	let wsDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, wsDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("produces one episode with correct user and assistant content", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				customTitle: "My Test Chat",
				requests: [
					{
						messageText: "What is 2+2?",
						response: [{ value: "It is 4." }],
					},
					{
						messageText: "And 3+3?",
						response: [{ value: "It is 6." }],
					},
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());

		expect(episodes).toHaveLength(1);
		expect(episodes[0].sessionId).toBe("session-1");
		expect(episodes[0].sessionTitle).toBe("My Test Chat");
		expect(episodes[0].contentType).toBe("messages");
		expect(episodes[0].content).toContain("What is 2+2");
		expect(episodes[0].content).toContain("It is 4");
		expect(episodes[0].content).toContain("And 3+3");
		expect(episodes[0].content).toContain("It is 6");
		reader.close();
	});

	it("falls back to sessionId-based title when customTitle is absent", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "abcd1234-5678-0000-0000-000000000000",
				requests: [
					{ messageText: "hi", response: [{ value: "hey" }] },
					{ messageText: "bye", response: [{ value: "ok" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].sessionTitle).toContain("abcd1234");
		reader.close();
	});

	it("derives projectName from workspace.json folder URI", () => {
		writeWorkspaceJson(wsDir, "/Users/alice/my-project");
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{ messageText: "hi", response: [{ value: "hey" }] },
					{ messageText: "bye", response: [{ value: "ok" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].projectName).toBe("my-project");
		expect(episodes[0].directory).toBe("/Users/alice/my-project");
		reader.close();
	});

	it("sets startMessageId to requestId and endMessageId to requestId-response", () => {
		// extractMessages produces messageIds as:
		//   user turn:      requestId
		//   assistant turn: `${requestId}-response`
		// The last chunk's endMessageId is therefore the last assistant messageId.
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						requestId: "req-aaa",
						messageText: "first",
						response: [{ value: "reply1" }],
					},
					{
						requestId: "req-bbb",
						messageText: "second",
						response: [{ value: "reply2" }],
					},
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].startMessageId).toBe("req-aaa");
		expect(episodes[0].endMessageId).toBe("req-bbb-response");
		reader.close();
	});
});

// ── getNewEpisodes — response part kind filtering ─────────────────────────────

describe("VSCodeEpisodeReader.getNewEpisodes — response part kind filtering", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("includes parts with no kind (plain text)", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						messageText: "summarise",
						response: [{ value: "Here is the summary." }],
					},
					{ messageText: "thanks", response: [{ value: "welcome" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].content).toContain("Here is the summary");
		reader.close();
	});

	it("includes parts with kind=markdownContent", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						messageText: "explain",
						response: [
							{ kind: "markdownContent", value: "Markdown explanation." },
						],
					},
					{ messageText: "ok", response: [{ value: "done" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].content).toContain("Markdown explanation");
		reader.close();
	});

	it("includes parts with kind=markdownVuln", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						messageText: "check security",
						response: [{ kind: "markdownVuln", value: "Vulnerability found." }],
					},
					{ messageText: "ok", response: [{ value: "noted" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].content).toContain("Vulnerability found");
		reader.close();
	});

	it("skips metadata parts (toolInvocationSerialized, codeblockUri, textEditGroup, inlineReference, undoStop)", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						messageText: "run tool",
						response: [
							{
								kind: "toolInvocationSerialized",
								value: '{"tool":"bash","input":"ls"}',
							},
							{ kind: "codeblockUri", value: "/some/file.ts" },
							{ kind: "textEditGroup", value: "edit data" },
							{ kind: "inlineReference", value: "ref data" },
							{ kind: "undoStop", value: "" },
							{ value: "Tool completed successfully." }, // no kind — should be included
						],
					},
					{ messageText: "done", response: [{ value: "all good" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes[0].content).toContain("Tool completed successfully");
		// All metadata part values must be absent
		expect(episodes[0].content).not.toContain("toolInvocationSerialized");
		expect(episodes[0].content).not.toContain("/some/file.ts");
		expect(episodes[0].content).not.toContain("edit data");
		expect(episodes[0].content).not.toContain("ref data");
		reader.close();
	});
});

// ── getNewEpisodes — session-level skip conditions ────────────────────────────

describe("VSCodeEpisodeReader.getNewEpisodes — session skip conditions", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("skips sessions with isEmpty=true", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				isEmpty: true,
				requests: [{ messageText: "hello", response: [{ value: "hi" }] }],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});

	it("skips sessions with missing sessionId", () => {
		writeSession(chatDir, "s1.json", {
			creationDate: BASE,
			lastMessageDate: BASE + 5000,
			requests: [
				{
					requestId: "req-1",
					message: { text: "hello" },
					response: [{ value: "hi" }],
				},
			],
		});

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});

	it("skips sessions with lastMessageDate=0", () => {
		writeSession(chatDir, "s1.json", {
			sessionId: "session-1",
			creationDate: 0,
			lastMessageDate: 0,
			requests: [{ requestId: "r1", message: { text: "hi" }, response: [] }],
		});

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});

	it("skips sessions where all requests have empty message text", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						messageText: "",
						response: [{ value: "response with no user text" }],
					},
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});

	it("skips malformed JSON session files gracefully", () => {
		writeFileSync(join(chatDir, "bad.json"), "{not valid json");
		// Also write one valid session so we can verify the reader still works
		writeSession(
			chatDir,
			"good.json",
			makeSession({
				sessionId: "session-good",
				requests: [
					{ messageText: "hello", response: [{ value: "hi" }] },
					{ messageText: "bye", response: [{ value: "ok" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe("session-good");
		reader.close();
	});

	it("skips sessions with empty requests array", () => {
		writeSession(chatDir, "s1.json", {
			sessionId: "session-1",
			creationDate: BASE,
			lastMessageDate: BASE + 5000,
			requests: [],
		});

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});
});

// ── getNewEpisodes — minSessionMessages filter ────────────────────────────────

describe("VSCodeEpisodeReader.getNewEpisodes — minSessionMessages filter", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("skips sessions that produce fewer messages than minSessionMessages", () => {
		// 1 request → 2 messages (1 user + 1 assistant), always below any threshold >= 3.
		// config.consolidation.minSessionMessages is frozen at process startup
		// (reflects CONSOLIDATION_MIN_MESSAGES env var, default 4 in config.ts).
		// Fail loudly if it's ever set below 3 — the 2-message session we construct
		// wouldn't reliably be "below threshold" in that case.
		const threshold = config.consolidation.minSessionMessages;
		expect(threshold).toBeGreaterThanOrEqual(3);
		const requestCount = 1;
		const requests = Array.from({ length: requestCount }, (_, i) => ({
			messageText: `msg ${i}`,
			response: [{ value: `reply ${i}` }],
		}));

		writeSession(
			chatDir,
			"s1.json",
			makeSession({ sessionId: "session-short", requests }),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes).toHaveLength(0);
		reader.close();
	});
});

// ── getNewEpisodes — processedRanges exclusion ────────────────────────────────

describe("VSCodeEpisodeReader.getNewEpisodes — processedRanges exclusion", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("skips episodes whose (startMessageId, endMessageId) are already processed", () => {
		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-1",
				requests: [
					{
						requestId: "r1",
						messageText: "start",
						response: [{ value: "reply1" }],
					},
					{
						requestId: "r2",
						messageText: "continue",
						response: [{ value: "reply2" }],
					},
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);

		// First call: get the episode range
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		expect(episodes).toHaveLength(1);

		// Second call: mark that range as processed
		const processedRanges = new Map([
			[
				"session-1",
				[
					{
						startMessageId: episodes[0].startMessageId,
						endMessageId: episodes[0].endMessageId,
					},
				],
			],
		]);
		const episodes2 = reader.getNewEpisodes(
			[candidates[0].id],
			processedRanges,
		);
		expect(episodes2).toHaveLength(0);
		reader.close();
	});
});

// ── Edge cases ────────────────────────────────────────────────────────────────

describe("VSCodeEpisodeReader — edge cases", () => {
	let tmpDir: string;
	let dataDir: string;
	let chatDir: string;

	beforeEach(() => {
		({ tmpDir, dataDir, chatDir } = setupDataDir());
	});

	afterEach(() => {
		rmSync(tmpDir, { recursive: true, force: true });
	});

	it("returns no sessions when workspaceStorage directory does not exist", () => {
		// dataDir exists but has no User/workspaceStorage
		const emptyDataDir = mkdtempSync(join(tmpdir(), "ks-vscode-empty-"));
		try {
			const reader = new VSCodeEpisodeReader(emptyDataDir);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			expect(reader.countNewSessions(0)).toBe(0);
			reader.close();
		} finally {
			rmSync(emptyDataDir, { recursive: true, force: true });
		}
	});

	it("handles workspace directory with no chatSessions subdirectory", () => {
		// wsDir exists but no chatSessions/ inside it
		rmSync(chatDir, { recursive: true, force: true });

		const reader = new VSCodeEpisodeReader(dataDir);
		expect(reader.getCandidateSessions(0)).toHaveLength(0);
		reader.close();
	});

	it("sessions across multiple workspace directories are all discovered", () => {
		// Create a second workspace directory with distinct lastMessageDate
		// to avoid non-deterministic ordering on equal timestamps.
		const wsDir2 = join(dataDir, "User", "workspaceStorage", "def456hash");
		const chatDir2 = join(wsDir2, "chatSessions");
		mkdirSync(chatDir2, { recursive: true });

		writeSession(
			chatDir,
			"s1.json",
			makeSession({
				sessionId: "session-ws1",
				lastMessageDate: BASE + 1000,
				requests: [
					{ messageText: "from ws1", response: [{ value: "reply1" }] },
					{ messageText: "more ws1", response: [{ value: "reply2" }] },
				],
			}),
		);
		writeSession(
			chatDir2,
			"s2.json",
			makeSession({
				sessionId: "session-ws2",
				lastMessageDate: BASE + 2000,
				requests: [
					{ messageText: "from ws2", response: [{ value: "reply3" }] },
					{ messageText: "more ws2", response: [{ value: "reply4" }] },
				],
			}),
		);

		const reader = new VSCodeEpisodeReader(dataDir);
		const candidates = reader.getCandidateSessions(0);
		expect(candidates).toHaveLength(2);
		const ids = candidates.map((c) => c.id);
		expect(ids).toContain("session-ws1");
		expect(ids).toContain("session-ws2");
		reader.close();
	});
});

// ── resolveWorkspaceFolder ────────────────────────────────────────────────────

describe("resolveWorkspaceFolder", () => {
	it("resolves a local file:// URI", () => {
		expect(resolveWorkspaceFolder("file:///Users/x/Documents/project")).toBe(
			"/Users/x/Documents/project",
		);
	});

	it("decodes percent-encoded characters in file:// URIs", () => {
		expect(
			resolveWorkspaceFolder("file:///Users/dennis%20paul/Documents/project"),
		).toBe("/Users/dennis paul/Documents/project");
	});

	it("resolves an SSH Remote vscode-remote:// URI to the remote pathname", () => {
		expect(
			resolveWorkspaceFolder(
				"vscode-remote://ssh-remote%2B192.168.0.6/home/dpaul/openclaw",
			),
		).toBe("/home/dpaul/openclaw");
	});

	it("resolves a Dev Container URI to the host machine's project path", () => {
		// Encode {"hostPath":"/Users/x/Documents/iu-dwh"} as hex
		const payload = Buffer.from(
			JSON.stringify({ hostPath: "/Users/x/Documents/iu-dwh" }),
		).toString("hex");
		const uri = `vscode-remote://dev-container%2B${encodeURIComponent(payload)}/opt/app`;
		expect(resolveWorkspaceFolder(uri)).toBe("/Users/x/Documents/iu-dwh");
	});

	it("returns empty string for a Dev Container URI when hex decoding fails", () => {
		// Malformed hex payload — should return "" not the container-internal path
		const uri =
			"vscode-remote://dev-container%2Bnotvalidhex/opt/app";
		expect(resolveWorkspaceFolder(uri)).toBe("");
	});

	it("returns empty string for a Dev Container URI when JSON has no hostPath", () => {
		const payload = Buffer.from(JSON.stringify({ other: "field" })).toString(
			"hex",
		);
		const uri = `vscode-remote://dev-container%2B${encodeURIComponent(payload)}/opt/app`;
		expect(resolveWorkspaceFolder(uri)).toBe("");
	});

	it("returns empty string for an unknown scheme", () => {
		expect(resolveWorkspaceFolder("customscheme://some/path")).toBe("");
	});

	it("returns empty string for a vscode-remote:// URI that fails URL parsing", () => {
		expect(resolveWorkspaceFolder("vscode-remote://")).toBe("");
	});
});

// ── resolveVSCodeDataDir — env var branch ─────────────────────────────────────

describe("resolveVSCodeDataDir", () => {
	it("returns null when VSCODE_DATA_DIR points to a non-existent directory", () => {
		const original = process.env.VSCODE_DATA_DIR;
		process.env.VSCODE_DATA_DIR = "/nonexistent/vscode/data";
		try {
			expect(resolveVSCodeDataDir()).toBeNull();
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "VSCODE_DATA_DIR");
			} else {
				process.env.VSCODE_DATA_DIR = original;
			}
		}
	});

	it("returns the path when VSCODE_DATA_DIR points to a valid VSCode data directory", () => {
		// resolveVSCodeDataDir checks for User/workspaceStorage inside the given path
		const dir = mkdtempSync(join(tmpdir(), "ks-vscode-dir-"));
		mkdirSync(join(dir, "User", "workspaceStorage"), { recursive: true });
		const original = process.env.VSCODE_DATA_DIR;
		process.env.VSCODE_DATA_DIR = dir;
		try {
			expect(resolveVSCodeDataDir()).toBe(dir);
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "VSCODE_DATA_DIR");
			} else {
				process.env.VSCODE_DATA_DIR = original;
			}
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
