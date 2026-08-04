/**
 * Tests for PiEpisodeReader.
 *
 * PiEpisodeReader reads pi's JSONL session files
 * (<agentDir>/sessions/--<cwd-encoded>--/<timestamp>_<uuid>.jsonl, format v3).
 * These tests create a minimal fake ~/.pi layout in a temp dir, seed it with
 * JSONL records, and exercise the parsing + segmentation logic without any
 * network calls.
 *
 * Covered:
 *   - resolvePiSessionsDirs — multi-layer discovery (<root>/<layer>/sessions)
 *   - getCandidateSessions / countNewSessions — mtime + timestamp cursor filtering
 *   - getNewEpisodes — no-compaction segmentation
 *   - getNewEpisodes — compaction + branch_summary summary path (inline summaries)
 *   - getNewEpisodes — already-processed range exclusion (processedRanges)
 *   - getNewEpisodes — minSessionMessages filter
 *   - user/assistant extraction (text + thinking; toolCall args skipped)
 *   - toolResult extraction with allowlist (toolName carried directly)
 *   - bashExecution (excludeFromContext + allowlist)
 *   - skipped entry types (custom_message, model_change, label, custom)
 *   - title fallback (session_info name → first user message → sessionId)
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import { PiEpisodeReader, resolvePiSessionsDirs } from "../src/daemon/readers/pi";

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Make a UTC ISO timestamp offset by `deltaMs` from `base`. */
function ts(base: number, deltaMs = 0): string {
	return new Date(base + deltaMs).toISOString();
}

/** pi session header record. */
function headerRecord(opts: {
	id: string;
	cwd?: string;
	timestamp: string;
}): string {
	return JSON.stringify({
		type: "session",
		version: 3,
		id: opts.id,
		timestamp: opts.timestamp,
		cwd: opts.cwd ?? CWD,
	});
}

let entryCounter = 0;
function eid(): string {
	entryCounter += 1;
	return entryCounter.toString(16).padStart(8, "0");
}

/** pi `message` entry. */
function msgRecord(opts: {
	role: "user" | "assistant" | "toolResult" | "bashExecution";
	timestamp: string;
	content?: string | object[];
	toolName?: string;
	command?: string;
	output?: string;
	excludeFromContext?: boolean;
}): string {
	const message: Record<string, unknown> = { role: opts.role };
	if (opts.content !== undefined) message.content = opts.content;
	if (opts.toolName !== undefined) message.toolName = opts.toolName;
	if (opts.command !== undefined) message.command = opts.command;
	if (opts.output !== undefined) message.output = opts.output;
	if (opts.excludeFromContext !== undefined)
		message.excludeFromContext = opts.excludeFromContext;
	return JSON.stringify({
		type: "message",
		id: eid(),
		parentId: null,
		timestamp: opts.timestamp,
		message,
	});
}

/** pi compaction / branch_summary entry (summary inlined). */
function summaryRecord(opts: {
	type: "compaction" | "branch_summary";
	timestamp: string;
	summary: string;
}): string {
	return JSON.stringify({
		type: opts.type,
		id: eid(),
		parentId: null,
		timestamp: opts.timestamp,
		summary: opts.summary,
		tokensBefore: 50000,
	});
}

/** pi metadata entries that must be skipped. */
function metaRecord(type: string, timestamp: string): string {
	return JSON.stringify({ type, id: eid(), parentId: null, timestamp });
}

// ── Fixtures ──────────────────────────────────────────────────────────────────

const PROJECT_DIR = "--home-user-project--"; // pi-encoded cwd dir name
const SESSION_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const SESSION_FILE = `2026-08-04T10-00-00-000Z_${SESSION_UUID}.jsonl`;
const CWD = "/home/user/project";

const BASE = 1_700_000_000_000; // arbitrary fixed unix ms

let tmpDir: string;
let piRoot: string;
let agentSessions: string;
let projectPath: string;
let sessionFilePath: string;
let reader: PiEpisodeReader;

beforeEach(() => {
	entryCounter = 0;
	tmpDir = mkdtempSync(join(tmpdir(), "ks-pi-test-"));
	piRoot = join(tmpDir, ".pi");
	agentSessions = join(piRoot, "agent", "sessions");
	projectPath = join(agentSessions, PROJECT_DIR);
	mkdirSync(projectPath, { recursive: true });
	sessionFilePath = join(projectPath, SESSION_FILE);
	reader = new PiEpisodeReader([agentSessions]);
});

afterEach(() => {
	reader.close();
	rmSync(tmpDir, { recursive: true, force: true });
});

function writeSession(lines: string[]): void {
	writeFileSync(sessionFilePath, `${lines.join("\n")}\n`);
}

/** Four plain messages — enough to pass minSessionMessages (default 4). */
function fourMessages(startDelta = 0): string[] {
	return [
		msgRecord({ role: "user", timestamp: ts(BASE, startDelta), content: "hello" }),
		msgRecord({
			role: "assistant",
			timestamp: ts(BASE, startDelta + 1000),
			content: [{ type: "text", text: "hi there" }],
		}),
		msgRecord({
			role: "user",
			timestamp: ts(BASE, startDelta + 2000),
			content: "how are you",
		}),
		msgRecord({
			role: "assistant",
			timestamp: ts(BASE, startDelta + 3000),
			content: [{ type: "text", text: "fine" }],
		}),
	];
}

// ── resolvePiSessionsDirs ─────────────────────────────────────────────────────

describe("resolvePiSessionsDirs", () => {
	it("discovers sessions dirs across multiple layers", () => {
		// Add a second layer (work) and a non-layer dir (shared — no sessions inside)
		mkdirSync(join(piRoot, "work", "sessions"), { recursive: true });
		mkdirSync(join(piRoot, "shared"), { recursive: true });

		const dirs = resolvePiSessionsDirs(piRoot);
		expect(dirs).toHaveLength(2);
		expect(dirs).toContain(agentSessions);
		expect(dirs).toContain(join(piRoot, "work", "sessions"));
	});

	it("includes <root>/sessions when the root is itself an agent dir", () => {
		const directRoot = join(tmpDir, "direct-agent");
		mkdirSync(join(directRoot, "sessions"), { recursive: true });

		const dirs = resolvePiSessionsDirs(directRoot);
		expect(dirs).toEqual([join(directRoot, "sessions")]);
	});

	it("returns empty when the root does not exist", () => {
		expect(resolvePiSessionsDirs(join(tmpDir, "nope"))).toEqual([]);
	});
});

// ── getCandidateSessions / countNewSessions ───────────────────────────────────

describe("PiEpisodeReader.getCandidateSessions", () => {
	it("returns the session when entries are newer than the cursor", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe(SESSION_UUID);
		expect(candidates[0].maxMessageTime).toBe(BASE + 3000);
	});

	it("excludes sessions whose entries are all at or before the cursor", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			msgRecord({ role: "user", timestamp: ts(BASE, 0), content: "old" }),
			msgRecord({
				role: "assistant",
				timestamp: ts(BASE, 0),
				content: [{ type: "text", text: "old reply" }],
			}),
		]);

		expect(reader.getCandidateSessions(BASE)).toHaveLength(0);
		expect(reader.countNewSessions(BASE)).toBe(0);
	});

	it("orders sessions by maxMessageTime ASC and respects limit", () => {
		const session2Uuid = "11111111-2222-3333-4444-555555555555";
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 5000) }),
			...fourMessages(5000),
		]);
		writeFileSync(
			join(projectPath, `2026-08-04T09-00-00-000Z_${session2Uuid}.jsonl`),
			`${[
				headerRecord({ id: session2Uuid, timestamp: ts(BASE, 1000) }),
				...fourMessages(1000),
			].join("\n")}\n`,
		);

		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(2);
		expect(candidates[0].id).toBe(session2Uuid); // earlier maxMessageTime first
		expect(candidates[1].id).toBe(SESSION_UUID);

		expect(reader.getCandidateSessions(BASE - 1, 1)).toHaveLength(1);
	});
});

// ── getNewEpisodes: no-compaction path ────────────────────────────────────────

describe("PiEpisodeReader.getNewEpisodes (no compaction)", () => {
	it("produces one messages episode with user + assistant content", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(1);
		const ep = episodes[0];
		expect(ep.source).toBe("pi");
		expect(ep.sessionId).toBe(SESSION_UUID);
		expect(ep.contentType).toBe("messages");
		expect(ep.projectName).toBe("project"); // last component of cwd
		expect(ep.directory).toBe(CWD);
		expect(ep.content).toContain("user: hello");
		expect(ep.content).toContain("assistant: fine");
		expect(ep.maxMessageTime).toBe(BASE + 3000);
		// start/end IDs are pi entry ids (8-char hex)
		expect(ep.startMessageId).toMatch(/^[0-9a-f]{8}$/);
		expect(ep.endMessageId).toMatch(/^[0-9a-f]{8}$/);
	});

	it("includes assistant thinking parts but skips toolCall arguments", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			msgRecord({ role: "user", timestamp: ts(BASE, 0), content: "fix the bug" }),
			msgRecord({
				role: "assistant",
				timestamp: ts(BASE, 1000),
				content: [
					{ type: "thinking", thinking: "THINKING TRACE" },
					{ type: "toolCall", id: "tc1", name: "bash", arguments: { command: "rm -rf /" } },
					{ type: "text", text: "Done." },
				],
			}),
			msgRecord({ role: "user", timestamp: ts(BASE, 2000), content: "thanks" }),
			msgRecord({
				role: "assistant",
				timestamp: ts(BASE, 3000),
				content: [{ type: "text", text: "welcome" }],
			}),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).toContain("THINKING TRACE");
		expect(episodes[0].content).not.toContain("rm -rf");
	});

	it("skips metadata entries and custom_message content", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			metaRecord("model_change", ts(BASE, 100)),
			metaRecord("thinking_level_change", ts(BASE, 200)),
			metaRecord("label", ts(BASE, 300)),
			metaRecord("custom", ts(BASE, 400)),
			JSON.stringify({
				type: "custom_message",
				id: eid(),
				parentId: null,
				timestamp: ts(BASE, 500),
				customType: "knowledge",
				content: "INJECTED CONTEXT — must not be re-ingested",
				display: false,
			}),
			...fourMessages(1000),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("INJECTED CONTEXT");
	});

	it("applies the minSessionMessages filter", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			msgRecord({ role: "user", timestamp: ts(BASE, 0), content: "hi" }),
			msgRecord({
				role: "assistant",
				timestamp: ts(BASE, 1000),
				content: [{ type: "text", text: "hello" }],
			}),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(1); // candidate, but too few messages
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);
		expect(episodes).toHaveLength(0);
	});
});

// ── getNewEpisodes: compaction / branch_summary path ──────────────────────────

describe("PiEpisodeReader.getNewEpisodes (summaries)", () => {
	it("turns a compaction entry into a compaction_summary episode plus tail", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
			summaryRecord({
				type: "compaction",
				timestamp: ts(BASE, 4000),
				summary: "COMPACTION SUMMARY: user set up pi config",
			}),
			...fourMessages(5000),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(2);
		const summary = episodes[0];
		expect(summary.contentType).toBe("compaction_summary");
		expect(summary.content).toBe("COMPACTION SUMMARY: user set up pi config");
		expect(summary.startMessageId).toBe(summary.endMessageId);

		const tail = episodes[1];
		expect(tail.contentType).toBe("messages");
		// Tail contains only post-compaction messages
		expect(tail.content).toContain("user: hello");
		expect(tail.maxMessageTime).toBe(BASE + 8000);
	});

	it("also treats branch_summary entries as summary episodes", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
			summaryRecord({
				type: "branch_summary",
				timestamp: ts(BASE, 4000),
				summary: "BRANCH: explored approach A",
			}),
			summaryRecord({
				type: "compaction",
				timestamp: ts(BASE, 5000),
				summary: "COMPACTION after branch",
			}),
			...fourMessages(6000),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(3);
		expect(episodes[0].content).toBe("BRANCH: explored approach A");
		expect(episodes[1].content).toBe("COMPACTION after branch");
		expect(episodes[2].contentType).toBe("messages");
	});

	it("omits the tail episode when post-summary messages are below the minimum", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
			summaryRecord({
				type: "compaction",
				timestamp: ts(BASE, 4000),
				summary: "SUMMARY",
			}),
			msgRecord({
				role: "user",
				timestamp: ts(BASE, 5000),
				content: "one more",
			}),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].contentType).toBe("compaction_summary");
	});
});

// ── getNewEpisodes: processedRanges exclusion ─────────────────────────────────

describe("PiEpisodeReader.getNewEpisodes (processedRanges)", () => {
	it("excludes episodes whose (start, end) range was already processed", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
		]);

		const first = reader.getNewEpisodes(
			reader.getCandidateSessions(BASE - 1).map((c) => c.id),
			new Map(),
		);
		expect(first).toHaveLength(1);

		const processed = new Map([
			[
				SESSION_UUID,
				[
					{
						source: "pi",
						startMessageId: first[0].startMessageId,
						endMessageId: first[0].endMessageId,
					},
				],
			],
		]);

		const second = reader.getNewEpisodes(
			reader.getCandidateSessions(BASE - 1).map((c) => c.id),
			processed,
		);
		expect(second).toHaveLength(0);
	});
});

// ── Tool outputs ──────────────────────────────────────────────────────────────

describe("PiEpisodeReader tool output extraction", () => {
	it("includes allowlisted toolResult output (toolName carried directly)", () => {
		const originalAllowlist = config.consolidation.includeToolOutputs;
		config.consolidation.includeToolOutputs = ["read"];
		try {
			writeSession([
				headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
				msgRecord({ role: "user", timestamp: ts(BASE, 0), content: "read the file" }),
				msgRecord({
					role: "toolResult",
					timestamp: ts(BASE, 1000),
					toolName: "read",
					content: [{ type: "text", text: "ALLOWLISTED FILE CONTENT" }],
				}),
				msgRecord({
					role: "toolResult",
					timestamp: ts(BASE, 2000),
					toolName: "other_tool",
					content: [{ type: "text", text: "NOT ALLOWLISTED" }],
				}),
				msgRecord({
					role: "assistant",
					timestamp: ts(BASE, 3000),
					content: [{ type: "text", text: "done reading" }],
				}),
				msgRecord({ role: "user", timestamp: ts(BASE, 4000), content: "ok" }),
			]);

			const candidates = reader.getCandidateSessions(BASE - 1);
			const episodes = reader.getNewEpisodes(
				candidates.map((c) => c.id),
				new Map(),
			);

			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).toContain("[tool: read]");
			expect(episodes[0].content).toContain("ALLOWLISTED FILE CONTENT");
			expect(episodes[0].content).not.toContain("NOT ALLOWLISTED");
		} finally {
			config.consolidation.includeToolOutputs = originalAllowlist;
		}
	});

	it("includes bashExecution when bash is allowlisted, skips excludeFromContext", () => {
		const originalAllowlist = config.consolidation.includeToolOutputs;
		config.consolidation.includeToolOutputs = ["bash"];
		try {
			writeSession([
				headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
				msgRecord({ role: "user", timestamp: ts(BASE, 0), content: "run it" }),
				msgRecord({
					role: "bashExecution",
					timestamp: ts(BASE, 1000),
					command: "ls -la",
					output: "VISIBLE OUTPUT",
				}),
				msgRecord({
					role: "bashExecution",
					timestamp: ts(BASE, 2000),
					command: "cat .env",
					output: "HIDDEN OUTPUT",
					excludeFromContext: true,
				}),
				msgRecord({
					role: "assistant",
					timestamp: ts(BASE, 3000),
					content: [{ type: "text", text: "ran it" }],
				}),
				msgRecord({ role: "user", timestamp: ts(BASE, 4000), content: "ok" }),
			]);

			const candidates = reader.getCandidateSessions(BASE - 1);
			const episodes = reader.getNewEpisodes(
				candidates.map((c) => c.id),
				new Map(),
			);

			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).toContain("$ ls -la");
			expect(episodes[0].content).toContain("VISIBLE OUTPUT");
			expect(episodes[0].content).not.toContain("HIDDEN OUTPUT");
		} finally {
			config.consolidation.includeToolOutputs = originalAllowlist;
		}
	});
});

// ── Title fallback ────────────────────────────────────────────────────────────

describe("PiEpisodeReader session title", () => {
	it("prefers session_info name over first message preview", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
			JSON.stringify({
				type: "session_info",
				id: eid(),
				parentId: null,
				timestamp: ts(BASE, 5000),
				name: "Set up pi knowledge integration",
			}),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);
		expect(episodes[0].sessionTitle).toBe("Set up pi knowledge integration");
	});

	it("falls back to a truncated first user message", () => {
		writeSession([
			headerRecord({ id: SESSION_UUID, timestamp: ts(BASE, 0) }),
			...fourMessages(0),
		]);

		const candidates = reader.getCandidateSessions(BASE - 1);
		const episodes = reader.getNewEpisodes(
			candidates.map((c) => c.id),
			new Map(),
		);
		expect(episodes[0].sessionTitle).toBe("hello");
	});
});
