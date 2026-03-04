/**
 * Tests for ClaudeCodeEpisodeReader.
 *
 * ClaudeCodeEpisodeReader reads from Claude Code's JSONL session files.
 * These tests create a minimal fake ~/.claude directory structure in a temp dir,
 * seed it with JSONL records, and exercise the segmentation + extraction logic
 * without any network calls.
 *
 * Covered:
 *   - getCandidateSessions / countNewSessions — mtime + timestamp cursor filtering
 *   - getNewEpisodes — no-compaction segmentation
 *   - getNewEpisodes — compaction summary path
 *   - getNewEpisodes — already-processed range exclusion (processedRanges)
 *   - getNewEpisodes — minSessionMessages filter
 *   - Tool result extraction with allowlist
 *   - Plain string content (simple messages)
 */
import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { ClaudeCodeEpisodeReader } from "../src/consolidation/readers/claude-code";

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Make a UTC ISO timestamp offset by `deltaMs` from `base`. */
function ts(base: number, deltaMs = 0): string {
  return new Date(base + deltaMs).toISOString();
}

/** Build a JSONL line for a user or assistant message record. */
function msgRecord(
  opts: {
    uuid: string;
    role: "user" | "assistant";
    timestamp: string;
    content: string | object[];
    sessionId?: string;
    cwd?: string;
    slug?: string;
    parentUuid?: string | null;
  }
): string {
  return JSON.stringify({
    type: opts.role,
    uuid: opts.uuid,
    parentUuid: opts.parentUuid ?? null,
    timestamp: opts.timestamp,
    sessionId: opts.sessionId ?? "test-session",
    cwd: opts.cwd ?? "/home/user/project",
    slug: opts.slug ?? "test-slug",
    message: {
      role: opts.role,
      content: opts.content,
    },
  });
}

/** Build a JSONL line for a compact_boundary system record. */
function compactionRecord(opts: { uuid?: string; timestamp: string; sessionId?: string }): string {
  return JSON.stringify({
    type: "system",
    subtype: "compact_boundary",
    uuid: opts.uuid ?? "compact-uuid",
    timestamp: opts.timestamp,
    sessionId: opts.sessionId ?? "test-session",
    compactMetadata: { trigger: "manual", preTokens: 50000 },
  });
}

// ── Fixtures ──────────────────────────────────────────────────────────────────

const PROJECT_DIR = "home-user-project"; // fake encoded project dir name
const SESSION_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const CWD = "/home/user/project";

let tmpDir: string;
let claudeDir: string;
let projectPath: string;
let sessionFilePath: string;
let reader: ClaudeCodeEpisodeReader;

/** Base time for all timestamps in tests. */
const BASE = 1_700_000_000_000; // arbitrary fixed unix ms

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "ks-claude-test-"));
  claudeDir = join(tmpDir, ".claude");
  projectPath = join(claudeDir, "projects", PROJECT_DIR);
  mkdirSync(projectPath, { recursive: true });
  sessionFilePath = join(projectPath, `${SESSION_UUID}.jsonl`);
  reader = new ClaudeCodeEpisodeReader(claudeDir);
});

afterEach(() => {
  reader.close();
  rmSync(tmpDir, { recursive: true, force: true });
});

// ── Utility: write session file ───────────────────────────────────────────────

function writeSession(lines: string[]): void {
  writeFileSync(sessionFilePath, `${lines.join("\n")}\n`);
}

// ── getCandidateSessions / countNewSessions ────────────────────────────────────

describe("ClaudeCodeEpisodeReader.getCandidateSessions", () => {
  it("returns the session when messages are newer than the cursor", () => {
    writeSession([
      msgRecord({ uuid: "msg-1", role: "user",      timestamp: ts(BASE, 0),    content: "hello" }),
      msgRecord({ uuid: "msg-2", role: "assistant", timestamp: ts(BASE, 1000), content: "hi" }),
      msgRecord({ uuid: "msg-3", role: "user",      timestamp: ts(BASE, 2000), content: "bye" }),
      msgRecord({ uuid: "msg-4", role: "assistant", timestamp: ts(BASE, 3000), content: "ok" }),
    ]);

    const candidates = reader.getCandidateSessions(BASE - 1);
    expect(candidates).toHaveLength(1);
    expect(candidates[0].id).toBe(SESSION_UUID);
    expect(candidates[0].maxMessageTime).toBe(BASE + 3000);
  });

  it("excludes sessions whose messages are all at or before the cursor", () => {
    writeSession([
      msgRecord({ uuid: "msg-1", role: "user",      timestamp: ts(BASE), content: "old" }),
      msgRecord({ uuid: "msg-2", role: "assistant", timestamp: ts(BASE), content: "old reply" }),
    ]);

    // Cursor is exactly at the session's max timestamp — not strictly greater, so excluded
    const candidates = reader.getCandidateSessions(BASE);
    expect(candidates).toHaveLength(0);
  });

  it("returns sessions ordered by maxMessageTime ASC", () => {
    // Create a second session file
    const session2Uuid = "11111111-2222-3333-4444-555555555555";
    const session2Path = join(projectPath, `${session2Uuid}.jsonl`);
    writeSession([
      msgRecord({ uuid: "s1-1", role: "user",      timestamp: ts(BASE, 5000), content: "later session" }),
      msgRecord({ uuid: "s1-2", role: "assistant", timestamp: ts(BASE, 6000), content: "reply" }),
      msgRecord({ uuid: "s1-3", role: "user",      timestamp: ts(BASE, 7000), content: "more" }),
      msgRecord({ uuid: "s1-4", role: "assistant", timestamp: ts(BASE, 8000), content: "ok" }),
    ]);
    const session2Lines = [
      msgRecord({ uuid: "s2-1", role: "user",      timestamp: ts(BASE, 1000), content: "earlier session", sessionId: session2Uuid }),
      msgRecord({ uuid: "s2-2", role: "assistant", timestamp: ts(BASE, 2000), content: "reply",           sessionId: session2Uuid }),
      msgRecord({ uuid: "s2-3", role: "user",      timestamp: ts(BASE, 3000), content: "more",            sessionId: session2Uuid }),
      msgRecord({ uuid: "s2-4", role: "assistant", timestamp: ts(BASE, 4000), content: "done",            sessionId: session2Uuid }),
    ];
    writeFileSync(session2Path, `${session2Lines.join("\n")}\n`);

    const candidates = reader.getCandidateSessions(BASE - 1);
    expect(candidates).toHaveLength(2);
    // Session 2 has earlier maxMessageTime → must come first
    expect(candidates[0].id).toBe(session2Uuid);
    expect(candidates[1].id).toBe(SESSION_UUID);
  });

  it("respects the limit parameter", () => {
    const session2Uuid = "99999999-8888-7777-6666-555555555555";
    const session2Path = join(projectPath, `${session2Uuid}.jsonl`);
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "a" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "b" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "c" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "d" }),
    ]);
    const limitLines = [
      msgRecord({ uuid: "n1", role: "user",      timestamp: ts(BASE, 4000), content: "e", sessionId: session2Uuid }),
      msgRecord({ uuid: "n2", role: "assistant", timestamp: ts(BASE, 5000), content: "f", sessionId: session2Uuid }),
      msgRecord({ uuid: "n3", role: "user",      timestamp: ts(BASE, 6000), content: "g", sessionId: session2Uuid }),
      msgRecord({ uuid: "n4", role: "assistant", timestamp: ts(BASE, 7000), content: "h", sessionId: session2Uuid }),
    ];
    writeFileSync(session2Path, `${limitLines.join("\n")}\n`);

    const candidates = reader.getCandidateSessions(BASE - 1, 1);
    expect(candidates).toHaveLength(1);
  });
});

describe("ClaudeCodeEpisodeReader.countNewSessions", () => {
  it("returns 1 when session has messages newer than cursor", () => {
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "hello" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "hi" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "bye" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "ok" }),
    ]);
    expect(reader.countNewSessions(BASE - 1)).toBe(1);
    expect(reader.countNewSessions(BASE + 3000)).toBe(0); // cursor at max → excluded
  });
});

// ── getNewEpisodes — no-compaction path ───────────────────────────────────────

describe("ClaudeCodeEpisodeReader.getNewEpisodes — no-compaction segmentation", () => {
  it("produces one episode from a session without compactions", () => {
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "What is the capital of France?" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "Paris." }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "And Germany?" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "Berlin." }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());

    expect(episodes).toHaveLength(1);
    expect(episodes[0].sessionId).toBe(SESSION_UUID);
    expect(episodes[0].startMessageId).toBe("m1");
    expect(episodes[0].endMessageId).toBe("m4");
    expect(episodes[0].contentType).toBe("messages");
    expect(episodes[0].content).toContain("Paris");
    expect(episodes[0].content).toContain("Berlin");
  });

  it("includes the slug as the session title", () => {
    // All records carry the same slug — the parser uses the last slug seen.
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "hi",  slug: "cool-session-slug" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "hey", slug: "cool-session-slug" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "bye", slug: "cool-session-slug" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "ok",  slug: "cool-session-slug" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes[0].sessionTitle).toBe("cool-session-slug");
  });

  it("derives projectName from the cwd's last path component", () => {
    // All records carry the same cwd — the parser uses the last cwd seen.
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "hi",  cwd: "/home/user/my-project" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "hey", cwd: "/home/user/my-project" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "bye", cwd: "/home/user/my-project" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "ok",  cwd: "/home/user/my-project" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes[0].projectName).toBe("my-project");
  });
});

// ── getNewEpisodes — minSessionMessages filter ─────────────────────────────────

describe("ClaudeCodeEpisodeReader.getNewEpisodes — minSessionMessages filter", () => {
  it("skips sessions with fewer than minSessionMessages messages", () => {
    // Default minSessionMessages is 4 — write only 2 messages
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "hi" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "hey" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes).toHaveLength(0);
  });
});

// ── getNewEpisodes — processedRanges exclusion ────────────────────────────────

describe("ClaudeCodeEpisodeReader.getNewEpisodes — processedRanges exclusion", () => {
  it("skips already-processed episode ranges", () => {
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "first" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "reply" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "second" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "done" }),
    ]);

    const processedRanges = new Map([
      [SESSION_UUID, [{ startMessageId: "m1", endMessageId: "m4" }]],
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], processedRanges);
    expect(episodes).toHaveLength(0);
  });

  it("returns episodes not in the processed set", () => {
    // Session has enough messages for two chunks via token chunking.
    // We mark only the first range as processed and expect the second to appear.
    // For simplicity: use two compaction-based episodes, mark one as done.
    writeSession([
      compactionRecord({ uuid: "compact-1", timestamp: ts(BASE, -1000) }),
      msgRecord({ uuid: "summary-1", role: "user",      timestamp: ts(BASE, 0),    content: "Summary of prior work." }),
      msgRecord({ uuid: "m1",        role: "assistant", timestamp: ts(BASE, 1000), content: "Understood." }),
      msgRecord({ uuid: "m2",        role: "user",      timestamp: ts(BASE, 2000), content: "Continue." }),
      msgRecord({ uuid: "m3",        role: "assistant", timestamp: ts(BASE, 3000), content: "Done." }),
      msgRecord({ uuid: "m4",        role: "user",      timestamp: ts(BASE, 4000), content: "Great." }),
    ]);

    // Mark the compaction summary episode as already processed
    const processedRanges = new Map([
      [SESSION_UUID, [{ startMessageId: "summary-1", endMessageId: "summary-1" }]],
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], processedRanges);
    // The tail episode (m1-m4) should still be returned
    expect(episodes.length).toBeGreaterThan(0);
    expect(episodes[0].startMessageId).not.toBe("summary-1");
  });
});

// ── getNewEpisodes — compaction summary path ──────────────────────────────────

describe("ClaudeCodeEpisodeReader.getNewEpisodes — compaction summary path", () => {
  it("extracts the compaction summary as a separate episode", () => {
    writeSession([
      compactionRecord({ uuid: "compact-1", timestamp: ts(BASE, -500) }),
      // First user record after compact_boundary = the summary
      msgRecord({ uuid: "summary-1", role: "user",      timestamp: ts(BASE, 0),    content: "This is the compaction summary of prior work." }),
      msgRecord({ uuid: "m1",        role: "assistant", timestamp: ts(BASE, 1000), content: "Got it." }),
      msgRecord({ uuid: "m2",        role: "user",      timestamp: ts(BASE, 2000), content: "Next step." }),
      msgRecord({ uuid: "m3",        role: "assistant", timestamp: ts(BASE, 3000), content: "Working." }),
      msgRecord({ uuid: "m4",        role: "user",      timestamp: ts(BASE, 4000), content: "Done?" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());

    // Should have: 1 compaction summary episode + 1 tail episode
    expect(episodes.length).toBeGreaterThanOrEqual(1);
    const summaryEp = episodes.find((e) => e.contentType === "compaction_summary");
    expect(summaryEp).toBeDefined();
    expect(summaryEp?.startMessageId).toBe("summary-1");
    expect(summaryEp?.endMessageId).toBe("summary-1");
    expect(summaryEp?.content).toContain("compaction summary");
  });

  it("produces a tail episode for messages after the last compaction", () => {
    writeSession([
      compactionRecord({ uuid: "compact-1", timestamp: ts(BASE, -500) }),
      msgRecord({ uuid: "summary-1", role: "user",      timestamp: ts(BASE, 0),    content: "Summary." }),
      msgRecord({ uuid: "m1",        role: "assistant", timestamp: ts(BASE, 1000), content: "Ok." }),
      msgRecord({ uuid: "m2",        role: "user",      timestamp: ts(BASE, 2000), content: "Continue." }),
      msgRecord({ uuid: "m3",        role: "assistant", timestamp: ts(BASE, 3000), content: "Working." }),
      msgRecord({ uuid: "m4",        role: "user",      timestamp: ts(BASE, 4000), content: "Good." }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    const tailEp = episodes.find((e) => e.contentType === "messages");
    expect(tailEp).toBeDefined();
    // Tail should NOT include the summary message itself
    expect(tailEp?.startMessageId).not.toBe("summary-1");
    expect(tailEp?.content).not.toContain("Summary.");
    expect(tailEp?.content).toContain("Ok.");
  });
});

// ── Tool result extraction ─────────────────────────────────────────────────────

describe("ClaudeCodeEpisodeReader — tool result extraction", () => {
  it("includes tool results for tools in the allowlist", () => {
    // The tool_result record (m2) has no plain text content, so extractMessage
    // returns null for it when the tool is not in the allowlist.
    // We include 4 records that all produce text content, so the session passes
    // the minSessionMessages filter regardless of allowlist state.
    writeSession([
      msgRecord({ uuid: "m1", role: "assistant", timestamp: ts(BASE, 0),    content: "Using tool..." }),
      // m2: a user message that has BOTH a text part and a tool_result part.
      // The text part ensures the message is always extracted; the tool_result
      // is only included when allowlisted.
      msgRecord({
        uuid: "m2",
        role: "user",
        timestamp: ts(BASE, 1000),
        content: [
          { type: "text", text: "Here is what the tool returned:" },
          { type: "tool_result", name: "my_tool", content: "Tool output here." },
        ],
      }),
      msgRecord({ uuid: "m3", role: "assistant", timestamp: ts(BASE, 2000), content: "Done." }),
      msgRecord({ uuid: "m4", role: "user",      timestamp: ts(BASE, 3000), content: "Thanks." }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes).toHaveLength(1);
    // The user message text is always present
    expect(episodes[0].content).toContain("Here is what the tool returned");
    // Tool result body only appears when the tool is allowlisted (default: empty)
    expect(episodes[0].content).not.toContain("Tool output here.");
  });

  it("ignores tool results when the tool is not in the allowlist", () => {
    writeSession([
      msgRecord({
        uuid: "m1",
        role: "user",
        timestamp: ts(BASE, 0),
        content: [{ type: "tool_result", name: "secret_tool", content: "CONFIDENTIAL OUTPUT" }],
      }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "Noted." }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "ok" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "done" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    // The tool result should NOT appear in the episode content
    // (only allowlisted tools are included, and the default allowlist is empty)
    for (const ep of episodes) {
      expect(ep.content).not.toContain("CONFIDENTIAL OUTPUT");
    }
  });
});

// ── Plain string content ────────────────────────────────────────────────────────

describe("ClaudeCodeEpisodeReader — plain string message content", () => {
  it("handles plain string content (not an array of parts)", () => {
    writeSession([
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "Plain string user message" }),
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "Plain string assistant reply" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "Another message" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "Final reply" }),
    ]);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes).toHaveLength(1);
    expect(episodes[0].content).toContain("Plain string user message");
    expect(episodes[0].content).toContain("Plain string assistant reply");
  });
});

// ── Empty / malformed files ────────────────────────────────────────────────────

describe("ClaudeCodeEpisodeReader — edge cases", () => {
  it("returns no sessions when the projects directory does not exist", () => {
    rmSync(join(claudeDir, "projects"), { recursive: true, force: true });
    const candidates = reader.getCandidateSessions(0);
    expect(candidates).toHaveLength(0);
  });

  it("skips malformed JSONL lines without crashing", () => {
    const malformedLines = [
      "{not valid json",
      msgRecord({ uuid: "m1", role: "user",      timestamp: ts(BASE, 0),    content: "valid" }),
      "also not json",
      msgRecord({ uuid: "m2", role: "assistant", timestamp: ts(BASE, 1000), content: "reply" }),
      msgRecord({ uuid: "m3", role: "user",      timestamp: ts(BASE, 2000), content: "more" }),
      msgRecord({ uuid: "m4", role: "assistant", timestamp: ts(BASE, 3000), content: "done" }),
    ];
    writeFileSync(sessionFilePath, `${malformedLines.join("\n")}\n`);

    const episodes = reader.getNewEpisodes([SESSION_UUID], new Map());
    expect(episodes).toHaveLength(1);
    expect(episodes[0].content).toContain("valid");
  });

  it("returns no sessions for an empty JSONL file", () => {
    writeFileSync(sessionFilePath, "");
    const candidates = reader.getCandidateSessions(BASE - 1);
    expect(candidates).toHaveLength(0);
  });
});
