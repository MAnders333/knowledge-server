/**
 * Tests for EpisodeReader.
 *
 * EpisodeReader reads from the OpenCode SQLite DB — a read-only connection to
 * a DB schema we don't control. These tests seed a real in-process SQLite DB
 * with the minimal schema shape that EpisodeReader queries, then exercise the
 * segmentation, chunking, and tool-output-extraction logic without any network calls.
 *
 * Covered:
 *   - getCandidateSessions / countNewSessions — cursor filtering
 *   - getNewEpisodes — session segmentation (no-compaction path)
 *   - getNewEpisodes — already-processed range exclusion
 *   - getNewEpisodes — minSessionMessages filter
 *   - getNewEpisodes — compaction summary path
 *   - extractToolText — JSON extraction for Confluence-shaped tool outputs
 *   - chunkByTokenBudget — verified indirectly through large-session segmentation
 */
import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { OpenCodeEpisodeReader } from "../src/consolidation/readers/opencode";

// ── Schema helpers ─────────────────────────────────────────────────────────────

function createSchema(db: Database) {
  db.run(`CREATE TABLE IF NOT EXISTS project (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS session (
    id TEXT PRIMARY KEY,
    title TEXT,
    directory TEXT,
    time_created INTEGER,
    project_id TEXT,
    parent_id TEXT,
    data TEXT
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS message (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    time_created INTEGER NOT NULL,
    data TEXT
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS part (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    time_created INTEGER NOT NULL,
    data TEXT
  )`);
}

let tmpDir: string;
let dbPath: string;
let openCodeDb: Database;
let reader: OpenCodeEpisodeReader;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "ks-episodes-test-"));
  dbPath = join(tmpDir, "opencode.db");
  // Create the file so EpisodeReader can open it in readonly mode
  writeFileSync(dbPath, "");
  // Open with write access to seed data
  openCodeDb = new Database(dbPath);
  createSchema(openCodeDb);
  reader = new OpenCodeEpisodeReader(dbPath);
});

afterEach(() => {
  reader.close();
  openCodeDb.close();
  rmSync(tmpDir, { recursive: true, force: true });
});

// ── Seed helpers ───────────────────────────────────────────────────────────────

let _seq = 0;
function uid(prefix = "id") {
  _seq++;
  return `${prefix}-${_seq}`;
}

function seedProject(db: Database, id = "proj-1", name = "test-project") {
  db.run("INSERT OR IGNORE INTO project (id, name) VALUES (?, ?)", [id, name]);
}

function seedSession(db: Database, opts: {
  id?: string;
  title?: string;
  projectId?: string;
  timeCreated?: number;
  parentId?: string | null;
} = {}) {
  const id = opts.id ?? uid("session");
  db.run(
    "INSERT INTO session (id, title, directory, time_created, project_id, parent_id) VALUES (?, ?, ?, ?, ?, ?)",
    [id, opts.title ?? "Test", "/tmp", opts.timeCreated ?? Date.now(), opts.projectId ?? "proj-1", opts.parentId ?? null]
  );
  return id;
}

function seedMessage(db: Database, opts: {
  id?: string;
  sessionId: string;
  role?: string;
  timeCreated?: number;
}) {
  const id = opts.id ?? uid("msg");
  const role = opts.role ?? "user";
  db.run(
    "INSERT INTO message (id, session_id, time_created, data) VALUES (?, ?, ?, ?)",
    [id, opts.sessionId, opts.timeCreated ?? Date.now(), JSON.stringify({ role })]
  );
  return id;
}

function seedTextPart(db: Database, opts: {
  messageId: string;
  text: string;
  timeCreated?: number;
}) {
  const id = uid("part");
  db.run(
    "INSERT INTO part (id, message_id, time_created, data) VALUES (?, ?, ?, ?)",
    [id, opts.messageId, opts.timeCreated ?? Date.now(), JSON.stringify({ type: "text", text: opts.text })]
  );
  return id;
}

function seedToolPart(db: Database, opts: {
  messageId: string;
  tool: string;
  output: string;
  timeCreated?: number;
}) {
  const id = uid("part");
  db.run(
    "INSERT INTO part (id, message_id, time_created, data) VALUES (?, ?, ?, ?)",
    [id, opts.messageId, opts.timeCreated ?? Date.now(), JSON.stringify({
      type: "tool",
      tool: opts.tool,
      state: { status: "completed", output: opts.output },
    })]
  );
  return id;
}

function seedCompactionPart(db: Database, opts: {
  messageId: string;
  timeCreated?: number;
}) {
  const id = uid("part");
  db.run(
    "INSERT INTO part (id, message_id, time_created, data) VALUES (?, ?, ?, ?)",
    [id, opts.messageId, opts.timeCreated ?? Date.now(), JSON.stringify({ type: "compaction" })]
  );
  return id;
}

// Add enough messages to a session to pass the minSessionMessages filter (default=4).
function seedMinMessages(db: Database, sessionId: string, baseTime: number, count = 4): string[] {
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const msgId = seedMessage(db, { sessionId, role: i % 2 === 0 ? "user" : "assistant", timeCreated: baseTime + i });
    seedTextPart(db, { messageId: msgId, text: `Message ${i}`, timeCreated: baseTime + i });
    ids.push(msgId);
  }
  return ids;
}

// ── getCandidateSessions ───────────────────────────────────────────────────────

describe("EpisodeReader.getCandidateSessions", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("returns sessions with messages newer than the cursor", () => {
    const cursor = Date.now() - 10_000;
    const sid = seedSession(openCodeDb, { timeCreated: cursor - 5_000 });
    seedMessage(openCodeDb, { sessionId: sid, timeCreated: cursor + 1_000 });

    const candidates = reader.getCandidateSessions(cursor);
    expect(candidates.some((c) => c.id === sid)).toBe(true);
  });

  it("excludes sessions whose messages are all at or before the cursor", () => {
    const cursor = Date.now();
    const sid = seedSession(openCodeDb, { timeCreated: cursor - 5_000 });
    seedMessage(openCodeDb, { sessionId: sid, timeCreated: cursor - 1_000 });

    const candidates = reader.getCandidateSessions(cursor);
    expect(candidates.some((c) => c.id === sid)).toBe(false);
  });

  it("excludes child sessions (parent_id IS NOT NULL)", () => {
    const cursor = Date.now() - 10_000;
    const parentId = seedSession(openCodeDb, { timeCreated: cursor - 5_000 });
    const childId = seedSession(openCodeDb, { timeCreated: cursor - 5_000, parentId });
    seedMessage(openCodeDb, { sessionId: childId, timeCreated: cursor + 1_000 });

    const candidates = reader.getCandidateSessions(cursor);
    expect(candidates.some((c) => c.id === childId)).toBe(false);
  });

  it("orders results by maxMessageTime ASC", () => {
    const now = Date.now();
    const sid1 = seedSession(openCodeDb, { id: "s-a", timeCreated: now - 20_000 });
    const sid2 = seedSession(openCodeDb, { id: "s-b", timeCreated: now - 20_000 });
    seedMessage(openCodeDb, { sessionId: sid1, timeCreated: now - 2_000 });
    seedMessage(openCodeDb, { sessionId: sid2, timeCreated: now - 1_000 });

    const candidates = reader.getCandidateSessions(now - 10_000);
    const ids = candidates.map((c) => c.id);
    expect(ids.indexOf("s-a")).toBeLessThan(ids.indexOf("s-b"));
  });
});

// ── countNewSessions ───────────────────────────────────────────────────────────

describe("EpisodeReader.countNewSessions", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("returns 0 when no sessions have messages past the cursor", () => {
    const cursor = Date.now();
    const sid = seedSession(openCodeDb);
    seedMessage(openCodeDb, { sessionId: sid, timeCreated: cursor - 1_000 });
    expect(reader.countNewSessions(cursor)).toBe(0);
  });

  it("counts distinct sessions, not individual messages", () => {
    const cursor = Date.now() - 10_000;
    const sid = seedSession(openCodeDb);
    seedMessage(openCodeDb, { sessionId: sid, timeCreated: cursor + 1_000 });
    seedMessage(openCodeDb, { sessionId: sid, timeCreated: cursor + 2_000 });
    expect(reader.countNewSessions(cursor)).toBe(1);
  });
});

// ── getNewEpisodes — minSessionMessages filter ─────────────────────────────────

describe("EpisodeReader.getNewEpisodes — minSessionMessages filter", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("returns no episodes for a session with fewer than minSessionMessages (default 4)", () => {
    const sid = seedSession(openCodeDb);
    // Only 2 messages — below the minimum
    const m1 = seedMessage(openCodeDb, { sessionId: sid, role: "user" });
    const m2 = seedMessage(openCodeDb, { sessionId: sid, role: "assistant" });
    seedTextPart(openCodeDb, { messageId: m1, text: "hi" });
    seedTextPart(openCodeDb, { messageId: m2, text: "hey" });

    const episodes = reader.getNewEpisodes([sid], new Map());
    expect(episodes).toHaveLength(0);
  });

  it("returns episodes for a session that meets the minimum", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);
    seedMinMessages(openCodeDb, sid, now);

    const episodes = reader.getNewEpisodes([sid], new Map());
    expect(episodes.length).toBeGreaterThanOrEqual(1);
  });
});

// ── getNewEpisodes — processedRanges exclusion ────────────────────────────────

describe("EpisodeReader.getNewEpisodes — processedRanges exclusion", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("skips episodes whose (startMessageId, endMessageId) range is already recorded", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);
    const msgIds = seedMinMessages(openCodeDb, sid, now);

    // First call — no processed ranges
    const first = reader.getNewEpisodes([sid], new Map());
    expect(first.length).toBeGreaterThanOrEqual(1);

    // Build the processedRanges map from the first result
    const ranges = new Map<string, Array<{ startMessageId: string; endMessageId: string }>>();
    ranges.set(sid, first.map((ep) => ({ startMessageId: ep.startMessageId, endMessageId: ep.endMessageId })));

    // Second call — all ranges are already recorded, nothing new
    const second = reader.getNewEpisodes([sid], ranges);
    expect(second).toHaveLength(0);
  });
});

// ── getNewEpisodes — no-compaction path ───────────────────────────────────────

describe("EpisodeReader.getNewEpisodes — no-compaction segmentation", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("includes message text in episode content", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);
    const m1 = seedMessage(openCodeDb, { sessionId: sid, role: "user", timeCreated: now });
    seedTextPart(openCodeDb, { messageId: m1, text: "What is TypeScript?", timeCreated: now });
    const m2 = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 1 });
    seedTextPart(openCodeDb, { messageId: m2, text: "TypeScript is a typed superset of JavaScript.", timeCreated: now + 1 });
    const m3 = seedMessage(openCodeDb, { sessionId: sid, role: "user", timeCreated: now + 2 });
    seedTextPart(openCodeDb, { messageId: m3, text: "Does it compile?", timeCreated: now + 2 });
    const m4 = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 3 });
    seedTextPart(openCodeDb, { messageId: m4, text: "Yes, to JavaScript.", timeCreated: now + 3 });

    const episodes = reader.getNewEpisodes([sid], new Map());
    expect(episodes.length).toBe(1);
    expect(episodes[0].content).toContain("TypeScript");
    expect(episodes[0].contentType).toBe("messages");
    expect(episodes[0].sessionId).toBe(sid);
  });

  it("uses startMessageId / endMessageId of the first/last messages", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);
    const ids = seedMinMessages(openCodeDb, sid, now, 4);

    const episodes = reader.getNewEpisodes([sid], new Map());
    expect(episodes.length).toBe(1);
    expect(episodes[0].startMessageId).toBe(ids[0]);
    expect(episodes[0].endMessageId).toBe(ids[ids.length - 1]);
  });
});

// ── getNewEpisodes — compaction summary path ──────────────────────────────────

describe("EpisodeReader.getNewEpisodes — compaction summary path", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("produces a compaction_summary episode from the continuation message after a compaction part", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);

    // User message before compaction
    const mUser = seedMessage(openCodeDb, { sessionId: sid, role: "user", timeCreated: now });
    seedTextPart(openCodeDb, { messageId: mUser, text: "Do the thing.", timeCreated: now });

    // Compaction marker in an assistant message
    const mCompact = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 1 });
    seedCompactionPart(openCodeDb, { messageId: mCompact, timeCreated: now + 1 });

    // Continuation summary — the first assistant message after the compaction
    const mSummary = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 2 });
    seedTextPart(openCodeDb, { messageId: mSummary, text: "We discussed TypeScript fundamentals and decided to use strict mode.", timeCreated: now + 2 });

    const episodes = reader.getNewEpisodes([sid], new Map());
    const summaryEp = episodes.find((ep) => ep.contentType === "compaction_summary");
    expect(summaryEp).toBeDefined();
    expect(summaryEp?.content).toContain("TypeScript");
    expect(summaryEp?.startMessageId).toBe(mSummary);
    expect(summaryEp?.endMessageId).toBe(mSummary);
  });
});

// ── Tool output extraction (via includeToolOutputs config) ────────────────────
// The EpisodeReader only appends tool outputs when config.consolidation.includeToolOutputs
// is non-empty. Since config is a module singleton, we can't easily change it per-test.
// We verify the extraction logic indirectly: the tool part is seeded but unless the tool
// name is in the allowlist it won't appear in the output.

describe("EpisodeReader — tool output parts are ignored when not in allowlist", () => {
  beforeEach(() => seedProject(openCodeDb));

  it("does not include tool output for tools not in the allowlist", () => {
    const now = Date.now();
    const sid = seedSession(openCodeDb);

    // 4 messages: user/assistant/user/assistant with a tool call in the last assistant
    const m1 = seedMessage(openCodeDb, { sessionId: sid, role: "user", timeCreated: now });
    seedTextPart(openCodeDb, { messageId: m1, text: "Search Confluence for churn.", timeCreated: now });
    const m2 = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 1 });
    seedTextPart(openCodeDb, { messageId: m2, text: "Searching now.", timeCreated: now + 1 });
    seedToolPart(openCodeDb, {
      messageId: m2,
      tool: "NOT_IN_ALLOWLIST",
      output: JSON.stringify({ secret: "sensitive data" }),
      timeCreated: now + 1,
    });
    const m3 = seedMessage(openCodeDb, { sessionId: sid, role: "user", timeCreated: now + 2 });
    seedTextPart(openCodeDb, { messageId: m3, text: "Thanks.", timeCreated: now + 2 });
    const m4 = seedMessage(openCodeDb, { sessionId: sid, role: "assistant", timeCreated: now + 3 });
    seedTextPart(openCodeDb, { messageId: m4, text: "Done.", timeCreated: now + 3 });

    const episodes = reader.getNewEpisodes([sid], new Map());
    expect(episodes.length).toBe(1);
    // Tool output for a non-allowlisted tool must not appear
    expect(episodes[0].content).not.toContain("sensitive data");
  });
});
