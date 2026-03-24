/**
 * Tests for the episode uploader daemon.
 *
 * Tests cover:
 * - pending_episodes DB operations (insert, fetch, delete)
 * - daemon_cursor operations (get, update, local-only semantics)
 * - EpisodeUploader core logic (upload, cursor advance, dedup)
 * - PendingEpisodesReader (prepare, getCandidateSessions, getNewEpisodes, afterConsolidated)
 */
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EpisodeUploader } from "../src/daemon/uploader";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { DaemonDB } from "../src/db/daemon/index";
import { ServerStateDB } from "../src/db/state/index";
import { PendingEpisodesReader } from "../src/consolidation/readers/pending";
import type { IEpisodeReader, PendingEpisode } from "../src/types";

// ── helpers ──────────────────────────────────────────────────────────────────

function makePendingEpisode(
	overrides: Partial<PendingEpisode> = {},
): PendingEpisode {
	const now = Date.now();
	return {
		id: "ep-test-1",
		userId: "alice",
		source: "opencode",
		sessionId: "session-abc",
		startMessageId: "msg-start",
		endMessageId: "msg-end",
		sessionTitle: "Test Session",
		projectName: "my-project",
		directory: "/home/alice/work/project",
		content: "User asked about TypeScript types.",
		contentType: "messages",
		timeCreated: now - 5000,
		maxMessageTime: now,
		approxTokens: 100,
		uploadedAt: now,
		...overrides,
	};
}

function makeMockReader(
	source: string,
	sessions: Array<{ id: string; maxMessageTime: number }>,
	episodes: import("../src/types").Episode[],
): IEpisodeReader {
	return {
		source,
		getCandidateSessions: () => sessions,
		countNewSessions: () => sessions.length,
		getNewEpisodes: () => episodes,
		close: () => {},
	};
}

// ── setup ─────────────────────────────────────────────────────────────────────

let tempDir: string;
let db: ServerStateDB;
let daemonDb: DaemonDB;

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), "ks-daemon-test-"));
	db = new ServerStateDB(join(tempDir, "state.db"));
	daemonDb = new DaemonDB(join(tempDir, "daemon.db"));
});

afterEach(async () => {
	mock.restore();
	await db.close();
	daemonDb.close();
	rmSync(tempDir, { recursive: true, force: true });
});

// ── pending_episodes DB operations ────────────────────────────────────────────

describe("pending_episodes DB operations", () => {
	it("inserts and retrieves pending episodes", async () => {
		const ep = makePendingEpisode();
		await db.insertPendingEpisode(ep);

		const rows = await db.getPendingEpisodes(0);
		expect(rows).toHaveLength(1);
		expect(rows[0].id).toBe("ep-test-1");
		expect(rows[0].userId).toBe("alice");
		expect(rows[0].content).toBe("User asked about TypeScript types.");
	});

	it("returns all episodes regardless of source or userId", async () => {
		await db.insertPendingEpisode(
			makePendingEpisode({ id: "ep-1", source: "opencode", userId: "alice" }),
		);
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-2",
				source: "claude-code",
				userId: "alice",
			}),
		);
		await db.insertPendingEpisode(
			makePendingEpisode({ id: "ep-3", source: "opencode", userId: "bob" }),
		);

		const all = await db.getPendingEpisodes(0);
		expect(all).toHaveLength(3);
	});

	it("filters by afterMaxMessageTime", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({ id: "old", maxMessageTime: now - 10000 }),
		);
		await db.insertPendingEpisode(
			makePendingEpisode({ id: "new", maxMessageTime: now }),
		);

		const rows = await db.getPendingEpisodes(now - 5000);
		expect(rows).toHaveLength(1);
		expect(rows[0].id).toBe("new");
	});

	it("is idempotent — duplicate inserts are ignored", async () => {
		const ep = makePendingEpisode();
		await db.insertPendingEpisode(ep);
		await db.insertPendingEpisode(ep); // same id

		const rows = await db.getPendingEpisodes(0);
		expect(rows).toHaveLength(1);
	});

	it("deletes episodes by id", async () => {
		await db.insertPendingEpisode(makePendingEpisode({ id: "ep-1" }));
		await db.insertPendingEpisode(makePendingEpisode({ id: "ep-2" }));

		await db.deletePendingEpisodes(["ep-1"]);

		const rows = await db.getPendingEpisodes(0);
		expect(rows).toHaveLength(1);
		expect(rows[0].id).toBe("ep-2");
	});

	it("deletePendingEpisodes is a no-op on empty array", async () => {
		await db.insertPendingEpisode(makePendingEpisode());
		await db.deletePendingEpisodes([]);
		const rows = await db.getPendingEpisodes(0);
		expect(rows).toHaveLength(1);
	});
});

// ── daemon_cursor ─────────────────────────────────────────────────────────────

describe("daemon_cursor operations", () => {
	it("returns zero-state cursor for unknown source", async () => {
		const cursor = await daemonDb.getDaemonCursor("opencode");
		expect(cursor.source).toBe("opencode");
		expect(cursor.lastMessageTimeCreated).toBe(0);
		expect(cursor.lastUploadedAt).toBe(0);
	});

	it("updates and retrieves cursor", async () => {
		await daemonDb.updateDaemonCursor("opencode", {
			lastMessageTimeCreated: 12345,
			lastUploadedAt: 67890,
		});
		const cursor = await daemonDb.getDaemonCursor("opencode");
		expect(cursor.lastMessageTimeCreated).toBe(12345);
		expect(cursor.lastUploadedAt).toBe(67890);
	});

	it("cursors are independent per source", async () => {
		await daemonDb.updateDaemonCursor("opencode", {
			lastMessageTimeCreated: 1000,
		});
		await daemonDb.updateDaemonCursor("claude-code", {
			lastMessageTimeCreated: 9999,
		});

		const oc = await daemonDb.getDaemonCursor("opencode");
		const cc = await daemonDb.getDaemonCursor("claude-code");
		expect(oc.lastMessageTimeCreated).toBe(1000);
		expect(cc.lastMessageTimeCreated).toBe(9999);
	});
});

// ── EpisodeUploader ───────────────────────────────────────────────────────────

describe("EpisodeUploader.upload", () => {
	it("uploads episodes to pending_episodes and advances daemon cursor", async () => {
		const now = Date.now();
		const reader = makeMockReader(
			"opencode",
			[{ id: "session-1", maxMessageTime: now }],
			[
				{
					source: "opencode",
					sessionId: "session-1",
					startMessageId: "msg-a",
					endMessageId: "msg-b",
					sessionTitle: "Test",
					projectName: "project",
					directory: "/home/alice/work",
					timeCreated: now - 1000,
					maxMessageTime: now,
					content: "session content",
					contentType: "messages",
					approxTokens: 50,
				},
			],
		);

		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice");
		const result = await uploader.upload();

		expect(result.episodesUploaded).toBe(1);
		expect(result.sessionsProcessed).toBe(1);

		const pending = await db.getPendingEpisodes(0);
		expect(pending).toHaveLength(1);
		expect(pending[0].sessionId).toBe("session-1");
		expect(pending[0].userId).toBe("alice");

		const cursor = await daemonDb.getDaemonCursor("opencode");
		expect(cursor.lastMessageTimeCreated).toBeGreaterThan(0);
	});

	it("getUploadedEpisodeRanges includes pending_episodes so daemon skips already-staged ranges", async () => {
		// Verifies that getUploadedEpisodeRanges (daemon path) returns ranges from pending_episodes,
		// not just consolidated_episode. This is the mechanism that replaced the old
		// pendingSet check in uploader.ts — readers use processedRanges for overlap
		// detection, so pending rows must appear there.
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "pre-existing",
				source: "opencode",
				userId: "alice",
				sessionId: "session-dedup",
				startMessageId: "msg-a",
				endMessageId: "msg-b",
				maxMessageTime: now,
			}),
		);

		const ranges = await db.getUploadedEpisodeRanges(["session-dedup"]);
		const sessionRanges = ranges.get("session-dedup");
		expect(sessionRanges).toBeDefined();
		expect(sessionRanges).toHaveLength(1);
		expect(sessionRanges![0].source).toBe("opencode");
		expect(sessionRanges![0].startMessageId).toBe("msg-a");
		expect(sessionRanges![0].endMessageId).toBe("msg-b");

		// Confirm it still returns consolidated_episode ranges too
		await db.recordEpisode(
			"opencode",
			"session-consolidated",
			"msg-x",
			"msg-y",
			"messages",
			1,
		);
		const ranges2 = await db.getUploadedEpisodeRanges(["session-consolidated"]);
		expect(ranges2.get("session-consolidated")).toHaveLength(1);
	});

	it("skips a failed reader and continues with others", async () => {
		const now = Date.now();
		const failingReader: IEpisodeReader = {
			source: "opencode",
			getCandidateSessions: () => {
				throw new Error("DB read failed");
			},
			countNewSessions: () => 0,
			getNewEpisodes: () => [],
			close: () => {},
		};
		const workingReader = makeMockReader(
			"claude-code",
			[{ id: "session-cc-1", maxMessageTime: now }],
			[
				{
					source: "claude-code",
					sessionId: "session-cc-1",
					startMessageId: "msg-x",
					endMessageId: "msg-y",
					sessionTitle: "CC Session",
					projectName: "project",
					directory: "/home/alice",
					timeCreated: now - 1000,
					maxMessageTime: now,
					content: "claude content",
					contentType: "messages",
					approxTokens: 30,
				},
			],
		);

		const uploader = new EpisodeUploader(
			[failingReader, workingReader],
			db,
			daemonDb,
			"alice",
		);
		const result = await uploader.upload();

		// opencode failed but claude-code succeeded
		expect(result.episodesUploaded).toBe(1);
		const pending = await db.getPendingEpisodes(0);
		expect(pending).toHaveLength(1);
		expect(pending[0].source).toBe("claude-code");
	});
});

// ── PendingEpisodesReader ─────────────────────────────────────────────────────

describe("PendingEpisodesReader", () => {
	it("prepare loads candidates and getNewEpisodes returns episodes", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-r1",
				source: "opencode",
				userId: "alice",
				sessionId: "session-r1",
				maxMessageTime: now,
			}),
		);

		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0);

		const candidates = reader.getCandidateSessions(0);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe("session-r1");

		const episodes = reader.getNewEpisodes(["session-r1"], new Map());
		expect(episodes).toHaveLength(1);
		expect(episodes[0].sessionId).toBe("session-r1");
		expect(episodes[0].source).toBe("opencode");
	});

	it("source name is 'pending'", () => {
		const reader = new PendingEpisodesReader(db);
		expect(reader.source).toBe("pending");
	});

	it("getNewEpisodes excludes already-processed ranges", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-proc",
				source: "opencode",
				userId: "alice",
				sessionId: "session-p1",
				startMessageId: "msg-start",
				endMessageId: "msg-end",
				maxMessageTime: now,
			}),
		);

		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0);

		// Mark as already processed — must include source for the match
		const processedRanges = new Map([
			[
				"session-p1",
				[
					{
						source: "opencode",
						startMessageId: "msg-start",
						endMessageId: "msg-end",
					},
				],
			],
		]);

		const episodes = reader.getNewEpisodes(["session-p1"], processedRanges);
		expect(episodes).toHaveLength(0);
	});

	it("getNewEpisodes does NOT exclude ranges from a different source", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-src",
				source: "opencode",
				userId: "alice",
				sessionId: "session-s1",
				startMessageId: "msg-start",
				endMessageId: "msg-end",
				maxMessageTime: now,
			}),
		);

		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0);

		// Processed range is for a different source — should not suppress opencode episode
		const processedRanges = new Map([
			[
				"session-s1",
				[
					{
						source: "claude-code",
						startMessageId: "msg-start",
						endMessageId: "msg-end",
					},
				],
			],
		]);

		const episodes = reader.getNewEpisodes(["session-s1"], processedRanges);
		expect(episodes).toHaveLength(1);
	});

	it("afterConsolidated deletes processed rows from pending_episodes", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-del-1",
				source: "opencode",
				userId: "alice",
				sessionId: "session-del-1",
				maxMessageTime: now,
			}),
		);
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-del-2",
				source: "opencode",
				userId: "alice",
				sessionId: "session-del-2",
				maxMessageTime: now,
			}),
		);

		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0);

		await reader.afterConsolidated(["session-del-1"]);

		const remaining = await db.getPendingEpisodes(0);
		expect(remaining).toHaveLength(1);
		expect(remaining[0].id).toBe("ep-del-2");
	});

	it("countNewSessions returns 1 (conservative) before prepare() is called", () => {
		const reader = new PendingEpisodesReader(db);
		// prepare() not called — should signal "there may be pending episodes"
		expect(reader.countNewSessions(0)).toBe(1);
	});

	it("countNewSessions returns 0 after prepare() when table is empty", async () => {
		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0); // table is empty
		// After prepare(), genuinely empty result must return 0, not 1
		expect(reader.countNewSessions(0)).toBe(0);
	});

	it("countNewSessions returns correct count after prepare() with episodes", async () => {
		const now = Date.now();
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-cnt-1",
				source: "opencode",
				userId: "alice",
				sessionId: "s1",
				maxMessageTime: now,
			}),
		);
		await db.insertPendingEpisode(
			makePendingEpisode({
				id: "ep-cnt-2",
				source: "opencode",
				userId: "alice",
				sessionId: "s2",
				maxMessageTime: now + 1,
			}),
		);

		const reader = new PendingEpisodesReader(db);
		await reader.prepare(0);
		expect(reader.countNewSessions(0)).toBe(2);
		// Filtered by cursor
		expect(reader.countNewSessions(now)).toBe(1);
	});
});
