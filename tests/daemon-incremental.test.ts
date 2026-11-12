/**
 * Incremental upload (ARC-07), poison-pill handling (SD-10) and first-run
 * lookback (PV-10) at the uploader level.
 *
 * The reader is an in-memory fake that uses the shared episodesFromMessages
 * helper, so these tests cover the same watermark and tail rules the real
 * readers rely on, against a real staging DB and daemon DB.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import {
	DEFAULT_INITIAL_LOOKBACK_DAYS,
	initialCursorMs,
	parseInitialLookbackDays,
} from "../src/daemon/lookback";
import {
	type EpisodeBase,
	dropProcessed,
	episodesFromMessages,
	recordSkip,
	takeSkipCount,
} from "../src/daemon/readers/shared";
import { EpisodeUploader } from "../src/daemon/uploader";
import { DaemonDB } from "../src/db/daemon/index";
import { ServerStateDB } from "../src/db/state/index";
import type {
	Episode,
	EpisodeMessage,
	IEpisodeReader,
	ProcessedRange,
} from "../src/types";

const SOURCE = "fake-source";
const MIN = config.consolidation.minSessionMessages;
const HOUR_MS = 60 * 60 * 1000;

interface FakeSession {
	id: string;
	messages: EpisodeMessage[];
}

/**
 * In-memory reader. `getNewEpisodes` mirrors what real readers do: chunk only
 * the tail after the processed watermark via episodesFromMessages.
 */
function fakeReader(sessions: Map<string, FakeSession>): IEpisodeReader & {
	sinceArgs: number[];
} {
	const sinceArgs: number[] = [];
	return {
		source: SOURCE,
		sinceArgs,
		getCandidateSessions(since: number) {
			sinceArgs.push(since);
			return [...sessions.values()]
				.map((s) => ({
					id: s.id,
					maxMessageTime: s.messages[s.messages.length - 1].timestamp,
				}))
				.filter((c) => c.maxMessageTime > since)
				.sort((a, b) => a.maxMessageTime - b.maxMessageTime);
		},
		countNewSessions: () => sessions.size,
		getNewEpisodes(ids: string[], ranges: Map<string, ProcessedRange[]>) {
			const out: Episode[] = [];
			for (const id of ids) {
				const session = sessions.get(id);
				if (!session) continue;
				const processed = ranges.get(id) ?? [];
				const base: EpisodeBase = {
					source: SOURCE,
					sessionId: id,
					sessionTitle: "title",
					projectName: "project",
					directory: "/work/project",
					timeCreated: session.messages[0].timestamp,
				};
				out.push(
					...dropProcessed(
						episodesFromMessages(base, session.messages, {
							processedRanges: processed,
						}),
						processed,
					),
				);
			}
			return out;
		},
		close() {},
	};
}

function msgs(ids: string[], startMs: number, stepMs = 1000): EpisodeMessage[] {
	return ids.map((id, i) => ({
		messageId: id,
		role: i % 2 === 0 ? "user" : "assistant",
		content: `<<${id}>>`,
		timestamp: startMs + i * stepMs,
	}));
}

/** Count how many staged rows contain each marker. */
async function markerCounts(
	db: ServerStateDB,
	ids: string[],
): Promise<Map<string, number>> {
	const rows = await db.getPendingEpisodes(0);
	const counts = new Map<string, number>();
	for (const id of ids) {
		counts.set(id, rows.filter((r) => r.content.includes(`<<${id}>>`)).length);
	}
	return counts;
}

let tempDir: string;
let db: ServerStateDB;
let daemonDb: DaemonDB;

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), "ks-incremental-test-"));
	db = new ServerStateDB(join(tempDir, "state.db"));
	daemonDb = new DaemonDB(join(tempDir, "daemon.db"));
});

afterEach(async () => {
	await db.close();
	daemonDb.close();
	rmSync(tempDir, { recursive: true, force: true });
});

describe("EpisodeUploader — session growth (ARC-07)", () => {
	it("a session that grows by k messages uploads only the tail, exactly once", async () => {
		// Recent timestamps: the session is active, so a tail below the minimum
		// is held. Growth below uses MIN new messages, which is eligible.
		const start = Date.now() - 2 * HOUR_MS;
		const first = Array.from({ length: MIN + 2 }, (_, i) => `a${i}`);
		const sessions = new Map<string, FakeSession>([
			["s1", { id: "s1", messages: msgs(first, start) }],
		]);
		const reader = fakeReader(sessions);
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: 0,
		});

		await uploader.upload();

		const grown = Array.from({ length: MIN }, (_, i) => `b${i}`);
		sessions
			.get("s1")
			?.messages.push(...msgs(grown, start + (first.length + 10) * 1000));
		await uploader.upload();
		// No change: nothing new should be staged.
		const idle = await uploader.upload();
		expect(idle.episodesUploaded).toBe(0);

		const counts = await markerCounts(db, [...first, ...grown]);
		for (const [id, n] of counts) {
			expect({ id, n }).toEqual({ id, n: 1 });
		}
	});

	it("a tail smaller than the minimum is held while the session is active", async () => {
		const start = Date.now() - 60_000;
		const first = Array.from({ length: MIN + 1 }, (_, i) => `h${i}`);
		const sessions = new Map<string, FakeSession>([
			["s1", { id: "s1", messages: msgs(first, start) }],
		]);
		const uploader = new EpisodeUploader(
			[fakeReader(sessions)],
			db,
			daemonDb,
			"alice",
			{ initialLookbackDays: 0 },
		);
		await uploader.upload();

		// One more message: tail is 1 < MIN and the session is still active.
		sessions.get("s1")?.messages.push(...msgs(["h-late"], Date.now() - 1000));
		const result = await uploader.upload();

		expect(result.episodesUploaded).toBe(0);
		const counts = await markerCounts(db, ["h-late"]);
		expect(counts.get("h-late")).toBe(0);
	});

	it("a held tail is flushed once the session has gone idle", async () => {
		const start = Date.now() - 3 * HOUR_MS;
		const first = Array.from({ length: MIN + 1 }, (_, i) => `i${i}`);
		const sessions = new Map<string, FakeSession>([
			["s1", { id: "s1", messages: msgs(first, start) }],
		]);
		const uploader = new EpisodeUploader(
			[fakeReader(sessions)],
			db,
			daemonDb,
			"alice",
			{ initialLookbackDays: 0 },
		);
		await uploader.upload();

		// Tail of one message, last activity two hours ago: idle, so it is flushed.
		sessions.get("s1")?.messages.push(...msgs(["i-late"], start + 1_000_000));
		const result = await uploader.upload();

		expect(result.episodesUploaded).toBe(1);
		const counts = await markerCounts(db, ["i-late"]);
		expect(counts.get("i-late")).toBe(1);
	});

	it("records the per-session watermark in the daemon DB", async () => {
		const start = Date.now() - 3 * HOUR_MS;
		const ids = Array.from({ length: MIN }, (_, i) => `w${i}`);
		const sessions = new Map<string, FakeSession>([
			["s1", { id: "s1", messages: msgs(ids, start) }],
		]);
		const uploader = new EpisodeUploader(
			[fakeReader(sessions)],
			db,
			daemonDb,
			"alice",
			{ initialLookbackDays: 0 },
		);
		await uploader.upload();

		const cursors = await daemonDb.getSessionCursors(SOURCE, ["s1"]);
		expect(cursors.get("s1")).toBe(ids[ids.length - 1]);
	});
});

describe("EpisodeUploader — poison pills (SD-10)", () => {
	it("strips NUL bytes from staged text instead of failing the insert", async () => {
		const start = Date.now() - 3 * HOUR_MS;
		const ids = Array.from({ length: MIN }, (_, i) => `n${i}`);
		const messages = msgs(ids, start);
		messages[1] = { ...messages[1], content: "bad\u0000text" };
		const sessions = new Map<string, FakeSession>([
			["s1", { id: "s1", messages }],
		]);
		const uploader = new EpisodeUploader(
			[fakeReader(sessions)],
			db,
			daemonDb,
			"alice",
			{ initialLookbackDays: 0 },
		);

		const result = await uploader.upload();

		expect(result.episodesUploaded).toBe(1);
		const rows = await db.getPendingEpisodes(0);
		expect(rows.every((r) => !r.content.includes("\u0000"))).toBe(true);
		expect(rows.some((r) => r.content.includes("badtext"))).toBe(true);
	});

	it("counts an unreadable session in the cycle summary and still uploads the rest", async () => {
		const start = Date.now() - 3 * HOUR_MS;
		const ids = Array.from({ length: MIN }, (_, i) => `ok${i}`);
		const sessions = new Map<string, FakeSession>([
			["good", { id: "good", messages: msgs(ids, start) }],
		]);
		const base = fakeReader(sessions);
		const reader: IEpisodeReader = {
			...base,
			getCandidateSessions(since: number) {
				recordSkip(SOURCE, "bad-session", "unreadable: boom");
				return base.getCandidateSessions(since);
			},
		};
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: 0,
		});

		const result = await uploader.upload();

		expect(result.episodesUploaded).toBe(1);
		expect(result.itemsSkipped).toBe(1);
		expect(takeSkipCount(SOURCE)).toBe(0);
	});
});

describe("EpisodeUploader — first-run lookback (PV-10)", () => {
	it("uses now minus the lookback window as the first-run cursor", async () => {
		const sessions = new Map<string, FakeSession>();
		const reader = fakeReader(sessions);
		const before = Date.now();
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: DEFAULT_INITIAL_LOOKBACK_DAYS,
		});

		await uploader.upload();

		expect(reader.sinceArgs).toHaveLength(1);
		const expected = initialCursorMs(DEFAULT_INITIAL_LOOKBACK_DAYS, before);
		expect(Math.abs(reader.sinceArgs[0] - expected)).toBeLessThan(5_000);
	});

	it("starts from 0 when the window is disabled (INITIAL_LOOKBACK_DAYS=0)", async () => {
		const reader = fakeReader(new Map());
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: 0,
		});
		await uploader.upload();
		expect(reader.sinceArgs).toEqual([0]);
	});

	it("uses the stored cursor once the source has run", async () => {
		await daemonDb.updateDaemonCursor(SOURCE, {
			lastMessageTimeCreated: 1_700_000_000_000,
			lastUploadedAt: Date.now(),
		});
		const reader = fakeReader(new Map());
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: 30,
		});
		await uploader.upload();
		expect(reader.sinceArgs).toEqual([1_700_000_000_000]);
	});

	it("parses INITIAL_LOOKBACK_DAYS, defaulting to 30 for unset or invalid values", () => {
		expect(parseInitialLookbackDays(undefined)).toBe(30);
		expect(parseInitialLookbackDays("")).toBe(30);
		expect(parseInitialLookbackDays("0")).toBe(0);
		expect(parseInitialLookbackDays("7")).toBe(7);
		expect(parseInitialLookbackDays("-1")).toBe(30);
		expect(parseInitialLookbackDays("abc")).toBe(30);
		expect(parseInitialLookbackDays("1.5")).toBe(30);
	});
});

describe("DaemonDB — session cursors", () => {
	it("stores, updates and looks up per-session watermarks", async () => {
		await daemonDb.setSessionCursor("claude-code", "s1", "m1");
		await daemonDb.setSessionCursor("claude-code", "s1", "m2");
		await daemonDb.setSessionCursor("codex", "s1", "x9");

		const cursors = await daemonDb.getSessionCursors("claude-code", [
			"s1",
			"unknown",
		]);
		expect(cursors.get("s1")).toBe("m2");
		expect(cursors.has("unknown")).toBe(false);
		expect((await daemonDb.getSessionCursors("codex", ["s1"])).get("s1")).toBe(
			"x9",
		);
	});

	it("resetDaemonCursors clears watermarks along with source cursors", async () => {
		await daemonDb.setSessionCursor(SOURCE, "s1", "m1");
		await daemonDb.updateDaemonCursor(SOURCE, { lastMessageTimeCreated: 5 });
		await daemonDb.resetDaemonCursors();

		expect((await daemonDb.getSessionCursors(SOURCE, ["s1"])).size).toBe(0);
		expect(await daemonDb.hasDaemonCursor(SOURCE)).toBe(false);
	});
});

describe("EpisodeUploader — free-text fields are scrubbed before staging", () => {
	it("redacts a credential in the session title, project and directory", async () => {
		const token = ["ghp_", "d".repeat(36)].join("");
		const start = Date.now() - 2 * HOUR_MS;
		const sessions = new Map<string, FakeSession>([
			[
				"s1",
				{
					id: "s1",
					messages: msgs(
						Array.from({ length: MIN + 2 }, (_, i) => `t${i}`),
						start,
					),
				},
			],
		]);
		const base = fakeReader(sessions);
		const reader = {
			...base,
			getNewEpisodes: (ids: string[], ranges: Map<string, ProcessedRange[]>) =>
				base.getNewEpisodes(ids, ranges).map((ep) => ({
					...ep,
					sessionTitle: `Debug ${token}`,
					projectName: `proj ${token}`,
					directory: `/work/${token}`,
				})),
		};
		const uploader = new EpisodeUploader([reader], db, daemonDb, "alice", {
			initialLookbackDays: 0,
		});

		await uploader.upload();

		const rows = await db.getPendingEpisodes(0);
		expect(rows.length).toBeGreaterThan(0);
		const staged = JSON.stringify(rows);
		expect(staged).not.toContain(token);
		expect(staged).toContain("[REDACTED:github_token]");
	});
});
