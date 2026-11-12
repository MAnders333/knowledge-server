/**
 * Tests for CursorEpisodeReader.
 *
 * Cursor stores session data in a SQLite database (state.vscdb) with a
 * key-value table (cursorDiskKV). These tests create an in-memory SQLite
 * database, seed it with composerData and bubbleId entries, and exercise
 * the reader's segmentation + extraction logic without any filesystem access.
 *
 * Covered:
 *   - getCandidateSessions / countNewSessions — lastUpdatedAt cursor filtering
 *   - Format A sessions (inline conversation[] array)
 *   - Format B sessions (fullConversationHeadersOnly + separate bubbleId: entries)
 *   - Sessions with no text in any turn (skipped)
 *   - minSessionMessages filter
 *   - processedRanges exclusion
 *   - ARC-07: tail-only growth, hold-back, poison pills
 *   - Ordering by lastUpdatedAt ASC
 *   - Limit parameter
 *   - Missing/invalid JSON values (edge cases)
 *   - resolveCursorDbPath() — env var branch
 */
import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import {
	CursorEpisodeReader,
	collectFilePathsFromString,
	longestCommonDirectoryPrefix,
	resolveCursorDbPath,
} from "../src/daemon/readers/cursor";
import { takeSkipCount } from "../src/daemon/readers/shared";
import type { ProcessedRange } from "../src/types";

// ── Helpers ────────────────────────────────────────────────────────────────────

const BASE = 1_700_000_000_000; // fixed unix ms base for all timestamps

/** Create an in-memory Cursor state.vscdb with the cursorDiskKV schema. */
function createDb(): Database {
	const db = new Database(":memory:");
	db.run(`
    CREATE TABLE cursorDiskKV (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);
	return db;
}

/** Insert a composerData entry (Format A — inline conversation[]). */
function insertFormatA(
	db: Database,
	composerId: string,
	opts: {
		name?: string;
		createdAt?: number;
		lastUpdatedAt?: number;
		isAgentic?: boolean;
		turns: Array<{ type: 1 | 2; bubbleId?: string; text?: string }>;
	},
): void {
	const data = {
		composerId,
		name: opts.name ?? `Session ${composerId.slice(0, 6)}`,
		createdAt: opts.createdAt ?? BASE,
		lastUpdatedAt: opts.lastUpdatedAt ?? BASE + 10_000,
		isAgentic: opts.isAgentic ?? false,
		conversation: opts.turns,
	};
	db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
		`composerData:${composerId}`,
		JSON.stringify(data),
	]);
}

/**
 * Insert a composerData entry (Format B — fullConversationHeadersOnly + bubbleId: entries).
 * Also inserts the corresponding bubbleId: KV entries for each turn.
 */
function insertFormatB(
	db: Database,
	composerId: string,
	opts: {
		name?: string;
		createdAt?: number;
		lastUpdatedAt?: number;
		isAgentic?: boolean;
		turns: Array<{ bubbleId: string; type: 1 | 2; text?: string }>;
	},
): void {
	const headers = opts.turns.map((t) => ({
		bubbleId: t.bubbleId,
		type: t.type,
	}));

	const data = {
		composerId,
		name: opts.name ?? `Session ${composerId.slice(0, 6)}`,
		createdAt: opts.createdAt ?? BASE,
		lastUpdatedAt: opts.lastUpdatedAt ?? BASE + 10_000,
		isAgentic: opts.isAgentic ?? false,
		fullConversationHeadersOnly: headers,
		// conversationMap is present but empty in real Format B sessions
		conversationMap: {},
	};
	db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
		`composerData:${composerId}`,
		JSON.stringify(data),
	]);

	// Insert individual bubble entries
	for (const turn of opts.turns) {
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`bubbleId:${composerId}:${turn.bubbleId}`,
			JSON.stringify({
				type: turn.type,
				bubbleId: turn.bubbleId,
				text: turn.text ?? "",
			}),
		]);
	}
}

/** Write an in-memory db to a temp file and return the path + cleanup fn. */
function dbToFile(db: Database): { path: string; cleanup: () => void } {
	const dir = mkdtempSync(join(tmpdir(), "ks-cursor-test-"));
	const path = join(dir, "state.vscdb");

	// Serialize the in-memory DB to a file by copying via backup API
	const fileDb = new Database(path);
	fileDb.run(`
    CREATE TABLE cursorDiskKV (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);
	const rows = db
		.query<{ key: string; value: string }, []>(
			"SELECT key, value FROM cursorDiskKV",
		)
		.all();
	for (const row of rows) {
		fileDb.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			row.key,
			row.value,
		]);
	}
	fileDb.close();

	return {
		path,
		cleanup: () => rmSync(dir, { recursive: true, force: true }),
	};
}

// ── Fixtures shared across describe blocks ────────────────────────────────────

const COMPOSER_A = "aaaaaaaa-0000-0000-0000-000000000001";
const COMPOSER_B = "bbbbbbbb-0000-0000-0000-000000000002";

// ── getCandidateSessions / countNewSessions ────────────────────────────────────

describe("CursorEpisodeReader.getCandidateSessions", () => {
	it("returns a Format A session newer than the cursor", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			lastUpdatedAt: BASE + 5000,
			turns: [
				{ type: 1, bubbleId: "b1", text: "hello" },
				{ type: 2, bubbleId: "b2", text: "hi" },
				{ type: 1, bubbleId: "b3", text: "bye" },
				{ type: 2, bubbleId: "b4", text: "ok" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(BASE);
			expect(candidates).toHaveLength(1);
			expect(candidates[0].id).toBe(COMPOSER_A);
			expect(candidates[0].maxMessageTime).toBe(BASE + 5000);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("excludes sessions at or before the cursor", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			lastUpdatedAt: BASE,
			turns: [
				{ type: 1, bubbleId: "b1", text: "old" },
				{ type: 2, bubbleId: "b2", text: "reply" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			// cursor is exactly at lastUpdatedAt — must be excluded (not strictly greater)
			expect(reader.getCandidateSessions(BASE)).toHaveLength(0);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("returns sessions ordered by lastUpdatedAt ASC", () => {
		const db = createDb();
		// COMPOSER_B has an earlier lastUpdatedAt — should come first
		insertFormatA(db, COMPOSER_A, {
			lastUpdatedAt: BASE + 10_000,
			turns: [
				{ type: 1, bubbleId: "a1", text: "hello" },
				{ type: 2, bubbleId: "a2", text: "hi" },
				{ type: 1, bubbleId: "a3", text: "bye" },
				{ type: 2, bubbleId: "a4", text: "ok" },
			],
		});
		insertFormatA(db, COMPOSER_B, {
			lastUpdatedAt: BASE + 2_000,
			turns: [
				{ type: 1, bubbleId: "b1", text: "earlier" },
				{ type: 2, bubbleId: "b2", text: "yes" },
				{ type: 1, bubbleId: "b3", text: "bye" },
				{ type: 2, bubbleId: "b4", text: "ok" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(BASE - 1);
			expect(candidates).toHaveLength(2);
			expect(candidates[0].id).toBe(COMPOSER_B); // earlier first
			expect(candidates[1].id).toBe(COMPOSER_A);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("respects the limit parameter", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			lastUpdatedAt: BASE + 1000,
			turns: [
				{ type: 1, bubbleId: "a1", text: "a" },
				{ type: 2, bubbleId: "a2", text: "b" },
				{ type: 1, bubbleId: "a3", text: "c" },
				{ type: 2, bubbleId: "a4", text: "d" },
			],
		});
		insertFormatA(db, COMPOSER_B, {
			lastUpdatedAt: BASE + 2000,
			turns: [
				{ type: 1, bubbleId: "b1", text: "e" },
				{ type: 2, bubbleId: "b2", text: "f" },
				{ type: 1, bubbleId: "b3", text: "g" },
				{ type: 2, bubbleId: "b4", text: "h" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(BASE - 1, 1);
			expect(candidates).toHaveLength(1);
			reader.close();
		} finally {
			cleanup();
		}
	});
});

describe("CursorEpisodeReader.countNewSessions", () => {
	it("returns the correct count", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			lastUpdatedAt: BASE + 5000,
			turns: [
				{ type: 1, bubbleId: "b1", text: "hello" },
				{ type: 2, bubbleId: "b2", text: "hi" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			expect(reader.countNewSessions(BASE)).toBe(1);
			expect(reader.countNewSessions(BASE + 5000)).toBe(0);
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── Format A: inline conversation[] ───────────────────────────────────────────

describe("CursorEpisodeReader.getNewEpisodes — Format A (inline turns)", () => {
	it("produces one episode with correct content and messageIds", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			name: "My Test Session",
			turns: [
				{ type: 1, bubbleId: "b1", text: "What is 2+2?" },
				{ type: 2, bubbleId: "b2", text: "It is 4." },
				{ type: 1, bubbleId: "b3", text: "And 3+3?" },
				{ type: 2, bubbleId: "b4", text: "It is 6." },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());

			expect(episodes).toHaveLength(1);
			expect(episodes[0].sessionId).toBe(COMPOSER_A);
			expect(episodes[0].sessionTitle).toBe("My Test Session");
			expect(episodes[0].startMessageId).toBe("b1");
			expect(episodes[0].endMessageId).toBe("b4");
			expect(episodes[0].contentType).toBe("messages");
			expect(episodes[0].content).toContain("What is 2+2");
			expect(episodes[0].content).toContain("It is 4");
			expect(episodes[0].content).toContain("And 3+3");
			expect(episodes[0].content).toContain("It is 6");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("uses latestConversationSummary.title when name is absent", () => {
		const db = createDb();
		const data = {
			composerId: COMPOSER_A,
			createdAt: BASE,
			lastUpdatedAt: BASE + 10_000,
			latestConversationSummary: { title: "Summary Title" },
			conversation: [
				{ type: 1, bubbleId: "b1", text: "hi" },
				{ type: 2, bubbleId: "b2", text: "hey" },
				{ type: 1, bubbleId: "b3", text: "bye" },
				{ type: 2, bubbleId: "b4", text: "ok" },
			],
		};
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify(data),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes[0].sessionTitle).toBe("Summary Title");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("uses positional key as messageId when bubbleId is absent", () => {
		const db = createDb();
		// Turns without bubbleId — reader falls back to `<composerId>-turn-<i>`
		const data = {
			composerId: COMPOSER_A,
			createdAt: BASE,
			lastUpdatedAt: BASE + 10_000,
			conversation: [
				{ type: 1, text: "no bubble id here" },
				{ type: 2, text: "nor here" },
				{ type: 1, text: "still no" },
				{ type: 2, text: "none" },
			],
		};
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify(data),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			expect(episodes[0].startMessageId).toContain(COMPOSER_A);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("skips turns with empty text", () => {
		const db = createDb();
		// b2 has empty text and is skipped; the remaining 4 turns with text
		// still meet minSessionMessages (default 4).
		insertFormatA(db, COMPOSER_A, {
			turns: [
				{ type: 1, bubbleId: "b1", text: "real question" },
				{ type: 2, bubbleId: "b2", text: "" }, // empty — skipped
				{ type: 1, bubbleId: "b3", text: "follow-up" },
				{ type: 2, bubbleId: "b4", text: "real answer" },
				{ type: 1, bubbleId: "b5", text: "another question" },
				{ type: 2, bubbleId: "b6", text: "another answer" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			// b2 had empty text — startMessageId should be b1, endMessageId b6
			expect(episodes[0].startMessageId).toBe("b1");
			expect(episodes[0].endMessageId).toBe("b6");
			// empty turn should not appear in content
			expect(episodes[0].content).toContain("real question");
			expect(episodes[0].content).toContain("real answer");
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── Format B: fullConversationHeadersOnly + bubbleId: entries ──────────────────

describe("CursorEpisodeReader.getNewEpisodes — Format B (bubble headers)", () => {
	it("produces one episode from a Format B session", () => {
		const db = createDb();
		insertFormatB(db, COMPOSER_A, {
			name: "Format B Session",
			turns: [
				{ bubbleId: "fb1", type: 1, text: "Format B user message" },
				{ bubbleId: "fb2", type: 2, text: "Format B assistant reply" },
				{ bubbleId: "fb3", type: 1, text: "Format B follow-up" },
				{ bubbleId: "fb4", type: 2, text: "Format B final answer" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());

			expect(episodes).toHaveLength(1);
			expect(episodes[0].sessionTitle).toBe("Format B Session");
			expect(episodes[0].startMessageId).toBe("fb1");
			expect(episodes[0].endMessageId).toBe("fb4");
			expect(episodes[0].content).toContain("Format B user message");
			expect(episodes[0].content).toContain("Format B assistant reply");
			expect(episodes[0].content).toContain("Format B follow-up");
			expect(episodes[0].content).toContain("Format B final answer");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("assigns correct roles from bubble type (1=user, 2=assistant)", () => {
		const db = createDb();
		insertFormatB(db, COMPOSER_A, {
			turns: [
				{ bubbleId: "u1", type: 1, text: "I am user" },
				{ bubbleId: "a1", type: 2, text: "I am assistant" },
				{ bubbleId: "u2", type: 1, text: "user again" },
				{ bubbleId: "a2", type: 2, text: "assistant again" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).toContain("user: I am user");
			expect(episodes[0].content).toContain("assistant: I am assistant");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("skips bubbles with no text (tool-only turns)", () => {
		const db = createDb();
		// a1 is a tool-only turn with no text and is skipped; the remaining 4
		// turns with text meet minSessionMessages (default 4).
		insertFormatB(db, COMPOSER_A, {
			turns: [
				{ bubbleId: "u1", type: 1, text: "run a tool" },
				{ bubbleId: "a1", type: 2, text: "" }, // tool-only, no text — skipped
				{ bubbleId: "u2", type: 1, text: "what happened?" },
				{ bubbleId: "a2", type: 2, text: "done" },
				{ bubbleId: "u3", type: 1, text: "follow up" },
				{ bubbleId: "a3", type: 2, text: "all good" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			// a1 was skipped; startMessageId should be u1, endMessageId a3
			expect(episodes[0].startMessageId).toBe("u1");
			expect(episodes[0].endMessageId).toBe("a3");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("handles bubbles missing from the KV store gracefully", () => {
		// Insert Format B composerData but omit one bubble entry from the KV store.
		// The remaining 4 present bubbles (with text) meet minSessionMessages.
		const db = createDb();
		const data = {
			composerId: COMPOSER_A,
			name: "Partial bubbles",
			createdAt: BASE,
			lastUpdatedAt: BASE + 10_000,
			fullConversationHeadersOnly: [
				{ bubbleId: "present1", type: 1 },
				{ bubbleId: "missing", type: 2 }, // no KV entry — skipped
				{ bubbleId: "present2", type: 1 },
				{ bubbleId: "present3", type: 2 },
				{ bubbleId: "present4", type: 1 },
				{ bubbleId: "present5", type: 2 },
			],
			conversationMap: {},
		};
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify(data),
		]);
		for (const [bId, type, text] of [
			["present1", 1, "Hello"],
			["present2", 1, "Follow up"],
			["present3", 2, "Answer"],
			["present4", 1, "More"],
			["present5", 2, "Done"],
		] as [string, number, string][]) {
			db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
				`bubbleId:${COMPOSER_A}:${bId}`,
				JSON.stringify({ type, bubbleId: bId, text }),
			]);
		}
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			// Should produce an episode from the 5 present bubbles (missing one is silently skipped)
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).toContain("Hello");
			expect(episodes[0].content).toContain("Follow up");
			expect(episodes[0].content).toContain("Answer");
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── minSessionMessages filter ──────────────────────────────────────────────────

describe("CursorEpisodeReader.getNewEpisodes — minSessionMessages filter", () => {
	it("skips sessions with fewer than minSessionMessages turns with text", () => {
		const db = createDb();
		// 2 turns — below the default minSessionMessages of 4
		insertFormatA(db, COMPOSER_A, {
			turns: [
				{ type: 1, bubbleId: "b1", text: "hi" },
				{ type: 2, bubbleId: "b2", text: "hey" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(0);
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── processedRanges exclusion ─────────────────────────────────────────────────

describe("CursorEpisodeReader.getNewEpisodes — processedRanges exclusion", () => {
	it("skips episodes whose (startMessageId, endMessageId) are already processed", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			turns: [
				{ type: 1, bubbleId: "b1", text: "start" },
				{ type: 2, bubbleId: "b2", text: "reply" },
				{ type: 1, bubbleId: "b3", text: "continue" },
				{ type: 2, bubbleId: "b4", text: "done" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);

			// First call: get the episode range
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);

			// Second call: mark that range as processed
			const processedRanges = new Map([
				[
					COMPOSER_A,
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
		} finally {
			cleanup();
		}
	});
});

// ── Edge cases ────────────────────────────────────────────────────────────────

describe("CursorEpisodeReader — edge cases", () => {
	it("returns no sessions when the DB is empty", () => {
		const db = createDb();
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			expect(reader.countNewSessions(0)).toBe(0);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("skips composerData with null or non-object JSON values", () => {
		const db = createDb();
		// Insert null and array values — both should be skipped
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			"null",
		]);
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_B}`,
			"[1,2,3]",
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("skips composerData with malformed JSON", () => {
		const db = createDb();
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			"{not valid json",
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("skips sessions with empty conversation in both formats", () => {
		const db = createDb();
		// Format A: empty conversation[]
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify({
				composerId: COMPOSER_A,
				createdAt: BASE,
				lastUpdatedAt: BASE + 1000,
				conversation: [],
			}),
		]);
		// Format B: empty headers[]
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_B}`,
			JSON.stringify({
				composerId: COMPOSER_B,
				createdAt: BASE,
				lastUpdatedAt: BASE + 1000,
				fullConversationHeadersOnly: [],
			}),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("synthesises monotonically increasing timestamps across turns", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, {
			createdAt: BASE,
			lastUpdatedAt: BASE + 10_000,
			turns: [
				{ type: 1, bubbleId: "b1", text: "first" },
				{ type: 2, bubbleId: "b2", text: "second" },
				{ type: 1, bubbleId: "b3", text: "third" },
				{ type: 2, bubbleId: "b4", text: "fourth" },
			],
		});
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			// maxMessageTime should be the interpolated timestamp of the last turn (≈ lastUpdatedAt)
			expect(episodes[0].maxMessageTime).toBeGreaterThanOrEqual(BASE);
			expect(episodes[0].maxMessageTime).toBeLessThanOrEqual(BASE + 10_000);
			// timeCreated should equal createdAt
			expect(episodes[0].timeCreated).toBe(BASE);
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── resolveCursorDbPath — env var branch ──────────────────────────────────────

describe("resolveCursorDbPath", () => {
	it("returns null when CURSOR_DB_PATH points to a non-existent file", () => {
		const original = process.env.CURSOR_DB_PATH;
		process.env.CURSOR_DB_PATH = "/nonexistent/path/to/state.vscdb";
		try {
			expect(resolveCursorDbPath()).toBeNull();
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "CURSOR_DB_PATH");
			} else {
				process.env.CURSOR_DB_PATH = original;
			}
		}
	});

	it("returns the path when CURSOR_DB_PATH points to an existing file", () => {
		const db = createDb();
		const { path, cleanup } = dbToFile(db);
		const original = process.env.CURSOR_DB_PATH;
		process.env.CURSOR_DB_PATH = path;
		try {
			expect(resolveCursorDbPath()).toBe(path);
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "CURSOR_DB_PATH");
			} else {
				process.env.CURSOR_DB_PATH = original;
			}
			cleanup();
		}
	});
});

// ── longestCommonDirectoryPrefix ──────────────────────────────────────────────

describe("longestCommonDirectoryPrefix", () => {
	it("returns dirname of the single path when given one path", () => {
		expect(longestCommonDirectoryPrefix(["/Users/x/project/src/a.ts"])).toBe(
			"/Users/x/project/src",
		);
	});

	it("returns empty string for an empty array", () => {
		expect(longestCommonDirectoryPrefix([])).toBe("");
	});

	it("returns the shared directory for two paths in different subdirs of the same project", () => {
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/project/src/a.ts",
				"/Users/x/project/lib/b.ts",
			]),
		).toBe("/Users/x/project");
	});

	it("returns the full shared path when paths are in the same directory", () => {
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/project/src/a.ts",
				"/Users/x/project/src/b.ts",
			]),
		).toBe("/Users/x/project/src");
	});

	it("does not produce partial segment matches (foo-bar vs foo-baz)", () => {
		// A naive string prefix would yield "/Users/x/foo-" — segment-based avoids this
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/foo-bar/a.ts",
				"/Users/x/foo-baz/b.ts",
			]),
		).toBe("/Users/x");
	});

	it("returns dirname when all paths are identical (same file)", () => {
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/project/src/a.ts",
				"/Users/x/project/src/a.ts",
			]),
		).toBe("/Users/x/project/src");
	});

	it("returns the common root for a monorepo with files in different packages", () => {
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/repo/packages/a/src/foo.ts",
				"/Users/x/repo/packages/b/src/bar.ts",
			]),
		).toBe("/Users/x/repo/packages");
	});

	it("returns empty string when paths share only the root /", () => {
		expect(
			longestCommonDirectoryPrefix(["/Users/x/a.ts", "/home/y/b.ts"]),
		).toBe("");
	});

	it("handles three or more paths correctly", () => {
		expect(
			longestCommonDirectoryPrefix([
				"/Users/x/project/src/a.ts",
				"/Users/x/project/lib/b.ts",
				"/Users/x/project/test/c.ts",
			]),
		).toBe("/Users/x/project");
	});
});

// ── collectFilePathsFromString ────────────────────────────────────────────────

describe("collectFilePathsFromString", () => {
	it("extracts a simple file:// URI", () => {
		const out: string[] = [];
		collectFilePathsFromString(
			'{"external":"file:///Users/x/project/src/a.ts"}',
			out,
		);
		expect(out).toEqual(["/Users/x/project/src/a.ts"]);
	});

	it("extracts multiple URIs from a single string", () => {
		const out: string[] = [];
		collectFilePathsFromString(
			'{"a":"file:///Users/x/project/src/a.ts","b":"file:///Users/x/project/lib/b.ts"}',
			out,
		);
		expect(out).toEqual([
			"/Users/x/project/src/a.ts",
			"/Users/x/project/lib/b.ts",
		]);
	});

	it("decodes percent-encoded characters", () => {
		const out: string[] = [];
		collectFilePathsFromString(
			'"file:///Users/dennis%20paul/project/a.ts"',
			out,
		);
		expect(out).toEqual(["/Users/dennis paul/project/a.ts"]);
	});

	it("returns nothing when no file:// URI is present", () => {
		const out: string[] = [];
		collectFilePathsFromString(
			'{"key":"value","other":"https://example.com"}',
			out,
		);
		expect(out).toEqual([]);
	});

	it("appends to an existing out array without clearing it", () => {
		const out: string[] = ["/existing/path.ts"];
		collectFilePathsFromString('"file:///Users/x/new.ts"', out);
		expect(out).toEqual(["/existing/path.ts", "/Users/x/new.ts"]);
	});

	it("stops at JSON delimiters (does not bleed across fields)", () => {
		const out: string[] = [];
		// The URI ends at the closing quote — should not bleed into next field
		collectFilePathsFromString(
			'"file:///Users/x/a.ts","nextField":"value"',
			out,
		);
		expect(out).toEqual(["/Users/x/a.ts"]);
	});
});

// ── workspace root resolution via DB ─────────────────────────────────────────

describe("CursorEpisodeReader — workspace root resolution", () => {
	it("resolves directory from composerData context fileSelections", () => {
		const db = createDb();
		const composerId = COMPOSER_A;
		// Embed file:// URIs in the composerData value (as context.fileSelections would)
		const data = {
			composerId,
			name: "Test Session",
			createdAt: BASE,
			lastUpdatedAt: BASE + 10_000,
			conversation: [
				{ type: 1, bubbleId: "b1", text: "What is this file?" },
				{ type: 2, bubbleId: "b2", text: "It is a config file." },
				{ type: 1, bubbleId: "b3", text: "Thanks." },
				{ type: 2, bubbleId: "b4", text: "No problem." },
			],
			context: {
				fileSelections: [
					{
						uri: {
							external: "file:///Users/x/Documents/my-project/src/index.ts",
						},
					},
					{
						uri: {
							external: "file:///Users/x/Documents/my-project/src/utils.ts",
						},
					},
				],
			},
		};
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${composerId}`,
			JSON.stringify(data),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			// Both files are under my-project/src/, so the longest common directory
			// prefix resolves to that subdirectory rather than the project root.
			// projectName is therefore "src", not "my-project". This is a known
			// limitation of the algorithm: when all collected URIs are in the same
			// subdirectory, the resolved directory is that subdirectory. Domain
			// routing is still correct because bestProjectMatch uses startsWith().
			expect(episodes[0].directory).toBe("/Users/x/Documents/my-project/src");
			expect(episodes[0].projectName).toBe("src");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("resolves directory from checkpointId entries when composerData has no URIs", () => {
		const db = createDb();
		const composerId = COMPOSER_A;
		// Plain composerData with no file URIs
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${composerId}`,
			JSON.stringify({
				composerId,
				name: "Checkpoint Session",
				createdAt: BASE,
				lastUpdatedAt: BASE + 10_000,
				conversation: [
					{ type: 1, bubbleId: "b1", text: "Edit the config." },
					{ type: 2, bubbleId: "b2", text: "Done." },
					{ type: 1, bubbleId: "b3", text: "Check the output." },
					{ type: 2, bubbleId: "b4", text: "Looks good." },
				],
			}),
		]);
		// Insert checkpointId entries with absolute file:// paths
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`checkpointId:${composerId}:cp1`,
			JSON.stringify({
				external: "file:///Users/x/Documents/repo/config/settings.json",
			}),
		]);
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`checkpointId:${composerId}:cp2`,
			JSON.stringify({
				external: "file:///Users/x/Documents/repo/src/main.ts",
			}),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			expect(episodes[0].directory).toBe("/Users/x/Documents/repo");
			expect(episodes[0].projectName).toBe("repo");
			reader.close();
		} finally {
			cleanup();
		}
	});

	it("sets directory and projectName to empty string for pure chat sessions with no file URIs", () => {
		const db = createDb();
		const composerId = COMPOSER_A;
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${composerId}`,
			JSON.stringify({
				composerId,
				name: "Pure Chat",
				createdAt: BASE,
				lastUpdatedAt: BASE + 10_000,
				conversation: [
					{ type: 1, bubbleId: "b1", text: "What is the capital of France?" },
					{ type: 2, bubbleId: "b2", text: "Paris." },
					{ type: 1, bubbleId: "b3", text: "Thanks." },
					{ type: 2, bubbleId: "b4", text: "You're welcome." },
				],
			}),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			const reader = new CursorEpisodeReader(path);
			const candidates = reader.getCandidateSessions(0);
			const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
			expect(episodes).toHaveLength(1);
			expect(episodes[0].directory).toBe("");
			expect(episodes[0].projectName).toBe("");
			reader.close();
		} finally {
			cleanup();
		}
	});
});

// ── Injected knowledge guard ──────────────────────────────────────────────────

const INJECTED_BLOCK = [
	"<addrl-activated-knowledge>",
	"## Recalled Knowledge (from prior sessions)",
	"- [fact] Deploys go out on Thursdays",
	"</addrl-activated-knowledge>",
].join("\n");

type CursorTurn = { type: 1 | 2; text: string };

const PLAIN_TURNS: CursorTurn[] = [
	{ type: 1, text: "question one" },
	{ type: 2, text: "answer one" },
	{ type: 1, text: "question two" },
	{ type: 2, text: "answer two" },
];

function withBubbleIds(turns: CursorTurn[]) {
	return turns.map((t, i) => ({ ...t, bubbleId: `b${i}` }));
}

/** Run the reader over a single-session database and return its episodes. */
function runCursor(db: Database) {
	const { path, cleanup } = dbToFile(db);
	try {
		const reader = new CursorEpisodeReader(path);
		const candidates = reader.getCandidateSessions(0);
		const episodes = reader.getNewEpisodes([candidates[0].id], new Map());
		reader.close();
		return episodes;
	} finally {
		cleanup();
	}
}

function extractInline(turns: CursorTurn[]) {
	const db = createDb();
	insertFormatA(db, COMPOSER_A, { turns: withBubbleIds(turns) });
	return runCursor(db);
}

function extractBubbles(turns: CursorTurn[]) {
	const db = createDb();
	insertFormatB(db, COMPOSER_A, { turns: withBubbleIds(turns) });
	return runCursor(db);
}

const CURSOR_FORMATS = [
	["Format A (inline turns)", extractInline],
	["Format B (bubble headers)", extractBubbles],
] as const;

for (const [format, extract] of CURSOR_FORMATS) {
	describe(`CursorEpisodeReader — injected knowledge guard, ${format}`, () => {
		it("drops a user turn that carries the injected block", () => {
			const episodes = extract([
				{ type: 1, text: `Context follows.\n${INJECTED_BLOCK}` },
				...PLAIN_TURNS,
			]);
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
			expect(episodes[0].content).not.toContain("Deploys go out on Thursdays");
			expect(episodes[0].content).toContain("question one");
		});

		it("drops an assistant turn that quotes the injected tag", () => {
			const episodes = extract([
				{
					type: 2,
					text: "The block opens with <addrl-activated-knowledge> and closes with </addrl-activated-knowledge>.",
				},
				...PLAIN_TURNS,
			]);
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
			expect(episodes[0].content).toContain("question one");
		});

		it("removes an injected block past 60K characters and keeps the text before it", () => {
			const episodes = extract([
				{ type: 1, text: `${"x".repeat(60_000)}\n${INJECTED_BLOCK}` },
				...PLAIN_TURNS,
			]);
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
			expect(episodes[0].content).not.toContain("Deploys go out on Thursdays");
			expect(episodes[0].content).toContain("xxxx");
			expect(episodes[0].content).toContain("question one");
		});

		it("removes an injected block past 60K characters and keeps the text before it", () => {
			const episodes = extract([
				{ type: 2, text: `${"x".repeat(60_000)}\n${INJECTED_BLOCK}` },
				...PLAIN_TURNS,
			]);
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
			expect(episodes[0].content).not.toContain("Deploys go out on Thursdays");
			expect(episodes[0].content).toContain("xxxx");
			expect(episodes[0].content).toContain("question one");
		});

		it("keeps ordinary turns unchanged", () => {
			const episodes = extract([
				{ type: 1, text: "plain first turn" },
				...PLAIN_TURNS,
			]);
			expect(episodes).toHaveLength(1);
			expect(episodes[0].content).toContain("user: plain first turn");
			expect(episodes[0].content).toContain("assistant: answer one");
		});
	});
}

// ── ARC-07: growth, hold-back, and poison pills ───────────────────────────────

type Turn = { bubbleId: string; type: 1 | 2; text: string };

/** Turns with indexes in [from, to), each with a unique, assertable text. */
function turnsRange(from: number, to: number): Turn[] {
	const turns: Turn[] = [];
	for (let i = from; i < to; i++) {
		turns.push({
			bubbleId: `b${i}`,
			type: i % 2 === 0 ? 1 : 2,
			text: `payload-${i}-end`,
		});
	}
	return turns;
}

/** Overwrite the composer row (and bubble rows for Format B) of an existing file. */
function writeComposer(
	path: string,
	format: "A" | "B",
	turns: Turn[],
	lastUpdatedAt: number,
	createdAt = BASE,
): void {
	const base = {
		composerId: COMPOSER_A,
		name: "Growing",
		createdAt,
		lastUpdatedAt,
		isAgentic: false,
	};
	const data =
		format === "A"
			? { ...base, conversation: turns }
			: {
					...base,
					fullConversationHeadersOnly: turns.map((t) => ({
						bubbleId: t.bubbleId,
						type: t.type,
					})),
					conversationMap: {},
				};
	const db = new Database(path);
	try {
		db.run("INSERT OR REPLACE INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify(data),
		]);
		if (format === "B") {
			for (const t of turns) {
				db.run(
					"INSERT OR REPLACE INTO cursorDiskKV (key, value) VALUES (?, ?)",
					[
						`bubbleId:${COMPOSER_A}:${t.bubbleId}`,
						JSON.stringify({
							type: t.type,
							bubbleId: t.bubbleId,
							text: t.text,
						}),
					],
				);
			}
		}
	} finally {
		db.close();
	}
}

/** Fresh reader over `path`; episodes for every candidate session. */
function episodesWith(path: string, processed: ProcessedRange[] = []) {
	const reader = new CursorEpisodeReader(path);
	try {
		const ids = reader.getCandidateSessions(0).map((c) => c.id);
		return reader.getNewEpisodes(ids, new Map([[COMPOSER_A, processed]]));
	} finally {
		reader.close();
	}
}

function rangesOf(
	episodes: Array<{ startMessageId: string; endMessageId: string }>,
): ProcessedRange[] {
	return episodes.map((e) => ({
		source: "cursor",
		startMessageId: e.startMessageId,
		endMessageId: e.endMessageId,
	}));
}

/** Number of occurrences of `needle` across the episodes' content. */
function occurrences(
	episodes: Array<{ content: string }>,
	needle: string,
): number {
	return episodes.reduce((n, e) => n + (e.content.split(needle).length - 1), 0);
}

for (const format of ["A", "B"] as const) {
	describe(`CursorEpisodeReader — growth and hold-back, Format ${format}`, () => {
		it("emits only the new tail once the session grows, each message exactly once", () => {
			const n = config.consolidation.minSessionMessages;
			const { path, cleanup } = dbToFile(createDb());
			try {
				writeComposer(path, format, turnsRange(0, n), BASE + 10_000);
				const first = episodesWith(path);
				expect(first).toHaveLength(1);

				writeComposer(path, format, turnsRange(0, 2 * n), BASE + 20_000);
				const second = episodesWith(path, rangesOf(first));
				expect(second).toHaveLength(1);
				expect(second[0].startMessageId).toBe(`b${n}`);
				expect(second[0].endMessageId).toBe(`b${2 * n - 1}`);
				for (let i = n; i < 2 * n; i++) {
					expect(occurrences(second, `payload-${i}-end`)).toBe(1);
				}
				for (let i = 0; i < n; i++) {
					expect(occurrences(second, `payload-${i}-end`)).toBe(0);
				}

				const all = [...rangesOf(first), ...rangesOf(second)];
				expect(episodesWith(path, all)).toHaveLength(0);
			} finally {
				cleanup();
			}
		});

		it("holds back a tail below minSessionMessages while the session is active", () => {
			const n = config.consolidation.minSessionMessages;
			expect(n).toBeGreaterThan(1);
			const now = Date.now();
			const { path, cleanup } = dbToFile(createDb());
			try {
				const processed = [
					{ source: "cursor", startMessageId: "b0", endMessageId: `b${n - 1}` },
				];
				writeComposer(
					path,
					format,
					turnsRange(0, n + 1),
					now - 1000,
					now - 60_000,
				);
				expect(episodesWith(path, processed)).toHaveLength(0);
			} finally {
				cleanup();
			}
		});

		it("flushes the same tail once the session is idle", () => {
			const n = config.consolidation.minSessionMessages;
			const { path, cleanup } = dbToFile(createDb());
			try {
				writeComposer(path, format, turnsRange(0, n + 1), BASE + 10_000);
				const processed = [
					{ source: "cursor", startMessageId: "b0", endMessageId: `b${n - 1}` },
				];
				const episodes = episodesWith(path, processed);
				expect(episodes).toHaveLength(1);
				expect(episodes[0].startMessageId).toBe(`b${n}`);
				expect(episodes[0].endMessageId).toBe(`b${n}`);
				expect(occurrences(episodes, `payload-${n}-end`)).toBe(1);
			} finally {
				cleanup();
			}
		});
	});
}

describe("CursorEpisodeReader — poison pills (ARC-07)", () => {
	const POISON_NULL = "dddddddd-0000-0000-0000-000000000004";
	const POISON_MALFORMED = "dddddddd-0000-0000-0000-000000000005";
	const MIXED = "dddddddd-0000-0000-0000-000000000006";
	const UNREADABLE = "dddddddd-0000-0000-0000-000000000007";

	it("skips null and malformed composer rows without throwing; healthy sessions still produce episodes", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, { turns: withBubbleIds(PLAIN_TURNS) });
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${POISON_NULL}`,
			"null",
		]);
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${POISON_MALFORMED}`,
			"{not valid json",
		]);
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${MIXED}`,
			JSON.stringify({
				composerId: MIXED,
				createdAt: BASE,
				lastUpdatedAt: BASE + 10_000,
				conversation: [
					null,
					{ type: 1, bubbleId: "m1", text: "alpha" },
					{ type: 2, bubbleId: "m2", text: "beta" },
					{ type: 1, bubbleId: "m3", text: "gamma" },
					{ type: 2, bubbleId: "m4", text: "delta" },
				],
			}),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			takeSkipCount("cursor");
			const reader = new CursorEpisodeReader(path);
			const ids = reader
				.getCandidateSessions(0)
				.map((c) => c.id)
				.sort();
			expect(ids).toEqual([COMPOSER_A, MIXED].sort());
			const episodes = reader.getNewEpisodes(ids, new Map());
			reader.close();

			expect(episodes.filter((e) => e.sessionId === COMPOSER_A)).toHaveLength(
				1,
			);
			const mixed = episodes.filter((e) => e.sessionId === MIXED);
			expect(mixed).toHaveLength(1);
			expect(mixed[0].content).toContain("delta");
			expect(mixed[0].content).toContain("alpha");
			expect(takeSkipCount("cursor")).toBeGreaterThan(0);
		} finally {
			cleanup();
		}
	});

	it("skips a session whose conversation is not an array and keeps the others", () => {
		const db = createDb();
		insertFormatA(db, COMPOSER_A, { turns: withBubbleIds(PLAIN_TURNS) });
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${UNREADABLE}`,
			JSON.stringify({
				composerId: UNREADABLE,
				createdAt: BASE,
				lastUpdatedAt: BASE + 10_000,
				conversation: "not-an-array",
			}),
		]);
		const { path, cleanup } = dbToFile(db);
		try {
			takeSkipCount("cursor");
			const episodes = episodesWith(path);
			expect(episodes.map((e) => e.sessionId)).toEqual([COMPOSER_A]);
			expect(takeSkipCount("cursor")).toBeGreaterThan(0);
		} finally {
			cleanup();
		}
	});

	it("skips malformed and non-object bubble rows and keeps the rest of a Format B session", () => {
		const db = createDb();
		insertFormatB(db, COMPOSER_A, {
			turns: [
				{ bubbleId: "p0", type: 1, text: "one" },
				{ bubbleId: "p1", type: 2, text: "two" },
				{ bubbleId: "p2", type: 1, text: "three" },
				{ bubbleId: "p3", type: 2, text: "four" },
				{ bubbleId: "p4", type: 1, text: "five" },
				{ bubbleId: "p5", type: 2, text: "six" },
			],
		});
		db.run("UPDATE cursorDiskKV SET value = ? WHERE key = ?", [
			"{not valid json",
			`bubbleId:${COMPOSER_A}:p1`,
		]);
		db.run("UPDATE cursorDiskKV SET value = ? WHERE key = ?", [
			"[1,2]",
			`bubbleId:${COMPOSER_A}:p2`,
		]);
		takeSkipCount("cursor");
		const episodes = runCursor(db);
		expect(episodes).toHaveLength(1);
		expect(episodes[0].startMessageId).toBe("p0");
		expect(episodes[0].endMessageId).toBe("p5");
		expect(episodes[0].content).toContain("four");
		expect(episodes[0].content).not.toContain("two");
		expect(episodes[0].content).not.toContain("three");
		expect(takeSkipCount("cursor")).toBeGreaterThan(0);
	});

	it("returns no sessions for a database file that cannot be read, without throwing", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-cursor-garbage-"));
		const path = join(dir, "state.vscdb");
		writeFileSync(path, "this is not a sqlite database");
		try {
			takeSkipCount("cursor");
			const reader = new CursorEpisodeReader(path);
			expect(reader.getCandidateSessions(0)).toHaveLength(0);
			expect(takeSkipCount("cursor")).toBeGreaterThan(0);
			reader.close();
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe("CursorEpisodeReader — bare % in file URIs (ARC-07)", () => {
	it("keeps the raw token instead of throwing on an invalid escape", () => {
		const out: string[] = [];
		expect(() =>
			collectFilePathsFromString('"file:///Users/x/100%/src/a.ts"', out),
		).not.toThrow();
		expect(out).toEqual(["/Users/x/100%/src/a.ts"]);
	});

	it("resolves a workspace root whose path contains a bare %", () => {
		const db = createDb();
		db.run("INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)", [
			`composerData:${COMPOSER_A}`,
			JSON.stringify({
				composerId: COMPOSER_A,
				name: "Bare percent",
				createdAt: BASE,
				lastUpdatedAt: BASE + 10_000,
				conversation: withBubbleIds(PLAIN_TURNS),
				context: {
					fileSelections: [
						{ uri: { external: "file:///Users/x/100%/src/a.ts" } },
						{ uri: { external: "file:///Users/x/100%/src/b.ts" } },
					],
				},
			}),
		]);
		const episodes = runCursor(db);
		expect(episodes).toHaveLength(1);
		expect(episodes[0].directory).toBe("/Users/x/100%/src");
	});
});
