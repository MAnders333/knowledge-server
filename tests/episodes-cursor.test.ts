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
import {
	CursorEpisodeReader,
	collectFilePathsFromString,
	longestCommonDirectoryPrefix,
	resolveCursorDbPath,
} from "../src/daemon/readers/cursor";

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
		collectFilePathsFromString('{"key":"value","other":"https://example.com"}', out);
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
							external:
								"file:///Users/x/Documents/my-project/src/index.ts",
						},
					},
					{
						uri: {
							external:
								"file:///Users/x/Documents/my-project/src/utils.ts",
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
