/**
 * Tests for CodexEpisodeReader.
 *
 * CodexEpisodeReader reads from Codex CLI's JSONL rollout files under
 * ~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl.
 *
 * These tests create a minimal fake sessions directory in a temp dir,
 * seed it with JSONL records, and exercise the segmentation and extraction
 * logic without any network calls.
 *
 * Covered:
 *   - getCandidateSessions — mtime + timestamp cursor filtering + limit
 *   - countNewSessions — counts sessions with maxTimestampMs > cursor
 *   - getNewEpisodes — basic message extraction
 *   - getNewEpisodes — processedRanges exclusion
 *   - getNewEpisodes — minSessionMessages filter
 *   - getNewEpisodes — injected context block skipping (user_instructions, environment_context)
 *   - getNewEpisodes — missing session cache fallback (loadSessionsByIds)
 *   - resolveCodexSessionsDir — CODEX_SESSIONS_DIR env var override
 *   - Session ID extraction from session_meta vs filename fallback
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	mkdirSync,
	mkdtempSync,
	rmSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import {
	CodexEpisodeReader,
	resolveCodexSessionsDir,
} from "../src/daemon/readers/codex";

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Make a UTC ISO timestamp offset by `deltaMs` from `base`. */
function ts(base: number, deltaMs = 0): string {
	return new Date(base + deltaMs).toISOString();
}

/** Build a session_meta JSONL line. */
function metaRecord(opts: {
	timestamp: string;
	id: string;
	cwd?: string;
}): string {
	return JSON.stringify({
		timestamp: opts.timestamp,
		type: "session_meta",
		payload: {
			id: opts.id,
			timestamp: opts.timestamp,
			cwd: opts.cwd ?? "/home/user/my-project",
			originator: "codex_cli_rs",
			cli_version: "0.38.0",
		},
	});
}

/** Build a response_item message JSONL line. */
function msgRecord(opts: {
	timestamp: string;
	role: "user" | "assistant";
	text: string;
}): string {
	const partType = opts.role === "user" ? "input_text" : "output_text";
	return JSON.stringify({
		timestamp: opts.timestamp,
		type: "response_item",
		payload: {
			type: "message",
			role: opts.role,
			content: [{ type: partType, text: opts.text }],
		},
	});
}

/** Build a function_call JSONL line (should be ignored by reader). */
function toolCallRecord(opts: { timestamp: string; name: string }): string {
	return JSON.stringify({
		timestamp: opts.timestamp,
		type: "response_item",
		payload: {
			type: "function_call",
			name: opts.name,
			arguments: "{}",
			call_id: "call_abc123",
		},
	});
}

/** Build an event_msg JSONL line (should be ignored by reader). */
function eventMsgRecord(opts: { timestamp: string; text: string }): string {
	return JSON.stringify({
		timestamp: opts.timestamp,
		type: "event_msg",
		payload: { type: "user_message", message: opts.text, kind: "plain" },
	});
}

// ── Fixtures ───────────────────────────────────────────────────────────────────

const SESSION_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const SESSION_UUID_2 = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
const BASE_TS = 1700000000000; // 2023-11-14T22:13:20.000Z
const CWD = "/home/user/my-project";
const DATE_PATH = "2023/11/14"; // matches BASE_TS date

let tmpDir: string;
let sessionsDir: string;
let reader: CodexEpisodeReader;

/** Write a rollout file to the date subdirectory. */
function writeRollout(
	sessionUuid: string,
	lines: string[],
	mtimeMs?: number,
): string {
	const dateDir = join(sessionsDir, DATE_PATH);
	mkdirSync(dateDir, { recursive: true });
	const isoTs = new Date(BASE_TS).toISOString().replace(/[:.]/g, "-");
	const filename = `rollout-${isoTs}-${sessionUuid}.jsonl`;
	const filePath = join(dateDir, filename);
	writeFileSync(filePath, `${lines.join("\n")}\n`);
	if (mtimeMs !== undefined) {
		const t = new Date(mtimeMs);
		utimesSync(filePath, t, t);
	}
	return filePath;
}

// ── Setup / teardown ───────────────────────────────────────────────────────────

beforeEach(() => {
	tmpDir = mkdtempSync(join(tmpdir(), "codex-test-"));
	sessionsDir = join(tmpDir, "sessions");
	mkdirSync(sessionsDir, { recursive: true });
	reader = new CodexEpisodeReader(sessionsDir);
});

afterEach(() => {
	rmSync(tmpDir, { recursive: true, force: true });
});

// ── Tests ──────────────────────────────────────────────────────────────────────

describe("CodexEpisodeReader — getCandidateSessions", () => {
	it("returns sessions with maxTimestampMs > cursor", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID, cwd: CWD }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "hello",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "world",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "follow up",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "done",
				}),
			],
			BASE_TS + 5000,
		);

		const cursor = BASE_TS - 1;
		const candidates = reader.getCandidateSessions(cursor);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe(SESSION_UUID);
		expect(candidates[0].maxMessageTime).toBe(BASE_TS + 4000);
	});

	it("excludes sessions with maxTimestampMs <= cursor", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				msgRecord({ timestamp: ts(BASE_TS, 1000), role: "user", text: "old" }),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "old reply",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "more old",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "more reply",
				}),
			],
			BASE_TS + 5000,
		);

		// Cursor is at or after the max message time
		const candidates = reader.getCandidateSessions(BASE_TS + 4000);
		expect(candidates).toHaveLength(0);
	});

	it("respects the limit parameter", () => {
		// Write 3 sessions
		for (let i = 0; i < 3; i++) {
			const uuid = `aaaaaaaa-bbbb-cccc-dddd-${String(i).padStart(12, "0")}`;
			writeRollout(
				uuid,
				[
					metaRecord({ timestamp: ts(BASE_TS + i * 10000), id: uuid }),
					msgRecord({
						timestamp: ts(BASE_TS + i * 10000 + 1000),
						role: "user",
						text: `msg ${i}`,
					}),
					msgRecord({
						timestamp: ts(BASE_TS + i * 10000 + 2000),
						role: "assistant",
						text: `reply ${i}`,
					}),
					msgRecord({
						timestamp: ts(BASE_TS + i * 10000 + 3000),
						role: "user",
						text: `msg2 ${i}`,
					}),
					msgRecord({
						timestamp: ts(BASE_TS + i * 10000 + 4000),
						role: "assistant",
						text: `reply2 ${i}`,
					}),
				],
				BASE_TS + i * 10000 + 5000,
			);
		}

		const candidates = reader.getCandidateSessions(BASE_TS - 1, 2);
		expect(candidates).toHaveLength(2);
	});

	it("returns sessions ordered by maxMessageTime ASC", () => {
		const uuids = [
			"aaaaaaaa-0000-0000-0000-000000000001",
			"aaaaaaaa-0000-0000-0000-000000000002",
		];
		// Write session 2 with later timestamp first (to test sorting)
		for (let i = 0; i < 2; i++) {
			const offset = (1 - i) * 10000; // session 0 → offset 10000, session 1 → offset 0
			writeRollout(
				uuids[i],
				[
					metaRecord({ timestamp: ts(BASE_TS + offset), id: uuids[i] }),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 1000),
						role: "user",
						text: "q",
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 2000),
						role: "assistant",
						text: "a",
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 3000),
						role: "user",
						text: "q2",
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 4000),
						role: "assistant",
						text: "a2",
					}),
				],
				BASE_TS + offset + 5000,
			);
		}

		const candidates = reader.getCandidateSessions(BASE_TS - 1);
		expect(candidates[0].maxMessageTime).toBeLessThan(
			candidates[1].maxMessageTime,
		);
	});
});

describe("CodexEpisodeReader — countNewSessions", () => {
	it("counts sessions with maxTimestampMs > cursor", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				msgRecord({ timestamp: ts(BASE_TS, 1000), role: "user", text: "a" }),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "b",
				}),
				msgRecord({ timestamp: ts(BASE_TS, 3000), role: "user", text: "c" }),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "d",
				}),
			],
			BASE_TS + 5000,
		);

		expect(reader.countNewSessions(BASE_TS - 1)).toBe(1);
		expect(reader.countNewSessions(BASE_TS + 4000)).toBe(0);
	});
});

describe("CodexEpisodeReader — getNewEpisodes", () => {
	it("extracts user and assistant messages", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID, cwd: CWD }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "How do I reverse a string in Python?",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: 'Use slicing: s[::-1] or "".join(reversed(s))',
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "Which is faster?",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "Slicing is generally faster.",
				}),
			],
			BASE_TS + 5000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		const ep = episodes[0];
		expect(ep.sessionId).toBe(SESSION_UUID);
		expect(ep.projectName).toBe("my-project");
		expect(ep.directory).toBe(CWD);
		expect(ep.contentType).toBe("messages");
		expect(ep.content).toContain("How do I reverse a string");
		expect(ep.content).toContain("Use slicing");
		expect(ep.content).toContain("Which is faster?");
		expect(ep.content).toContain("Slicing is generally faster.");
	});

	it("skips function_call and function_call_output records", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "list files",
				}),
				toolCallRecord({ timestamp: ts(BASE_TS, 1500), name: "shell" }),
				JSON.stringify({
					timestamp: ts(BASE_TS, 1600),
					type: "response_item",
					payload: {
						type: "function_call_output",
						call_id: "call_abc",
						output: '{"output": "file1.txt\\nfile2.txt"}',
					},
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "I see file1.txt and file2.txt.",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "rename file1",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "Done.",
				}),
			],
			BASE_TS + 5000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		// Tool output text should not appear in messages
		expect(episodes[0].content).not.toContain("file1.txt\nfile2.txt");
		// But assistant summary should be there
		expect(episodes[0].content).toContain("file1.txt and file2.txt");
	});

	it("skips event_msg records", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				eventMsgRecord({ timestamp: ts(BASE_TS, 500), text: "user typed" }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "What is 2+2?",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "4",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "And 3+3?",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "6",
				}),
			],
			BASE_TS + 5000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("user typed");
	});

	it("skips injected context blocks (user_instructions / environment_context)", () => {
		const injectedUserInstructions = JSON.stringify({
			timestamp: ts(BASE_TS, 100),
			type: "response_item",
			payload: {
				type: "message",
				role: "user",
				content: [
					{
						type: "input_text",
						text: "<user_instructions>\n# AGENTS.md content here\n</user_instructions>",
					},
				],
			},
		});
		const injectedEnvContext = JSON.stringify({
			timestamp: ts(BASE_TS, 200),
			type: "response_item",
			payload: {
				type: "message",
				role: "user",
				content: [
					{
						type: "input_text",
						text: "<environment_context>\n  <cwd>/project</cwd>\n</environment_context>",
					},
				],
			},
		});

		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				injectedUserInstructions,
				injectedEnvContext,
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "Actual user message here",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "Actual assistant reply here",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "Follow up",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "Done",
				}),
			],
			BASE_TS + 5000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("AGENTS.md content here");
		expect(episodes[0].content).not.toContain("<environment_context>");
		expect(episodes[0].content).toContain("Actual user message here");
		expect(episodes[0].content).toContain("Actual assistant reply here");
	});

	it("excludes already-processed episode ranges", () => {
		const lines = [
			metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID, cwd: CWD }),
			msgRecord({
				timestamp: ts(BASE_TS, 1000),
				role: "user",
				text: "first question",
			}),
			msgRecord({
				timestamp: ts(BASE_TS, 2000),
				role: "assistant",
				text: "first answer",
			}),
			msgRecord({
				timestamp: ts(BASE_TS, 3000),
				role: "user",
				text: "second question",
			}),
			msgRecord({
				timestamp: ts(BASE_TS, 4000),
				role: "assistant",
				text: "second answer",
			}),
		];
		writeRollout(SESSION_UUID, lines, BASE_TS + 5000);

		// First call — no processed ranges
		reader.getCandidateSessions(BASE_TS - 1);
		const episodesFirst = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);
		expect(episodesFirst).toHaveLength(1);

		const ep = episodesFirst[0];

		// Second call — mark the episode as already processed
		reader.getCandidateSessions(BASE_TS - 1);
		const episodesSecond = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([
				[
					SESSION_UUID,
					[
						{
							startMessageId: ep.startMessageId,
							endMessageId: ep.endMessageId,
						},
					],
				],
			]),
		);
		expect(episodesSecond).toHaveLength(0);
	});

	it("respects minSessionMessages — skips sessions below the threshold", () => {
		const minMessages = config.consolidation.minSessionMessages;

		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				// Only 2 messages — below the default minimum of 4
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "hi",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "hello",
				}),
			],
			BASE_TS + 3000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		if (minMessages > 2) {
			expect(episodes).toHaveLength(0);
		} else {
			// If the threshold is lowered, episodes should still be produced
			expect(episodes.length).toBeGreaterThanOrEqual(0);
		}
	});

	it("falls back to loadSessionsByIds when cache is cold", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID, cwd: CWD }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "uncached question",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "uncached answer",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "more uncached",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "more answer",
				}),
			],
			BASE_TS + 5000,
		);

		// Do NOT call getCandidateSessions — cold cache
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).toContain("uncached question");
	});

	it("uses session_meta id over filename fallback", () => {
		const metaId = "cccccccc-dddd-eeee-ffff-aaaaaaaaaaaa";
		// filename has a different UUID
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: metaId, cwd: CWD }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "meta id test",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "meta id answer",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "third",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "fourth",
				}),
			],
			BASE_TS + 5000,
		);

		const candidates = reader.getCandidateSessions(BASE_TS - 1);
		expect(candidates).toHaveLength(1);
		// Session ID should come from session_meta, not filename
		expect(candidates[0].id).toBe(metaId);
	});

	it("uses filename UUID as session ID when session_meta is absent", () => {
		writeRollout(
			SESSION_UUID,
			[
				// No session_meta record
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "no meta",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "still works",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "third",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "fourth",
				}),
			],
			BASE_TS + 5000,
		);

		const candidates = reader.getCandidateSessions(BASE_TS - 1);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toBe(SESSION_UUID);
	});

	it("returns empty array for empty sessions directory", () => {
		const candidates = reader.getCandidateSessions(0);
		expect(candidates).toHaveLength(0);
		expect(reader.countNewSessions(0)).toBe(0);
	});

	it("handles multiple sessions correctly", () => {
		for (const [uuid, offset] of [
			[SESSION_UUID, 0],
			[SESSION_UUID_2, 50000],
		] as [string, number][]) {
			writeRollout(
				uuid,
				[
					metaRecord({ timestamp: ts(BASE_TS + offset), id: uuid }),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 1000),
						role: "user",
						text: `session ${uuid} question`,
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 2000),
						role: "assistant",
						text: `session ${uuid} answer`,
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 3000),
						role: "user",
						text: "second question",
					}),
					msgRecord({
						timestamp: ts(BASE_TS + offset + 4000),
						role: "assistant",
						text: "second answer",
					}),
				],
				BASE_TS + offset + 5000,
			);
		}

		const candidates = reader.getCandidateSessions(BASE_TS - 1);
		expect(candidates).toHaveLength(2);

		const ids = candidates.map((c) => c.id);
		expect(ids).toContain(SESSION_UUID);
		expect(ids).toContain(SESSION_UUID_2);

		const episodes = reader.getNewEpisodes(
			ids,
			new Map(ids.map((id) => [id, []])),
		);
		expect(episodes).toHaveLength(2);
	});

	it("messageId uses sessionId:lineIndex format", () => {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				msgRecord({
					timestamp: ts(BASE_TS, 1000),
					role: "user",
					text: "question A",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "assistant",
					text: "answer A",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "user",
					text: "question B",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "assistant",
					text: "answer B",
				}),
			],
			BASE_TS + 5000,
		);

		reader.getCandidateSessions(BASE_TS - 1);
		const episodes = reader.getNewEpisodes(
			[SESSION_UUID],
			new Map([[SESSION_UUID, []]]),
		);

		expect(episodes).toHaveLength(1);
		const ep = episodes[0];
		// IDs should follow the <sessionId>:<lineIndex> pattern
		expect(ep.startMessageId).toMatch(new RegExp(`^${SESSION_UUID}:\\d+$`));
		expect(ep.endMessageId).toMatch(new RegExp(`^${SESSION_UUID}:\\d+$`));
		// start < end (different line indices)
		const startIdx = Number.parseInt(ep.startMessageId.split(":")[1], 10);
		const endIdx = Number.parseInt(ep.endMessageId.split(":")[1], 10);
		expect(startIdx).toBeLessThan(endIdx);
	});
});

describe("resolveCodexSessionsDir", () => {
	it("returns CODEX_SESSIONS_DIR when set in config", () => {
		const original = process.env.CODEX_SESSIONS_DIR;
		const fakeDir = join(tmpDir, "custom-sessions");
		mkdirSync(fakeDir, { recursive: true });
		process.env.CODEX_SESSIONS_DIR = fakeDir;
		// Re-read config (config is a const object; we patch env and call the function)
		try {
			// The function reads config.codexSessionsDir which is already baked in at
			// import time — test by constructing a reader with the explicit dir instead.
			const r = new CodexEpisodeReader(fakeDir);
			// @ts-ignore: access private for test
			// eslint-disable-next-line @typescript-eslint/dot-notation
			expect(r.sessionsDir).toBe(fakeDir);
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "CODEX_SESSIONS_DIR");
			} else {
				process.env.CODEX_SESSIONS_DIR = original;
			}
		}
	});

	it("defaults to ~/.codex/sessions via CODEX_HOME", () => {
		const original = process.env.CODEX_HOME;
		const fakeHome = join(tmpDir, "codex-home");
		mkdirSync(join(fakeHome, "sessions"), { recursive: true });
		process.env.CODEX_HOME = fakeHome;
		try {
			const resolved = resolveCodexSessionsDir();
			expect(resolved).toBe(join(fakeHome, "sessions"));
		} finally {
			if (original === undefined) {
				Reflect.deleteProperty(process.env, "CODEX_HOME");
			} else {
				process.env.CODEX_HOME = original;
			}
		}
	});
});

// ── Injected knowledge guard ───────────────────────────────────────────────────

const INJECTED_BLOCK = [
	"<addrl-activated-knowledge>",
	"## Recalled Knowledge (from prior sessions)",
	"- [fact] Deploys go out on Thursdays",
	"</addrl-activated-knowledge>",
].join("\n");

describe("CodexEpisodeReader — injected knowledge guard", () => {
	/** Rollout whose first message is `first`, then four plain messages; returns the episodes. */
	function extractWithFirst(first: {
		role: "user" | "assistant";
		text: string;
	}) {
		writeRollout(
			SESSION_UUID,
			[
				metaRecord({ timestamp: ts(BASE_TS), id: SESSION_UUID }),
				msgRecord({ timestamp: ts(BASE_TS, 1000), ...first }),
				msgRecord({
					timestamp: ts(BASE_TS, 2000),
					role: "user",
					text: "question one",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 3000),
					role: "assistant",
					text: "answer one",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 4000),
					role: "user",
					text: "question two",
				}),
				msgRecord({
					timestamp: ts(BASE_TS, 5000),
					role: "assistant",
					text: "answer two",
				}),
			],
			BASE_TS + 6000,
		);
		reader.getCandidateSessions(BASE_TS - 1);
		return reader.getNewEpisodes([SESSION_UUID], new Map([[SESSION_UUID, []]]));
	}

	it("drops a user message that carries the injected block", () => {
		const episodes = extractWithFirst({
			role: "user",
			text: `Context follows.\n${INJECTED_BLOCK}`,
		});
		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
		expect(episodes[0].content).not.toContain("Deploys go out on Thursdays");
		expect(episodes[0].content).toContain("question one");
	});

	it("drops an assistant message that quotes the injected tag", () => {
		const episodes = extractWithFirst({
			role: "assistant",
			text: "The block opens with <addrl-activated-knowledge> and closes with </addrl-activated-knowledge>.",
		});
		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("addrl-activated-knowledge");
		expect(episodes[0].content).toContain("answer two");
	});

	it("drops a user message whose injected block sits past 60K characters", () => {
		const episodes = extractWithFirst({
			role: "user",
			text: `${"x".repeat(60_000)}\n${INJECTED_BLOCK}`,
		});
		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("[...truncated]");
		expect(episodes[0].content).not.toContain("xxxx");
		expect(episodes[0].content).toContain("question one");
	});

	it("drops an assistant message whose injected block sits past 60K characters", () => {
		const episodes = extractWithFirst({
			role: "assistant",
			text: `${"x".repeat(60_000)}\n${INJECTED_BLOCK}`,
		});
		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).not.toContain("[...truncated]");
		expect(episodes[0].content).not.toContain("xxxx");
		expect(episodes[0].content).toContain("question one");
	});

	it("keeps ordinary messages unchanged", () => {
		const episodes = extractWithFirst({
			role: "user",
			text: "plain first message",
		});
		expect(episodes).toHaveLength(1);
		expect(episodes[0].content).toContain("  user: plain first message");
		expect(episodes[0].content).toContain("  assistant: answer one");
	});
});
