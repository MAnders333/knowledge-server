/**
 * Tests for OhMyPiEpisodeReader — the oh-my-pi (omp) session source.
 *
 * oh-my-pi writes the same session JSONL (v3) as pi, so parsing and
 * segmentation are covered by the shared pi suite (episodes-pi.test.ts).
 * These tests cover what is NEW here:
 *   - resolveOhMyPiSessionsDirs — shared agent layer + profiles/<mode>/agent layers
 *   - the "oh-my-pi" source id (idempotency stays separate from "pi")
 *   - tolerance of oh-my-pi's extra `title` preamble record
 *   - episode extraction end-to-end on a real-shaped session file
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	appendFileSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../src/config";
import {
	OhMyPiEpisodeReader,
	resolveOhMyPiSessionsDirs,
} from "../src/daemon/readers/oh-my-pi";
import {
	SESSION_IDLE_FLUSH_MS,
	takeSkipCount,
} from "../src/daemon/readers/shared";

const CWD = "/Users/test/project";
const SESSION_ID = "01a0e75b-1962-7000-b289-432b0cefbdbe";
const T0 = Date.parse("2026-09-28T09:31:42.306Z");

let root: string;

beforeEach(() => {
	root = mkdtempSync(join(tmpdir(), "ks-oh-my-pi-"));
});

afterEach(() => {
	rmSync(root, { recursive: true, force: true });
});

function writeSession(sessionsDir: string): void {
	const projectDir = join(sessionsDir, "-test-project");
	mkdirSync(projectDir, { recursive: true });
	const lines = [
		// oh-my-pi's title preamble — must be tolerated by the pi parser
		JSON.stringify({
			type: "title",
			v: 1,
			title: "",
			updatedAt: "2026-09-28T09:31:42.306Z",
			pad: " ",
		}),
		JSON.stringify({
			type: "session",
			version: 3,
			id: SESSION_ID,
			timestamp: "2026-09-28T09:31:42.306Z",
			cwd: CWD,
		}),
		JSON.stringify({
			type: "message",
			id: "00000001",
			parentId: null,
			timestamp: new Date(T0 + 1000).toISOString(),
			message: { role: "user", content: "how does the launcher work" },
		}),
		JSON.stringify({
			type: "message",
			id: "00000002",
			parentId: "00000001",
			timestamp: new Date(T0 + 2000).toISOString(),
			message: {
				role: "assistant",
				content: [{ type: "text", text: "the launcher execs the binary" }],
			},
		}),
		JSON.stringify({
			type: "message",
			id: "00000003",
			parentId: "00000002",
			timestamp: new Date(T0 + 3000).toISOString(),
			message: { role: "user", content: "and the PID file" },
		}),
		JSON.stringify({
			type: "message",
			id: "00000004",
			parentId: "00000003",
			timestamp: new Date(T0 + 4000).toISOString(),
			message: {
				role: "assistant",
				content: [
					{ type: "text", text: "the PID file prevents double starts" },
				],
			},
		}),
	];
	writeFileSync(
		join(
			projectDir,
			"2026-09-28T09-31-42-306Z_01a0e75b-1962-7000-b289-432b0cefbdbe.jsonl",
		),
		`${lines.join("\n")}\n`,
	);
}

describe("resolveOhMyPiSessionsDirs", () => {
	it("finds the shared agent layer and every profiles/<mode>/agent layer", () => {
		const expected = [
			join(root, "agent", "sessions"),
			join(root, "profiles", "personal", "agent", "sessions"),
			join(root, "profiles", "work", "agent", "sessions"),
		];
		for (const dir of expected) mkdirSync(dir, { recursive: true });
		// Not a sessions dir — must not be picked up.
		mkdirSync(join(root, "profiles", "personal", "agent", "cache"), {
			recursive: true,
		});

		expect(resolveOhMyPiSessionsDirs(root).sort()).toEqual(expected.sort());
	});

	it("returns the shared layer alone when no profiles exist", () => {
		const shared = join(root, "agent", "sessions");
		mkdirSync(shared, { recursive: true });
		expect(resolveOhMyPiSessionsDirs(root)).toEqual([shared]);
	});

	it("returns [] when oh-my-pi is not installed", () => {
		expect(resolveOhMyPiSessionsDirs(join(root, "nope"))).toEqual([]);
	});
});

describe("OhMyPiEpisodeReader", () => {
	it("carries the oh-my-pi source id (idempotency separate from pi)", () => {
		const reader = new OhMyPiEpisodeReader([join(root, "agent", "sessions")]);
		try {
			expect(reader.source).toBe("oh-my-pi");
		} finally {
			reader.close();
		}
	});

	it("extracts an episode from a real-shaped session file (title preamble tolerated)", () => {
		const sessionsDir = join(root, "agent", "sessions");
		writeSession(sessionsDir);

		const reader = new OhMyPiEpisodeReader([sessionsDir]);
		try {
			const candidates = reader.getCandidateSessions(0);
			expect(candidates).toHaveLength(1);
			expect(candidates[0].id).toBe(SESSION_ID);

			const episodes = reader.getNewEpisodes(
				candidates.map((c) => c.id),
				new Map(),
			);
			expect(episodes.length).toBeGreaterThan(0);

			const episode = episodes[0];
			expect(episode.source).toBe("oh-my-pi");
			expect(episode.sessionId).toBe(SESSION_ID);
			expect(episode.directory).toBe(CWD);
			expect(episode.projectName).toBe("project");
			expect(episode.content).toContain("how does the launcher work");
			expect(episode.content).toContain("the launcher execs the binary");
			// Title fallback: no session_info name → first user message preview.
			expect(episode.sessionTitle).toContain("how does the launcher");
		} finally {
			reader.close();
		}
	});
});

// ── ARC-07: chunk growth, tail hold-back, poison pills ───────────────────────

const OTHER_ID = "02b1f86c-2a73-7000-c3aa-543c1d0cefcd";
const OMP_PROJECT = "-test-project";

function ompHeader(id: string, startMs: number): string {
	return JSON.stringify({
		type: "session",
		version: 3,
		id,
		timestamp: new Date(startMs).toISOString(),
		cwd: CWD,
	});
}

/** Entry id used by ompTurns for turn `n` (8-char hex, like pi entry ids). */
function entryId(n: number): string {
	return n.toString(16).padStart(8, "0");
}

/** `count` plain messages numbered from `first`: user on even turns, assistant on odd. */
function ompTurns(first: number, count: number, startMs: number): string[] {
	const lines: string[] = [];
	for (let i = 0; i < count; i++) {
		const n = first + i;
		lines.push(
			JSON.stringify({
				type: "message",
				id: entryId(n),
				parentId: null,
				timestamp: new Date(startMs + i * 1000).toISOString(),
				message:
					n % 2 === 0
						? { role: "user", content: `turn ${n}` }
						: {
								role: "assistant",
								content: [{ type: "text", text: `turn ${n}` }],
							},
			}),
		);
	}
	return lines;
}

/** Write a session file under <sessionsDir>/<project>/ and return its path. */
function ompWrite(sessionsDir: string, name: string, lines: string[]): string {
	mkdirSync(join(sessionsDir, OMP_PROJECT), { recursive: true });
	const file = join(sessionsDir, OMP_PROJECT, name);
	writeFileSync(file, `${lines.join("\n")}\n`);
	return file;
}

/** processedRanges for one session, one range per episode already produced. */
function ompRanges(
	sessionId: string,
	episodes: Array<{ startMessageId: string; endMessageId: string }>,
) {
	return new Map([
		[
			sessionId,
			episodes.map((ep) => ({
				source: "oh-my-pi",
				startMessageId: ep.startMessageId,
				endMessageId: ep.endMessageId,
			})),
		],
	]);
}

describe("OhMyPiEpisodeReader — chunk growth (ARC-07)", () => {
	it("emits only the appended tail, and nothing when unchanged", () => {
		const k = config.consolidation.minSessionMessages;
		const sessionsDir = join(root, "agent", "sessions");
		const file = ompWrite(sessionsDir, "grow.jsonl", [
			ompHeader(SESSION_ID, T0),
			...ompTurns(0, 4, T0),
		]);
		const reader = new OhMyPiEpisodeReader([sessionsDir]);
		try {
			const candidates = () => reader.getCandidateSessions(0).map((c) => c.id);
			const first = reader.getNewEpisodes(candidates(), new Map());
			expect(first).toHaveLength(1);

			const added = ompTurns(4, k, T0 + 10_000);
			appendFileSync(file, `${added.join("\n")}\n`);
			const second = reader.getNewEpisodes(
				candidates(),
				ompRanges(SESSION_ID, first),
			);
			expect(second).toHaveLength(1);
			expect(second[0].startMessageId).toBe(entryId(4));
			expect(second[0].endMessageId).toBe(entryId(3 + k));
			expect(second[0].content.split("turn ").length - 1).toBe(k);
			expect(second[0].content).not.toContain("turn 0");

			expect(
				reader.getNewEpisodes(
					candidates(),
					ompRanges(SESSION_ID, [...first, ...second]),
				),
			).toEqual([]);
		} finally {
			reader.close();
		}
	});
});

describe("OhMyPiEpisodeReader — tail hold-back (ARC-07)", () => {
	it("holds back a small tail while active, flushes it once idle", () => {
		const now = Date.now();
		const sessionsDir = join(root, "agent", "sessions");
		const activeFile = ompWrite(sessionsDir, "active.jsonl", [
			ompHeader(SESSION_ID, now - 7_200_000),
			...ompTurns(0, 4, now - 7_200_000),
		]);
		const idleFile = ompWrite(sessionsDir, "idle.jsonl", [
			ompHeader(OTHER_ID, now - 7_200_000),
			...ompTurns(0, 4, now - 7_200_000),
		]);
		const reader = new OhMyPiEpisodeReader([sessionsDir]);
		try {
			const candidates = () => reader.getCandidateSessions(0).map((c) => c.id);
			const first = reader.getNewEpisodes(candidates(), new Map());
			expect(first).toHaveLength(2);

			appendFileSync(activeFile, `${ompTurns(4, 2, now - 1_000).join("\n")}\n`);
			appendFileSync(
				idleFile,
				`${ompTurns(4, 2, now - SESSION_IDLE_FLUSH_MS - 60_000).join("\n")}\n`,
			);
			const processed = new Map([
				...ompRanges(
					SESSION_ID,
					first.filter((ep) => ep.sessionId === SESSION_ID),
				),
				...ompRanges(
					OTHER_ID,
					first.filter((ep) => ep.sessionId === OTHER_ID),
				),
			]);
			const episodes = reader.getNewEpisodes(candidates(), processed);
			expect(episodes.map((ep) => ep.sessionId)).toEqual([OTHER_ID]);
			expect(episodes[0].content).toContain("turn 5");
		} finally {
			reader.close();
		}
	});
});

describe("OhMyPiEpisodeReader — poison pills (ARC-07)", () => {
	it("skips null, non-object and malformed lines without dropping other sessions", () => {
		takeSkipCount("oh-my-pi");
		const sessionsDir = join(root, "agent", "sessions");
		ompWrite(sessionsDir, "poison.jsonl", [
			ompHeader(SESSION_ID, T0),
			"null",
			...ompTurns(0, 4, T0),
			"[1,2]",
			"{oops",
		]);
		ompWrite(sessionsDir, "healthy.jsonl", [
			ompHeader(OTHER_ID, T0),
			...ompTurns(0, 4, T0),
		]);
		const reader = new OhMyPiEpisodeReader([sessionsDir]);
		try {
			const episodes = reader.getNewEpisodes(
				reader.getCandidateSessions(0).map((c) => c.id),
				new Map(),
			);
			expect(episodes.map((ep) => ep.sessionId).sort()).toEqual(
				[SESSION_ID, OTHER_ID].sort(),
			);
			expect(takeSkipCount("oh-my-pi")).toBeGreaterThan(0);
		} finally {
			reader.close();
		}
	});

	it("counts an unreadable session file as a skip and keeps the others", () => {
		takeSkipCount("oh-my-pi");
		const sessionsDir = join(root, "agent", "sessions");
		ompWrite(sessionsDir, "ok.jsonl", [
			ompHeader(SESSION_ID, T0),
			...ompTurns(0, 4, T0),
		]);
		// Listed as *.jsonl and stat-able, but reading it as a file fails.
		mkdirSync(join(sessionsDir, OMP_PROJECT, "broken.jsonl"));
		const reader = new OhMyPiEpisodeReader([sessionsDir]);
		try {
			const episodes = reader.getNewEpisodes(
				reader.getCandidateSessions(0).map((c) => c.id),
				new Map(),
			);
			expect(episodes.map((ep) => ep.sessionId)).toEqual([SESSION_ID]);
			expect(takeSkipCount("oh-my-pi")).toBeGreaterThan(0);
		} finally {
			reader.close();
		}
	});
});
