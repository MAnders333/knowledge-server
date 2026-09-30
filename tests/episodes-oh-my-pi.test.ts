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
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	OhMyPiEpisodeReader,
	resolveOhMyPiSessionsDirs,
} from "../src/daemon/readers/oh-my-pi";

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
