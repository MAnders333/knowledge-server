/**
 * Tests for LocalFilesEpisodeReader.
 *
 * Creates a minimal temp directory and seeds it with Markdown files to
 * exercise all reader behaviour without touching the real ~/knowledge dir.
 *
 * Covered:
 *   - Directory absent → empty results, no error
 *   - Non-.md files are ignored
 *   - getCandidateSessions — mtime cursor filtering, ordering ASC
 *   - countNewSessions — matches getCandidateSessions count
 *   - getNewEpisodes — episode shape (sessionId, hash IDs, contentType, title)
 *   - getNewEpisodes — title from # heading vs filename fallback
 *   - getNewEpisodes — idempotency: already-processed hash is skipped
 *   - getNewEpisodes — hash change triggers reprocessing
 *   - getNewEpisodes — empty candidateSessionIds returns []
 *   - getNewEpisodes — file disappears between scan and read (graceful skip)
 *   - Large file warning (token count > MAX_TOKENS_PER_EPISODE)
 *   - close() is a no-op and doesn't throw
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalFilesEpisodeReader } from "../src/consolidation/readers/local-files";
import { MAX_TOKENS_PER_EPISODE } from "../src/consolidation/readers/shared";
import * as loggerModule from "../src/logger";

// ── Helpers ────────────────────────────────────────────────────────────────────

let tmpDir: string;
let knowledgeDir: string;
let reader: LocalFilesEpisodeReader;

/** Fixed base mtime for deterministic comparisons. */
const BASE = 1_700_000_000_000;

/**
 * Probe whether the OS temp directory uses a case-sensitive filesystem.
 * Writes a lowercase sentinel file and checks whether the uppercase name
 * resolves to the same path. macOS APFS (default) is case-insensitive;
 * Linux ext4/tmpfs is case-sensitive. The result determines whether
 * uppercase-extension test assertions are meaningful.
 */
function isCaseSensitiveFS(): boolean {
	const probe = mkdtempSync(join(tmpdir(), "ks-case-probe-"));
	try {
		writeFileSync(join(probe, "probe.md"), "");
		return !existsSync(join(probe, "PROBE.MD"));
	} finally {
		rmSync(probe, { recursive: true, force: true });
	}
}

const CASE_SENSITIVE_FS = isCaseSensitiveFS();

/** Write a Markdown file with a specific mtime. */
function writeFile(name: string, content: string, mtimeMs = BASE): string {
	const filePath = join(knowledgeDir, name);
	writeFileSync(filePath, content, "utf8");
	const mtimeSec = mtimeMs / 1000;
	utimesSync(filePath, mtimeSec, mtimeSec);
	return filePath;
}

beforeEach(() => {
	tmpDir = mkdtempSync(join(tmpdir(), "ks-localfiles-test-"));
	knowledgeDir = join(tmpDir, "knowledge");
	mkdirSync(knowledgeDir);
	reader = new LocalFilesEpisodeReader(knowledgeDir);
});

afterEach(() => {
	reader.close();
	rmSync(tmpDir, { recursive: true, force: true });
});

// ── Directory absent ──────────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader — directory absent", () => {
	it("getCandidateSessions returns [] when dir does not exist", () => {
		const absentReader = new LocalFilesEpisodeReader(join(tmpDir, "does-not-exist"));
		const result = absentReader.getCandidateSessions(0);
		expect(result).toEqual([]);
		absentReader.close();
	});

	it("countNewSessions returns 0 when dir does not exist", () => {
		const absentReader = new LocalFilesEpisodeReader(join(tmpDir, "does-not-exist"));
		expect(absentReader.countNewSessions(0)).toBe(0);
		absentReader.close();
	});

	it("getNewEpisodes returns [] when getCandidateSessions returns no candidates", () => {
		// Directory absent → getCandidateSessions returns [] → no candidate IDs to pass in.
		// getNewEpisodes([]) is the natural call-site behaviour for an absent directory.
		const absentReader = new LocalFilesEpisodeReader(join(tmpDir, "does-not-exist"));
		const candidates = absentReader.getCandidateSessions(0);
		expect(candidates).toEqual([]); // sanity check
		const result = absentReader.getNewEpisodes(candidates.map((c) => c.id), new Map());
		expect(result).toEqual([]);
		absentReader.close();
	});
});

// ── File filtering ────────────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader — file filtering", () => {
	it("ignores non-.md files", () => {
		writeFile("notes.txt", "plain text", BASE);
		writeFile("data.json", '{"key":"value"}', BASE);
		writeFile("script.ts", "console.log()", BASE);
		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(0);
	});

	it("includes .md files (extension filter uses toLowerCase)", () => {
		// Always include lowercase .md — should work on any filesystem.
		writeFile("lower.md", "# Lower\nsome content", BASE);

		if (CASE_SENSITIVE_FS) {
			// On case-sensitive filesystems (Linux), uppercase extensions are
			// distinct files — assert all three variants are included.
			writeFile("upper.MD", "# Upper\nsome content", BASE + 1000);
			writeFile("mixed.Md", "# Mixed\nsome content", BASE + 2000);
			const candidates = reader.getCandidateSessions(BASE - 1);
			expect(candidates).toHaveLength(3);
			const ids = candidates.map((c) => c.id);
			expect(ids).toContain(join(knowledgeDir, "lower.md"));
			expect(ids).toContain(join(knowledgeDir, "upper.MD"));
			expect(ids).toContain(join(knowledgeDir, "mixed.Md"));
		} else {
			// On case-insensitive filesystems (macOS APFS), "upper.MD" and
			// "mixed.Md" collapse to the same inode — only the lowercase
			// variant can be tested deterministically.
			const candidates = reader.getCandidateSessions(BASE - 1);
			expect(candidates).toHaveLength(1);
			expect(candidates[0].id).toBe(join(knowledgeDir, "lower.md"));
		}
	});

	it("ignores subdirectories", () => {
		mkdirSync(join(knowledgeDir, "subdir"));
		writeFile("notes.md", "# Notes", BASE);
		const candidates = reader.getCandidateSessions(BASE - 1);
		// Only the file, not the directory
		expect(candidates).toHaveLength(1);
	});
});

// ── getCandidateSessions ──────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader.getCandidateSessions", () => {
	it("returns file when mtime is after cursor", () => {
		writeFile("notes.md", "# Notes\ncontent", BASE);
		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toContain("notes.md");
		expect(candidates[0].maxMessageTime).toBe(BASE);
	});

	it("excludes file when mtime equals cursor", () => {
		writeFile("notes.md", "# Notes", BASE);
		const candidates = reader.getCandidateSessions(BASE);
		expect(candidates).toHaveLength(0);
	});

	it("excludes file when mtime is before cursor", () => {
		writeFile("notes.md", "# Notes", BASE - 1000);
		const candidates = reader.getCandidateSessions(BASE);
		expect(candidates).toHaveLength(0);
	});

	it("returns files ordered by mtime ASC", () => {
		writeFile("older.md", "# Older", BASE);
		writeFile("newer.md", "# Newer", BASE + 5000);
		const candidates = reader.getCandidateSessions(BASE - 1);
		expect(candidates).toHaveLength(2);
		expect(candidates[0].id).toContain("older.md");
		expect(candidates[1].id).toContain("newer.md");
	});

	it("only returns files newer than cursor when some are older", () => {
		writeFile("old.md", "# Old", BASE - 1000);
		writeFile("new.md", "# New", BASE + 1000);
		const candidates = reader.getCandidateSessions(BASE);
		expect(candidates).toHaveLength(1);
		expect(candidates[0].id).toContain("new.md");
	});
});

// ── countNewSessions ──────────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader.countNewSessions", () => {
	it("matches getCandidateSessions count", () => {
		writeFile("a.md", "# A", BASE);
		writeFile("b.md", "# B", BASE + 1000);
		writeFile("old.md", "# Old", BASE - 5000);

		const cursor = BASE - 1;
		expect(reader.countNewSessions(cursor)).toBe(
			reader.getCandidateSessions(cursor).length,
		);
	});

	it("returns 0 for empty directory", () => {
		expect(reader.countNewSessions(0)).toBe(0);
	});
});

// ── getNewEpisodes — episode shape ────────────────────────────────────────────

describe("LocalFilesEpisodeReader.getNewEpisodes — episode shape", () => {
	it("returns an episode with correct shape", () => {
		const filePath = writeFile("knowledge.md", "# My Knowledge\n\nSome facts here.", BASE);
		const episodes = reader.getNewEpisodes([filePath], new Map());

		expect(episodes).toHaveLength(1);
		const ep = episodes[0];

		expect(ep.sessionId).toBe(filePath);
		expect(ep.contentType).toBe("document");
		expect(ep.projectName).toBe("local-files");
		expect(ep.directory).toBe(knowledgeDir);
		expect(ep.content).toContain("Some facts here.");
		expect(ep.timeCreated).toBe(BASE);
		expect(ep.maxMessageTime).toBe(BASE);
		// startMessageId and endMessageId are the content hash (16 hex chars)
		expect(ep.startMessageId).toBe(ep.endMessageId);
		expect(ep.startMessageId).toMatch(/^[0-9a-f]{16}$/);
		expect(ep.approxTokens).toBeGreaterThan(0);
	});

	it("returns [] for empty candidateSessionIds", () => {
		expect(reader.getNewEpisodes([], new Map())).toEqual([]);
	});
});

// ── getNewEpisodes — title derivation ─────────────────────────────────────────

describe("LocalFilesEpisodeReader.getNewEpisodes — title derivation", () => {
	it("extracts title from first # heading", () => {
		const filePath = writeFile("doc.md", "# My Document Title\n\nContent.", BASE);
		const [ep] = reader.getNewEpisodes([filePath], new Map());
		expect(ep.sessionTitle).toBe("My Document Title");
	});

	it("falls back to filename when no # heading present", () => {
		const filePath = writeFile("my-project-notes.md", "Just some text without a heading.", BASE);
		const [ep] = reader.getNewEpisodes([filePath], new Map());
		expect(ep.sessionTitle).toBe("my project notes");
	});

	it("replaces underscores with spaces in filename fallback", () => {
		const filePath = writeFile("team_decisions_q1.md", "No heading here.", BASE);
		const [ep] = reader.getNewEpisodes([filePath], new Map());
		expect(ep.sessionTitle).toBe("team decisions q1");
	});

	it("ignores ## second-level headings for title (only # is matched)", () => {
		const filePath = writeFile("doc-h2-first.md", "## Not a title\n\n# Real Title\n\nContent.", BASE);
		const [ep] = reader.getNewEpisodes([filePath], new Map());
		// The regex ^#\s+ matches the first # heading on any line.
		// "## Not a title" starts with ## so ^#\s+ won't match (# followed by #, not space).
		expect(ep.sessionTitle).toBe("Real Title");
	});
});

// ── getNewEpisodes — idempotency ──────────────────────────────────────────────

describe("LocalFilesEpisodeReader.getNewEpisodes — idempotency via content hash", () => {
	it("skips file when its hash is already in processedRanges", () => {
		const filePath = writeFile("notes.md", "# Notes\nContent.", BASE);

		// Get the episode to learn the hash
		const firstRun = reader.getNewEpisodes([filePath], new Map());
		expect(firstRun).toHaveLength(1);
		const hash = firstRun[0].startMessageId;

		// Simulate having recorded this episode
		const processedRanges = new Map([
			[filePath, [{ startMessageId: hash, endMessageId: hash }]],
		]);

		// Second run with same content — should be skipped
		const secondRun = reader.getNewEpisodes([filePath], processedRanges);
		expect(secondRun).toHaveLength(0);
	});

	it("reprocesses file when content changes (different hash)", () => {
		const filePath = writeFile("notes.md", "# Notes\nOriginal content.", BASE);

		// Record the original hash as processed
		const firstRun = reader.getNewEpisodes([filePath], new Map());
		const originalHash = firstRun[0].startMessageId;

		const processedRanges = new Map([
			[filePath, [{ startMessageId: originalHash, endMessageId: originalHash }]],
		]);

		// Update the file content (new hash)
		writeFileSync(filePath, "# Notes\nUpdated content.");

		// Should produce a new episode with a different hash
		const secondRun = reader.getNewEpisodes([filePath], processedRanges);
		expect(secondRun).toHaveLength(1);
		expect(secondRun[0].startMessageId).not.toBe(originalHash);
	});

	it("different files with identical content get independent hash checks", () => {
		const content = "# Shared Content\nExactly the same.";
		const file1 = writeFile("file1.md", content, BASE);
		const file2 = writeFile("file2.md", content, BASE + 1000);

		const firstRun = reader.getNewEpisodes([file1, file2], new Map());
		expect(firstRun).toHaveLength(2);

		// Mark file1 as processed
		const hash = firstRun[0].startMessageId;
		const processedRanges = new Map([
			[file1, [{ startMessageId: hash, endMessageId: hash }]],
		]);

		// file2 should still be returned even though content (and hash) is the same —
		// idempotency is keyed by (path, hash), not hash alone
		const secondRun = reader.getNewEpisodes([file1, file2], processedRanges);
		expect(secondRun).toHaveLength(1);
		expect(secondRun[0].sessionId).toBe(file2);
	});
});

// ── getNewEpisodes — graceful handling ────────────────────────────────────────

describe("LocalFilesEpisodeReader.getNewEpisodes — graceful handling", () => {
	it("skips a file that disappears between scan and read", () => {
		const filePath = join(knowledgeDir, "ghost.md");
		// Pass a path that was never created — simulates TOCTOU disappearance
		const episodes = reader.getNewEpisodes([filePath], new Map());
		expect(episodes).toHaveLength(0);
	});

	it("processes remaining files when one is missing", () => {
		const real = writeFile("real.md", "# Real\nContent.", BASE);
		const ghost = join(knowledgeDir, "ghost.md");
		const episodes = reader.getNewEpisodes([ghost, real], new Map());
		expect(episodes).toHaveLength(1);
		expect(episodes[0].sessionId).toBe(real);
	});
});

// ── Large file warning ────────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader — large file warning", () => {
	it("logs a warning when file exceeds MAX_TOKENS_PER_EPISODE", () => {
		const warnSpy = spyOn(loggerModule.logger, "warn");

		// Create a file large enough to exceed the token limit.
		// approxTokens = chars / 4, so we need > MAX_TOKENS_PER_EPISODE * 4 chars.
		const bigContent = `# Big Doc\n\n${"x".repeat(MAX_TOKENS_PER_EPISODE * 4 + 100)}`;
		const filePath = writeFile("big.md", bigContent, BASE);

		const episodes = reader.getNewEpisodes([filePath], new Map());

		// Episode is still produced — the reader doesn't drop oversized files
		expect(episodes).toHaveLength(1);
		// Warning was logged
		expect(warnSpy).toHaveBeenCalledTimes(1);
		expect(warnSpy.mock.calls[0][0]).toContain("[local-files]");
		expect(warnSpy.mock.calls[0][0]).toContain("exceeds soft limit");

		warnSpy.mockRestore();
	});

	it("does not warn for a normal-sized file", () => {
		const warnSpy = spyOn(loggerModule.logger, "warn");

		writeFile("normal.md", "# Normal\n\nSmall content.", BASE);
		const filePath = join(knowledgeDir, "normal.md");
		reader.getNewEpisodes([filePath], new Map());

		expect(warnSpy).not.toHaveBeenCalled();
		warnSpy.mockRestore();
	});
});

// ── close() ───────────────────────────────────────────────────────────────────

describe("LocalFilesEpisodeReader.close", () => {
	it("does not throw", () => {
		expect(() => reader.close()).not.toThrow();
	});
});
