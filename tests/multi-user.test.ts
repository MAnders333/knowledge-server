import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import * as os from "node:os";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveUserId } from "../src/config-file";
import { KnowledgeDB } from "../src/db/database";

describe("resolveUserId", () => {
	const envKey = "KNOWLEDGE_USER_ID";
	let originalValue: string | undefined;

	beforeEach(() => {
		originalValue = process.env[envKey];
		delete process.env[envKey];
	});

	afterEach(() => {
		if (originalValue !== undefined) {
			process.env[envKey] = originalValue;
		} else {
			delete process.env[envKey];
		}
	});

	it("returns KNOWLEDGE_USER_ID env var when set", () => {
		process.env[envKey] = "alice";
		expect(resolveUserId()).toBe("alice");
	});

	it("env var takes precedence over config userId", () => {
		process.env[envKey] = "from-env";
		expect(resolveUserId("from-config")).toBe("from-env");
	});

	it("uses config userId when env var is absent", () => {
		expect(resolveUserId("from-config")).toBe("from-config");
	});

	it("falls back to hostname when neither env nor config", () => {
		// Spy to ensure a non-empty hostname is returned
		const hostnameSpy = spyOn(os, "hostname").mockReturnValue("test-machine");
		const result = resolveUserId();
		expect(result).toBe("test-machine");
		hostnameSpy.mockRestore();
	});

	it("returns 'default' when hostname is empty and no other source is set", () => {
		const hostnameSpy = spyOn(os, "hostname").mockReturnValue("");
		const result = resolveUserId();
		expect(result).toBe("default");
		hostnameSpy.mockRestore();
	});
});

describe("KnowledgeDB multi-user cursor isolation", () => {
	let tempDir: string;
	let db: KnowledgeDB;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-multiuser-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── getSourceCursor ────────────────────────────────────────────────────────

	it("returns zero-state cursor for unknown source+user", async () => {
		const cursor = await db.getSourceCursor("opencode", "alice");
		expect(cursor.source).toBe("opencode");
		expect(cursor.userId).toBe("alice");
		expect(cursor.lastMessageTimeCreated).toBe(0);
		expect(cursor.lastConsolidatedAt).toBe(0);
	});

	it("isolates cursors by user_id — different users have independent cursors", async () => {
		await db.updateSourceCursor("opencode", "alice", {
			lastMessageTimeCreated: 1000,
			lastConsolidatedAt: 2000,
		});
		await db.updateSourceCursor("opencode", "bob", {
			lastMessageTimeCreated: 5000,
			lastConsolidatedAt: 6000,
		});

		const alice = await db.getSourceCursor("opencode", "alice");
		const bob = await db.getSourceCursor("opencode", "bob");

		expect(alice.lastMessageTimeCreated).toBe(1000);
		expect(bob.lastMessageTimeCreated).toBe(5000);
	});

	it("advancing one user's cursor does not affect another's", async () => {
		await db.updateSourceCursor("opencode", "alice", {
			lastMessageTimeCreated: 1000,
		});

		// Bob's cursor is still zero
		const bob = await db.getSourceCursor("opencode", "bob");
		expect(bob.lastMessageTimeCreated).toBe(0);

		// Advance Alice further
		await db.updateSourceCursor("opencode", "alice", {
			lastMessageTimeCreated: 9999,
		});

		// Bob is still zero
		const bobAfter = await db.getSourceCursor("opencode", "bob");
		expect(bobAfter.lastMessageTimeCreated).toBe(0);

		// Alice is advanced
		const aliceAfter = await db.getSourceCursor("opencode", "alice");
		expect(aliceAfter.lastMessageTimeCreated).toBe(9999);
	});

	// ── recordEpisode / getProcessedEpisodeRanges ──────────────────────────────

	it("recordEpisode scopes to user — one user's episodes don't block another's", async () => {
		// Alice processes an episode
		await db.recordEpisode(
			"opencode",
			"alice",
			"session-1",
			"msg-start",
			"msg-end",
			"messages",
			1,
		);

		// Bob queries processed ranges for the same session — should see nothing
		const bobRanges = await db.getProcessedEpisodeRanges("opencode", "bob", [
			"session-1",
		]);
		expect(bobRanges.size).toBe(0);

		// Alice queries — should see her episode
		const aliceRanges = await db.getProcessedEpisodeRanges(
			"opencode",
			"alice",
			["session-1"],
		);
		expect(aliceRanges.size).toBe(1);
		expect(aliceRanges.get("session-1")).toHaveLength(1);
	});

	it("same session can be processed by multiple users independently", async () => {
		// Both Alice and Bob process the same session
		await db.recordEpisode(
			"opencode",
			"alice",
			"shared-session",
			"s",
			"e",
			"messages",
			1,
		);
		await db.recordEpisode(
			"opencode",
			"bob",
			"shared-session",
			"s",
			"e",
			"messages",
			1,
		);

		const aliceRanges = await db.getProcessedEpisodeRanges(
			"opencode",
			"alice",
			["shared-session"],
		);
		const bobRanges = await db.getProcessedEpisodeRanges("opencode", "bob", [
			"shared-session",
		]);

		expect(aliceRanges.size).toBe(1);
		expect(bobRanges.size).toBe(1);
	});

	// ── Backwards compatibility (default user) ─────────────────────────────────

	it("'default' user works as a single-user fallback", async () => {
		await db.updateSourceCursor("opencode", "default", {
			lastMessageTimeCreated: 42,
		});
		const cursor = await db.getSourceCursor("opencode", "default");
		expect(cursor.lastMessageTimeCreated).toBe(42);
		expect(cursor.userId).toBe("default");
	});
});
