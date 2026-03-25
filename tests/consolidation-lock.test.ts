/**
 * Tests for the per-store consolidation lock (tryAcquireConsolidationLock /
 * releaseConsolidationLock) on IKnowledgeStore implementations.
 *
 * Tests cover:
 * - KnowledgeDB (SQLite): always-acquires no-op (single-process, no cross-process risk)
 * - ConsolidationEngine: skips a store whose lock is held and leaves its episodes for retry
 * - ConsolidationEngine: releases store lock in finally even when consolidation throws
 * - ConsolidationEngine.tryLock / unlock: in-process guard is independent of store locks
 */
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import type { Episode, IEpisodeReader, ProcessedRange } from "../src/types";
import { fakeEmbedding } from "./fixtures";

// ── SQLite KnowledgeDB lock (no-op) ──────────────────────────────────────────

describe("KnowledgeDB.tryAcquireConsolidationLock (SQLite no-op)", () => {
	let tempDir: string;
	let db: KnowledgeDB;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-db-lock-test-"));
		db = new KnowledgeDB(join(tempDir, "knowledge.db"));
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("always returns true (no cross-process lock needed for SQLite)", async () => {
		expect(await db.tryAcquireConsolidationLock()).toBe(true);
	});

	it("returns true even when called multiple times without release", async () => {
		await db.tryAcquireConsolidationLock();
		expect(await db.tryAcquireConsolidationLock()).toBe(true);
	});

	it("releaseConsolidationLock is a no-op and does not throw", async () => {
		await expect(db.releaseConsolidationLock()).resolves.toBeUndefined();
	});
});

// ── ConsolidationEngine: store-level lock integration ────────────────────────

/**
 * Minimal mock episode reader that serves a single episode.
 */
class SingleEpisodeReader implements IEpisodeReader {
	readonly source = "opencode";
	private readonly episode: Episode;

	constructor(episode: Episode) {
		this.episode = episode;
	}

	getCandidateSessions(_after: number) {
		return [
			{
				id: this.episode.sessionId,
				maxMessageTime: this.episode.maxMessageTime,
			},
		];
	}

	countNewSessions(_after: number) {
		return 1;
	}

	getNewEpisodes(
		_ids: string[],
		_ranges: Map<string, ProcessedRange[]>,
	): Episode[] {
		return [this.episode];
	}

	close() {}
}

const FAKE_EPISODE: Episode = {
	id: "ep-lock-test-1",
	userId: "default",
	source: "opencode",
	sessionId: "session-lock-test",
	startMessageId: "msg-0",
	endMessageId: "msg-10",
	sessionTitle: "Lock test session",
	projectName: "test-project",
	directory: "/tmp/test",
	content: "The sky is blue.",
	contentType: "messages",
	timeCreated: Date.now() - 1000,
	maxMessageTime: Date.now() - 500,
	approxTokens: 10,
	uploadedAt: Date.now(),
};

describe("ConsolidationEngine: store lock in consolidateExtractedToStore", () => {
	let tempDir: string;
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let engine: ConsolidationEngine;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-engine-lock-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
		const activation = new ActivationEngine(db);
		const reader = new SingleEpisodeReader(FAKE_EPISODE);
		engine = new ConsolidationEngine(
			db,
			serverStateDb,
			activation,
			[reader],
			null,
		);
	});

	afterEach(async () => {
		// Restore all prototype spies (e.g. ConsolidationLLM.prototype.extractKnowledge)
		// to prevent leaking into other test files in CI where Bun runs all tests in one process.
		mock.restore();
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("skips a store and leaves episodes for retry when store lock is held", async () => {
		// Simulate another process holding the store lock by making tryAcquire return false.
		spyOn(db, "tryAcquireConsolidationLock").mockResolvedValue(false);
		const releaseSpy = spyOn(db, "releaseConsolidationLock");

		// LLM must return at least one entry so consolidateExtractedToStore is reached
		// (the entries.length === 0 early-return guard would bypass the lock entirely).
		const { ConsolidationLLM } = await import("../src/consolidation/llm");
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "The sky is blue.",
				topics: ["sky"],
				confidence: 0.9,
			},
		]);

		engine.tryLock();
		const result = await engine.consolidate();
		engine.unlock();

		// Entries were not written — the locked store was skipped.
		expect(result.entriesCreated).toBe(0);
		// release must NOT be called when acquire returned false.
		expect(releaseSpy).not.toHaveBeenCalled();
	});

	it("releases the store lock after successful consolidation", async () => {
		const acquireSpy = spyOn(
			db,
			"tryAcquireConsolidationLock",
		).mockResolvedValue(true);
		const releaseSpy = spyOn(db, "releaseConsolidationLock").mockResolvedValue(
			undefined,
		);

		// Must return ≥1 entry so consolidateExtractedToStore is entered past the early-return guard.
		const { ConsolidationLLM } = await import("../src/consolidation/llm");
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "The sky is blue.",
				topics: ["sky"],
				confidence: 0.9,
			},
		]);
		// Stub embeddings to avoid real HTTP calls.
		spyOn(
			(engine as unknown as { activation: ActivationEngine }).activation
				.embeddings,
			"embed",
		).mockResolvedValue(fakeEmbedding(384));

		engine.tryLock();
		await engine.consolidate();
		engine.unlock();

		expect(acquireSpy).toHaveBeenCalled();
		expect(releaseSpy).toHaveBeenCalled();
	});

	it("releases the store lock even when a write inside the store throws", async () => {
		spyOn(db, "tryAcquireConsolidationLock").mockResolvedValue(true);
		const releaseSpy = spyOn(db, "releaseConsolidationLock").mockResolvedValue(
			undefined,
		);

		// Return ≥1 entry so we enter consolidateExtractedToStore past the lock.
		const { ConsolidationLLM } = await import("../src/consolidation/llm");
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "The sky is blue.",
				topics: ["sky"],
				confidence: 0.9,
			},
		]);
		// Stub embeddings to avoid real HTTP, then make getActiveEntriesWithEmbeddings throw
		// to simulate a DB error inside the try block (after lock is acquired).
		spyOn(
			(engine as unknown as { activation: ActivationEngine }).activation
				.embeddings,
			"embed",
		).mockResolvedValue(fakeEmbedding(384));
		spyOn(db, "getActiveEntriesWithEmbeddings").mockRejectedValue(
			new Error("Simulated DB failure"),
		);

		engine.tryLock();
		// consolidate() must not throw — Promise.allSettled absorbs per-store errors.
		const result = await engine.consolidate();
		engine.unlock();

		// Lock was released despite the in-store error.
		expect(releaseSpy).toHaveBeenCalled();
		expect(result.entriesCreated).toBe(0);
	});
});

// ── ConsolidationEngine: in-process tryLock guard (Layer 1) ──────────────────

describe("ConsolidationEngine.tryLock / unlock (in-process guard)", () => {
	let tempDir: string;
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let engine: ConsolidationEngine;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-trylock-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
		const activation = new ActivationEngine(db);
		engine = new ConsolidationEngine(db, serverStateDb, activation, [], null);
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("tryLock returns true when not locked", () => {
		expect(engine.tryLock()).toBe(true);
	});

	it("tryLock returns false when already locked", () => {
		engine.tryLock();
		expect(engine.tryLock()).toBe(false);
	});

	it("tryLock returns true again after unlock", () => {
		engine.tryLock();
		engine.unlock();
		expect(engine.tryLock()).toBe(true);
	});

	it("isConsolidating reflects lock state", () => {
		expect(engine.isConsolidating).toBe(false);
		engine.tryLock();
		expect(engine.isConsolidating).toBe(true);
		engine.unlock();
		expect(engine.isConsolidating).toBe(false);
	});
});
