import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { CREATE_TABLES } from "../src/db/sqlite/schema";
import { ServerStateDB } from "../src/db/state/index";

describe("KnowledgeDB", () => {
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("should create tables on initialization", async () => {
		const stats = await db.getStats();
		expect(stats.total).toBe(0);
	});

	it("recordInjections is a no-op for an empty batch", async () => {
		await db.recordInjections([], Date.now());
	});

	it("reports strength computed on read; the stored column is not rewritten", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "live-strength",
			type: "fact",
			content: "Strength is live",
			topics: [],
			confidence: 0.9,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now - 400 * 86_400_000,
			updatedAt: now,
			lastAccessedAt: now - 400 * 86_400_000,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});
		const live = (await db.getEntry("live-strength"))?.strength ?? 1;
		expect(live).toBeLessThan(0.05);
		const raw = new Database(join(tempDir, "test.db"), { readonly: true });
		try {
			const row = raw
				.prepare("SELECT strength FROM knowledge_entry WHERE id = ?")
				.get("live-strength") as { strength: number };
			expect(row.strength).toBe(1.0);
		} finally {
			raw.close();
		}
		// Ordering and the review stale filter use the live value.
		const stale = (await db.getActiveEntries()).filter(
			(e) => e.id === "live-strength",
		);
		expect(stale[0]?.strength).toBeLessThan(0.3);
	});

	it("should manage consolidation state", async () => {
		const state = await serverStateDb.getConsolidationState();
		expect(state.lastConsolidatedAt).toBe(0);
		expect(state.totalSessionsProcessed).toBe(0);

		await serverStateDb.updateConsolidationState({
			lastConsolidatedAt: 1000000,
			totalSessionsProcessed: 50,
			totalEntriesCreated: 25,
		});

		const updated = await serverStateDb.getConsolidationState();
		expect(updated.lastConsolidatedAt).toBe(1000000);
		expect(updated.totalSessionsProcessed).toBe(50);
		expect(updated.totalEntriesCreated).toBe(25);
	});

	it("should record and retrieve episode ranges", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-start-1",
			"msg-end-1",
			"messages",
			3,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-start-2",
			"msg-end-2",
			"compaction_summary",
			1,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-2",
			"msg-start-3",
			"msg-end-3",
			"messages",
			0,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges([
			"session-1",
			"session-2",
		]);

		expect(ranges.size).toBe(2);
		const s1 = ranges.get("session-1");
		expect(s1).toBeDefined();
		expect(s1).toHaveLength(2);
		expect(
			(s1 ?? []).some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-1" &&
					r.endMessageId === "msg-end-1",
			),
		).toBe(true);
		expect(
			(s1 ?? []).some(
				(r) =>
					r.source === "opencode" &&
					r.startMessageId === "msg-start-2" &&
					r.endMessageId === "msg-end-2",
			),
		).toBe(true);

		const s2 = ranges.get("session-2");
		expect(s2).toBeDefined();
		expect(s2).toHaveLength(1);
		expect((s2 ?? [{}])[0].startMessageId).toBe("msg-start-3");
		expect((s2 ?? [{}])[0].endMessageId).toBe("msg-end-3");
	});

	it("episodes from different sources for the same session are both returned", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);
		await serverStateDb.recordEpisode(
			"claude-code",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges(["session-1"]);
		// Both source entries are returned — idempotency is per (source, session, start, end)
		expect(ranges.get("session-1")).toHaveLength(2);
	});

	it("recordEpisode is idempotent — duplicate inserts are ignored", async () => {
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);
		await serverStateDb.recordEpisode(
			"opencode",
			"session-1",
			"msg-a",
			"msg-b",
			"messages",
			2,
		);

		const ranges = await serverStateDb.getProcessedEpisodeRanges(["session-1"]);
		expect(ranges.get("session-1")).toHaveLength(1);
	});

	it("getProcessedEpisodeRanges returns empty map for unknown session", async () => {
		const ranges = await serverStateDb.getProcessedEpisodeRanges([
			"no-such-session",
		]);
		expect(ranges.size).toBe(0);
	});

	// ── Embedding Metadata ──

	// ── getEntriesMissingEmbeddings ──

	// ── reinitialize clears embedding_metadata ──
});
