/**
 * Tests for ActivationEngine.checkAndReEmbed() — the auto re-embed
 * mechanism that detects embedding model changes and regenerates vectors.
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
import { config } from "../src/config";
import { KnowledgeDB } from "../src/db/database";
import { fakeEmbedding, makeEntry } from "./fixtures";

describe("checkAndReEmbed", () => {
	let db: KnowledgeDB;
	let activation: ActivationEngine;
	let tempDir: string;
	let originalModel: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-reembed-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
		activation = new ActivationEngine(db);
		originalModel = config.embedding.model;
	});

	afterEach(() => {
		config.embedding.model = originalModel;
		mock.restore();
		db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── First run / no metadata ──

	it("seeds metadata from existing entries on first run (no stored metadata)", async () => {
		const emb = fakeEmbedding("test content");
		db.insertEntry(makeEntry({ id: "e1", embedding: emb }));

		// No metadata stored yet — first run scenario
		expect(db.getEmbeddingMetadata()).toBeNull();

		// Mock embed to prevent real API calls (used only in the no-entries probe path)
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);

		const result = await activation.checkAndReEmbed();

		expect(result).toBe(false); // no re-embed performed
		const meta = db.getEmbeddingMetadata();
		expect(meta).not.toBeNull();
		expect(meta?.model).toBe(config.embedding.model);
		expect(meta?.dimensions).toBe(emb.length);
	});

	it("probes API for dimensions when no entries exist on first run", async () => {
		const probeEmb = fakeEmbedding("dimension probe");
		const embedSpy = spyOn(activation.embeddings, "embed").mockResolvedValue(
			probeEmb,
		);

		expect(db.getEmbeddingMetadata()).toBeNull();

		const result = await activation.checkAndReEmbed();

		expect(result).toBe(false);
		expect(embedSpy).toHaveBeenCalledWith("dimension probe");
		const meta = db.getEmbeddingMetadata();
		expect(meta?.model).toBe(config.embedding.model);
		expect(meta?.dimensions).toBe(probeEmb.length);
	});

	it("handles API probe failure gracefully on first run", async () => {
		spyOn(activation.embeddings, "embed").mockRejectedValue(
			new Error("network error"),
		);

		const result = await activation.checkAndReEmbed();

		expect(result).toBe(false);
		// Metadata is NOT recorded — will be seeded on first successful embedding call
		expect(db.getEmbeddingMetadata()).toBeNull();
	});

	// ── Model matches — no-op ──

	it("returns false when stored model matches configured model", async () => {
		db.setEmbeddingMetadata(config.embedding.model, 1536);

		const result = await activation.checkAndReEmbed();

		expect(result).toBe(false);
	});

	// ── Model change — re-embed ──

	it("re-embeds all entries in-place when model changes", async () => {
		const oldEmb = fakeEmbedding("old");
		const newEmb = fakeEmbedding("new");

		// Insert entries with "old model" embeddings
		db.insertEntry(makeEntry({ id: "e1", content: "Entry one", embedding: oldEmb }));
		db.insertEntry(makeEntry({ id: "e2", content: "Entry two", embedding: oldEmb }));

		// Record metadata for the old model
		db.setEmbeddingMetadata("old-model", oldEmb.length);

		// Configure a different model
		config.embedding.model = "new-model";

		// Mock embedBatch to return new embeddings
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			newEmb,
			newEmb,
		]);

		const result = await activation.checkAndReEmbed();

		expect(result).toBe(true);

		// Verify metadata was updated
		const meta = db.getEmbeddingMetadata();
		expect(meta?.model).toBe("new-model");
		expect(meta?.dimensions).toBe(newEmb.length);

		// Verify entries have new embeddings (not NULL, not old)
		const entries = db.getActiveEntriesWithEmbeddings();
		expect(entries.length).toBe(2);
		for (const entry of entries) {
			// New embeddings should be written
			expect(entry.embedding).toBeDefined();
			expect(entry.embedding.length).toBe(newEmb.length);
		}
	});

	it("old embeddings remain intact when embedBatch fails (no NULL gap)", async () => {
		const oldEmb = fakeEmbedding("old");

		db.insertEntry(makeEntry({ id: "e1", content: "Entry one", embedding: oldEmb }));
		db.insertEntry(makeEntry({ id: "e2", content: "Entry two", embedding: oldEmb }));
		db.setEmbeddingMetadata("old-model", oldEmb.length);

		config.embedding.model = "new-model";

		// embedBatch fails — simulates network error
		spyOn(activation.embeddings, "embedBatch").mockRejectedValue(
			new Error("rate limit exceeded"),
		);

		await expect(activation.checkAndReEmbed()).rejects.toThrow(
			"rate limit exceeded",
		);

		// Entries should still have their old embeddings — NOT NULL
		const entries = db.getActiveEntriesWithEmbeddings();
		expect(entries.length).toBe(2);
		for (const entry of entries) {
			expect(entry.embedding).toBeDefined();
			expect(entry.embedding.length).toBe(oldEmb.length);
		}

		// Metadata should still point to old model (not updated on failure)
		const meta = db.getEmbeddingMetadata();
		expect(meta?.model).toBe("old-model");
	});

	it("updates metadata even when no entries need re-embedding (model change, 0 entries)", async () => {
		db.setEmbeddingMetadata("old-model", 1536);

		config.embedding.model = "new-model";

		const probeEmb = fakeEmbedding("dimension probe");
		spyOn(activation.embeddings, "embed").mockResolvedValue(probeEmb);

		const result = await activation.checkAndReEmbed();

		// No entries to re-embed, but metadata should still be updated
		expect(result).toBe(false);
		const meta = db.getEmbeddingMetadata();
		expect(meta?.model).toBe("new-model");
		expect(meta?.dimensions).toBe(probeEmb.length);
	});

	it("handles model change with 0 entries and API probe failure", async () => {
		db.setEmbeddingMetadata("old-model", 1536);

		config.embedding.model = "new-model";

		spyOn(activation.embeddings, "embed").mockRejectedValue(
			new Error("api down"),
		);

		const result = await activation.checkAndReEmbed();

		// No entries, probe failed — metadata stays as old model
		expect(result).toBe(false);
		const meta = db.getEmbeddingMetadata();
		expect(meta?.model).toBe("old-model");
	});

	it("does not re-embed superseded entries", async () => {
		const oldEmb = fakeEmbedding("old");
		const newEmb = fakeEmbedding("new");

		// Active entry — should be re-embedded
		db.insertEntry(makeEntry({ id: "e1", status: "active", embedding: oldEmb }));
		// Superseded entry — should NOT be re-embedded
		db.insertEntry(
			makeEntry({
				id: "e2",
				status: "superseded",
				supersededBy: "e1",
				embedding: oldEmb,
			}),
		);

		db.setEmbeddingMetadata("old-model", oldEmb.length);
		config.embedding.model = "new-model";

		// Only 1 entry should be passed to embedBatch (the active one)
		const embedBatchSpy = spyOn(
			activation.embeddings,
			"embedBatch",
		).mockResolvedValue([newEmb]);

		await activation.checkAndReEmbed();

		// embedBatch should only receive 1 text (for the active entry)
		expect(embedBatchSpy).toHaveBeenCalledTimes(1);
		const callArgs = embedBatchSpy.mock.calls[0][0] as string[];
		expect(callArgs.length).toBe(1);
	});

	it("re-embeds conflicted entries alongside active ones", async () => {
		const oldEmb = fakeEmbedding("old");
		const newEmb = fakeEmbedding("new");

		db.insertEntry(makeEntry({ id: "e1", status: "active", embedding: oldEmb }));
		db.insertEntry(
			makeEntry({ id: "e2", status: "conflicted", embedding: oldEmb }),
		);

		db.setEmbeddingMetadata("old-model", oldEmb.length);
		config.embedding.model = "new-model";

		const embedBatchSpy = spyOn(
			activation.embeddings,
			"embedBatch",
		).mockResolvedValue([newEmb, newEmb]);

		await activation.checkAndReEmbed();

		// Both active and conflicted entries should be re-embedded
		const callArgs = embedBatchSpy.mock.calls[0][0] as string[];
		expect(callArgs.length).toBe(2);

		const entries = db.getActiveEntriesWithEmbeddings();
		expect(entries.length).toBe(2);
	});
});
