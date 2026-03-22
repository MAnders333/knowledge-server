import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EmbeddingClient } from "../src/activation/embeddings";
import { KnowledgeDB } from "../src/db/database";
import { KnowledgeService } from "../src/services/knowledge-service";
import { makeEntry } from "./fixtures";

describe("KnowledgeService.updateEntry", () => {
	let db: KnowledgeDB;
	let service: KnowledgeService;
	let embedSpy: ReturnType<typeof spyOn>;
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-service-test-"));
		db = new KnowledgeDB(
			join(tempDir, "test.db"),
			join(tempDir, "opencode-fake.db"),
		);
		const embedder = new EmbeddingClient();
		embedSpy = spyOn(embedder, "embed").mockResolvedValue([0.1, 0.2, 0.3]);
		service = new KnowledgeService(db, embedder);
	});

	afterEach(async () => {
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("does not re-embed when only non-semantic fields change", async () => {
		await db.insertEntry(makeEntry({ id: "e1", embedding: [0.9, 0.9, 0.9] }));

		await service.updateEntry("e1", { status: "archived" });

		expect(embedSpy).not.toHaveBeenCalled();
		// Embedding should be unchanged
		const entry = await db.getEntry("e1");
		expect(entry?.embedding?.[0]).toBeCloseTo(0.9);
	});

	it("re-embeds when content changes", async () => {
		await db.insertEntry(makeEntry({ id: "e2", embedding: [0.9, 0.9, 0.9] }));

		await service.updateEntry("e2", { content: "Updated content" });

		expect(embedSpy).toHaveBeenCalledTimes(1);
		const entry = await db.getEntry("e2");
		expect(entry?.content).toBe("Updated content");
		expect(entry?.embedding?.[0]).toBeCloseTo(0.1);
	});

	it("re-embeds when topics change", async () => {
		await db.insertEntry(makeEntry({ id: "e3", embedding: [0.9, 0.9, 0.9] }));

		await service.updateEntry("e3", { topics: ["new-topic"] });

		expect(embedSpy).toHaveBeenCalledTimes(1);
		const entry = await db.getEntry("e3");
		expect(entry?.topics).toEqual(["new-topic"]);
		expect(entry?.embedding?.[0]).toBeCloseTo(0.1);
	});

	it("re-embeds using new values when both content and topics change simultaneously", async () => {
		await db.insertEntry(
			makeEntry({
				id: "e4",
				content: "old content",
				topics: ["old-topic"],
				embedding: [0.9, 0.9, 0.9],
			}),
		);

		await service.updateEntry("e4", {
			content: "new content",
			topics: ["new-topic"],
		});

		expect(embedSpy).toHaveBeenCalledTimes(1);
		const entry = await db.getEntry("e4");
		expect(entry?.content).toBe("new content");
		expect(entry?.topics).toEqual(["new-topic"]);
		expect(entry?.embedding?.[0]).toBeCloseTo(0.1);
	});

	it("propagates embed errors without writing to DB", async () => {
		await db.insertEntry(
			makeEntry({ id: "e5", content: "original", topics: ["original-topic"], embedding: [0.9, 0.9, 0.9] }),
		);

		embedSpy.mockRejectedValueOnce(new Error("API quota exceeded"));

		// Update both content AND topics so the rollback assertion for topics is
		// meaningful — if the DB were partially written, topics would be "changed-topic".
		await expect(
			service.updateEntry("e5", { content: "updated", topics: ["changed-topic"] }),
		).rejects.toThrow("API quota exceeded");

		// DB should be unchanged — no partial write (content, topics, embedding all intact)
		const entry = await db.getEntry("e5");
		expect(entry?.content).toBe("original");
		expect(entry?.topics).toEqual(["original-topic"]);
		expect(entry?.embedding?.[0]).toBeCloseTo(0.9);
	});

	it("throws when entry does not exist", async () => {
		await expect(
			service.updateEntry("nonexistent", { content: "x" }),
		).rejects.toThrow("KnowledgeService.updateEntry: entry not found: nonexistent");
	});
});
