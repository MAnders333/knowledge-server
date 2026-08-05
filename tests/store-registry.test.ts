import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { StoreRegistry } from "../src/db/store-registry";
import { fakeEmbedding, makeEntry } from "./fixtures";

describe("StoreRegistry", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-registry-test-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── No config file (default) ───────────────────────────────────────────────

	it("creates a single default SQLite store when no config file exists", async () => {
		const registry = await StoreRegistry.create(
			join(tempDir, "nonexistent.jsonc"),
		);
		try {
			expect(registry.writableStore()).toBeDefined();
			expect(registry.readStores()).toHaveLength(1);
			expect(registry.readStores()[0]).toBe(registry.writableStore());
		} finally {
			await registry.close();
		}
	});

	// ── Single SQLite store ────────────────────────────────────────────────────

	it("creates a single SQLite store from config", async () => {
		const dbPath = join(tempDir, "test.db");
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [{ id: "main", kind: "sqlite", path: dbPath, writable: true }],
			}),
		);

		const registry = await StoreRegistry.create(configPath);
		try {
			const writable = registry.writableStore();
			expect(writable).toBeInstanceOf(KnowledgeDB);
			expect(registry.readStores()).toHaveLength(1);
			expect(registry.readStores()[0]).toBe(writable);
		} finally {
			await registry.close();
		}
	});

	// ── Multi-store ────────────────────────────────────────────────────────────

	it("exposes all stores for reads and only the writable store for writes", async () => {
		const db1Path = join(tempDir, "db1.db");
		const db2Path = join(tempDir, "db2.db");
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "primary", kind: "sqlite", path: db1Path, writable: true },
					{ id: "secondary", kind: "sqlite", path: db2Path, writable: false },
				],
			}),
		);

		const registry = await StoreRegistry.create(configPath);
		try {
			expect(registry.readStores()).toHaveLength(2);
			// writableStore is the one with writable: true
			const writable = registry.writableStore();
			// writable store is included in readStores
			expect(registry.readStores()).toContain(writable);
		} finally {
			await registry.close();
		}
	});

	it("closes all stores on registry.close()", async () => {
		const db1Path = join(tempDir, "db1.db");
		const db2Path = join(tempDir, "db2.db");
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "a", kind: "sqlite", path: db1Path, writable: true },
					{ id: "b", kind: "sqlite", path: db2Path, writable: false },
				],
			}),
		);

		const registry = await StoreRegistry.create(configPath);
		const readStores = registry.readStores();
		const closeSpy0 = spyOn(readStores[0], "close");
		const closeSpy1 = spyOn(readStores[1], "close");

		await registry.close();

		expect(closeSpy0).toHaveBeenCalledTimes(1);
		expect(closeSpy1).toHaveBeenCalledTimes(1);
	});

	// ── Error cases ────────────────────────────────────────────────────────────

	it("throws on invalid config file", async () => {
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(configPath, "{ not valid }");
		await expect(StoreRegistry.create(configPath)).rejects.toThrow(
			/Failed to load config/,
		);
	});
});

describe("ActivationEngine multi-store fan-out", () => {
	let tempDir: string;
	let db1: KnowledgeDB;
	let db2: KnowledgeDB;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-activation-multi-test-"));
		db1 = new KnowledgeDB(join(tempDir, "db1.db"));
		db2 = new KnowledgeDB(join(tempDir, "db2.db"));
	});

	afterEach(async () => {
		await db1.close();
		await db2.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("fans out activation reads across all read stores", async () => {
		// Insert one entry in each store
		await db1.insertEntry(
			makeEntry({
				id: "e1",
				content: "entry from store 1",
				embedding: fakeEmbedding("entry from store 1"),
			}),
		);
		await db2.insertEntry(
			makeEntry({
				id: "e2",
				content: "entry from store 2",
				embedding: fakeEmbedding("entry from store 2"),
			}),
		);

		const engine = new ActivationEngine(db1, [db1, db2]);
		const embedSpy = spyOn(engine.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("query"),
		]);

		const result = await engine.activate("query");

		// Should have seen entries from both stores
		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("e1");
		expect(ids).toContain("e2");
		expect(embedSpy).toHaveBeenCalledTimes(1);
	});

	it("deduplicates entries with the same id across stores", async () => {
		// Same entry ID in both stores (e.g. synced)
		const entry = makeEntry({
			id: "shared",
			content: "shared entry",
			embedding: fakeEmbedding("shared"),
		});
		await db1.insertEntry(entry);
		await db2.insertEntry(entry);

		const engine = new ActivationEngine(db1, [db1, db2]);
		spyOn(engine.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("shared"),
		]);

		const result = await engine.activate("shared");

		const ids = result.entries.map((e) => e.entry.id);
		// Should appear only once even though it was in both stores
		expect(ids.filter((id) => id === "shared")).toHaveLength(1);
	});

	it("single-store mode works when readDbs is omitted", async () => {
		await db1.insertEntry(
			makeEntry({
				id: "e1",
				content: "only store",
				embedding: fakeEmbedding("only store"),
			}),
		);

		// No readDbs passed — defaults to [writableDb]
		const engine = new ActivationEngine(db1);
		spyOn(engine.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("only store"),
		]);

		const result = await engine.activate("only store");
		expect(result.entries.map((e) => e.entry.id)).toContain("e1");
	});

	it("returns results from healthy stores when one store throws", async () => {
		// Simulate a dead store: db2.getActiveEntriesWithEmbeddings throws.
		// The activation should still return entries from db1.
		await db1.insertEntry(
			makeEntry({
				id: "e1",
				content: "entry from healthy store",
				embedding: fakeEmbedding("entry from healthy store"),
			}),
		);
		await db2.insertEntry(
			makeEntry({
				id: "e2",
				content: "entry from dead store",
				embedding: fakeEmbedding("entry from dead store"),
			}),
		);

		// Set store IDs so warnings are meaningful
		db1.id = "healthy";
		db2.id = "dead";

		const engine = new ActivationEngine(db1, [db1, db2]);
		spyOn(engine.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("entry from healthy store"),
		]);
		// Make db2 throw on full-scan load (Path B — neither store has ANN)
		spyOn(db2, "getActiveEntriesWithEmbeddings").mockRejectedValue(
			new Error("connection refused"),
		);

		const result = await engine.activate("entry from healthy store");

		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("e1");
		expect(ids).not.toContain("e2");
	});

	it("skips a previously-failed store on subsequent calls (health tracking)", async () => {
		await db1.insertEntry(
			makeEntry({
				id: "e1",
				content: "healthy entry",
				embedding: fakeEmbedding("healthy entry"),
			}),
		);
		await db2.insertEntry(
			makeEntry({
				id: "e2",
				content: "dead entry",
				embedding: fakeEmbedding("dead entry"),
			}),
		);

		db1.id = "healthy";
		db2.id = "dead";

		const engine = new ActivationEngine(db1, [db1, db2]);
		spyOn(engine.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("healthy entry"),
		]);
		// db2 always fails
		const failSpy = spyOn(
			db2,
			"getActiveEntriesWithEmbeddings",
		).mockRejectedValue(new Error("connection refused"));

		// First call — db2 is tried and fails
		const result1 = await engine.activate("healthy entry");
		expect(result1.entries.map((e) => e.entry.id)).toContain("e1");
		expect(failSpy).toHaveBeenCalledTimes(1);

		// Second call — db2 should be skipped (health tracking), not retried
		const result2 = await engine.activate("healthy entry");
		expect(result2.entries.map((e) => e.entry.id)).toContain("e1");
		// failSpy still at 1 — db2 was not called again
		expect(failSpy).toHaveBeenCalledTimes(1);
	});
});
