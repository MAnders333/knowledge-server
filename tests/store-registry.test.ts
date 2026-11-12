import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { type AddressInfo, type Socket, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine, StoreBreaker } from "../src/activation/activate";
import { DEFAULT_CONFIG } from "../src/config-file";
import type { IKnowledgeStore } from "../src/db/interface";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { StoreRegistry } from "../src/db/store-registry";
import * as loggerModule from "../src/logger";
import { fakeEmbedding, makeEntry } from "./fixtures";

describe("StoreRegistry", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-registry-test-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── No config file (fallback) ──────────────────────────────────────────────

	it("with no config file, opens the fallback config's stores and state DB", async () => {
		const knowledgePath = join(tempDir, "knowledge.db");
		const statePath = join(tempDir, "state.db");
		const registry = await StoreRegistry.create(
			join(tempDir, "nonexistent.jsonc"),
			{
				...DEFAULT_CONFIG,
				stores: [
					{
						id: "default",
						kind: "sqlite",
						writable: true,
						path: knowledgePath,
					},
				],
				stateDb: { kind: "sqlite", path: statePath },
			},
		);
		try {
			expect(registry.writableStore()).toBeInstanceOf(KnowledgeDB);
			expect(registry.readStores()).toHaveLength(1);
			expect(existsSync(knowledgePath)).toBe(true);
			expect(existsSync(statePath)).toBe(true);
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
				stateDb: { kind: "sqlite", path: join(tempDir, "state.db") },
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
				stateDb: { kind: "sqlite", path: join(tempDir, "state.db") },
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
				stateDb: { kind: "sqlite", path: join(tempDir, "state.db") },
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

	// ── Degraded init (unreachable / accepting-but-silent servers) ────────────

	function startStallServer(): Promise<{
		server: ReturnType<typeof createServer>;
		port: number;
		sockets: Set<Socket>;
	}> {
		return new Promise((resolve) => {
			const sockets = new Set<Socket>();
			// Accepts TCP but never speaks the Postgres protocol — the shape of a
			// proxy whose backend is down: nothing in initStore can reject on its own.
			const server = createServer((socket) => {
				sockets.add(socket);
				socket.on("close", () => sockets.delete(socket));
			});
			server.listen(0, "127.0.0.1", () => {
				const { port } = server.address() as AddressInfo;
				resolve({ server, port, sockets });
			});
		});
	}

	function restoreTimeoutEnv(previous: string | undefined): void {
		// "" reads as absent to parseIntEnv (NaN -> default), so this restores
		// the unset state without the delete operator.
		process.env.STORE_INIT_TIMEOUT_MS = previous ?? "";
	}

	it("bounds an accepting-but-silent store, excludes it, and warns immediately", async () => {
		const { server, port, sockets } = await startStallServer();
		const previousTimeout = process.env.STORE_INIT_TIMEOUT_MS;
		process.env.STORE_INIT_TIMEOUT_MS = "500";
		const warnSpy = spyOn(loggerModule.logger, "warn");
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stateDb: { kind: "sqlite", path: join(tempDir, "state.db") },
				stores: [
					{
						id: "tunnel",
						kind: "postgres",
						uri: `postgres://user@127.0.0.1:${port}/knowledge`,
						writable: false,
					},
					{
						id: "main",
						kind: "sqlite",
						path: join(tempDir, "degraded.db"),
						writable: true,
					},
				],
			}),
		);

		try {
			const started = Date.now();
			const registry = await StoreRegistry.create(configPath);
			try {
				expect(Date.now() - started).toBeLessThan(4_000);
				expect(registry.unavailableStoreIds.has("tunnel")).toBe(true);
				expect(registry.readStores()).toHaveLength(1);
				expect(registry.writableStore()).toBeInstanceOf(KnowledgeDB);
				expect(warnSpy).toHaveBeenCalledWith(
					expect.stringContaining('Store "tunnel" is unavailable'),
				);
			} finally {
				await registry.close();
			}
		} finally {
			warnSpy.mockRestore();
			restoreTimeoutEnv(previousTimeout);
			for (const socket of sockets) socket.destroy();
			server.close();
		}
	});

	it("hard-fails promptly when the only writable store hangs, instead of blocking forever", async () => {
		const { server, port, sockets } = await startStallServer();
		const previousTimeout = process.env.STORE_INIT_TIMEOUT_MS;
		process.env.STORE_INIT_TIMEOUT_MS = "500";
		const configPath = join(tempDir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stateDb: { kind: "sqlite", path: join(tempDir, "state.db") },
				stores: [
					{
						id: "only",
						kind: "postgres",
						uri: `postgres://user@127.0.0.1:${port}/knowledge`,
						writable: true,
					},
				],
			}),
		);

		try {
			const started = Date.now();
			await expect(StoreRegistry.create(configPath)).rejects.toThrow(
				/No writable stores are reachable/,
			);
			expect(Date.now() - started).toBeLessThan(4_000);
		} finally {
			restoreTimeoutEnv(previousTimeout);
			for (const socket of sockets) socket.destroy();
			server.close();
		}
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
		spyOn(db2, "scanEmbeddings").mockRejectedValue(
			new Error("connection refused"),
		);

		const result = await engine.activate("entry from healthy store");

		const ids = result.entries.map((e) => e.entry.id);
		expect(ids).toContain("e1");
		expect(ids).not.toContain("e2");
	});

	it("skips a store once its circuit opens (three consecutive failures)", async () => {
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
		// db2 is dead: every call fails, so its consecutive-failure count only grows.
		const failSpy = spyOn(db2, "scanEmbeddings").mockRejectedValue(
			new Error("connection refused"),
		);
		spyOn(db2, "getContradictPairsForIds").mockRejectedValue(
			new Error("connection refused"),
		);

		// Failing calls accumulate until three in a row open the circuit; then the store
		// is no longer called at all, however many activations follow.
		for (let call = 1; call <= 3; call++) {
			const result = await engine.activate("healthy entry");
			expect(result.entries.map((e) => e.entry.id)).toContain("e1");
		}
		const callsWhileOpening = failSpy.mock.calls.length;
		expect(callsWhileOpening).toBeLessThanOrEqual(3);

		const result = await engine.activate("healthy entry");
		expect(result.entries.map((e) => e.entry.id)).toContain("e1");
		expect(failSpy).toHaveBeenCalledTimes(callsWhileOpening);
	});
});

describe("StoreBreaker and activation fan-out", () => {
	const WINDOW_MS = 60_000;
	const ANN = "ANN search";
	let clock = 0;
	const newBreaker = () =>
		new StoreBreaker({ threshold: 3, windowMs: WINDOW_MS, now: () => clock });

	afterEach(() => {
		mock.restore();
	});

	it("opens after 3 consecutive failures and warns once", () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const breaker = newBreaker();
		breaker.recordFailure("s", ANN, "boom");
		breaker.recordFailure("s", ANN, "boom");
		expect(breaker.admit("s", ANN)).toBe(true);
		breaker.recordFailure("s", ANN, "boom");
		expect(breaker.admit("s", ANN)).toBe(false);
		expect(warn).toHaveBeenCalledTimes(1);
	});

	it("counts consecutive failures only: a success resets the count", () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const breaker = newBreaker();
		breaker.recordFailure("s", ANN, "boom");
		breaker.recordFailure("s", ANN, "boom");
		breaker.recordSuccess("s", ANN);
		breaker.recordFailure("s", ANN, "boom");
		breaker.recordFailure("s", ANN, "boom");
		expect(breaker.admit("s", ANN)).toBe(true);
		expect(warn).not.toHaveBeenCalled();
	});

	it("a success of one operation does not reset another operation's failures", () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const breaker = newBreaker();
		breaker.recordFailure("s", ANN, "timeout");
		breaker.recordFailure("s", ANN, "timeout");
		breaker.recordSuccess("s", "active entry count");
		breaker.recordFailure("s", ANN, "timeout");
		expect(breaker.admit("s", ANN)).toBe(false);
		expect(breaker.admit("s", "active entry count")).toBe(true);
		expect(warn).toHaveBeenCalledTimes(1);
	});

	it("skips calls while open, allows one probe after the window, closes on success", () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		clock = 1_000;
		const breaker = newBreaker();
		for (let i = 0; i < 3; i++) breaker.recordFailure("s", ANN, "boom");

		clock += WINDOW_MS - 1;
		expect(breaker.admit("s", ANN)).toBe(false);

		clock += 1;
		expect(breaker.admit("s", ANN)).toBe(true); // the probe
		expect(breaker.admit("s", ANN)).toBe(false); // no second call while probing

		breaker.recordSuccess("s", ANN);
		expect(breaker.admit("s", ANN)).toBe(true);
		// one warning for open, one for closed; nothing per refused call
		expect(warn).toHaveBeenCalledTimes(2);
	});

	it("re-opens for another window when the probe fails", () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		clock = 5_000;
		const breaker = newBreaker();
		for (let i = 0; i < 3; i++) breaker.recordFailure("s", ANN, "boom");

		clock += WINDOW_MS;
		expect(breaker.admit("s", ANN)).toBe(true); // probe
		breaker.recordFailure("s", ANN, "still down");
		expect(breaker.admit("s", ANN)).toBe(false);

		clock += WINDOW_MS - 1;
		expect(breaker.admit("s", ANN)).toBe(false);
		clock += 1;
		expect(breaker.admit("s", ANN)).toBe(true); // next probe
		// open, probe failed, next probe: exactly one warning per state change
		expect(warn).toHaveBeenCalledTimes(2);
	});

	it("a dead store: at most 3 ANN calls per circuit, none during its window; the count gets one call per activation until its own circuit opens", async () => {
		const warn = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const findSimilarEntries = mock(() =>
			Promise.reject(new Error("ECONNREFUSED")),
		);
		const getActiveEntryCount = mock(() =>
			Promise.reject(new Error("ECONNREFUSED")),
		);
		const dead = {
			id: "dead",
			findSimilarEntries,
			getActiveEntryCount,
		} as unknown as IKnowledgeStore;
		const engine = new ActivationEngine(dead);
		spyOn(engine.embeddings, "embedBatch").mockImplementation(
			async (texts: string[]) => texts.map(() => [1, 0, 0, 0, 0, 0, 0, 0]),
		);
		const cues = Array.from(
			{ length: 8 },
			(_, i) => `distinct cue number ${i}`,
		);

		const started = Date.now();
		await engine.activate(cues);
		expect(Date.now() - started).toBeLessThan(1_000);
		// Activation 1: the ANN circuit opens on its third failure; the count is
		// one fan-out call, so it has failed once.
		expect(findSimilarEntries.mock.calls.length).toBe(3);
		expect(getActiveEntryCount.mock.calls.length).toBe(1);
		expect(warn).toHaveBeenCalledTimes(1);

		await engine.activate(cues);
		expect(findSimilarEntries.mock.calls.length).toBe(3);
		expect(getActiveEntryCount.mock.calls.length).toBe(2);
		expect(warn).toHaveBeenCalledTimes(1);

		// The count's third failure opens its own circuit.
		await engine.activate(cues);
		expect(findSimilarEntries.mock.calls.length).toBe(3);
		expect(getActiveEntryCount.mock.calls.length).toBe(3);
		expect(warn).toHaveBeenCalledTimes(2);

		await engine.activate(cues);
		expect(findSimilarEntries.mock.calls.length).toBe(3);
		expect(getActiveEntryCount.mock.calls.length).toBe(3);
		expect(warn).toHaveBeenCalledTimes(2);
	});

	it("an ANN timeout opens its own circuit while the count keeps answering: later activations do not wait", async () => {
		const STORE_TIMEOUT_MS = 40;
		spyOn(loggerModule.logger, "warn").mockImplementation(() => {});
		spyOn(loggerModule.logger, "log").mockImplementation(() => {});
		const findSimilarEntries = mock(() => new Promise<never>(() => {}));
		const getActiveEntryCount = mock(() => Promise.resolve(2));
		const slow = {
			id: "slow",
			findSimilarEntries,
			getActiveEntryCount,
		} as unknown as IKnowledgeStore;
		const engine = new ActivationEngine(slow, undefined, undefined, null, {
			storeTimeoutMs: STORE_TIMEOUT_MS,
		});
		spyOn(engine.embeddings, "embedBatch").mockImplementation(
			async (texts: string[]) => texts.map(() => [1, 0, 0, 0, 0, 0, 0, 0]),
		);

		for (let i = 0; i < 3; i++) {
			await engine.activate(`slow cue number ${i}`);
		}
		const started = performance.now();
		const result = await engine.activate("slow cue number 3");
		expect(performance.now() - started).toBeLessThan(STORE_TIMEOUT_MS * 0.75);
		expect(findSimilarEntries.mock.calls.length).toBe(3);
		expect(result.totalActive).toBe(2);
	});
});
