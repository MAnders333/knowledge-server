import { Database } from "bun:sqlite";
/**
 * busy_timeout on every SQLite connection: a write that collides with another
 * process's write waits for it, instead of failing with SQLITE_BUSY at once.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DaemonDB } from "../src/db/daemon/index";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import type { PendingEpisode } from "../src/types";
import { holdSqliteWriteLock } from "./support/sqlite-write-lock-holder";

/** Long enough to be a real collision, short enough to keep the suite quick. */
const HOLD_MS = 800;

function pendingEpisode(id: string): PendingEpisode {
	return {
		id,
		userId: "tester",
		source: "test",
		uploadedAt: Date.now(),
		sessionId: `session-${id}`,
		startMessageId: "m1",
		endMessageId: "m2",
		sessionTitle: "t",
		projectName: "p",
		directory: "/tmp",
		timeCreated: Date.now(),
		maxMessageTime: Date.now(),
		content: "hello",
		contentType: "messages",
		approxTokens: 2,
	};
}

describe("SQLite busy_timeout", () => {
	let dir: string;
	let dbPath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-sqlite-busy-"));
		dbPath = join(dir, "shared.db");
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("control: a connection without busy_timeout fails immediately under contention", async () => {
		const db = new Database(dbPath);
		db.exec("PRAGMA journal_mode=WAL");
		db.exec("CREATE TABLE IF NOT EXISTS lock_probe (x INTEGER)");
		db.close();

		const holder = holdSqliteWriteLock(dbPath, HOLD_MS);
		await holder.ready;
		const raw = new Database(dbPath);
		try {
			expect(() => raw.exec("INSERT INTO lock_probe (x) VALUES (2)")).toThrow(
				/locked|busy/i,
			);
		} finally {
			raw.close();
		}
		expect(await holder.exited).toBe(0);
	});

	it("state.db (ServerStateDB) waits for another process's write", async () => {
		const state = new ServerStateDB(dbPath);
		try {
			const holder = holdSqliteWriteLock(dbPath, HOLD_MS);
			await holder.ready;
			const started = performance.now();
			// Would throw SQLITE_BUSY with busy_timeout 0.
			await state.insertPendingEpisode(pendingEpisode("a"));
			expect(performance.now() - started).toBeGreaterThanOrEqual(HOLD_MS / 2);
			expect(await holder.exited).toBe(0);
			expect(await state.countPendingSessions()).toBe(1);
		} finally {
			await state.close();
		}
	});

	it("knowledge DB waits for another process's write", async () => {
		const knowledge = new KnowledgeDB(dbPath);
		try {
			const holder = holdSqliteWriteLock(dbPath, HOLD_MS);
			await holder.ready;
			await knowledge.reinitialize();
			expect(await holder.exited).toBe(0);
		} finally {
			await knowledge.close();
		}
	});

	it("daemon DB waits for another process's write", async () => {
		const daemonPath = join(dir, "daemon.db");
		const daemon = new DaemonDB(daemonPath);
		try {
			const holder = holdSqliteWriteLock(daemonPath, HOLD_MS);
			await holder.ready;
			await daemon.updateDaemonCursor("opencode", {
				lastMessageTimeCreated: 1,
				lastUploadedAt: Date.now(),
			});
			expect(await holder.exited).toBe(0);
			expect(await daemon.getDaemonCursor("opencode")).toMatchObject({
				lastMessageTimeCreated: 1,
			});
		} finally {
			daemon.close();
		}
	});
});
