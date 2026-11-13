/**
 * state.db housekeeping: compactIfNeeded() reclaims free pages once, only for large
 * mostly-empty files, and only when no other process holds the maintenance lock.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ServerStateDB } from "../src/db/state/index";
import { tryAcquireProcessLock } from "../src/process-lock";
import type { PendingEpisode } from "../src/types";
import { startLockHolder } from "./support/process-lock-holder";

const ROWS = 400;
const PAYLOAD = "x".repeat(16 * 1024);

function episode(i: number): PendingEpisode {
	return {
		id: `ep-${i}`,
		userId: "tester",
		source: "test",
		uploadedAt: Date.now(),
		sessionId: `session-${i}`,
		startMessageId: "m1",
		endMessageId: "m2",
		sessionTitle: "t",
		projectName: "p",
		directory: "/tmp",
		timeCreated: Date.now(),
		maxMessageTime: Date.now(),
		content: PAYLOAD,
		contentType: "messages",
		approxTokens: 4000,
	};
}

describe("ServerStateDB.compactIfNeeded", () => {
	let dir: string;
	let dbPath: string;
	let lockPath: string;
	let db: ServerStateDB;

	beforeEach(async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-state-compact-"));
		dbPath = join(dir, "state.db");
		lockPath = join(dir, "state-maintenance.lock");
		db = new ServerStateDB(dbPath);
		for (let i = 0; i < ROWS; i++) await db.insertPendingEpisode(episode(i));
		// Consolidation deletes processed episodes; their pages stay in the file.
		await db.deletePendingEpisodes(
			Array.from({ length: ROWS }, (_, i) => `ep-${i}`),
		);
	});

	afterEach(async () => {
		await db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	it("reclaims free pages from a large, mostly empty file", () => {
		const before = statSync(dbPath).size;
		const result = db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 });
		expect(result.ran).toBe(true);
		expect(result.freedPages ?? 0).toBeGreaterThan(0);
		expect(statSync(dbPath).size).toBeLessThan(before);
	});

	it("does not run again once the free space is reclaimed", () => {
		expect(db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 }).ran).toBe(
			true,
		);
		// The file is now small (live rows only), so the size gate stops the second run.
		const second = db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 });
		expect(second.ran).toBe(false);
		expect(second.reason).toBe("below size threshold");
	});

	it("skips files below the size threshold", () => {
		const result = db.compactIfNeeded({
			lockPath,
			minBytes: 1024 * 1024 * 1024,
		});
		expect(result).toEqual({ ran: false, reason: "below size threshold" });
	});

	it("skips when the file is not mostly free", async () => {
		const fresh = new ServerStateDB(join(dir, "fresh.db"));
		try {
			for (let i = 0; i < 50; i++) await fresh.insertPendingEpisode(episode(i));
			const result = fresh.compactIfNeeded({
				lockPath: join(dir, "fresh.lock"),
				minBytes: 1,
			});
			expect(result.ran).toBe(false);
			expect(result.reason).toBe("free space below threshold");
		} finally {
			await fresh.close();
		}
	});

	it("skips while another process holds the maintenance lock", async () => {
		const holder = await startLockHolder(lockPath);
		try {
			const result = db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 });
			expect(result.ran).toBe(false);
			expect(result.reason).toBe("another process holds the maintenance lock");
		} finally {
			await holder.release();
		}
		expect(db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 }).ran).toBe(
			true,
		);
	});

	it("releases the maintenance lock when it is done", () => {
		db.compactIfNeeded({ lockPath, minBytes: 1024 * 1024 });
		const again = tryAcquireProcessLock(lockPath);
		expect(again).not.toBeNull();
		again?.release();
	});
});
