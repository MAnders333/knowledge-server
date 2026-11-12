/**
 * ConsolidationEngine.tryLock with a cross-process lock path: the server and the
 * CLI share one lockfile, so a consolidation in one process blocks the other.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import { consolidationLockPath } from "../src/runtime-files";
import { startLockHolder } from "./support/process-lock-holder";

describe("ConsolidationEngine cross-process run lock", () => {
	let dir: string;
	let db: KnowledgeDB;
	let serverStateDb: ServerStateDB;
	let activation: ActivationEngine;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-engine-lock-"));
		db = new KnowledgeDB(join(dir, "knowledge.db"));
		serverStateDb = new ServerStateDB(join(dir, "state.db"));
		activation = new ActivationEngine(db);
	});

	afterEach(async () => {
		await serverStateDb.close();
		await db.close();
		rmSync(dir, { recursive: true, force: true });
	});

	function engine(lockPath?: string): ConsolidationEngine {
		return new ConsolidationEngine(
			db,
			serverStateDb,
			activation,
			[],
			null,
			lockPath === undefined ? {} : { lockPath },
		);
	}

	it("two engines in one data dir: the second waits for the first to unlock", () => {
		const lockPath = consolidationLockPath(dir);
		const server = engine(lockPath);
		const cli = engine(lockPath);

		expect(server.tryLock()).toBe(true);
		expect(cli.tryLock()).toBe(false);
		expect(cli.crossProcessLockHolder()?.pid).toBe(process.pid);

		server.unlock();
		expect(cli.tryLock()).toBe(true);
		cli.unlock();
	});

	it("a consolidation running in another process blocks this engine", async () => {
		const lockPath = consolidationLockPath(dir);
		const holder = await startLockHolder(lockPath);
		const cli = engine(lockPath);
		try {
			expect(cli.tryLock()).toBe(false);
			expect(cli.crossProcessLockHolder()?.pid).toBe(holder.pid);
		} finally {
			await holder.release();
		}
		expect(cli.tryLock()).toBe(true);
		cli.unlock();
	});

	it("close() releases a lock the engine still holds", () => {
		const lockPath = consolidationLockPath(dir);
		const first = engine(lockPath);
		expect(first.tryLock()).toBe(true);
		first.close();
		expect(engine(lockPath).tryLock()).toBe(true);
	});

	it("without a lock path the engine keeps the in-process guard only", () => {
		const a = engine();
		const b = engine();
		expect(a.tryLock()).toBe(true);
		expect(b.tryLock()).toBe(true);
		a.unlock();
		b.unlock();
	});

	it("records consolidate() outcomes for /status", async () => {
		const e = engine();
		expect(e.consolidationOutcome().consecutiveFailures).toBe(0);
		await expect(e.consolidate()).resolves.toBeDefined();
		const outcome = e.consolidationOutcome();
		expect(outcome.consecutiveFailures).toBe(0);
		expect(outcome.lastSuccessAt).not.toBeNull();
	});
});
