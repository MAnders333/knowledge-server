/**
 * Tests for consolidation-drain helpers.
 *
 * Tests cover:
 * - runEmbeddingCheck: swallows errors, calls activation.checkAndReEmbed
 * - tryAcquireLock: returns true when lock acquired immediately, false on timeout
 * - drainConsolidation: batches until sessionsProcessed === 0, accumulates totals,
 *   stops and returns on batch error
 * - runSynthesisPass: acquires lock, calls runSynthesis, releases lock;
 *   skips gracefully when lock is unavailable; swallows synthesis errors
 */
import { beforeEach, describe, expect, it, mock, spyOn } from "bun:test";
import {
	drainConsolidation,
	runEmbeddingCheck,
	runSynthesisPass,
	tryAcquireLock,
} from "../src/commands/consolidation-drain";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { ActivationEngine } from "../src/activation/activate";

// ── Minimal stubs ─────────────────────────────────────────────────────────────

function makeActivation(opts: { reEmbedThrows?: boolean } = {}): ActivationEngine {
	return {
		checkAndReEmbed: opts.reEmbedThrows
			? async () => { throw new Error("embed model unavailable"); }
			: async () => {},
	} as unknown as ActivationEngine;
}

interface BatchSpec {
	sessionsProcessed: number;
	entriesCreated: number;
	entriesUpdated: number;
}

/**
 * Build a ConsolidationEngine stub.
 *
 * @param batches      Sequence of results consolidate() returns on successive calls.
 *                     After exhausting the list it returns sessionsProcessed=0.
 * @param lockHeld     If true, tryLock() always returns false (lock unavailable).
 * @param throwOnBatch 1-based index of the batch that should throw.
 */
function makeConsolidation(opts: {
	batches?: BatchSpec[];
	lockHeld?: boolean;
	throwOnBatch?: number;
} = {}): ConsolidationEngine & { synthesisCalled: number } {
	const { batches = [], lockHeld = false, throwOnBatch } = opts;
	let callCount = 0;
	let locked = false;
	let synthesisCalled = 0;

	const engine = {
		get synthesisCalled() { return synthesisCalled; },

		tryLock(): boolean {
			if (lockHeld || locked) return false;
			locked = true;
			return true;
		},
		unlock(): void {
			locked = false;
		},

		async consolidate(): Promise<{
			sessionsProcessed: number;
			entriesCreated: number;
			entriesUpdated: number;
		}> {
			const idx = callCount++;
			if (throwOnBatch !== undefined && callCount === throwOnBatch) {
				throw new Error(`Batch ${throwOnBatch} failed`);
			}
			return batches[idx] ?? { sessionsProcessed: 0, entriesCreated: 0, entriesUpdated: 0 };
		},

		async runSynthesis(): Promise<number> {
			synthesisCalled++;
			return 0;
		},
	} as unknown as ConsolidationEngine & { synthesisCalled: number };

	return engine;
}

// ── runEmbeddingCheck ─────────────────────────────────────────────────────────

describe("runEmbeddingCheck", () => {
	it("calls checkAndReEmbed on the activation engine", async () => {
		let called = false;
		const activation = { checkAndReEmbed: async () => { called = true; } } as unknown as ActivationEngine;
		await runEmbeddingCheck(activation);
		expect(called).toBe(true);
	});

	it("does not throw when checkAndReEmbed fails", async () => {
		const activation = makeActivation({ reEmbedThrows: true });
		// Should complete without throwing.
		await expect(runEmbeddingCheck(activation)).resolves.toBeUndefined();
	});
});

// ── tryAcquireLock ────────────────────────────────────────────────────────────

describe("tryAcquireLock", () => {
	it("returns true immediately when the lock is free", async () => {
		const engine = makeConsolidation();
		const acquired = await tryAcquireLock(engine);
		expect(acquired).toBe(true);
	});

	it("returns false when the lock is held and the timeout expires", async () => {
		const engine = makeConsolidation({ lockHeld: true });
		// Use a very short timeout so the test doesn't actually wait 10 s.
		const acquired = await tryAcquireLock(engine, 50, 20);
		expect(acquired).toBe(false);
	});

	it("acquires the lock when it becomes free before the timeout", async () => {
		// Lock is held for the first two poll intervals, then released.
		let holdCount = 0;
		const MAX_HOLDS = 2;
		let locked = false;

		const engine = {
			tryLock(): boolean {
				if (holdCount < MAX_HOLDS) { holdCount++; return false; }
				if (locked) return false;
				locked = true;
				return true;
			},
			unlock(): void { locked = false; },
		} as unknown as ConsolidationEngine;

		const acquired = await tryAcquireLock(engine, 500, 50);
		expect(acquired).toBe(true);
	});
});

// ── drainConsolidation ────────────────────────────────────────────────────────

describe("drainConsolidation", () => {
	it("returns zero totals when no sessions are pending", async () => {
		const engine = makeConsolidation({ batches: [] });
		const result = await drainConsolidation(engine);
		expect(result.totalSessions).toBe(0);
		expect(result.totalCreated).toBe(0);
		expect(result.totalUpdated).toBe(0);
	});

	it("accumulates totals across multiple batches", async () => {
		const engine = makeConsolidation({
			batches: [
				{ sessionsProcessed: 3, entriesCreated: 5, entriesUpdated: 1 },
				{ sessionsProcessed: 2, entriesCreated: 3, entriesUpdated: 4 },
				// third call returns sessionsProcessed=0 → loop exits
			],
		});
		const result = await drainConsolidation(engine);
		expect(result.totalSessions).toBe(5);
		expect(result.totalCreated).toBe(8);
		expect(result.totalUpdated).toBe(5);
	});

	it("stops after a single batch when sessionsProcessed drops to 0", async () => {
		const engine = makeConsolidation({
			batches: [{ sessionsProcessed: 2, entriesCreated: 1, entriesUpdated: 0 }],
		});
		const result = await drainConsolidation(engine);
		expect(result.totalSessions).toBe(2);
	});

	it("stops and returns partial totals when a batch throws", async () => {
		const engine = makeConsolidation({
			batches: [
				{ sessionsProcessed: 2, entriesCreated: 4, entriesUpdated: 0 },
				// batch 2 (throwOnBatch is 1-indexed call count, but we want batch 3 to succeed)
			],
			throwOnBatch: 2,
		});
		// First batch succeeds (callCount becomes 1), second call throws (callCount becomes 2).
		const result = await drainConsolidation(engine);
		expect(result.totalSessions).toBe(2);
		expect(result.totalCreated).toBe(4);
	});

	it("releases the lock in finally even when a batch throws", async () => {
		let unlockCalled = false;
		let locked = false;

		const engine = {
			tryLock(): boolean { if (locked) return false; locked = true; return true; },
			unlock(): void { unlockCalled = true; locked = false; },
			async consolidate(): Promise<never> { throw new Error("consolidate exploded"); },
			async runSynthesis(): Promise<number> { return 0; },
		} as unknown as ConsolidationEngine;

		await drainConsolidation(engine);
		expect(unlockCalled).toBe(true);
	});
});

// ── runSynthesisPass ──────────────────────────────────────────────────────────

describe("runSynthesisPass", () => {
	it("calls runSynthesis when the lock is free", async () => {
		const engine = makeConsolidation();
		await runSynthesisPass(engine);
		expect(engine.synthesisCalled).toBe(1);
	});

	it("releases the lock after synthesis", async () => {
		const engine = makeConsolidation();
		await runSynthesisPass(engine);
		// After synthesis, the lock should be released — another tryLock should succeed.
		expect(engine.tryLock()).toBe(true);
	});

	it("skips synthesis gracefully when the lock is unavailable", async () => {
		const engine = makeConsolidation({ lockHeld: true });
		// Should complete without throwing.
		await expect(runSynthesisPass(engine)).resolves.toBeUndefined();
		expect(engine.synthesisCalled).toBe(0);
	});

	it("swallows errors thrown by runSynthesis", async () => {
		let locked = false;
		const engine = {
			tryLock(): boolean { if (locked) return false; locked = true; return true; },
			unlock(): void { locked = false; },
			async runSynthesis(): Promise<never> { throw new Error("synthesis exploded"); },
			synthesisCalled: 0,
		} as unknown as ConsolidationEngine & { synthesisCalled: number };

		await expect(runSynthesisPass(engine)).resolves.toBeUndefined();
	});
});
