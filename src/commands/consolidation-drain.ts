/**
 * Shared consolidation drain helpers.
 *
 * Extracted by Dennis Paul (PR #67) to eliminate duplication between the
 * `consolidate` CLI command and the `process-inbox` batch command. Both
 * commands need the same lock-acquire loop, batch drain, and KB synthesis
 * pass — this module is the single source of truth for that logic.
 *
 * The server-side drain in index.ts is intentionally separate: it has
 * different semantics (shutdown-aware, exponential backoff, logger vs
 * console output).
 */

import type { ActivationEngine } from "../activation/activate.js";
import type { ConsolidationEngine } from "../consolidation/consolidate.js";

/**
 * Results from a full consolidation drain (all batches).
 */
export interface DrainResult {
	totalSessions: number;
	totalCreated: number;
	totalUpdated: number;
}

/**
 * Run the embedding model consistency check before consolidation.
 *
 * Non-fatal — logs a warning and continues if the check fails.
 */
export async function runEmbeddingCheck(
	activation: ActivationEngine,
): Promise<void> {
	try {
		await activation.checkAndReEmbed();
	} catch (e) {
		console.error(
			`Warning: embedding model check failed — ${e instanceof Error ? e.message : String(e)}`,
		);
	}
}

/**
 * Acquire the consolidation lock, polling up to `timeoutMs`.
 *
 * Returns `true` when the lock is acquired, `false` when the timeout
 * expires without acquiring it. Callers decide how to handle failure
 * (e.g. log and exit) — this function does not call `process.exit`.
 */
export async function tryAcquireLock(
	consolidation: ConsolidationEngine,
	timeoutMs = 10_000,
	pollMs = 500,
): Promise<boolean> {
	if (consolidation.tryLock()) return true;

	let waited = 0;
	while (!consolidation.tryLock()) {
		if (waited >= timeoutMs) return false;
		await new Promise((r) => setTimeout(r, pollMs));
		waited += pollMs;
	}
	return true;
}

/**
 * Drain all pending sessions through consolidation in batches.
 *
 * Repeatedly calls `consolidation.consolidate()` until no sessions remain
 * or a batch fails. Each batch acquires/releases the consolidation lock
 * independently.
 *
 * When the lock cannot be acquired after `lockTimeoutMs`, logs an error and
 * calls `process.exit(1)` — in CLI mode this indicates a running server is
 * holding the lock and the user should stop it first.
 *
 * Returns aggregate totals across all batches.
 */
export async function drainConsolidation(
	consolidation: ConsolidationEngine,
	lockTimeoutMs = 10_000,
): Promise<DrainResult> {
	let batch = 1;
	let totalSessions = 0;
	let totalCreated = 0;
	let totalUpdated = 0;

	while (true) {
		const acquired = await tryAcquireLock(consolidation, lockTimeoutMs);
		if (!acquired) {
			console.error(
				"Could not acquire consolidation lock after 10 s — is the server running? Stop it first.",
			);
			process.exit(1);
		}

		try {
			const result = await consolidation.consolidate();

			if (result.sessionsProcessed === 0) break;

			totalSessions += result.sessionsProcessed;
			totalCreated += result.entriesCreated;
			totalUpdated += result.entriesUpdated;

			console.log(
				`  Batch ${batch}: ${result.sessionsProcessed} sessions → ` +
					`${result.entriesCreated} created, ${result.entriesUpdated} updated`,
			);
			batch++;
		} catch (err) {
			console.error(
				`Batch ${batch} failed: ${err instanceof Error ? err.message : String(err)}`,
			);
			break;
		} finally {
			consolidation.unlock();
		}
	}

	return { totalSessions, totalCreated, totalUpdated };
}

/**
 * Run KB synthesis under the consolidation lock.
 *
 * Non-fatal — logs a warning and continues if the lock can't be acquired or
 * synthesis fails.
 */
export async function runSynthesisPass(
	consolidation: ConsolidationEngine,
): Promise<void> {
	try {
		if (consolidation.tryLock()) {
			try {
				await consolidation.runSynthesis();
			} finally {
				consolidation.unlock();
			}
		} else {
			console.warn("Warning: could not acquire lock for synthesis — skipping.");
		}
	} catch (e) {
		console.error(
			`Warning: KB synthesis failed — ${e instanceof Error ? e.message : String(e)}`,
		);
	}
}
