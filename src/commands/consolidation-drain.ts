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
 * Calls `process.exit(1)` if the lock cannot be acquired within the timeout —
 * in CLI mode this indicates the server is holding the lock and the user should
 * stop it first.
 */
function acquireLockOrExit(
	consolidation: ConsolidationEngine,
	timeoutMs = 10_000,
	pollMs = 500,
): Promise<void> {
	if (consolidation.tryLock()) return Promise.resolve();

	return (async () => {
		let waited = 0;
		while (!consolidation.tryLock()) {
			if (waited >= timeoutMs) {
				console.error(
					"Could not acquire consolidation lock after 10 s — is the server running? Stop it first.",
				);
				process.exit(1);
			}
			await new Promise((r) => setTimeout(r, pollMs));
			waited += pollMs;
		}
	})();
}

/**
 * Drain all pending sessions through consolidation in batches.
 *
 * Repeatedly calls `consolidation.consolidate()` until no sessions remain or a
 * batch fails. Each batch acquires/releases the consolidation lock independently.
 *
 * Returns aggregate totals across all batches.
 */
export async function drainConsolidation(
	consolidation: ConsolidationEngine,
): Promise<DrainResult> {
	let batch = 1;
	let totalSessions = 0;
	let totalCreated = 0;
	let totalUpdated = 0;

	while (true) {
		await acquireLockOrExit(consolidation);

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
