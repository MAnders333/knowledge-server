import { ActivationEngine } from "../activation/activate.js";
import { ConsolidationEngine } from "../consolidation/consolidate.js";
import { createEpisodeReaders } from "../consolidation/readers/index.js";
import { StoreRegistry } from "../db/store-registry.js";
import { logger } from "../logger.js";

/**
 * `knowledge-server consolidate`
 *
 * Runs a full consolidation drain: repeatedly processes batches of pending
 * sessions until none remain. Prints live progress to stdout.
 */
export async function runConsolidate(): Promise<void> {
	logger.init(""); // disable file logging — output goes to stdout only

	const registry = await StoreRegistry.create();
	const db = registry.writableStore();
	const activation = new ActivationEngine(db, registry.readStores());
	const readers = createEpisodeReaders();
	const consolidation = new ConsolidationEngine(
		db,
		activation,
		readers,
		registry.domainRouter,
	);

	try {
		// Check for embedding model change before consolidating — ensures all
		// vectors are consistent when reconsolidation compares new extractions
		// against existing entries.
		try {
			await activation.checkAndReEmbed();
		} catch (e) {
			console.error(
				`Warning: embedding model check failed — ${e instanceof Error ? e.message : String(e)}`,
			);
		}

		const { pendingSessions } = await consolidation.checkPending();

		if (pendingSessions > 0) {
			console.log(
				`${pendingSessions} sessions pending — starting consolidation...`,
			);
			console.log("");
		} else {
			console.log(
				"No new sessions to consolidate. Running KB synthesis pass...",
			);
		}

		let batch = 1;
		let totalSessions = 0;
		let totalCreated = 0;
		let totalUpdated = 0;

		// tryLock timeout — spin at most 10 seconds before giving up.
		// In CLI mode there should be no concurrent callers, but if the server is
		// running and holds the lock we surface a clear error rather than looping
		// indefinitely.
		const LOCK_TIMEOUT_MS = 10_000;
		const LOCK_POLL_MS = 500;

		while (true) {
			if (!consolidation.tryLock()) {
				// Shouldn't happen in CLI mode (no concurrent callers), but guard
				// against a running server holding the lock.
				let waited = 0;
				while (!consolidation.tryLock()) {
					if (waited >= LOCK_TIMEOUT_MS) {
						console.error(
							"Could not acquire consolidation lock after 10 s — is the server running? Stop it first.",
						);
						process.exit(1);
					}
					await new Promise((r) => setTimeout(r, LOCK_POLL_MS));
					waited += LOCK_POLL_MS;
				}
			}
			let result: Awaited<ReturnType<typeof consolidation.consolidate>>;
			try {
				result = await consolidation.consolidate();
			} finally {
				consolidation.unlock();
			}

			if (result.sessionsProcessed === 0) break;

			totalSessions += result.sessionsProcessed;
			totalCreated += result.entriesCreated;
			totalUpdated += result.entriesUpdated;

			console.log(
				`  Batch ${batch}: ${result.sessionsProcessed} sessions → ` +
					`${result.entriesCreated} created, ${result.entriesUpdated} updated`,
			);
			batch++;
		}

		// Run KB synthesis once after all batches, same as the server-side drain.
		// Runs unconditionally — existing entries may still be ripe even when no
		// new sessions were processed (e.g. after a re-embedding pass).
		// Wrapped in its own try/catch so a synthesis failure doesn't suppress the
		// completion summary.
		if (pendingSessions > 0) console.log("\nRunning KB synthesis pass...");
		try {
			if (consolidation.tryLock()) {
				try {
					await consolidation.runSynthesis();
				} finally {
					consolidation.unlock();
				}
			} else {
				console.warn(
					"Warning: could not acquire lock for synthesis — skipping.",
				);
			}
		} catch (e) {
			console.error(
				`Warning: KB synthesis failed — ${e instanceof Error ? e.message : String(e)}`,
			);
		}

		console.log("");
		console.log("Consolidation complete.");
		console.log(`  Sessions processed: ${totalSessions}`);
		console.log(`  Entries created:    ${totalCreated}`);
		console.log(`  Entries updated:    ${totalUpdated}`);
	} finally {
		consolidation.close();
		await registry.close();
	}
}
