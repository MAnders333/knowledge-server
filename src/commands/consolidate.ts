import { ActivationEngine } from "../activation/activate.js";
import { ConsolidationEngine } from "../consolidation/consolidate.js";
import { PendingEpisodesReader } from "../consolidation/readers/pending.js";
import { StoreRegistry } from "../db/store-registry.js";
import { logger } from "../logger.js";
import {
	drainConsolidation,
	runEmbeddingCheck,
	runSynthesisPass,
} from "./consolidation-drain.js";

/**
 * `knowledge-server consolidate`
 *
 * Runs a full consolidation drain: repeatedly processes batches of pending
 * sessions until none remain. Prints live progress to stdout.
 */
export async function runConsolidate(): Promise<void> {
	// Log to both stdout and the server log file so all consolidation activity
	// (whether triggered via HTTP, polling, or CLI) appears in one place.
	const { config } = await import("../config.js");
	logger.init(config.logPath);

	const registry = await StoreRegistry.create();
	const db = registry.writableStore();
	const { serverStateDb } = registry;
	const activation = new ActivationEngine(
		db,
		registry.readStores(),
		registry.writableStores(),
	);
	const consolidation = new ConsolidationEngine(
		db,
		serverStateDb,
		activation,
		[new PendingEpisodesReader(serverStateDb)],
		registry.domainRouter,
	);

	try {
		// Check for embedding model change before consolidating — ensures all
		// vectors are consistent when reconsolidation compares new extractions
		// against existing entries.
		await runEmbeddingCheck(activation);

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

		const { totalSessions, totalCreated, totalUpdated } =
			await drainConsolidation(consolidation);

		// Run KB synthesis once after all batches, same as the server-side drain.
		// Runs unconditionally — existing entries may still be ripe even when no
		// new sessions were processed (e.g. after a re-embedding pass).
		console.log("\nRunning KB synthesis pass...");
		await runSynthesisPass(consolidation);

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
