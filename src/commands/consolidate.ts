import { ActivationEngine } from "../activation/activate.js";
import { ConsolidationEngine } from "../consolidation/consolidate.js";
import { createEpisodeReaders } from "../consolidation/readers/index.js";
import { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";

/**
 * `knowledge-server consolidate`
 *
 * Runs a full consolidation drain: repeatedly processes batches of pending
 * sessions until none remain. Prints live progress to stdout.
 */
export async function runConsolidate(): Promise<void> {
	logger.init(""); // disable file logging — output goes to stdout only

	const db = new KnowledgeDB();
	const activation = new ActivationEngine(db);
	const readers = createEpisodeReaders();
	const consolidation = new ConsolidationEngine(db, activation, readers);

	try {
		const { pendingSessions } = consolidation.checkPending();
		if (pendingSessions === 0) {
			console.log("Nothing to consolidate — knowledge graph is up to date.");
			return;
		}

		console.log(`${pendingSessions} sessions pending — starting consolidation...`);
		console.log("");

		let batch = 1;
		let totalSessions = 0;
		let totalCreated = 0;
		let totalUpdated = 0;

		while (true) {
			if (!consolidation.tryLock()) {
				// Shouldn't happen in CLI mode (no concurrent callers), but be safe.
				await new Promise((r) => setTimeout(r, 500));
				continue;
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

		console.log("");
		console.log("Consolidation complete.");
		console.log(`  Sessions processed: ${totalSessions}`);
		console.log(`  Entries created:    ${totalCreated}`);
		console.log(`  Entries updated:    ${totalUpdated}`);
	} finally {
		consolidation.close();
		db.close();
	}
}
