import { KnowledgeDB } from "../db/database.js";

/**
 * `knowledge-server reinitialize [--confirm]`
 *
 * Wipes all knowledge entries and resets the consolidation cursor so the
 * next server start re-processes all sessions from scratch.
 *
 * Requires --confirm to apply; prints a preview without it.
 */
export function runReinitialize(args: string[]): void {
	const flag = args[0];
	const db = new KnowledgeDB();

	try {
		const stats = db.getStats();
		const entryCount = stats.total ?? 0;

		if (flag === "--dry-run") {
			console.log("Dry run — no changes made.");
			console.log(
				`Would delete ${entryCount} entries and reset the consolidation cursor.`,
			);
			console.log("Run with --confirm to proceed.");
			return;
		}

		if (flag !== "--confirm") {
			console.log(
				"This will DELETE all knowledge entries and reset the consolidation cursor.",
			);
			console.log(`  Entries that would be deleted: ${entryCount}`);
			console.log("");
			console.log(
				"Run with --confirm to proceed:  knowledge-server reinitialize --confirm",
			);
			console.log("Run with --dry-run to preview:  knowledge-server reinitialize --dry-run");
			process.exit(1);
		}

		db.reinitialize();
		console.log(
			`Knowledge DB reinitialized. ${entryCount} entries deleted, cursor reset.`,
		);
	} finally {
		db.close();
	}
}
