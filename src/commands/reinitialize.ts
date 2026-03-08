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
			if (flag !== undefined) {
				// Unrecognised flag — warn so the user knows their input was ignored
				// rather than silently falling through to the usage message.
				console.error(`Unknown flag: ${flag}`);
				console.error(
					"Valid flags: --confirm, --dry-run",
				);
				console.error("");
			}
			console.log(
				"This will DELETE all knowledge entries and reset the consolidation cursor.",
			);
			console.log(`  Entries that would be deleted: ${entryCount}`);
			console.log("");
			console.log(
				"Run with --confirm to proceed:  knowledge-server reinitialize --confirm",
			);
			console.log("Run with --dry-run to preview:  knowledge-server reinitialize --dry-run");
			// Return rather than process.exit(0) so the finally block runs db.close().
			// The caller (src/index.ts) exits 0 after runReinitialize() returns.
			return;
		}

		db.reinitialize();
		console.log(
			`Knowledge DB reinitialized. ${entryCount} entries deleted, cursor reset.`,
		);
	} finally {
		db.close();
	}
}
