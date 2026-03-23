import { StoreRegistry } from "../db/store-registry.js";

/**
 * `knowledge-server reinitialize [--store=<id>] [--confirm|--dry-run]`
 *
 * Wipes knowledge entries from one or all stores and resets the consolidation
 * state so the next server start re-processes all sessions from scratch.
 *
 * --store=<id>   Only wipe the named store (e.g. --store=personal).
 *                When omitted, all writable stores are wiped.
 * --confirm      Apply the changes (required to avoid accidental wipes).
 * --dry-run      Preview what would be deleted without making changes.
 */
export async function runReinitialize(args: string[]): Promise<void> {
	// Parse flags — allow any order
	const storeArg = args.find((a) => a.startsWith("--store="));
	const storeId = storeArg?.split("=")[1];
	const remaining = args.filter((a) => !a.startsWith("--store="));
	const flag = remaining[0];

	const registry = await StoreRegistry.create();
	const { serverLocalDb } = registry;

	// Resolve target stores
	const writableStoreEntries = registry.writableStoreEntries();

	let targetStores: Array<{
		id: string;
		db: import("../db/interface.js").IKnowledgeDB;
	}>;

	if (storeId) {
		const match = writableStoreEntries.find((s) => s.id === storeId);
		if (!match) {
			const available = writableStoreEntries.map((s) => s.id).join(", ");
			console.error(
				`Unknown store: "${storeId}". Available writable stores: ${available}`,
			);
			await registry.close();
			process.exit(1);
		}
		targetStores = [match];
	} else {
		targetStores = writableStoreEntries;
	}

	try {
		// Collect entry counts across target stores
		let totalEntries = 0;
		for (const { db } of targetStores) {
			const stats = await db.getStats();
			totalEntries += stats.total ?? 0;
		}

		const storeLabel = storeId
			? `store "${storeId}"`
			: `all ${targetStores.length} writable store(s)`;

		if (flag === "--dry-run") {
			console.log("Dry run — no changes made.");
			console.log(
				`Would delete ${totalEntries} entries from ${storeLabel} and reset consolidation state.`,
			);
			console.log("Run with --confirm to proceed.");
			return;
		}

		if (flag !== "--confirm") {
			if (flag !== undefined) {
				console.error(`Unknown flag: ${flag}`);
				console.error("Valid flags: --confirm, --dry-run, --store=<id>");
				console.error("");
			}
			console.log(
				`This will DELETE all knowledge entries from ${storeLabel} and reset consolidation state.`,
			);
			console.log(`  Entries that would be deleted: ${totalEntries}`);
			if (!storeId && targetStores.length > 1) {
				console.log(`  Stores: ${targetStores.map((s) => s.id).join(", ")}`);
			}
			console.log("");
			console.log(
				"Run with --confirm to proceed:  knowledge-server reinitialize --confirm",
			);
			if (!storeId) {
				console.log(
					"To wipe a single store:         knowledge-server reinitialize --store=<id> --confirm",
				);
			}
			console.log(
				"Run with --dry-run to preview:  knowledge-server reinitialize --dry-run",
			);
			return;
		}

		// Apply
		for (const { id, db } of targetStores) {
			await db.reinitialize();
			console.log(`  ✓ Wiped store "${id}"`);
		}

		if (!storeId) {
			// Full reset: clear all staging and bookkeeping data.
			await serverLocalDb.reinitializeLocal();
			console.log("  ✓ Reset consolidation state and staging tables");
		} else {
			// Partial wipe: knowledge entries in the named store are removed.
			// consolidated_episode and pending_episodes are NOT touched — they are
			// shared across all stores and cannot be cleanly scoped to a single store
			// without knowing the source→store mapping. Run without --store for a
			// full reset including staging tables.
			console.log(
				"  Note: server.db staging tables (consolidated_episode, pending_episodes,\n" +
					"  daemon_cursor, consolidation_state) were not cleared — they are shared\n" +
					"  across all stores. Run without --store for a full reset including staging.",
			);
		}

		console.log(`\nDone. ${totalEntries} entries deleted from ${storeLabel}.`);
	} finally {
		await registry.close();
	}
}
