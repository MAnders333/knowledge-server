import { StoreRegistry } from "../db/store-registry.js";

/**
 * `knowledge-server reinitialize [--store=<id>] [--reset-state] [--reset-store] [--confirm|--dry-run]`
 *
 * Resets local state, server state, and/or knowledge store entries.
 * Flags are additive — each level includes everything below it.
 *
 * ── Levels (choose one) ───────────────────────────────────────────────────────
 *
 * (default, no level flag)
 *   Resets the daemon upload cursor only. The daemon will re-upload all
 *   historical episodes on its next tick. Knowledge entries and server state
 *   are untouched. Safe for shared stores.
 *   Use when: connecting to a new or existing store for the first time.
 *
 * --reset-state
 *   Also wipes consolidated_episode and resets consolidation_state counters.
 *   Episodes re-upload AND re-consolidate under the current domain config.
 *   Safe for shared stores (session IDs are per-machine by nature).
 *   Use when: retroactively rerouting knowledge after adding a new domain.
 *
 * --reset-store
 *   Also wipes all knowledge entries from the target store(s).
 *   Implies --reset-state. Use --store=<id> to scope to a single store.
 *   NOT safe for shared stores with other active users.
 *   Use when: full fresh start from scratch.
 *
 * ── Options ───────────────────────────────────────────────────────────────────
 *
 * --store=<id>   Scope store wipe to the named store (only meaningful with
 *                --reset-store). Daemon cursor and state resets are always
 *                global — they cannot be scoped to a single store.
 * --confirm      Apply the changes (required to avoid accidental wipes).
 * --dry-run      Preview what would happen without making changes.
 */
export async function runReinitialize(args: string[]): Promise<void> {
	const storeArg = args.find((a) => a.startsWith("--store="));
	const storeId = storeArg?.split("=")[1];
	const remaining = args.filter((a) => !a.startsWith("--store="));

	const resetStore = remaining.includes("--reset-store");
	const resetState = remaining.includes("--reset-state") || resetStore;
	const dryRun = remaining.includes("--dry-run");
	const confirm = remaining.includes("--confirm");

	const unknownFlags = remaining.filter(
		(a) =>
			a !== "--reset-store" &&
			a !== "--reset-state" &&
			a !== "--confirm" &&
			a !== "--dry-run",
	);
	if (unknownFlags.length > 0) {
		console.error(`Unknown flag(s): ${unknownFlags.join(", ")}`);
		console.error(
			"Valid flags: --reset-state, --reset-store, --store=<id>, --confirm, --dry-run",
		);
		process.exit(1);
	}

	if (storeId && !resetStore) {
		console.error(
			"--store=<id> only applies with --reset-store. Daemon cursor and state resets are always global.",
		);
		process.exit(1);
	}

	const registry = await StoreRegistry.create();
	const { serverStateDb } = registry;

	try {
		// Resolve target stores
		const writableStoreEntries = registry.writableStoreEntries();
		let targetStores: Array<{
			id: string;
			db: import("../db/interface.js").IKnowledgeStore;
		}>;

		if (storeId) {
			const match = writableStoreEntries.find((s) => s.id === storeId);
			if (!match) {
				const available = writableStoreEntries.map((s) => s.id).join(", ");
				console.error(
					`Unknown store: "${storeId}". Available writable stores: ${available}`,
				);
				process.exit(1);
			}
			targetStores = [match];
		} else {
			targetStores = writableStoreEntries;
		}

		// Collect entry counts for display
		let totalEntries = 0;
		if (resetStore) {
			for (const { db } of targetStores) {
				const stats = await db.getStats();
				totalEntries += stats.total ?? 0;
			}
		}

		const storeLabel = storeId
			? `store "${storeId}"`
			: `all ${targetStores.length} writable store(s)`;

		// Describe what will happen
		const actions: string[] = [
			"Reset daemon cursor — daemon re-uploads all historical episodes on next tick",
		];
		if (resetState) {
			actions.push(
				"Wipe consolidated_episode and reset consolidation state — episodes re-consolidate with current domain config",
			);
		}
		if (resetStore) {
			actions.push(
				`Delete ${totalEntries} knowledge entries from ${storeLabel}`,
			);
		}

		if (dryRun) {
			console.log("Dry run — no changes made.\nWould perform:");
			for (const action of actions) console.log(`  • ${action}`);
			console.log("\nRun with --confirm to proceed.");
			return;
		}

		if (!confirm) {
			console.log("This will:");
			for (const action of actions) console.log(`  • ${action}`);
			console.log("");

			const flagsForConfirm = args
				.filter((a) => a !== "--confirm" && a !== "--dry-run")
				.join(" ");
			const base = flagsForConfirm
				? `knowledge-server reinitialize ${flagsForConfirm}`
				: "knowledge-server reinitialize";

			console.log(`Run with --confirm to proceed:\n  ${base} --confirm`);

			if (!resetState && !resetStore) {
				console.log(
					"\nTo also re-consolidate with current domain config:\n" +
						"  knowledge-server reinitialize --reset-state --confirm\n\n" +
						"To also wipe all knowledge entries (full reset):\n" +
						"  knowledge-server reinitialize --reset-store --confirm",
				);
			}
			return;
		}

		// Apply
		await serverStateDb.resetDaemonCursors();
		console.log("  ✓ Reset daemon cursor (all sources)");

		if (resetState) {
			await serverStateDb.reinitialize();
			console.log(
				"  ✓ Wiped consolidated_episode and reset consolidation state",
			);
		}

		if (resetStore) {
			for (const { id, db } of targetStores) {
				await db.reinitialize();
				console.log(`  ✓ Wiped store "${id}"`);
			}
		}

		console.log("\nDone.");
		if (!resetState) {
			console.log(
				"  Daemon will re-upload all historical episodes on its next tick.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		} else if (!resetStore) {
			console.log(
				"  Episodes will be re-uploaded and re-consolidated on the next run.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		} else {
			console.log(
				"  Store wiped, state reset, daemon will re-upload all episodes.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		}
	} finally {
		await registry.close();
	}
}
