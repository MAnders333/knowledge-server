import { existsSync, readFileSync } from "node:fs";
import { config } from "../config.js";
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
	// Use slice to handle store IDs that contain '=' (e.g. connection string fragments)
	const storeId = storeArg ? storeArg.slice("--store=".length) : undefined;
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
			"--store=<id> scopes which store's entries are deleted and is only meaningful with --reset-store.\n" +
				"Without --reset-store, there is no store-scoped operation to perform.\n" +
				"Daemon cursor and state resets are always global.",
		);
		process.exit(1);
	}

	// Guard: refuse to run destructive operations while the server is live.
	// Concurrent writes from the server (recordEpisode, insertEntry) and this
	// command's deletes are not coordinated — running both at once can corrupt
	// in-flight consolidation state or leave knowledge entries in a mixed state.
	// Run unconditionally so even the default cursor-reset is guarded.
	const pidPath = config.pidPath;
	if (pidPath && existsSync(pidPath)) {
		const pid = Number.parseInt(readFileSync(pidPath, "utf8").trim(), 10);
		if (!Number.isNaN(pid) && pid > 0) {
			// Send signal 0 to probe liveness without killing the process.
			// kill(pid, 0) succeeds (no throw) when alive and we have permission.
			// EPERM: process exists but we can't signal it — still alive (e.g. daemon).
			// ESRCH: no such process — dead.
			// Unknown codes are treated as dead (defensive — avoids blocking reinitialize
			// on unexpected OS errors). Matches the pattern used in stop.ts.
			let alive = true;
			try {
				process.kill(pid, 0);
			} catch (e: unknown) {
				const code = (e as NodeJS.ErrnoException).code;
				alive = code === "EPERM"; // EPERM = exists, no permission; ESRCH = dead
			}
			if (alive) {
				console.error(
					`Server is running (PID ${pid}). Stop it first:\n  knowledge-server stop`,
				);
				process.exit(1);
			}
		}
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

		const storeLabel = storeId
			? `store "${storeId}"`
			: `all ${targetStores.length} writable store(s)`;

		// Describe what will happen (entry count fetched lazily, only when confirming)
		const actions: string[] = [
			"Reset daemon cursor — daemon re-uploads all historical episodes on next tick",
		];
		if (resetState) {
			actions.push(
				"Wipe consolidated_episode and reset consolidation state — episodes re-consolidate with current domain config",
			);
		}
		if (resetStore) {
			// Placeholder; replaced with actual count when --confirm is provided
			actions.push(`Delete knowledge entries from ${storeLabel}`);
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

			// Build suggested confirm command from remaining (already stripped of --store=)
			// then re-add --store=<id> at the front if present, for clarity.
			const levelFlags = remaining
				.filter((a) => a !== "--confirm" && a !== "--dry-run")
				.join(" ");
			const storeFlag = storeId ? `--store=${storeId}` : "";
			const baseParts = [storeFlag, levelFlags].filter(Boolean).join(" ");
			const base = baseParts
				? `knowledge-server reinitialize ${baseParts}`
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

		// Collect entry counts now (only needed for the confirm path + --reset-store)
		let totalEntries = 0;
		if (resetStore) {
			for (const { db } of targetStores) {
				const stats = await db.getStats();
				totalEntries += stats.total ?? 0;
			}
		}

		// Apply — operations are not cross-store atomic. If one step fails,
		// subsequent steps are skipped. Run the command again to retry.
		try {
			await serverStateDb.resetDaemonCursors();
			console.log(
				"  ✓ Reset daemon cursor (all sources) — daemon will re-upload all episodes on next tick",
			);

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
		} catch (err) {
			console.error(
				"\nReinitialize failed partway through — state may be partially reset.\n" +
					"Run the same command again to retry; already-completed steps are safe to repeat.",
			);
			throw err;
		}

		const level = resetStore ? "full" : resetState ? "state" : "cursor";
		console.log("\nDone.");
		if (level === "cursor") {
			console.log(
				"  Daemon will re-upload all historical episodes on its next tick.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		} else if (level === "state") {
			console.log(
				"  Episodes will be re-uploaded and re-consolidated on the next run.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		} else {
			console.log(
				`  Deleted ${totalEntries} entries from ${storeLabel}.\n` +
					"  Episodes will be re-uploaded and re-consolidated on the next run.\n" +
					"  Trigger consolidation when ready: knowledge-server consolidate",
			);
		}
	} finally {
		await registry.close();
	}
}
