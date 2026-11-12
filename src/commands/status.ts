import { existsSync, readFileSync } from "node:fs";
import pkg from "../../package.json" with { type: "json" };
import { StoreRegistry } from "../db/store-registry.js";
import { errCode } from "../utils.js";

/**
 * `knowledge-server status`
 *
 * Prints a human-readable summary of the knowledge store and whether the
 * HTTP server is currently running (detected via PID file).
 */
export async function runStatus(pidPath: string): Promise<void> {
	// Server running state — detected via PID file.
	let serverLine: string;
	if (!pidPath || !existsSync(pidPath)) {
		serverLine = "stopped";
	} else {
		const raw = readFileSync(pidPath, "utf8").trim();
		const pid = Number.parseInt(raw, 10);
		if (Number.isNaN(pid)) {
			serverLine = "unknown (malformed PID file)";
		} else {
			const isAlive = (() => {
				try {
					process.kill(pid, 0);
					return true;
				} catch (e) {
					return errCode(e) === "EPERM";
				}
			})();
			serverLine = isAlive
				? `running (PID ${pid})`
				: "stopped (stale PID file)";
		}
	}

	const registry = await StoreRegistry.create();
	const { serverStateDb } = registry;
	try {
		// Fan out stats across all readable stores and sum counts.
		const allStats = await Promise.all(
			registry.readStores().map((s) => s.getStats()),
		);
		const stats = allStats.reduce(
			(acc, s) => ({
				total: acc.total + s.total,
				active: acc.active + s.active,
				superseded: acc.superseded + (s.superseded ?? 0),
				archived: acc.archived + (s.archived ?? 0),
				conflicted: acc.conflicted + (s.conflicted ?? 0),
				tombstoned: (acc.tombstoned ?? 0) + (s.tombstoned ?? 0),
			}),
			{
				total: 0,
				active: 0,
				superseded: 0,
				archived: 0,
				conflicted: 0,
				tombstoned: 0,
			},
		);
		const state = await serverStateDb.getConsolidationState();

		// Count pending sessions directly from state.db — avoids constructing
		// a full ConsolidationEngine (which costs LLM/embedding client setup)
		// and avoids the stale-1 bug from PendingEpisodesReader.countNewSessions()
		// when prepare() has not been called.
		const pendingSessions = await serverStateDb.countPendingSessions();

		console.log("Knowledge Server Status");
		console.log("───────────────────────────────────────");
		console.log(`  Version:            ${pkg.version}`);
		console.log(`  Server:             ${serverLine}`);
		console.log(
			`  Knowledge entries:  ${stats.total ?? 0} total, ${stats.active ?? 0} active`,
		);
		if ((stats.conflicted ?? 0) > 0)
			console.log(`  Conflicted:         ${stats.conflicted}`);
		if ((stats.archived ?? 0) > 0)
			console.log(`  Archived:           ${stats.archived}`);
		console.log(
			`  Last consolidation: ${state.lastConsolidatedAt ? new Date(state.lastConsolidatedAt).toISOString() : "never"}`,
		);
		console.log(`  Pending sessions:   ${pendingSessions}`);
		const deadLettered = await serverStateDb.countDeadLetteredEpisodes();
		console.log(
			`  Dead-lettered:      ${deadLettered} (requeue with: knowledge-server consolidate --retry-dead)`,
		);
		console.log(
			`  Total processed:    ${state.totalSessionsProcessed} sessions, ${state.totalEntriesCreated} created, ${state.totalEntriesUpdated} updated`,
		);
	} finally {
		await registry.close();
	}
}
