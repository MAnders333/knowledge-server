import { existsSync, readFileSync } from "node:fs";
import pkg from "../../package.json" with { type: "json" };
import { ActivationEngine } from "../activation/activate.js";
import { ConsolidationEngine } from "../consolidation/consolidate.js";
import { createEpisodeReaders } from "../consolidation/readers/index.js";
import { createKnowledgeDB } from "../db/index.js";

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
					return (e as NodeJS.ErrnoException).code === "EPERM";
				}
			})();
			serverLine = isAlive
				? `running (PID ${pid})`
				: "stopped (stale PID file)";
		}
	}

	const db = await createKnowledgeDB();
	try {
		const stats = await db.getStats();
		const state = await db.getConsolidationState();

		const activation = new ActivationEngine(db);
		const readers = createEpisodeReaders();
		const consolidation = new ConsolidationEngine(db, activation, readers);
		const { pendingSessions } = await consolidation.checkPending();
		consolidation.close();

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
		console.log(
			`  Total processed:    ${state.totalSessionsProcessed} sessions, ${state.totalEntriesCreated} created, ${state.totalEntriesUpdated} updated`,
		);
	} finally {
		await db.close();
	}
}
