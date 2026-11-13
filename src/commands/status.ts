import { readFileSync } from "node:fs";
import pkg from "../../package.json" with { type: "json" };
import { resolveServerAddress } from "../config-file.js";
import { StoreRegistry } from "../db/store-registry.js";
import {
	type ProcessTable,
	classifyPidFile,
	isServerCommand,
	locatePidFile,
	systemProcessTable,
} from "../process-identity.js";

/** Window for the per-source capture vs injection check. */
const COVERAGE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * Capture source (reader `source`) -> activation client tag, where they differ.
 * The oh-my-pi reader writes episodes as "oh-my-pi"; its plugin and MCP calls
 * send the client tag "omp". Other sources use the same name on both sides.
 */
const SOURCE_TO_CLIENT: Readonly<Record<string, string>> = {
	"oh-my-pi": "omp",
};
const CLIENT_TO_SOURCE = Object.fromEntries(
	Object.entries(SOURCE_TO_CLIENT).map(([source, client]) => [client, source]),
);

interface CoverageRow {
	source: string;
	captured: number;
	injected: number;
}

/**
 * Per-source captured (consolidated_episode) vs injected (activation_event)
 * counts, plus a warning for each source that was captured but never injected.
 * Pure: the caller supplies both maps.
 */
export function summarizeCoverage(
	captured: Record<string, number>,
	injected: Record<string, number>,
): { rows: CoverageRow[]; warnings: string[] } {
	// Injected counts are keyed by client tag; fold them onto the capture source.
	const injectedBySource: Record<string, number> = {};
	for (const [client, count] of Object.entries(injected)) {
		const source = CLIENT_TO_SOURCE[client] ?? client;
		injectedBySource[source] = (injectedBySource[source] ?? 0) + count;
	}
	const sources = [
		...new Set([...Object.keys(captured), ...Object.keys(injectedBySource)]),
	].sort();
	const rows = sources.map((source) => ({
		source,
		captured: captured[source] ?? 0,
		injected: injectedBySource[source] ?? 0,
	}));
	const warnings = rows
		.filter((r) => r.captured > 0 && r.injected === 0)
		.map(
			(r) =>
				`${r.source}: ${r.captured} episodes captured in the last 7 days but no activation injected any entries (each returned nothing or missed the client deadline). Check that the client calls /activate with its client tag, and that the server answers within the client's deadline.`,
		);
	return { rows, warnings };
}

interface ServerHealth {
	health: string;
	reasons: string[];
}

/**
 * Health verdict from a running server's GET /status. Null when the server does not
 * answer or the reply has no health field (for example an older server). With a stable
 * admin token the server may require it on /status (KNOWLEDGE_PROTECT_READS), so it is
 * sent when given.
 */
export async function fetchServerHealth(
	baseUrl: string,
	timeoutMs = 2000,
	adminToken: string | null = null,
): Promise<ServerHealth | null> {
	try {
		const res = await fetch(`${baseUrl}/status`, {
			headers:
				adminToken === null ? {} : { Authorization: `Bearer ${adminToken}` },
			signal: AbortSignal.timeout(timeoutMs),
		});
		if (!res.ok) return null;
		const body = (await res.json()) as { health?: unknown; reasons?: unknown };
		if (typeof body.health !== "string") return null;
		const reasons = Array.isArray(body.reasons)
			? body.reasons.filter((r): r is string => typeof r === "string")
			: [];
		return { health: body.health, reasons };
	} catch {
		return null;
	}
}

/**
 * The server line of `status`, from the contents of the PID file. A PID counts as
 * running only when it still runs knowledge-server (see `classifyPidFile`).
 */
export function serverStateLine(
	raw: string,
	table: ProcessTable = systemProcessTable,
): string {
	const state = classifyPidFile(raw, table, isServerCommand);
	switch (state.kind) {
		case "invalid":
			return "unknown (malformed PID file)";
		case "running":
			return `running (PID ${state.pid})`;
		case "unverified":
			return `unknown (PID ${state.pid} is alive; its command line is unreadable)`;
		case "stale":
			return state.reason === "dead"
				? "stopped (stale PID file)"
				: `stopped (stale PID file; PID ${state.pid} belongs to another program)`;
	}
}

/**
 * `knowledge-server status`
 *
 * Prints a human-readable summary of the knowledge store and whether the
 * HTTP server is currently running (detected via PID file).
 */
export async function runStatus(
	pidPath: string,
	legacyPidPath: string | null = null,
	adminToken: string | null = null,
): Promise<void> {
	// Server running state — detected via PID file.
	const located = locatePidFile(pidPath, legacyPidPath);
	const serverLine =
		located === null
			? "stopped"
			: serverStateLine(readFileSync(located, "utf8"));

	const address = resolveServerAddress();
	const serverHealth = await fetchServerHealth(
		`http://${address.host}:${address.port}`,
		2000,
		adminToken,
	);
	const healthLine = serverHealth
		? serverHealth.reasons.length > 0
			? `${serverHealth.health} (${serverHealth.reasons.join("; ")})`
			: serverHealth.health
		: "unavailable (server not answering)";

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
		console.log(`  Health:             ${healthLine}`);
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

		const since = Date.now() - COVERAGE_WINDOW_MS;
		const { rows, warnings } = summarizeCoverage(
			await serverStateDb.countEpisodesBySource(since),
			await serverStateDb.countActivationsByClient(since),
		);
		if (rows.length > 0) {
			console.log("");
			console.log("  Last 7 days (captured → injected):");
			for (const r of rows) {
				console.log(
					`    ${r.source.padEnd(14)} ${String(r.captured).padStart(6)} → ${r.injected}`,
				);
			}
		}
		for (const w of warnings) console.log(`  ⚠ ${w}`);
	} finally {
		await registry.close();
	}
}
