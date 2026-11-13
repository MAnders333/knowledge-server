/**
 * Runtime file locations shared by the server, the daemon and the CLI.
 * Every process derives these from the same data directory (see resolveDataDir),
 * so they agree on which lock or status file they mean.
 */

import { join } from "node:path";

/** Single-instance lock for knowledge-daemon (one daemon per data dir). */
export function daemonLockPath(dataDir: string): string {
	return join(dataDir, "daemon.lock");
}

/** Cross-process consolidation lock: held by the server and the CLI while they consolidate. */
export function consolidationLockPath(dataDir: string): string {
	return join(dataDir, "consolidate.lock");
}

/** Lock held while state.db housekeeping (VACUUM, incremental vacuum) runs. */
export function stateMaintenanceLockPath(dataDir: string): string {
	return join(dataDir, "state-maintenance.lock");
}

/** Daemon status file (last cycle, last upload, failure count), written by the daemon. */
export function daemonStatusPath(dataDir: string): string {
	return join(dataDir, "daemon.status.json");
}

/** Default file for daemon log lines when the daemon runs on its own. */
export function daemonLogPath(dataDir: string): string {
	return join(dataDir, "daemon.log");
}

/** Per-SQLite-database consolidation lock, beside the knowledge DB file. */
export function sqliteConsolidationLockPath(dbPath: string): string {
	return `${dbPath}.consolidate.lock`;
}
