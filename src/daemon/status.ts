/**
 * Daemon status file: the daemon writes it after every upload cycle, the server
 * reads it for /status. It holds no secrets and lives in the data directory.
 */

import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export interface DaemonStatus {
	pid: number;
	startedAt: number;
	intervalMs: number;
	/** When the last upload cycle finished, success or not. */
	lastCycleAt: number | null;
	/** Upload cycles in a row that reported a failure. Reset by a clean cycle. */
	consecutiveFailures: number;
	lastError: string | null;
	/** When an upload cycle last stored episodes. */
	lastUploadAt: number | null;
}

/** Parse the status file, or null when it is missing or unreadable. */
export function readDaemonStatus(path: string): DaemonStatus | null {
	try {
		const parsed = JSON.parse(
			readFileSync(path, "utf8"),
		) as Partial<DaemonStatus>;
		if (
			typeof parsed.pid !== "number" ||
			typeof parsed.startedAt !== "number"
		) {
			return null;
		}
		return {
			pid: parsed.pid,
			startedAt: parsed.startedAt,
			intervalMs: typeof parsed.intervalMs === "number" ? parsed.intervalMs : 0,
			lastCycleAt: parsed.lastCycleAt ?? null,
			consecutiveFailures: parsed.consecutiveFailures ?? 0,
			lastError: parsed.lastError ?? null,
			lastUploadAt: parsed.lastUploadAt ?? null,
		};
	} catch {
		return null;
	}
}

/** Write atomically (temp file + rename) so readers never see a torn file. */
export function writeDaemonStatus(path: string, status: DaemonStatus): void {
	mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
	const tmp = `${path}.${process.pid}.tmp`;
	writeFileSync(tmp, JSON.stringify(status, null, 2), { mode: 0o600 });
	renameSync(tmp, path);
}
