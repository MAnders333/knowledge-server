/**
 * Cross-process exclusive lock backed by a lockfile.
 *
 * The lockfile holds `{pid, acquiredAt, heartbeatAt}` as JSON. Acquisition is an
 * exclusive create (`wx`), so at most one process wins. The holder refreshes
 * `heartbeatAt` on a timer. A lock is stale, and may be taken over, when either:
 *   - its PID is no longer alive (crash, reboot, kill -9), or
 *   - its heartbeat is older than `staleAfterMs` (backstop for PID reuse).
 *
 * Used for: the single knowledge-daemon per data dir (`daemon.lock`), the
 * consolidation run lock shared by the server and the CLI (`consolidate.lock`),
 * and the per-SQLite-database consolidation lock.
 *
 * Takeover of a stale lock is best-effort: two processes that discover the same
 * stale lock in the same instant can both remove it. Crashed holders are the only
 * way a lock goes stale, so this window is narrow and the consequence (one extra
 * process) is no worse than having no lock at all.
 */

import {
	closeSync,
	mkdirSync,
	openSync,
	readFileSync,
	renameSync,
	statSync,
	unlinkSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { dirname } from "node:path";
import { errCode } from "./utils.js";

export interface LockRecord {
	pid: number;
	acquiredAt: number;
	heartbeatAt: number;
}

export interface LockHandle {
	readonly path: string;
	/** Remove the lockfile if this process still owns it. Idempotent. */
	release(): void;
}

export interface LockOptions {
	/** Heartbeat holder with no refresh for this long is treated as gone. */
	staleAfterMs?: number;
	/** How often the holder refreshes its heartbeat. */
	heartbeatMs?: number;
}

export const LOCK_STALE_AFTER_MS = 10 * 60_000;
export const LOCK_HEARTBEAT_MS = 30_000;
/** An unreadable lockfile (created but not yet written, or torn) is held this long. */
const UNREADABLE_STALE_MS = 60_000;

/** True when `pid` names a live process (EPERM means it exists but is not ours). */
export function isProcessAlive(pid: number): boolean {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (e) {
		return errCode(e) === "EPERM";
	}
}

/** Parse the lockfile, or null when it is missing or not a lock record. */
export function readLockRecord(path: string): LockRecord | null {
	try {
		const parsed = JSON.parse(
			readFileSync(path, "utf8"),
		) as Partial<LockRecord>;
		if (
			Number.isInteger(parsed.pid) &&
			typeof parsed.acquiredAt === "number" &&
			typeof parsed.heartbeatAt === "number"
		) {
			return parsed as LockRecord;
		}
		return null;
	} catch {
		return null;
	}
}

/**
 * The live holder of the lock at `path`, or null when the lock is free or stale.
 * A holder with `pid` 0 means the file exists but its record is not readable yet.
 */
export function lockHolder(
	path: string,
	options: LockOptions = {},
	now = Date.now(),
): LockRecord | null {
	const staleAfterMs = options.staleAfterMs ?? LOCK_STALE_AFTER_MS;
	const record = readLockRecord(path);
	if (record === null) {
		try {
			const mtime = statSync(path).mtimeMs;
			return now - mtime < UNREADABLE_STALE_MS
				? { pid: 0, acquiredAt: mtime, heartbeatAt: mtime }
				: null;
		} catch {
			return null;
		}
	}
	if (!isProcessAlive(record.pid)) return null;
	if (now - record.heartbeatAt > staleAfterMs) return null;
	return record;
}

function writeRecord(path: string, record: LockRecord): void {
	const tmp = `${path}.${record.pid}.tmp`;
	writeFileSync(tmp, JSON.stringify(record), { mode: 0o600 });
	renameSync(tmp, path);
}

function makeHandle(
	path: string,
	record: LockRecord,
	heartbeatMs: number,
): LockHandle {
	let released = false;
	const timer = setInterval(() => {
		// Another process may have taken over after a stall; never overwrite it.
		if (readLockRecord(path)?.pid !== process.pid) return;
		record.heartbeatAt = Date.now();
		writeRecord(path, record);
	}, heartbeatMs);
	timer.unref?.();
	return {
		path,
		release() {
			if (released) return;
			released = true;
			clearInterval(timer);
			if (readLockRecord(path)?.pid === process.pid) {
				try {
					unlinkSync(path);
				} catch (e) {
					if (errCode(e) !== "ENOENT") throw e;
				}
			}
		},
	};
}

/**
 * Try to take the lock at `path` without waiting. Returns a handle when acquired,
 * or null when a live holder (including this process) owns it.
 */
export function tryAcquireProcessLock(
	path: string,
	options: LockOptions = {},
): LockHandle | null {
	const staleAfterMs = options.staleAfterMs ?? LOCK_STALE_AFTER_MS;
	const heartbeatMs = options.heartbeatMs ?? LOCK_HEARTBEAT_MS;
	mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
	// Two attempts: the second runs only after a stale lockfile was removed.
	for (let attempt = 0; attempt < 2; attempt++) {
		try {
			const fd = openSync(path, "wx", 0o600);
			const now = Date.now();
			const record: LockRecord = {
				pid: process.pid,
				acquiredAt: now,
				heartbeatAt: now,
			};
			try {
				writeSync(fd, JSON.stringify(record));
			} finally {
				closeSync(fd);
			}
			return makeHandle(path, record, heartbeatMs);
		} catch (e) {
			if (errCode(e) !== "EEXIST") throw e;
		}
		if (lockHolder(path, { staleAfterMs }) !== null) return null;
		try {
			unlinkSync(path);
		} catch (e) {
			if (errCode(e) !== "ENOENT") throw e;
		}
	}
	return null;
}
