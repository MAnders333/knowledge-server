/**
 * Cross-process exclusive lock backed by a lockfile.
 *
 * The lockfile holds `{pid, acquiredAt, heartbeatAt}` as JSON. Acquisition is an
 * exclusive create (`wx`), so at most one process wins. The holder refreshes
 * `heartbeatAt` on a timer. A record is stale, and may be taken over, when any of:
 *   - its PID is no longer alive (crash, reboot, kill -9);
 *   - it names this process's PID but this process holds no handle for the path:
 *     an earlier process that had the same PID crashed while holding it;
 *   - it was acquired before this boot, and its heartbeat is also stale: the PID
 *     was handed out again by the reboot. A fresh heartbeat on such a record means
 *     the wall clock was stepped, not a crash, so it stays held;
 *   - its PID is alive, the record is from this boot, but the heartbeat has not
 *     moved for LOCK_STALE_AFTER_MS. That bound is long on purpose: a live holder
 *     whose machine slept, or whose timers lagged, keeps its lock.
 *
 * A holder whose lockfile is removed, or replaced by a record naming another PID,
 * has lost the lock. It logs an error, sets `lost` on its handle, calls `onLost`,
 * and never writes the lockfile again. A lockfile that cannot be read right now
 * (torn by a concurrent write, transient I/O error) is not a loss: the next beat
 * decides. Callers stop their work when they see `lost`.
 *
 * Used for: the single knowledge-daemon per data dir (`daemon.lock`), the
 * consolidation run lock shared by the server and the CLI (`consolidate.lock`),
 * the per-SQLite-database consolidation lock and the state-maintenance lock.
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
import { uptime } from "node:os";
import { dirname } from "node:path";
import { logger } from "./logger.js";
import { errCode } from "./utils.js";

export interface LockRecord {
	pid: number;
	acquiredAt: number;
	heartbeatAt: number;
}

export interface LockHandle {
	readonly path: string;
	/** True once another process has replaced or removed the lockfile. */
	readonly lost: boolean;
	/** Remove the lockfile if this process still owns it. Idempotent. */
	release(): void;
}

export interface LockOptions {
	/** How often the holder refreshes its heartbeat. */
	heartbeatMs?: number;
	/** Called once when the holder finds the lock taken by another process. */
	onLost?: () => void;
	/** PID liveness check. Defaults to isProcessAlive; tests inject one. */
	isAlive?: (pid: number) => boolean;
	/** Milliseconds since boot. Defaults to the OS uptime; tests inject one. */
	uptimeMs?: number;
}

/** Heartbeat age after which a live holder from this boot is presumed to be a reused PID (24 h). */
const LOCK_STALE_AFTER_MS = 24 * 60 * 60_000;
const LOCK_HEARTBEAT_MS = 30_000;
/** An unreadable lockfile (created but not yet written, or torn) is held this long. */
const UNREADABLE_STALE_MS = 60_000;

/**
 * Lockfile paths this process holds right now. A record that names this PID but
 * is not in this set was left by an earlier process that had the same PID.
 */
const heldPaths = new Set<string>();

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

type LockFileState =
	| { kind: "missing" }
	| { kind: "unreadable" }
	| { kind: "record"; record: LockRecord };

/** What the lockfile holds right now. Only a missing file or a parsed record is decisive. */
function readLockState(path: string): LockFileState {
	let text: string;
	try {
		text = readFileSync(path, "utf8");
	} catch (e) {
		return errCode(e) === "ENOENT"
			? { kind: "missing" }
			: { kind: "unreadable" };
	}
	try {
		const parsed = JSON.parse(text) as Partial<LockRecord>;
		if (
			Number.isInteger(parsed.pid) &&
			typeof parsed.acquiredAt === "number" &&
			typeof parsed.heartbeatAt === "number"
		) {
			return { kind: "record", record: parsed as LockRecord };
		}
	} catch {
		// Torn or partial: handled below.
	}
	return { kind: "unreadable" };
}

/** Parse the lockfile, or null when it is missing or not a lock record. */
export function readLockRecord(path: string): LockRecord | null {
	const state = readLockState(path);
	return state.kind === "record" ? state.record : null;
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
	const isAlive = options.isAlive ?? isProcessAlive;
	const state = readLockState(path);
	if (state.kind === "missing") return null;
	if (state.kind === "unreadable") {
		try {
			const mtime = statSync(path).mtimeMs;
			return now - mtime < UNREADABLE_STALE_MS
				? { pid: 0, acquiredAt: mtime, heartbeatAt: mtime }
				: null;
		} catch {
			return null;
		}
	}
	const record = state.record;
	if (!isAlive(record.pid)) return null;
	if (record.pid === process.pid && !heldPaths.has(path)) return null;
	const bootMs = now - (options.uptimeMs ?? uptime() * 1000);
	if (
		record.acquiredAt < bootMs &&
		now - record.heartbeatAt > 2 * LOCK_HEARTBEAT_MS
	) {
		return null;
	}
	if (now - record.heartbeatAt > LOCK_STALE_AFTER_MS) return null;
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
	options: { heartbeatMs: number; onLost?: () => void },
): LockHandle {
	let released = false;
	let lost = false;
	const timer = setInterval(() => {
		const state = readLockState(path);
		// A torn read (another process is writing its record) is not a loss.
		if (state.kind === "unreadable") return;
		if (state.kind === "record" && state.record.pid === process.pid) {
			record.heartbeatAt = Date.now();
			writeRecord(path, record);
			return;
		}
		// Removed, or replaced by another process's record. Never write it back: the
		// lock now belongs to someone else, and this holder must stop.
		lost = true;
		heldPaths.delete(path);
		clearInterval(timer);
		logger.error(
			`[lock] ${path} was taken over by another process; this process no longer holds the lock.`,
		);
		options.onLost?.();
	}, options.heartbeatMs);
	timer.unref?.();
	return {
		path,
		get lost() {
			return lost;
		},
		release() {
			if (released) return;
			released = true;
			clearInterval(timer);
			heldPaths.delete(path);
			// Unlink only a file that names this PID; another holder's file is left alone.
			const state = readLockState(path);
			if (state.kind === "record" && state.record.pid === process.pid) {
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
			heldPaths.add(path);
			return makeHandle(path, record, {
				heartbeatMs,
				onLost: options.onLost,
			});
		} catch (e) {
			if (errCode(e) !== "EEXIST") throw e;
		}
		if (lockHolder(path, options) !== null) return null;
		if (!removeStaleLock(path, options)) return null;
	}
	return null;
}

/**
 * Remove a stale lockfile without racing another starter. Two starters can both
 * see the same stale lock; without this, the slower one would unlink the fresh
 * lock the faster one just created. The removal runs under a second exclusive
 * file (`<lock>.takeover`), and the lock is re-checked under it, so only a
 * starter that still sees the lock as stale removes it. Returns false when
 * another starter is taking over or the lock turned live; the caller then
 * reports the lock as held.
 *
 * A takeover file older than UNREADABLE_STALE_MS is left by a crashed starter
 * and is removed so the lock is not stuck. A release that races with a takeover
 * can still lose one window; the lock file is short-lived and this is narrow.
 */
function removeStaleLock(path: string, options: LockOptions): boolean {
	const takeover = `${path}.takeover`;
	let fd: number;
	try {
		fd = openSync(takeover, "wx", 0o600);
	} catch (e) {
		if (errCode(e) !== "EEXIST") throw e;
		try {
			if (Date.now() - statSync(takeover).mtimeMs > UNREADABLE_STALE_MS) {
				unlinkSync(takeover);
			}
		} catch {
			// Removed by its owner in the meantime.
		}
		return false;
	}
	closeSync(fd);
	try {
		if (lockHolder(path, options) !== null) return false;
		try {
			unlinkSync(path);
		} catch (e) {
			if (errCode(e) !== "ENOENT") throw e;
		}
		return true;
	} finally {
		try {
			unlinkSync(takeover);
		} catch {
			// Already gone.
		}
	}
}
