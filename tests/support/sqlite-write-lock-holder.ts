/**
 * Cross-process test helper: a child process holds a SQLite write transaction on
 * `dbPath` for `holdMs`, then commits. While it runs, another connection's write
 * either waits (busy_timeout) or fails with SQLITE_BUSY (no busy_timeout).
 *
 * Run as a script: `bun tests/support/sqlite-write-lock-holder.ts <dbPath> <holdMs>`.
 */
import { Database } from "bun:sqlite";

export interface WriteLockChild {
	/** Resolves once the child holds the write lock. */
	ready: Promise<void>;
	exited: Promise<number>;
}

export function holdSqliteWriteLock(
	dbPath: string,
	holdMs: number,
): WriteLockChild {
	const child = Bun.spawn(
		[process.execPath, import.meta.path, dbPath, String(holdMs)],
		{ stdout: "pipe", stderr: "pipe" },
	);
	const reader = child.stdout.getReader();
	const decoder = new TextDecoder();
	let text = "";
	const ready = (async () => {
		while (!text.includes("locked")) {
			const { value, done } = await reader.read();
			if (done) throw new Error("write-lock holder exited before locking");
			text += decoder.decode(value, { stream: true });
		}
		reader.releaseLock();
	})();
	return { ready, exited: child.exited };
}

if (import.meta.main) {
	const [dbPath, holdMs] = [process.argv[2], Number(process.argv[3])];
	const db = new Database(dbPath);
	db.exec("PRAGMA journal_mode=WAL");
	db.exec("CREATE TABLE IF NOT EXISTS lock_probe (x INTEGER)");
	db.exec("BEGIN IMMEDIATE");
	db.exec("INSERT INTO lock_probe (x) VALUES (1)");
	console.log("locked");
	// The hold must be real wall-clock time: another process has to contend for it.
	await Bun.sleep(holdMs);
	db.exec("COMMIT");
	db.close();
}
