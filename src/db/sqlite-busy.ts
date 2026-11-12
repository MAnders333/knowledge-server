/**
 * How long a SQLite connection waits for a lock held by another process before it
 * fails with SQLITE_BUSY. The server, the daemon and the CLI all write the same
 * state.db; with 0 a colliding write failed immediately. Every connection sets this
 * as its first statement, before journal_mode, because switching to WAL takes a lock.
 */
export const SQLITE_BUSY_TIMEOUT_MS = 5000;
