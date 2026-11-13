import { homedir } from "node:os";
import { join } from "node:path";

/**
 * The user's home directory, read from `HOME` at call time. Bun fixes
 * `os.homedir()` at process start, so a runtime change to `HOME` (tests point
 * it at a temp dir) would otherwise be ignored. Falls back to `os.homedir()`
 * when `HOME` is unset.
 *
 * This module is a leaf: it reads nothing from the environment at import time.
 * env-file.ts imports it, and env.ts loads env-file before config-file.ts can
 * freeze DEFAULT_CONFIG, so anything that would make this module import
 * config-file.ts must stay out.
 */
export function homeDir(): string {
	return process.env.HOME || homedir();
}

/**
 * Default data directory: state.db, knowledge.db, the lockfiles, the daemon
 * status file, server.log and server.pid unless stateDb.path moves them.
 */
export function defaultDataDir(home: string = homeDir()): string {
	return join(home, ".local", "share", "knowledge-server");
}
