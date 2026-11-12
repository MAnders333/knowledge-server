import { homedir } from "node:os";

/**
 * The user's home directory, read from `HOME` at call time. Bun fixes
 * `os.homedir()` at process start, so a runtime change to `HOME` (tests point
 * it at a temp dir) would otherwise be ignored. Falls back to `os.homedir()`
 * when `HOME` is unset.
 */
export function homeDir(): string {
	return process.env.HOME || homedir();
}
