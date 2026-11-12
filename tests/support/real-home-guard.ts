import { existsSync, lstatSync, readdirSync } from "node:fs";
import { userInfo } from "node:os";
import { join } from "node:path";

/**
 * Guard against test runs writing to the developer's real home. Snapshots are
 * stat-only: names, sizes, mtimes and inodes. File contents are never read, so
 * secrets in these config files never enter test output or memory here.
 *
 * Only configuration locations are watched. Session stores that a running
 * agent writes continuously (for example ~/.claude/projects) are not.
 */

/** Files that setup-tool writes. Stat-compared. */
const WATCHED_FILES = [
	".cursor/mcp.json",
	".codex/hooks.json",
	".codex/config.toml",
	".claude/settings.json",
	".config/opencode/opencode.json",
	".pi/agent/settings.json",
];

/** Directories whose direct entry names are compared (creation or removal). */
const WATCHED_DIRS = [
	".cursor",
	".codex",
	".claude",
	".config/opencode",
	".config/systemd/user",
	"Library/LaunchAgents",
];

/**
 * The real home from the passwd entry (independent of `HOME`, which the isolated
 * test runs override). `KS_REAL_HOME_GUARD_ROOT` points the guard at another
 * directory; only the guard's own tests use it, to prove the guard fires.
 */
export const REAL_HOME =
	process.env.KS_REAL_HOME_GUARD_ROOT || userInfo().homedir;

/** Map of relative path to a stat-only fingerprint. Missing paths are absent from the map. */
export function snapshotHome(home: string = REAL_HOME): Map<string, string> {
	const out = new Map<string, string>();
	for (const rel of WATCHED_FILES) {
		const abs = join(home, rel);
		if (!existsSync(abs)) continue;
		const st = lstatSync(abs);
		out.set(rel, `file ino=${st.ino} size=${st.size} mtime=${st.mtimeMs}`);
	}
	for (const rel of WATCHED_DIRS) {
		const abs = join(home, rel);
		if (!existsSync(abs)) continue;
		const names = readdirSync(abs).sort().join("\n");
		out.set(rel, `dir entries:\n${names}`);
	}
	return out;
}

/** Relative paths that were created, removed or changed between two snapshots. */
export function diffSnapshots(
	before: Map<string, string>,
	after: Map<string, string>,
): string[] {
	const changed: string[] = [];
	for (const key of new Set([...before.keys(), ...after.keys()])) {
		if (before.get(key) !== after.get(key)) changed.push(key);
	}
	return changed.sort();
}
