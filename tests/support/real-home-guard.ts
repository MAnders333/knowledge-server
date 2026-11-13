import { existsSync, lstatSync, mkdtempSync, readdirSync } from "node:fs";
import { tmpdir, userInfo } from "node:os";
import { join } from "node:path";

/**
 * Guard against test runs writing to the developer's real home. Snapshots are
 * stat-only: names, sizes, mtimes and inodes. File contents are never read, so
 * secrets in these config files never enter test output or memory here.
 *
 * Writes are prevented where possible: redirectHomeEnv points HOME and the config
 * and state variables at a temp home for the whole run. Code that still calls
 * os.homedir() cannot be redirected (Bun fixes it at process start), so the
 * snapshot check reports its writes after the run instead.
 *
 * Only configuration locations are watched. Session stores that a running
 * agent writes continuously (for example ~/.claude/projects) are not.
 */

/** Files that setup-tool writes, and the server's own config. Stat-compared. */
const WATCHED_FILES = [
	".cursor/mcp.json",
	".codex/hooks.json",
	".codex/config.toml",
	".claude/settings.json",
	".config/opencode/opencode.jsonc",
	".pi/agent/settings.json",
	".config/knowledge-server/config.jsonc",
	".config/knowledge-server/.env",
	".local/share/knowledge-server/.env",
];

/** Directories whose direct entry names are compared (creation or removal). */
const WATCHED_DIRS = [
	".cursor",
	".codex",
	".claude",
	".config/opencode",
	".config/systemd/user",
	"Library/LaunchAgents",
	".config/knowledge-server",
	".local/share/knowledge-server",
];

/**
 * The real home from the passwd entry (independent of `HOME`, which the isolated
 * test runs override). `KS_REAL_HOME_GUARD_ROOT` points the guard at another
 * directory; only the guard's own tests use it, to prove the guard fires.
 */
export const REAL_HOME =
	process.env.KS_REAL_HOME_GUARD_ROOT || userInfo().homedir;

/**
 * Points HOME and the config and state variables at a fresh temp home, for the
 * whole run (the preload calls this). Code that reads them at call time, such as
 * homeDir() and the setup-tool paths, then writes into the sandbox. Returns the
 * sandbox path so the preload can remove it afterwards.
 */
export function redirectHomeEnv(): string {
	const sandbox = mkdtempSync(join(tmpdir(), "ks-test-home-"));
	const config = join(sandbox, ".config");
	process.env.HOME = sandbox;
	process.env.XDG_CONFIG_HOME = config;
	process.env.KNOWLEDGE_CONFIG_HOME = join(config, "knowledge-server");
	process.env.CURSOR_HOME = join(sandbox, ".cursor");
	process.env.CODEX_HOME = join(sandbox, ".codex");
	process.env.CLAUDE_CONFIG_DIR = join(sandbox, ".claude");
	process.env.CLAUDE_DB_PATH = join(sandbox, ".claude");
	return sandbox;
}

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
