import { spawnSync } from "node:child_process";
import { existsSync, lstatSync, mkdtempSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
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
 * Names a running server creates and rewrites on its own: locks, pid and log files,
 * SQLite journals, daemon state and the model cache. Directory listings skip them,
 * so a server running beside the tests does not fail the run. Database and config
 * files are still reported when they appear.
 */
const SERVER_RUNTIME_NAME =
	/^(?:server\.pid|server\.log.*|daemon\..*|models|.*\.lock|.*\.tmp|.*-(?:wal|shm|journal))$/;

/**
 * The real home from the passwd entry (independent of `HOME`, which the isolated
 * test runs override). `KS_REAL_HOME_GUARD_ROOT` points the guard at another
 * directory; only the guard's own tests use it, to prove the guard fires.
 */
export const REAL_HOME = process.env.KS_REAL_HOME_GUARD_ROOT || passwdHome();

/**
 * The account's home from the passwd entry. os.homedir() and os.userInfo() read HOME
 * when the process starts, so an isolated run (HOME set in the shell) would make the
 * guard watch the temp home instead of the real one. `~name` in sh resolves through
 * the passwd database and ignores HOME. A failure is an error, not a silent fallback.
 */
function passwdHome(): string {
	const result = spawnSync("/bin/sh", ["-c", 'eval "printf %s ~$(id -un)"'], {
		encoding: "utf8",
	});
	const home = result.stdout.trim();
	if (result.status !== 0 || !home.startsWith("/")) {
		throw new Error("real-home guard: could not resolve the account home");
	}
	return home;
}

/**
 * The real model cache, where the default data directory keeps downloaded models.
 * Resolved here, when this module loads, which is before the preload moves HOME, so
 * the real-model tests can still find a model that was fetched earlier.
 */
export const REAL_MODELS_DIR = join(
	REAL_HOME,
	".local",
	"share",
	"knowledge-server",
	"models",
);

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
		const names = readdirSync(abs)
			.filter((name) => !SERVER_RUNTIME_NAME.test(name))
			.sort()
			.join("\n");
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
