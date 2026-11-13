import { chmodSync, existsSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import {
	DEFAULT_CONFIG,
	DEFAULT_CONFIG_PATH,
	type KnowledgeServerConfig,
	loadConfigFile,
	resolveSqlitePath,
} from "./config-file.js";
import { config } from "./config.js";
import { DEFAULT_DAEMON_DB_PATH } from "./db/daemon/index.js";
import { DEFAULT_SERVER_STATE_PATH } from "./db/state/index.js";
import { logger } from "./logger.js";

export interface PrivatePathTargets {
	/** Files that hold secrets, logs or state: owner-only (0600). */
	files: string[];
	/** Directories we own: owner-only (0700). */
	dirs: string[];
}

const SQLITE_SIDECARS = ["", "-wal", "-shm", "-journal"];

/**
 * Paths this install owns that hold credentials, logs or state. Files created
 * before 0600 was enforced (upgraded installs) are tightened from these at startup.
 * Missing paths are harmless; a config that fails to load falls back to defaults.
 */
export function ownedPrivatePaths(
	configPath = DEFAULT_CONFIG_PATH,
): PrivatePathTargets {
	let file: KnowledgeServerConfig = DEFAULT_CONFIG;
	try {
		file = loadConfigFile(configPath) ?? DEFAULT_CONFIG;
	} catch {
		// Invalid config: the server reports that itself; still tighten known paths.
	}

	const sqlitePaths = [
		...file.stores
			.filter((s) => s.kind === "sqlite")
			.map((s) => resolveSqlitePath(s)),
		...(file.stateDb.kind === "sqlite"
			? [file.stateDb.path ?? DEFAULT_SERVER_STATE_PATH]
			: []),
		DEFAULT_DAEMON_DB_PATH,
	];

	const configDir = dirname(configPath);
	// Legacy .env location (pre-config-dir installs) lives in the data dir.
	const dataDir = dirname(DEFAULT_SERVER_STATE_PATH);
	const envFiles = [
		join(configDir, ".env"),
		join(dataDir, ".env"),
		...(process.env.KNOWLEDGE_CONFIG_HOME
			? [join(process.env.KNOWLEDGE_CONFIG_HOME, ".env")]
			: []),
	];

	const files = [
		config.logPath,
		config.pidPath,
		join(dataDir, "daemon.log"),
		...envFiles,
		...sqlitePaths.flatMap((p) => SQLITE_SIDECARS.map((s) => `${p}${s}`)),
	];

	return {
		files: [...new Set(files.filter((p): p is string => Boolean(p)))],
		dirs: [configDir],
	};
}

/**
 * Best-effort: set each existing path to owner-only when it is readable or
 * writable by group/other. Never throws; a failed chmod is logged and skipped.
 * `chmod` is injectable for tests.
 */
export function tightenPrivatePaths(
	targets: PrivatePathTargets,
	chmod: (path: string, mode: number) => void = chmodSync,
): void {
	for (const path of targets.files) tighten(path, 0o600, chmod);
	for (const path of targets.dirs) tighten(path, 0o700, chmod);
}

function tighten(
	path: string,
	mode: number,
	chmod: (path: string, mode: number) => void,
): void {
	try {
		if (!existsSync(path)) return;
		const current = statSync(path).mode & 0o777;
		if ((current & ~mode) === 0) return;
		chmod(path, mode);
	} catch (err) {
		logger.warn(
			`[startup] Could not tighten permissions on ${path}: ${err instanceof Error ? err.message : String(err)}`,
		);
	}
}

/** Tighten every path this install owns. Used at server and daemon startup. */
export function tightenOwnedPrivatePaths(): void {
	tightenPrivatePaths(ownedPrivatePaths());
}
