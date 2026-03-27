/**
 * Binary-install .env loader.
 *
 * Must be imported as the very first import in each binary entry point
 * (src/index.ts, src/daemon/index.ts) so that process.env is populated
 * before the `config` object is constructed.
 *
 * For source installs (running under `bun run`), Bun automatically loads
 * `.env` from the project root — this module is a no-op in that case.
 *
 * For compiled binary installs, Bun's automatic .env loading does NOT apply.
 * This module finds `.env` using the same priority-ordered search as the
 * wrapper script and resolveEnvFilePath() in src/config.ts:
 *   1. $KNOWLEDGE_CONFIG_HOME/.env
 *   2. $XDG_CONFIG_HOME/knowledge-server/.env  (default: ~/.config/...)
 *   3. ~/.local/share/knowledge-server/.env    (legacy location)
 *
 * Parsing is delegated to `dotenv` (override: false — explicit env vars
 * always take precedence over .env values).
 *
 * Note: src/mcp/index.ts is not a standalone binary entry point — it is
 * always invoked as a subcommand through src/index.ts, which already imports
 * this module. No separate import is needed there.
 */

import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import * as dotenv from "dotenv";

/** Returns true when running as a compiled Bun binary (not `bun run`). */
function isBinaryInstall(): boolean {
	// Under `bun run`, process.execPath is the bun binary itself.
	// Under a compiled binary, process.execPath is the binary path.
	return !basename(process.execPath).startsWith("bun");
}

/** Resolve the .env file path using the same priority order as resolveEnvFilePath(). */
function findEnvFile(): string | null {
	const xdgConfigHome =
		process.env.XDG_CONFIG_HOME || join(homedir(), ".config");

	const candidates: string[] = [];

	if (process.env.KNOWLEDGE_CONFIG_HOME) {
		candidates.push(join(process.env.KNOWLEDGE_CONFIG_HOME, ".env"));
	}
	candidates.push(join(xdgConfigHome, "knowledge-server", ".env"));
	candidates.push(
		join(homedir(), ".local", "share", "knowledge-server", ".env"),
	);

	for (const candidate of candidates) {
		if (existsSync(candidate)) return candidate;
	}
	return null;
}

// ── Main: load .env for binary installs only ──────────────────────────────────

if (isBinaryInstall()) {
	const envFile = findEnvFile();
	if (envFile) {
		// override: false — explicit env vars (e.g. from launchd EnvironmentVariables)
		// always take precedence over values in .env.
		const result = dotenv.config({ path: envFile, override: false });
		if (result.error) {
			// Non-fatal — log a warning but let the process continue.
			// Missing vars will surface as clear errors from validateConfig()
			// or at connection time.
			process.stderr.write(
				`[env] Warning: failed to load .env from ${envFile}: ${result.error.message}\n`,
			);
		}
	}
	// If no .env found, proceed silently — validateConfig() will surface any
	// missing required vars with a clear error message.
}
