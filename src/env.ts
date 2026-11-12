/**
 * `.env` loader for knowledge-server processes.
 *
 * Must be imported as the very first import in each entry point
 * (src/index.ts, src/daemon/index.ts) so that process.env is populated
 * before the `config` object is constructed.
 *
 * Loads the file from {@link findEnvFile} (see env-file.ts for the search
 * order) when running as a compiled binary, or when KNOWLEDGE_LOAD_ENV=1 is
 * set (source installs: scripts/start.sh sets it). Under plain `bun run`/tests
 * Bun's own project `.env` loading applies and nothing else is read.
 *
 * Explicit environment variables always win over `.env` values.
 */

import { basename } from "node:path";
import { findEnvFile, loadEnvFile } from "./env-file.js";

/** Returns true when running as a compiled Bun binary (not `bun run`). */
function isBinaryInstall(): boolean {
	// Under `bun run`, process.execPath is the bun binary itself.
	// Under a compiled binary, process.execPath is the binary path.
	return !basename(process.execPath).startsWith("bun");
}

if (isBinaryInstall() || process.env.KNOWLEDGE_LOAD_ENV === "1") {
	const envFile = findEnvFile();
	if (envFile) {
		const result = loadEnvFile(envFile);
		if (result.error) {
			// Non-fatal — missing vars surface as clear errors from validateConfig().
			process.stderr.write(
				`[env] Warning: failed to load .env from ${envFile}: ${result.error}\n`,
			);
		}
	}
}
