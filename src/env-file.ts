/**
 * The single `.env` parser for knowledge-server.
 *
 * Parsing is delegated to `dotenv`, which strips inline `# comments` from
 * unquoted values the same way the documented templates describe. The shell
 * launchers do not parse `.env`; they exec the binary, which loads it here.
 *
 * Search order (first existing file wins):
 *   1. $KNOWLEDGE_CONFIG_HOME/.env
 *   2. $XDG_CONFIG_HOME/knowledge-server/.env  (default: ~/.config/...)
 *   3. ~/.local/share/knowledge-server/.env    (legacy location)
 */

import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import * as dotenv from "dotenv";
import { defaultDataDir } from "./config-file.js";

/**
 * Keys a `.env` file may never set. These change how the process loads code or
 * runs its shell, so a `.env` (even one a user pasted in) must not control them.
 */
const DENIED_KEYS: Record<string, true> = {
	PATH: true,
	HOME: true,
	TMPDIR: true,
	IFS: true,
	SHELL: true,
	SHELLOPTS: true,
	BASHOPTS: true,
	BASH_ENV: true,
	ENV: true,
	PS4: true,
	PROMPT_COMMAND: true,
	CDPATH: true,
	GLOBIGNORE: true,
	NODE_OPTIONS: true,
	NODE_PATH: true,
	NODE_TLS_REJECT_UNAUTHORIZED: true,
	BUN_OPTIONS: true,
	// Trust store and client config: a pasted .env could otherwise redirect TLS or
	// log TLS session keys. (Proxy and CA variables are allowed, see below.)
	SSL_CERT_DIR: true,
	CURL_HOME: true,
	SSLKEYLOGFILE: true,
	KNOWLEDGE_CONFIG_HOME: true,
};
const DENIED_PREFIXES = ["LD_", "DYLD_", "BUN_INSTALL", "BUN_CONFIG_", "XDG_"];

/**
 * Keys checked case-insensitively. Proxy and CA variables (HTTPS_PROXY,
 * NODE_EXTRA_CA_CERTS, CURL_CA_BUNDLE, SSL_CERT_FILE, ...) are not listed: they
 * cannot run code, and the .env is owner-only config for installs behind a
 * corporate proxy or CA.
 */
function isDenied(key: string): boolean {
	const upper = key.toUpperCase();
	return (
		DENIED_KEYS[upper] === true ||
		DENIED_PREFIXES.some((p) => upper.startsWith(p))
	);
}

/** The preferred (XDG) `.env` location. Reported when no file exists yet. */
export function preferredEnvFile(
	env: NodeJS.ProcessEnv = process.env,
	home: string = homedir(),
): string {
	const xdgConfigHome = env.XDG_CONFIG_HOME || join(home, ".config");
	return join(xdgConfigHome, "knowledge-server", ".env");
}

/** Candidate `.env` paths in priority order, whether or not they exist. */
export function envFileCandidates(
	env: NodeJS.ProcessEnv = process.env,
	home: string = homedir(),
): string[] {
	const candidates: string[] = [];
	if (env.KNOWLEDGE_CONFIG_HOME) {
		candidates.push(join(env.KNOWLEDGE_CONFIG_HOME, ".env"));
	}
	candidates.push(preferredEnvFile(env, home));
	candidates.push(join(defaultDataDir(home), ".env"));
	return candidates;
}

/** First existing `.env` from {@link envFileCandidates}, or null. */
export function findEnvFile(
	env: NodeJS.ProcessEnv = process.env,
	home: string = homedir(),
): string | null {
	return envFileCandidates(env, home).find((p) => existsSync(p)) ?? null;
}

/** Parse `.env` text into its allowed keys and the names of dropped keys. */
function splitEnvText(text: string): {
	values: Record<string, string>;
	ignored: string[];
} {
	const values: Record<string, string> = {};
	const ignored: string[] = [];
	for (const [key, value] of Object.entries(dotenv.parse(text))) {
		if (isDenied(key)) ignored.push(key);
		else values[key] = value;
	}
	return { values, ignored };
}

/** Parse `.env` text. Denied process-control keys are dropped. */
export function parseEnvText(text: string): Record<string, string> {
	return splitEnvText(text).values;
}

export interface LoadEnvResult {
	/** Keys assigned from the file (keys already set in the environment are kept). */
	loaded: string[];
	/** Keys dropped because a `.env` may not set them. Names only, never values. */
	ignored: string[];
	/** Read error, if the file could not be read. */
	error?: string;
}

/**
 * Load a `.env` file into `env`. Explicit environment variables always win
 * (same as dotenv's `override: false`).
 */
export function loadEnvFile(
	path: string,
	env: NodeJS.ProcessEnv = process.env,
): LoadEnvResult {
	let text: string;
	try {
		text = readFileSync(path, "utf8");
	} catch (err) {
		return {
			loaded: [],
			ignored: [],
			error: err instanceof Error ? err.message : String(err),
		};
	}
	const { values, ignored } = splitEnvText(text);
	const loaded: string[] = [];
	for (const [key, value] of Object.entries(values)) {
		if (env[key] === undefined) {
			env[key] = value;
			loaded.push(key);
		}
	}
	return { loaded, ignored };
}
