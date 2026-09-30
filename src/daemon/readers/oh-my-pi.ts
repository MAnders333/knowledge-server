import { readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { config } from "../../config.js";
import { PiEpisodeReader } from "./pi.js";

/**
 * oh-my-pi (the `omp` harness, package `@oh-my-pi/pi-coding-agent`) is a
 * pi-family fork and writes the same session JSONL format (v3), so the parser
 * is pi's — this class supplies only oh-my-pi's directory layout and the
 * distinct source id (idempotency keys must stay separate from "pi").
 *
 * Session locations under `config.ompSessionsRoot` (default `~/.omp`):
 *
 *   <root>/agent/sessions/<projectDir>/<timestamp>_<uuid>.jsonl
 *   <root>/profiles/<mode>/agent/sessions/<projectDir>/<timestamp>_<uuid>.jsonl
 *
 * The shared `agent` layer covers pre-profile installs; each mode keeps its own
 * layer under `profiles/<mode>/agent`.
 */
export class OhMyPiEpisodeReader extends PiEpisodeReader {
	constructor(sessionsDirs?: string[]) {
		super(sessionsDirs ?? resolveOhMyPiSessionsDirs(), "oh-my-pi");
	}
}

/**
 * Resolve oh-my-pi session directories: `<root>/agent/sessions` plus every
 * `<root>/profiles/<mode>/agent/sessions`. Missing directories are skipped —
 * oh-my-pi may simply not be installed on this machine (mirrors the pi
 * reader's soft-skip behavior).
 *
 * @param root Override for `config.ompSessionsRoot` (tests).
 */
export function resolveOhMyPiSessionsDirs(root?: string): string[] {
	const ompRoot = root ?? config.ompSessionsRoot;
	const dirs: string[] = [];

	const pushIfDirectory = (candidate: string): void => {
		try {
			if (statSync(candidate).isDirectory()) dirs.push(candidate);
		} catch {
			// not there — fine
		}
	};

	pushIfDirectory(join(ompRoot, "agent", "sessions"));

	let profileEntries: string[];
	try {
		profileEntries = readdirSync(join(ompRoot, "profiles"), {
			withFileTypes: true,
		})
			.filter((entry) => entry.isDirectory())
			.map((entry) => entry.name);
	} catch {
		profileEntries = []; // no profiles dir — shared layer may still exist
	}

	for (const mode of profileEntries) {
		pushIfDirectory(join(ompRoot, "profiles", mode, "agent", "sessions"));
	}

	return dirs;
}
