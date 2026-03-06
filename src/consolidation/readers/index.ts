import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { config } from "../../config.js";
import { logger } from "../../logger.js";
import type { IEpisodeReader } from "../../types.js";
import { ClaudeCodeEpisodeReader } from "./claude-code.js";
import { CodexEpisodeReader, resolveCodexSessionsDir } from "./codex.js";
import { CursorEpisodeReader, resolveCursorDbPath } from "./cursor.js";
import { OpenCodeEpisodeReader } from "./opencode.js";
import { VSCodeEpisodeReader, resolveVSCodeDataDir } from "./vscode.js";

export { OpenCodeEpisodeReader } from "./opencode.js";
export { ClaudeCodeEpisodeReader } from "./claude-code.js";
export { CursorEpisodeReader } from "./cursor.js";
export { CodexEpisodeReader } from "./codex.js";
export { VSCodeEpisodeReader } from "./vscode.js";

/**
 * Probe list of candidate OpenCode DB paths to check when OPENCODE_DB_PATH is not set.
 * Ordered by likelihood — the standard XDG path first, then legacy macOS locations.
 */
const OPENCODE_DB_PROBE_PATHS = [
	// Primary: XDG data dir (used by the current opencode release on all platforms)
	config.opencodeDbPath, // already resolves ~/.local/share/opencode/opencode.db
];

/**
 * Try to resolve the OpenCode DB path.
 *
 * Resolution order:
 * 1. OPENCODE_DB_PATH env var (already baked into config.opencodeDbPath)
 * 2. `opencode db path` CLI command output
 * 3. Probe list of well-known default locations
 *
 * Returns null if no path can be found or the file doesn't exist.
 */
function resolveOpenCodeDbPath(): string | null {
	// If explicitly set via env var, trust it (validateConfig already checked existence).
	if (process.env.OPENCODE_DB_PATH) {
		return existsSync(config.opencodeDbPath) ? config.opencodeDbPath : null;
	}

	// Try the CLI — most reliable since the binary knows its own path.
	try {
		const cliPath = execSync("opencode db path", {
			encoding: "utf8",
			timeout: 3000,
			stdio: ["ignore", "pipe", "ignore"],
		}).trim();
		if (cliPath && existsSync(cliPath)) {
			return cliPath;
		}
	} catch {
		// CLI not on PATH or returned non-zero — fall through to probe list
	}

	// Probe well-known paths
	for (const candidate of OPENCODE_DB_PROBE_PATHS) {
		if (candidate && existsSync(candidate)) {
			return candidate;
		}
	}

	return null;
}

/**
 * Try to resolve the Claude Code config directory.
 *
 * Resolution order:
 * 1. CLAUDE_DB_PATH env var (takes precedence over everything)
 * 2. CLAUDE_CONFIG_DIR env var (what Claude Code itself uses)
 * 3. ~/.claude (default location)
 *
 * Returns null if the directory doesn't exist.
 */
function resolveClaudeDbPath(): string | null {
	const candidate = config.claudeDbPath; // already merged from env vars in config.ts
	return existsSync(candidate) ? candidate : null;
}

/**
 * Create the list of active episode readers based on config and auto-detection.
 *
 * Each source is independently enabled/disabled:
 * - If a source is disabled via config (OPENCODE_ENABLED=false / CLAUDE_ENABLED=false),
 *   it is skipped silently.
 * - If a source is enabled but its path cannot be found, a warning is logged and
 *   the source is skipped (non-fatal — allows running with only one source active).
 *
 * The returned array determines which sources the ConsolidationEngine will process.
 * Order matters for log output but not for correctness — sources are independent.
 */
export function createEpisodeReaders(): IEpisodeReader[] {
	const readers: IEpisodeReader[] = [];

	// ── OpenCode ──
	if (config.opencodeEnabled) {
		const dbPath = resolveOpenCodeDbPath();
		if (dbPath) {
			readers.push(new OpenCodeEpisodeReader(dbPath));
			logger.log(`[sources] OpenCode: ${dbPath}`);
		} else {
			logger.warn(
				"[sources] OpenCode source enabled but DB not found. " +
					"Set OPENCODE_DB_PATH or disable with OPENCODE_ENABLED=false.",
			);
		}
	} else {
		logger.log("[sources] OpenCode: disabled (OPENCODE_ENABLED=false)");
	}

	// ── Claude Code ──
	if (config.claudeEnabled) {
		const claudeDir = resolveClaudeDbPath();
		if (claudeDir) {
			readers.push(new ClaudeCodeEpisodeReader(claudeDir));
			logger.log(`[sources] Claude Code: ${claudeDir}`);
		} else {
			logger.warn(
				`[sources] Claude Code source enabled but directory not found at ${config.claudeDbPath}. Set CLAUDE_DB_PATH or disable with CLAUDE_ENABLED=false.`,
			);
		}
	} else {
		logger.log("[sources] Claude Code: disabled (CLAUDE_ENABLED=false)");
	}

	// ── Codex CLI ──
	if (config.codexEnabled) {
		const sessionsDir = resolveCodexSessionsDir();
		if (existsSync(sessionsDir)) {
			readers.push(new CodexEpisodeReader(sessionsDir));
			logger.log(`[sources] Codex: ${sessionsDir}`);
		} else {
			const hint = config.codexSessionsDir
				? `Expected at ${config.codexSessionsDir}.`
				: "Auto-detection found no Codex sessions directory on this platform.";
			logger.warn(
				`[sources] Codex source enabled but sessions directory not found. ${hint} Set CODEX_SESSIONS_DIR or disable with CODEX_ENABLED=false.`,
			);
		}
	} else {
		logger.log("[sources] Codex: disabled (CODEX_ENABLED=false)");
	}

	// ── Cursor ──
	if (config.cursorEnabled) {
		const cursorDbPath = resolveCursorDbPath();
		if (cursorDbPath) {
			readers.push(new CursorEpisodeReader(cursorDbPath));
			logger.log(`[sources] Cursor: ${cursorDbPath}`);
		} else {
			const hint = config.cursorDbPath
				? `Expected at ${config.cursorDbPath}.`
				: "Auto-detection found no Cursor installation on this platform.";
			logger.warn(
				`[sources] Cursor source enabled but database not found. ${hint} Set CURSOR_DB_PATH or disable with CURSOR_ENABLED=false.`,
			);
		}
	} else {
		logger.log("[sources] Cursor: disabled (CURSOR_ENABLED=false)");
	}

	// ── VSCode ──
	if (config.vscodeEnabled) {
		const vscodeDataDir = resolveVSCodeDataDir();
		if (vscodeDataDir) {
			readers.push(new VSCodeEpisodeReader(vscodeDataDir));
			logger.log(`[sources] VSCode: ${vscodeDataDir}`);
		} else {
			const hint = config.vscodeDataDir
				? `Expected at ${config.vscodeDataDir}.`
				: "Auto-detection found no VSCode installation on this platform.";
			logger.warn(
				`[sources] VSCode source enabled but data directory not found. ${hint} Set VSCODE_DATA_DIR or disable with VSCODE_ENABLED=false.`,
			);
		}
	} else {
		logger.log("[sources] VSCode: disabled (VSCODE_ENABLED=false)");
	}

	return readers;
}
