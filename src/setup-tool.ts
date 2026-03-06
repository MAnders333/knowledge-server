import { spawnSync } from "node:child_process";
import {
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	symlinkSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { config } from "./config.js";

/**
 * `knowledge-server setup-tool <opencode|claude-code|cursor|codex>`
 *
 * Idempotent tool-specific wiring:
 *
 * opencode:
 *   - Symlink plugin/knowledge.ts → ~/.config/opencode/plugins/knowledge.ts
 *   - Symlink commands/*.md → ~/.config/opencode/command/*.md
 *   - Merge mcp.knowledge entry into ~/.config/opencode/opencode.jsonc
 *
 * claude-code:
 *   - Register MCP server via `claude mcp add-json` (→ ~/.claude.json)
 *   - Merge UserPromptSubmit hook into ~/.claude/settings.json
 *   - Symlink commands/*.md → ~/.claude/commands/*.md
 *
 * cursor / codex:
 *   - Register MCP server in ~/.cursor/mcp.json / ~/.codex/config.toml
 *
 * There are two installation modes:
 *
 *   Source install  — cloned repo, run via `bun run src/index.ts`.
 *                     Bun.main ends in `.ts`. MCP command → `bun run src/index.ts mcp`.
 *
 *   Binary install  — compiled binary on PATH (`knowledge-server`).
 *                     MCP command → `knowledge-server mcp`
 *                     (the `mcp` subcommand is built into the main binary).
 */

/** True when running from source (`bun run src/index.ts`), false when compiled binary. */
function isSourceInstall(): boolean {
	const mainFile = typeof Bun !== "undefined" ? Bun.main : process.argv[1];
	return mainFile.endsWith(".ts");
}

function getProjectDir(): string {
	// dirname(src/index.ts) = src/ → resolve(..) = project root.
	// Only meaningful for source installs; binary installs use the companion binary directly.
	const mainFile = typeof Bun !== "undefined" ? Bun.main : process.argv[1];
	return resolve(dirname(mainFile), "..");
}

// ── OpenCode MCP config ────────────────────────────────────────────────────────

/**
 * Build the MCP server entry for opencode.jsonc.
 *
 * OpenCode's `opencode mcp add` is interactive-only (no JSON flag), so we write
 * directly to opencode.jsonc. The file is JSONC — we strip line and block
 * comments before parsing, merge in the `mcp.knowledge` key, and re-serialize.
 * Comment loss in the rewritten file is acceptable: the MCP block is machine-
 * managed and users rarely comment individual server entries.
 *
 * Source install: command is `[bunBin, "run", "<projectDir>/src/index.ts", "mcp"]`
 * Binary install: command is `["<path>/knowledge-server", "mcp"]`
 *   — binary install can't know INSTALL_DIR at runtime, so we search PATH for
 *   `knowledge-server` and use that absolute path.
 */
function makeOpenCodeMcpEntry(): { command: string[] } & Record<
	string,
	unknown
> {
	const env = {
		// Thin HTTP proxy — only needs to locate the knowledge HTTP server.
		KNOWLEDGE_HOST: config.host,
		KNOWLEDGE_PORT: String(config.port),
	};

	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homedir(), ".bun", "bin", "bun");
		return {
			type: "local",
			command: [bunBin, "run", join(projectDir, "src", "index.ts"), "mcp"],
			enabled: true,
			environment: env,
		};
	}

	// Binary install: find knowledge-server on PATH; append "mcp" subcommand.
	const whichResult = spawnSync("which", ["knowledge-server"], {
		encoding: "utf8",
	});
	const mainBin =
		whichResult.status === 0
			? whichResult.stdout.trim()
			: "knowledge-server"; // fallback: hope it's on PATH at runtime
	return {
		type: "local",
		command: [mainBin, "mcp"],
		enabled: true,
		environment: env,
	};
}

/**
 * Strip single-line (`//`) and block comments from a JSONC string.
 *
 * Uses a character-by-character state machine so that `//` or block-comment
 * delimiters inside string values are never mistaken for comments. Handles:
 *   - Escaped characters inside strings (`\"`, `\\`)
 *   - Block comments that span multiple lines
 *   - Adjacent or nested-looking comment sequences
 */
export function stripJsoncComments(jsonc: string): string {
	let result = "";
	let i = 0;
	let inString = false;

	while (i < jsonc.length) {
		const ch = jsonc[i];

		if (inString) {
			if (ch === "\\") {
				// Escaped character — copy both chars verbatim and skip ahead.
				result += ch + (jsonc[i + 1] ?? "");
				i += 2;
				continue;
			}
			if (ch === '"') {
				inString = false;
			}
			result += ch;
			i++;
			continue;
		}

		// Outside a string — check for comment delimiters.
		if (ch === '"') {
			inString = true;
			result += ch;
			i++;
			continue;
		}

		if (ch === "/" && jsonc[i + 1] === "/") {
			// Line comment — skip to end of line.
			while (i < jsonc.length && jsonc[i] !== "\n") i++;
			continue;
		}

		if (ch === "/" && jsonc[i + 1] === "*") {
			// Block comment — skip to closing delimiter.
			i += 2;
			while (i < jsonc.length) {
				if (jsonc[i] === "*" && jsonc[i + 1] === "/") {
					i += 2;
					break;
				}
				i++;
			}
			continue;
		}

		result += ch;
		i++;
	}

	return result;
}

/**
 * Merge `mcp.knowledge` into `~/.config/opencode/opencode.jsonc`.
 *
 * Idempotent: if the key already exists with the same command, prints "no
 * change". If it exists with a different command (e.g. stale path after a
 * reinstall), replaces it and logs the update.
 *
 * Creates a minimal opencode.jsonc with only the mcp.knowledge block if the
 * file does not yet exist.
 */
function writeOpenCodeMcpEntry(opencodeConfigDir: string): void {
	const configPath = join(opencodeConfigDir, "opencode.jsonc");
	const entry = makeOpenCodeMcpEntry();

	let parsed: Record<string, unknown> = {};

	if (existsSync(configPath)) {
		try {
			const raw = readFileSync(configPath, "utf8");
			parsed = JSON.parse(stripJsoncComments(raw)) as Record<string, unknown>;
		} catch (e) {
			console.error(`  ✗ Failed to parse ${configPath}: ${e}`);
			console.error(
				"    Fix the syntax error and retry, or add the MCP entry manually.",
			);
			console.error(
				`    Entry to add: "knowledge": ${JSON.stringify(entry, null, 6)}`,
			);
			return; // non-fatal: rest of setup continues
		}
	}

	const mcp = (parsed.mcp ?? {}) as Record<string, unknown>;
	const existing = mcp.knowledge as
		| ({ command: string[] } & Record<string, unknown>)
		| undefined;

	if (existing) {
		const existingCmd = JSON.stringify(existing.command);
		const newCmd = JSON.stringify(entry.command);
		if (existingCmd === newCmd) {
			console.log(
				"  ✓ MCP server 'knowledge' already in opencode.jsonc (no change)",
			);
			return;
		}
		// Command changed (e.g. project moved or reinstalled) — update in place.
		console.log(
			"  ✓ MCP server 'knowledge' updated in opencode.jsonc (command changed)",
		);
	} else {
		console.log("  ✓ MCP server 'knowledge' added to opencode.jsonc");
	}

	mcp.knowledge = entry;
	parsed.mcp = mcp;

	mkdirSync(opencodeConfigDir, { recursive: true });
	const tmpPath = `${configPath}.tmp`;
	try {
		writeFileSync(tmpPath, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
		renameSync(tmpPath, configPath);
	} catch (e) {
		// Clean up the temp file so it doesn't litter the config directory.
		try {
			unlinkSync(tmpPath);
		} catch {
			// Ignore — tmp file may not have been created yet.
		}
		throw e;
	}
}

// ── OpenCode setup ─────────────────────────────────────────────────────────────

function setupOpenCode(): void {
	const projectDir = getProjectDir();
	const configDir = join(homedir(), ".config", "opencode");

	console.log("Setting up OpenCode integration...\n");

	// Plugin symlink
	const pluginDir = join(configDir, "plugins");
	mkdirSync(pluginDir, { recursive: true });
	const pluginSrc = join(projectDir, "plugin", "knowledge.ts");
	const pluginDst = join(pluginDir, "knowledge.ts");

	if (!existsSync(pluginSrc)) {
		console.error(`  ✗ Plugin source not found: ${pluginSrc}`);
		console.error(
			"    Make sure you are running from the knowledge-server project directory.",
		);
		process.exit(1);
	}

	forceSymlink(pluginSrc, pluginDst);
	console.log(`  ✓ Plugin: ${pluginDst}`);
	console.log(`       → ${pluginSrc}`);

	// Command symlinks
	const commandSrcDir = join(projectDir, "commands");
	const commandDstDir = join(configDir, "command");
	mkdirSync(commandDstDir, { recursive: true });

	const commandFiles = ["consolidate.md", "knowledge-review.md"];
	for (const file of commandFiles) {
		const src = join(commandSrcDir, file);
		const dst = join(commandDstDir, file);
		if (!existsSync(src)) {
			console.log(`  ⚠ Command source not found (skipping): ${src}`);
			continue;
		}
		forceSymlink(src, dst);
		console.log(`  ✓ Command ${file}: ${dst}`);
	}

	// MCP server entry — written directly to opencode.jsonc
	writeOpenCodeMcpEntry(configDir);

	const startHint = isSourceInstall()
		? `bun run ${join(projectDir, "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using OpenCode:
  ${startHint}

Setup complete!`);
}

// ── Claude Code setup ──────────────────────────────────────────────────────────

/**
 * Build the MCP server entry for Claude Code.
 *
 * Source install: `bun run <projectDir>/src/index.ts mcp`
 *   - Always reflects current source; no stale compiled binary risk.
 *   - Bun executable resolved from the running process or ~/.bun/bin/bun.
 *
 * Binary install: `knowledge-server mcp`
 *   - The `mcp` subcommand is built into the main binary.
 */
function makeClaudeMcpEntry() {
	const env = {
		// The MCP subcommand is a thin HTTP proxy — it only needs to locate the
		// knowledge HTTP server. No LLM credentials required here.
		KNOWLEDGE_HOST: config.host,
		KNOWLEDGE_PORT: String(config.port),
	};
	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		// In source mode, process.execPath is the bun binary itself.
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homedir(), ".bun", "bin", "bun");
		return {
			type: "stdio",
			command: bunBin,
			args: ["run", join(projectDir, "src", "index.ts"), "mcp"],
			env,
		};
	}
	// Binary install: `knowledge-server mcp` — mcp is a built-in subcommand.
	const whichResult = spawnSync("which", ["knowledge-server"], { encoding: "utf8" });
	const mainBin = whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
	return {
		type: "stdio",
		command: mainBin,
		args: ["mcp"],
		env,
	};
}

/**
 * The UserPromptSubmit hook entry for Claude Code.
 * Points to the local knowledge server's hook endpoint.
 *
 * The URL is derived from KNOWLEDGE_HOST / KNOWLEDGE_PORT env vars (same as the
 * server uses), so running `setup-tool claude-code` with the same env as the
 * server will always write the correct URL into settings.json.
 *
 * Claude Code hook format: each entry in the UserPromptSubmit array is a
 * matcher group: { matcher?: string, hooks: [...] }. UserPromptSubmit does not
 * support matchers (fires on every prompt), so matcher is omitted.
 */
function claudeHookUrl(): string {
	return `http://${config.host}:${config.port}/hooks/claude-code/user-prompt`;
}

function makeClaudeHookEntry() {
	return {
		hooks: [
			{
				type: "http",
				url: claudeHookUrl(),
				timeout: 5,
			},
		],
	};
}

function setupClaudeCode(): void {
	const claudeDir =
		process.env.CLAUDE_DB_PATH ??
		process.env.CLAUDE_CONFIG_DIR ??
		join(homedir(), ".claude");

	const settingsPath = join(claudeDir, "settings.json");

	console.log("Setting up Claude Code integration...\n");

	// ── MCP server — registered via `claude mcp add-json` (user scope → ~/.claude.json) ──
	// Claude Code does NOT read mcpServers from settings.json (that's Claude Desktop format).
	// We use add-json to safely embed env var values (which may contain +, =, etc.)
	// without shell quoting issues.
	const mcpEntry = makeClaudeMcpEntry();
	const mcpJson = JSON.stringify(mcpEntry);

	// Check if already registered — `claude mcp get knowledge` exits 0 if found.
	const getResult = spawnSync("claude", ["mcp", "get", "knowledge"], {
		encoding: "utf8",
	});
	if (getResult.status === 0) {
		console.log("  ✓ MCP server 'knowledge' already registered (no change)");
		console.log(
			"    To update: run `claude mcp remove knowledge -s user` then re-run this command",
		);
	} else {
		// Not registered yet — add via add-json with --scope user.
		// spawnSync avoids shell quoting/injection issues with JSON values.
		const addResult = spawnSync(
			"claude",
			["mcp", "add-json", "knowledge", mcpJson, "--scope", "user"],
			{ encoding: "utf8" },
		);
		if (addResult.status === 0) {
			console.log(
				"  ✓ MCP server 'knowledge' registered (claude mcp add-json --scope user)",
			);
		} else {
			console.error("  ✗ Failed to register MCP server:", addResult.stderr);
			console.error("    Make sure `claude` is on your PATH, then retry.");
		}
	}

	// ── UserPromptSubmit hook — written to ~/.claude/settings.json ──
	// Hooks live in settings.json (not ~/.claude.json).
	let settings: Record<string, unknown> = {};
	if (existsSync(settingsPath)) {
		try {
			settings = JSON.parse(readFileSync(settingsPath, "utf8")) as Record<
				string,
				unknown
			>;
		} catch (e) {
			console.error(`  ✗ Failed to parse ${settingsPath}: ${e}`);
			console.error("    Fix the JSON syntax error and retry.");
			process.exit(1);
		}
	} else {
		mkdirSync(claudeDir, { recursive: true });
	}

	const hooks = (settings.hooks ?? {}) as Record<string, unknown>;
	const existingHooks = (hooks.UserPromptSubmit ?? []) as unknown[];

	const alreadyHasHook = existingHooks.some((h) => {
		if (typeof h !== "object" || h === null) return false;
		const entry = h as Record<string, unknown>;
		const innerHooks = entry.hooks;
		if (Array.isArray(innerHooks)) {
			return innerHooks.some(
				(ih) =>
					typeof ih === "object" &&
					ih !== null &&
					(ih as Record<string, unknown>).url === claudeHookUrl(),
			);
		}
		// Old format (pre-matcher): { type, url, ... }
		return entry.url === claudeHookUrl();
	});

	if (alreadyHasHook) {
		console.log("  ✓ UserPromptSubmit hook already configured (no change)");
	} else {
		hooks.UserPromptSubmit = [...existingHooks, makeClaudeHookEntry()];
		console.log(`  ✓ UserPromptSubmit hook added (${claudeHookUrl()})`);
	}
	settings.hooks = hooks;

	// Write atomically via a temp file + rename so a crash mid-write never
	// leaves settings.json in a truncated/unparseable state.
	const tmpPath = `${settingsPath}.tmp`;
	try {
		writeFileSync(tmpPath, `${JSON.stringify(settings, null, 2)}\n`, "utf8");
		renameSync(tmpPath, settingsPath);
	} catch (e) {
		try {
			unlinkSync(tmpPath);
		} catch {
			// Ignore — tmp file may not have been created yet.
		}
		throw e;
	}
	console.log(`  ✓ Wrote ${settingsPath}`);

	// ── Slash command symlinks — ~/.claude/commands/*.md ──
	const commandDstDir = join(claudeDir, "commands");
	mkdirSync(commandDstDir, { recursive: true });

	const commandSrcDir = isSourceInstall()
		? join(getProjectDir(), "commands")
		: ""; // binary install: no source tree available

	const commandFiles = ["consolidate.md", "knowledge-review.md"];
	if (commandSrcDir) {
		for (const file of commandFiles) {
			const src = join(commandSrcDir, file);
			const dst = join(commandDstDir, file);
			if (!existsSync(src)) {
				console.log(`  ⚠ Command source not found (skipping): ${src}`);
				continue;
			}
			forceSymlink(src, dst);
			console.log(`  ✓ Command ${file}: ${dst}`);
		}
	} else {
		// Binary install: commands/ is not bundled; skip silently.
		// Users can symlink manually or run `setup-tool claude-code` from source.
		console.log(
			"  ℹ Slash commands not installed (binary install — no source tree).",
		);
		console.log(
			"    To install slash commands, clone the repo and run setup-tool from source.",
		);
	}

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using Claude Code:
  ${startHint}

Setup complete!`);
}

// ── Cursor setup ───────────────────────────────────────────────────────────────

/**
 * Build the MCP server entry for Cursor's ~/.cursor/mcp.json.
 *
 * Cursor uses the standard `mcpServers` JSON format (same as Claude Desktop).
 * Each entry: { command, args?, env? }
 */
function makeCursorMcpEntry() {
	const env = {
		KNOWLEDGE_HOST: config.host,
		KNOWLEDGE_PORT: String(config.port),
	};

	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homedir(), ".bun", "bin", "bun");
		return {
			command: bunBin,
			args: ["run", join(projectDir, "src", "index.ts"), "mcp"],
			env,
		};
	}
	const whichResult = spawnSync("which", ["knowledge-server"], { encoding: "utf8" });
	const mainBin = whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
	return { command: mainBin, args: ["mcp"], env };
}

export function setupCursor(): void {
	const cursorDir = process.env.CURSOR_HOME ?? join(homedir(), ".cursor");
	const mcpPath = join(cursorDir, "mcp.json");

	console.log("Setting up Cursor integration...\n");

	// Read or create ~/.cursor/mcp.json
	let mcpConfig: { mcpServers: Record<string, unknown> } = { mcpServers: {} };
	if (existsSync(mcpPath)) {
		try {
			mcpConfig = JSON.parse(readFileSync(mcpPath, "utf8")) as {
				mcpServers: Record<string, unknown>;
			};
			if (!mcpConfig.mcpServers || typeof mcpConfig.mcpServers !== "object") {
				mcpConfig.mcpServers = {};
			}
		} catch (e) {
			console.error(`  ✗ Failed to parse ${mcpPath}: ${e}`);
			console.error("    Fix the JSON syntax error and retry.");
			process.exit(1);
		}
	} else {
		mkdirSync(cursorDir, { recursive: true });
	}

	const entry = makeCursorMcpEntry();
	const existing = mcpConfig.mcpServers.knowledge as
		| { command: string; args?: string[] }
		| undefined;

	let needsWrite = true;
	if (existing) {
		const existingCmd = JSON.stringify([existing.command, ...(existing.args ?? [])]);
		const newCmd = JSON.stringify([entry.command, ...(entry.args ?? [])]);
		if (existingCmd === newCmd) {
			console.log("  ✓ MCP server 'knowledge' already in ~/.cursor/mcp.json (no change)");
			console.log("    To update: remove the 'knowledge' entry from ~/.cursor/mcp.json and re-run this command.");
			needsWrite = false;
		} else {
			mcpConfig.mcpServers.knowledge = entry;
			console.log("  ✓ MCP server 'knowledge' updated in ~/.cursor/mcp.json (command changed)");
		}
	} else {
		mcpConfig.mcpServers.knowledge = entry;
		console.log("  ✓ MCP server 'knowledge' added to ~/.cursor/mcp.json");
	}

	if (needsWrite) {
		const tmpPath = `${mcpPath}.tmp`;
		try {
			writeFileSync(tmpPath, `${JSON.stringify(mcpConfig, null, 2)}\n`, "utf8");
			renameSync(tmpPath, mcpPath);
		} catch (e) {
			try { unlinkSync(tmpPath); } catch { /* ignore */ }
			throw e;
		}
		console.log(`  ✓ Wrote ${mcpPath}`);
	}

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using Cursor:
  ${startHint}

Setup complete!`);
}

// ── Codex setup ─────────────────────────────────────────────────────────────────

/**
 * Merge the `[mcp_servers.knowledge]` entry into ~/.codex/config.toml.
 *
 * Codex uses TOML. We do not parse/rewrite the whole file to avoid
 * disturbing existing content and comments. Instead we:
 *   1. Check if `[mcp_servers.knowledge]` is already present (string search).
 *   2. If not, append the new block at the end of the file.
 *   3. If present with a different command, warn and skip — TOML merging is
 *      error-prone and a manual update is safer for an existing entry.
 *
 * This is deliberately simpler than the opencode.jsonc approach because
 * config.toml typically has user-authored content we don't want to disturb.
 */
export function setupCodex(): void {
	const codexHome = process.env.CODEX_HOME ?? join(homedir(), ".codex");
	const configPath = join(codexHome, "config.toml");

	console.log("Setting up Codex CLI integration...\n");

	mkdirSync(codexHome, { recursive: true });

	// Build the MCP command string for TOML.
	let commandToml: string;
	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homedir(), ".bun", "bin", "bun");
		// TOML array: ["bun", "run", "/path/to/src/index.ts", "mcp"]
		const args = ["run", join(projectDir, "src", "index.ts"), "mcp"];
		const argsToml = args.map((a) => `"${a.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`).join(", ");
		commandToml = `command = "${bunBin.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"\nargs = [${argsToml}]`;
	} else {
		const whichResult = spawnSync("which", ["knowledge-server"], { encoding: "utf8" });
		const mainBin = whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
		commandToml = `command = "${mainBin.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"\nargs = ["mcp"]`;
	}

	const block = `
[mcp_servers.knowledge]
${commandToml}
env = { KNOWLEDGE_HOST = "${config.host}", KNOWLEDGE_PORT = "${config.port}" }
`;

	// Check if the section already exists (simple string match on the header).
	let existing = "";
	if (existsSync(configPath)) {
		try {
			existing = readFileSync(configPath, "utf8");
		} catch (e) {
			console.error(`  ✗ Failed to read ${configPath}: ${e}`);
			process.exit(1);
		}
	}

	if (existing.includes("[mcp_servers.knowledge]")) {
		console.log("  ✓ MCP server 'knowledge' already in ~/.codex/config.toml (no change)");
		console.log("    To update: remove the [mcp_servers.knowledge] block and re-run this command.");
	} else {
		const updated = `${existing.trimEnd()}\n${block}`;
		const tmpPath = `${configPath}.tmp`;
		try {
			writeFileSync(tmpPath, updated, "utf8");
			renameSync(tmpPath, configPath);
		} catch (e) {
			try { unlinkSync(tmpPath); } catch { /* ignore */ }
			throw e;
		}
		console.log("  ✓ MCP server 'knowledge' added to ~/.codex/config.toml");
		console.log(`  ✓ Wrote ${configPath}`);
	}

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using Codex:
  ${startHint}

Setup complete!`);
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/**
 * Create (or replace) a symlink at `dst` pointing to `src`.
 * Removes an existing symlink or file at `dst` before creating the new one.
 */
function forceSymlink(src: string, dst: string): void {
	if (existsSync(dst)) {
		unlinkSync(dst);
	}
	symlinkSync(src, dst);
}

// ── Entry point ────────────────────────────────────────────────────────────────

export function runSetupTool(args: string[]): void {
	const tool = args[0];

	if (!tool || tool === "--help" || tool === "-h") {
		console.log(`Usage: knowledge-server setup-tool <tool>

Available tools:
  opencode      Symlink plugin + commands; register MCP server in opencode.jsonc
  claude-code   Register MCP server + hook; symlink commands into ~/.claude/commands/
  cursor        Register MCP server in ~/.cursor/mcp.json
  codex         Register MCP server in ~/.codex/config.toml
`);
		process.exit(0);
	}

	switch (tool) {
		case "opencode":
			setupOpenCode();
			break;
		case "claude-code":
			setupClaudeCode();
			break;
		case "cursor":
			setupCursor();
			break;
		case "codex":
			setupCodex();
			break;
		default:
			console.error(`Unknown tool: ${tool}`);
			console.error("Valid options: opencode, claude-code, cursor, codex");
			process.exit(1);
	}
}
