import { spawnSync } from "node:child_process";
import {
	copyFileSync,
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
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../../package.json" with { type: "json" };
import { config } from "../config.js";

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
		whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server"; // fallback: hope it's on PATH at runtime
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

/**
 * Resolve the installed asset directory for binary installs.
 * Convention from install.sh: binary lives at <install-dir>/libexec/knowledge-server,
 * so assets (knowledge.ts, *.md) live two levels up at <install-dir>/.
 * Uses process.execPath (the real binary on disk), not Bun.main (which is a
 * virtual bundle path like /$bunfs/root/src/index.ts in compiled binaries).
 */
function getBinaryInstallDir(): string {
	// e.g. ~/.local/share/knowledge-server/libexec/knowledge-server → ~/.local/share/knowledge-server
	return resolve(dirname(process.execPath), "..");
}

const GITHUB_RELEASES =
	"https://github.com/MAnders333/knowledge-server/releases/download";

/**
 * Download a text asset from the current release and write it atomically to dst.
 * Used by binary installs to fetch plugin/command files that aren't bundled.
 * Uses curl (always available on macOS/Linux) to stay synchronous.
 */
function downloadAssetSync(name: string, dst: string): void {
	// Strict allowlist: bare filename, letters/digits/dots/hyphens/underscores only.
	if (!/^[a-zA-Z0-9._-]+$/.test(name)) {
		throw new Error(`Invalid asset name: ${name}`);
	}
	// Validate version is a plain semver — guards against a crafted package.json
	// producing an arbitrary URL component.
	if (!/^\d+\.\d+\.\d+$/.test(pkg.version)) {
		throw new Error(`Unexpected version format: ${pkg.version}`);
	}
	const url = `${GITHUB_RELEASES}/v${pkg.version}/${name}`;
	const tmp = `${dst}.tmp`;
	const result = spawnSync(
		"curl",
		[
			"-fsSL",
			"--proto",
			"=https",
			"--tlsv1.2",
			"--max-time",
			"30",
			"--output",
			tmp,
			url,
		],
		{ encoding: "utf8", timeout: 35_000 },
	);
	if (result.error) {
		try {
			unlinkSync(tmp);
		} catch {
			/* ignore */
		}
		throw new Error(`Failed to run curl: ${result.error.message}`);
	}
	if (result.status !== 0) {
		try {
			unlinkSync(tmp);
		} catch {
			/* ignore */
		}
		throw new Error(
			`curl failed for ${name}: ${result.stderr?.trim() || "unknown error"}`,
		);
	}
	try {
		renameSync(tmp, dst);
	} catch (err) {
		try {
			unlinkSync(tmp);
		} catch {
			/* ignore */
		}
		throw err;
	}
}

function setupOpenCode(): void {
	const configDir = join(homedir(), ".config", "opencode");

	console.log("Setting up OpenCode integration...\n");

	const pluginDir = join(configDir, "plugins");
	mkdirSync(pluginDir, { recursive: true });
	const pluginDst = join(pluginDir, "knowledge.ts");

	const sourceInstall = isSourceInstall();
	const projectDir = sourceInstall ? getProjectDir() : "";

	if (sourceInstall) {
		// Source install: symlink so edits to the repo are reflected immediately.
		const pluginSrc = join(projectDir, "plugin", "knowledge.ts");
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
		for (const file of ["consolidate.md", "knowledge-review.md"]) {
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
		// Binary install: copy from <install-dir>/ if present, otherwise download
		// from the GitHub release. install.sh and `knowledge-server update` both
		// place knowledge.ts / *.md in <install-dir>/ alongside the binary.
		//
		// Assets are processed in order: plugin first (required), then commands (optional).
		// The command/ dir is created before the loop so .md destinations are valid.
		const installDir = getBinaryInstallDir();
		const requiredAssets: Array<[string, string]> = [
			["knowledge.ts", pluginDst],
		];
		const optionalAssets: Array<[string, string]> = [
			["consolidate.md", join(configDir, "command", "consolidate.md")],
			[
				"knowledge-review.md",
				join(configDir, "command", "knowledge-review.md"),
			],
		];
		mkdirSync(join(configDir, "command"), { recursive: true });

		const installAsset = (name: string, dst: string): boolean => {
			const cached = join(installDir, name);
			const label = name === "knowledge.ts" ? "Plugin" : `Command ${name}`;
			if (existsSync(cached)) {
				try {
					// Copy rather than symlink — install dir is internal implementation detail.
					copyFileSync(cached, dst);
					console.log(`  ✓ ${label}: ${dst}`);
					return true;
				} catch (err) {
					console.error(`  ✗ Failed to copy ${name}: ${err}`);
					return false;
				}
			} else {
				// Not on disk yet (e.g. fresh install before first update) — download.
				process.stdout.write(`  Downloading ${name}... `);
				try {
					downloadAssetSync(name, dst);
					console.log("done");
					return true;
				} catch (err) {
					console.error(`failed: ${err instanceof Error ? err.message : err}`);
					return false;
				}
			}
		};

		for (const [name, dst] of requiredAssets) {
			if (!installAsset(name, dst)) {
				console.error(
					"    Plugin is required for OpenCode integration — aborting.",
				);
				process.exit(1);
			}
		}
		for (const [name, dst] of optionalAssets) {
			installAsset(name, dst);
		}
	}

	// MCP server entry — written directly to opencode.jsonc
	writeOpenCodeMcpEntry(configDir);

	const startHint = sourceInstall
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
	const whichResult = spawnSync("which", ["knowledge-server"], {
		encoding: "utf8",
	});
	const mainBin =
		whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
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
	const whichResult = spawnSync("which", ["knowledge-server"], {
		encoding: "utf8",
	});
	const mainBin =
		whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
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
		const existingCmd = JSON.stringify([
			existing.command,
			...(existing.args ?? []),
		]);
		const newCmd = JSON.stringify([entry.command, ...(entry.args ?? [])]);
		if (existingCmd === newCmd) {
			console.log(
				"  ✓ MCP server 'knowledge' already in ~/.cursor/mcp.json (no change)",
			);
			console.log(
				"    To update: remove the 'knowledge' entry from ~/.cursor/mcp.json and re-run this command.",
			);
			needsWrite = false;
		} else {
			mcpConfig.mcpServers.knowledge = entry;
			console.log(
				"  ✓ MCP server 'knowledge' updated in ~/.cursor/mcp.json (command changed)",
			);
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
			try {
				unlinkSync(tmpPath);
			} catch {
				/* ignore */
			}
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
		const argsToml = args
			.map((a) => `"${a.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`)
			.join(", ");
		commandToml = `command = "${bunBin.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"\nargs = [${argsToml}]`;
	} else {
		const whichResult = spawnSync("which", ["knowledge-server"], {
			encoding: "utf8",
		});
		const mainBin =
			whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
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
		console.log(
			"  ✓ MCP server 'knowledge' already in ~/.codex/config.toml (no change)",
		);
		console.log(
			"    To update: remove the [mcp_servers.knowledge] block and re-run this command.",
		);
	} else {
		const updated = `${existing.trimEnd()}\n${block}`;
		const tmpPath = `${configPath}.tmp`;
		try {
			writeFileSync(tmpPath, updated, "utf8");
			renameSync(tmpPath, configPath);
		} catch (e) {
			try {
				unlinkSync(tmpPath);
			} catch {
				/* ignore */
			}
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

// ── VSCode setup ───────────────────────────────────────────────────────────────

/**
 * Build the MCP server entry for VSCode's mcp.json.
 *
 * VSCode uses a `servers` key in mcp.json (different from Cursor's `mcpServers`).
 * Each entry: { command, args?, env? }
 *
 * The MCP server is registered via `code --add-mcp` which writes to the active
 * profile's mcp.json. This avoids hardcoding profile-specific paths.
 */
function makeVSCodeMcpEntry() {
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
			name: "knowledge",
			command: bunBin,
			args: ["run", join(projectDir, "src", "index.ts"), "mcp"],
			env,
		};
	}
	const whichResult = spawnSync("which", ["knowledge-server"], {
		encoding: "utf8",
	});
	const mainBin =
		whichResult.status === 0 ? whichResult.stdout.trim() : "knowledge-server";
	return { name: "knowledge", command: mainBin, args: ["mcp"], env };
}

export function setupVSCode(): void {
	console.log("Setting up VSCode integration...\n");

	const entry = makeVSCodeMcpEntry();

	// Use `code --add-mcp` to register the MCP server in the active profile.
	// This is the official VSCode CLI approach and handles profile-specific
	// mcp.json paths transparently. The --add-mcp flag accepts a JSON object
	// with { name, command, args?, env? }.
	//
	// Idempotency: --add-mcp does a keyed upsert by server name in mcp.json
	// (existingServers[name] = config). Re-running overwrites the entry rather
	// than detecting "no change" — the end result is identical either way.
	const mcpJson = JSON.stringify(entry);

	// First check if `code` is available on PATH
	const codeCheck = spawnSync("which", ["code"], { encoding: "utf8" });
	if (codeCheck.status !== 0) {
		console.error("  ✗ 'code' command not found on PATH.");
		console.error(
			"    Install the VSCode CLI: open VSCode → Cmd+Shift+P → 'Shell Command: Install code command in PATH'",
		);
		console.error("");
		console.error("    Alternatively, add the MCP server manually:");
		console.error(
			"    1. Open VSCode → Cmd+Shift+P → 'MCP: Open User Configuration'",
		);
		console.error(`    2. Add to the "servers" object:`);
		console.error(
			`       "knowledge": ${JSON.stringify({ command: entry.command, args: entry.args, env: entry.env }, null, 6)}`,
		);
		process.exit(1);
	}

	const addResult = spawnSync("code", ["--add-mcp", mcpJson], {
		encoding: "utf8",
	});

	if (addResult.status === 0) {
		console.log("  ✓ MCP server 'knowledge' registered via `code --add-mcp`");
	} else {
		// Fallback: `code --add-mcp` may not be available in older VSCode versions.
		// Print manual instructions instead.
		console.error(
			"  ✗ Failed to register MCP server via `code --add-mcp`:",
			addResult.stderr?.trim(),
		);
		console.error("");
		console.error("    Add the MCP server manually:");
		console.error(
			"    1. Open VSCode → Cmd+Shift+P → 'MCP: Open User Configuration'",
		);
		console.error(`    2. Add to the "servers" object:`);
		console.error(
			`       "knowledge": ${JSON.stringify({ command: entry.command, args: entry.args, env: entry.env }, null, 6)}`,
		);
	}

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using VSCode:
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

// ── Daemon service registration ───────────────────────────────────────────────

/**
 * Register the knowledge-daemon as a background system service.
 *
 * macOS:  writes ~/Library/LaunchAgents/com.knowledge-server.daemon.plist
 *         and loads it via `launchctl load`
 *
 * Linux:  writes ~/.config/systemd/user/knowledge-daemon.service
 *         and enables + starts it via `systemctl --user`
 *
 * Idempotent: safe to re-run after updating the binary path.
 */
function setupDaemon(): void {
	const platform = process.platform;
	const home = homedir();

	const autoSpawnNote =
		"  Note: knowledge-server auto-spawns the daemon by default.\n" +
		"  After registering it as a system service, set DAEMON_AUTO_SPAWN=false\n" +
		'  in your .env or add "daemonAutoSpawn": false to config.jsonc,\n' +
		"  otherwise two daemon instances will run simultaneously.";

	// Resolve daemon binary path
	let daemonBin: string;
	if (isSourceInstall()) {
		const bunBin = typeof Bun !== "undefined" ? process.argv[0] : "bun";
		const projectDir = getProjectDir();
		daemonBin = `${bunBin} run ${join(projectDir, "src", "daemon", "index.ts")}`;
	} else {
		// Binary install — daemon is a sibling binary in the same directory.
		const binDir = dirname(process.execPath);
		daemonBin = join(binDir, "knowledge-daemon");
		if (!existsSync(daemonBin)) {
			daemonBin = "knowledge-daemon";
		}
	}

	if (platform === "darwin") {
		// macOS launchd
		const plistDir = join(home, "Library", "LaunchAgents");
		const plistPath = join(plistDir, "com.knowledge-server.daemon.plist");

		mkdirSync(plistDir, { recursive: true });

		const parts = daemonBin.split(" ");
		const programArgs = parts
			.map((p) => `    <string>${p}</string>`)
			.join("\n");

		const plist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.knowledge-server.daemon</string>

  <key>ProgramArguments</key>
  <array>
${programArgs}
  </array>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${home}/.local/share/knowledge-server/daemon.log</string>

  <key>StandardErrorPath</key>
  <string>${home}/.local/share/knowledge-server/daemon.log</string>
</dict>
</plist>
`;
		writeFileSync(plistPath, plist);

		// Unload first in case it was already loaded (idempotency)
		spawnSync("launchctl", ["unload", plistPath], { stdio: "ignore" });
		const load = spawnSync("launchctl", ["load", plistPath], {
			stdio: "pipe",
		});
		if (load.status !== 0) {
			console.warn(
				`  ⚠ launchctl load returned ${load.status}. The daemon plist was written but may not be running. Try: launchctl load ${plistPath}`,
			);
		} else {
			console.log("  ✓ Daemon registered as macOS launchd service");
			console.log(`    Plist: ${plistPath}`);
			console.log(
				`    Log:   ${join(home, ".local", "share", "knowledge-server", "daemon.log")}`,
			);
			console.log("");
			console.log(autoSpawnNote);
		}
	} else if (platform === "linux") {
		// Linux systemd user service
		const serviceDir = join(home, ".config", "systemd", "user");
		const servicePath = join(serviceDir, "knowledge-daemon.service");

		mkdirSync(serviceDir, { recursive: true });

		const service = `[Unit]
Description=knowledge-daemon — episode uploader for knowledge-server
After=network.target

[Service]
Type=simple
ExecStart=${daemonBin}
Restart=on-failure
RestartSec=30
StandardOutput=append:${home}/.local/share/knowledge-server/daemon.log
StandardError=append:${home}/.local/share/knowledge-server/daemon.log

[Install]
WantedBy=default.target
`;
		writeFileSync(servicePath, service);

		spawnSync("systemctl", ["--user", "daemon-reload"], { stdio: "ignore" });
		const enable = spawnSync(
			"systemctl",
			["--user", "enable", "--now", "knowledge-daemon"],
			{ stdio: "pipe" },
		);
		if (enable.status !== 0) {
			console.warn(
				`  ⚠ systemctl enable returned ${enable.status}. The service file was written but may not be running. Try: systemctl --user enable --now knowledge-daemon`,
			);
		} else {
			console.log("  ✓ Daemon registered as systemd user service");
			console.log(`    Service: ${servicePath}`);
			console.log(
				`    Log:     ${join(home, ".local", "share", "knowledge-server", "daemon.log")}`,
			);
			console.log("");
			console.log(autoSpawnNote);
		}
	} else {
		console.error(
			`  ✗ Unsupported platform: ${platform}. Automatic service registration is only supported on macOS and Linux.`,
		);
		console.log(`  Run the daemon manually: ${daemonBin}`);
		process.exit(1);
	}
}

export function runSetupTool(args: string[]): void {
	const tool = args[0];

	if (!tool || tool === "--help" || tool === "-h") {
		console.log(`Usage: knowledge-server setup-tool <tool>

Available tools:
  opencode      Symlink plugin + commands; register MCP server in opencode.jsonc
  claude-code   Register MCP server + hook; symlink commands into ~/.claude/commands/
  cursor        Register MCP server in ~/.cursor/mcp.json
  codex         Register MCP server in ~/.codex/config.toml
  vscode        Register MCP server via \`code --add-mcp\`
  daemon        Register knowledge-daemon as a background service (launchd/systemd)
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
		case "vscode":
			setupVSCode();
			break;
		case "daemon":
			setupDaemon();
			break;
		default:
			console.error(`Unknown tool: ${tool}`);
			console.error(
				"Valid options: opencode, claude-code, cursor, codex, vscode, daemon",
			);
			process.exit(1);
	}
}
