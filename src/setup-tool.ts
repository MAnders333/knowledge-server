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
 * `knowledge-server setup-tool <opencode|claude-code>`
 *
 * Idempotent tool-specific wiring:
 *
 * opencode:
 *   - Symlink plugin/knowledge.ts → ~/.config/opencode/plugins/knowledge.ts
 *   - Symlink commands/*.md → ~/.config/opencode/command/*.md
 *   - Print MCP config block for opencode.jsonc
 *
 * claude-code:
 *   - Register MCP server via `claude mcp add-json` (→ ~/.claude.json)
 *   - Merge UserPromptSubmit hook into ~/.claude/settings.json
 *   - Symlink commands/*.md → ~/.claude/commands/*.md
 *
 * There are two installation modes:
 *
 *   Source install  — cloned repo, run via `bun run src/index.ts`.
 *                     Bun.main ends in `.ts`. MCP server → `bun run src/mcp/index.ts`.
 *
 *   Binary install  — compiled binary on PATH (`knowledge-server`).
 *                     Bun.main does NOT end in `.ts`. MCP server → `knowledge-server-mcp`
 *                     (the companion binary, expected to be on PATH alongside the main one).
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

	// MCP config hint
	console.log(`
To enable the MCP 'activate' tool, add this to ~/.config/opencode/opencode.jsonc:

  "mcp": {
    "knowledge": {
      "type": "local",
      "command": ["bun", "run", "${join(projectDir, "src", "mcp", "index.ts")}"],
      "enabled": true,
      "environment": {
        "KNOWLEDGE_HOST": "${config.host}",
        "KNOWLEDGE_PORT": "${config.port}"
      }
    }
  }

Setup complete!`);
}

// ── Claude Code setup ──────────────────────────────────────────────────────────

/**
 * Build the MCP server entry for Claude Code.
 *
 * Source install: `bun run <projectDir>/src/mcp/index.ts`
 *   - Always reflects current source; no stale compiled binary risk.
 *   - Bun executable resolved from the running process or ~/.bun/bin/bun.
 *
 * Binary install: `knowledge-server-mcp` (companion binary, must be on PATH
 *   alongside `knowledge-server`).
 */
function makeClaudeMcpEntry() {
	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		// In source mode, process.execPath is the bun binary itself.
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homedir(), ".bun", "bin", "bun");
		return {
			type: "stdio",
			command: bunBin,
			args: ["run", join(projectDir, "src", "mcp", "index.ts")],
			env: {
				// The MCP server is a thin HTTP proxy — it only needs to locate the
				// knowledge HTTP server. No LLM credentials required here.
				KNOWLEDGE_HOST: config.host,
				KNOWLEDGE_PORT: String(config.port),
			},
		};
	}
	// Binary install: companion binary knowledge-server-mcp must be on PATH.
	return {
		type: "stdio",
		command: "knowledge-server-mcp",
		env: {
			// Same as source install — just KNOWLEDGE_HOST / KNOWLEDGE_PORT.
			KNOWLEDGE_HOST: config.host,
			KNOWLEDGE_PORT: String(config.port),
		},
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
	writeFileSync(tmpPath, `${JSON.stringify(settings, null, 2)}\n`, "utf8");
	renameSync(tmpPath, settingsPath);
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
  opencode      Symlink plugin + commands into ~/.config/opencode/; print MCP config hint
  claude-code   Register MCP server + hook; symlink commands into ~/.claude/commands/
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
		default:
			console.error(`Unknown tool: ${tool}`);
			console.error("Valid options: opencode, claude-code");
			process.exit(1);
	}
}
