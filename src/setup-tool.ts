import {
	existsSync,
	mkdirSync,
	readFileSync,
	symlinkSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

/**
 * `knowledge-server setup-tool <opencode|claude-code>`
 *
 * Idempotent tool-specific wiring:
 *
 * opencode:
 *   - Symlink plugin/knowledge.ts → ~/.config/opencode/plugins/knowledge.ts
 *   - Symlink opencode/command/*.md → ~/.config/opencode/command/*.md
 *   - Print MCP config block for opencode.jsonc
 *
 * claude-code:
 *   - Merge MCP server + UserPromptSubmit hook into ~/.claude/settings.json
 *
 * The binary's own directory is used as the project root (fallback: cwd).
 * When running via `bun run src/index.ts`, Bun.main resolves to the source file.
 * When running as a compiled binary, `process.execPath` is the binary itself.
 */
function getProjectDir(): string {
	// Compiled binary: execPath is the binary in dist/ — project root is one level up.
	// Source mode: Bun.main is src/index.ts — project root is one level up from src/.
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
	const commandSrcDir = join(projectDir, "opencode", "command");
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
        "LLM_API_KEY": "<copy from .env>",
        "LLM_BASE_ENDPOINT": "<copy from .env>"
      }
    }
  }

Setup complete!`);
}

// ── Claude Code setup ──────────────────────────────────────────────────────────

/**
 * The knowledge-server MCP entry for Claude Code.
 * Uses the `knowledge-server-mcp` compiled binary (must be on PATH).
 * If the binary is not on PATH, the user can substitute `bun run <path>/src/mcp/index.ts`.
 */
const CLAUDE_MCP_ENTRY = {
	type: "stdio",
	command: "knowledge-server-mcp",
} as const;

/**
 * The UserPromptSubmit hook entry for Claude Code.
 * Points to the local knowledge server's hook endpoint.
 */
const CLAUDE_HOOK_ENTRY = {
	type: "http",
	url: "http://127.0.0.1:3179/hooks/claude-code/user-prompt",
	timeout: 5,
} as const;

function setupClaudeCode(): void {
	const claudeDir =
		process.env.CLAUDE_DB_PATH ??
		process.env.CLAUDE_CONFIG_DIR ??
		join(homedir(), ".claude");

	const settingsPath = join(claudeDir, "settings.json");

	console.log("Setting up Claude Code integration...\n");

	// Read existing settings (create empty object if file doesn't exist)
	let settings: Record<string, unknown> = {};
	if (existsSync(settingsPath)) {
		try {
			settings = JSON.parse(readFileSync(settingsPath, "utf8")) as Record<
				string,
				unknown
			>;
			console.log(`  ✓ Read existing settings: ${settingsPath}`);
		} catch (e) {
			console.error(`  ✗ Failed to parse ${settingsPath}: ${e}`);
			console.error("    Fix the JSON syntax error and retry.");
			process.exit(1);
		}
	} else {
		mkdirSync(claudeDir, { recursive: true });
		console.log(`  ✓ Creating settings: ${settingsPath}`);
	}

	// Merge MCP server entry
	const mcpServers = (settings.mcpServers ?? {}) as Record<string, unknown>;
	const existingMcp = mcpServers.knowledge as
		| Record<string, unknown>
		| undefined;

	if (existingMcp) {
		console.log("  ✓ MCP server 'knowledge' already configured (no change)");
	} else {
		mcpServers.knowledge = CLAUDE_MCP_ENTRY;
		console.log("  ✓ MCP server 'knowledge' added");
	}
	settings.mcpServers = mcpServers;

	// Merge UserPromptSubmit hook
	const hooks = (settings.hooks ?? {}) as Record<string, unknown>;
	const existingHooks = (hooks.UserPromptSubmit ?? []) as unknown[];

	const alreadyHasHook = existingHooks.some(
		(h) =>
			typeof h === "object" &&
			h !== null &&
			(h as Record<string, unknown>).url === CLAUDE_HOOK_ENTRY.url,
	);

	if (alreadyHasHook) {
		console.log("  ✓ UserPromptSubmit hook already configured (no change)");
	} else {
		hooks.UserPromptSubmit = [...existingHooks, CLAUDE_HOOK_ENTRY];
		console.log("  ✓ UserPromptSubmit hook added");
	}
	settings.hooks = hooks;

	// Write back
	writeFileSync(settingsPath, `${JSON.stringify(settings, null, 2)}\n`, "utf8");
	console.log(`\n  ✓ Wrote ${settingsPath}`);

	console.log(`
Note: 'knowledge-server-mcp' must be on your PATH for the MCP tool to work.
If it isn't, edit mcpServers.knowledge in ${settingsPath} to use:

  "command": "bun",
  "args": ["run", "${join(getProjectDir(), "src", "mcp", "index.ts")}"]

Start the knowledge server before using Claude Code:
  knowledge-server   (or: bun run ${join(getProjectDir(), "src", "index.ts")})

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
  opencode      Symlink plugin + commands; print MCP config hint
  claude-code   Merge MCP server + hook into ~/.claude/settings.json
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
