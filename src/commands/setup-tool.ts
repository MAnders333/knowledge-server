import { spawnSync } from "node:child_process";
import {
	chmodSync,
	copyFileSync,
	existsSync,
	lstatSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	renameSync,
	rmSync,
	statSync,
	symlinkSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../../package.json" with { type: "json" };
import {
	DEFAULT_CONFIG,
	defaultConfigPath,
	loadConfigFile,
} from "../config-file.js";
import { config } from "../config.js";
import { homeDir } from "../home.js";
import { recordExtensionTarget } from "./extension-targets.js";
import {
	GITHUB_RELEASES,
	detectPlatform,
	fetchBufferSync,
	parseChecksums,
	releaseAssetUrl,
	requireExpectedSha,
	sha256Hex,
} from "./release-download.js";

// Resolve port/host from config.jsonc (with env var override via KNOWLEDGE_PORT/KNOWLEDGE_HOST).
// Falls back to the env-var-only config for installs that have no config.jsonc yet.
// Evaluated once at module load — setup-tool is a CLI command, not a long-running server.
const fileConfig = (() => {
	try {
		return loadConfigFile(defaultConfigPath()) ?? DEFAULT_CONFIG;
	} catch (err) {
		// Malformed config.jsonc — warn so user knows why defaults are used.
		console.warn(
			`[setup-tool] Could not parse config.jsonc: ${err instanceof Error ? err.message : String(err)}. Using defaults for port/host.`,
		);
		return DEFAULT_CONFIG;
	}
})();
const resolvedPort = fileConfig.port;
const resolvedHost = fileConfig.host;

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
 * codex:
 *   - Also write a UserPromptSubmit hook to ~/.codex/hooks.json (Codex skips it until trusted in /hooks)
 * pi / oh-my-pi:
 *   - Place the extension file (no MCP: pi and oh-my-pi have none for this)
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

/**
 * Replaces a config file atomically (temp file + rename), so a crash never leaves a
 * truncated file. The replacement keeps the mode of the file it replaces. A new file
 * is 0600, because these configs can hold credentials. The mode is set explicitly,
 * so the umask cannot widen it.
 */
function replaceConfigFile(path: string, content: string): void {
	const mode = existsSync(path) ? statSync(path).mode & 0o777 : 0o600;
	const tmpPath = `${path}.tmp`;
	try {
		rmSync(tmpPath, { force: true });
		writeFileSync(tmpPath, content, { encoding: "utf8", mode });
		chmodSync(tmpPath, mode);
		renameSync(tmpPath, path);
	} catch (e) {
		try {
			unlinkSync(tmpPath);
		} catch {
			// Ignore — tmp file may not have been created yet.
		}
		throw e;
	}
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
		KNOWLEDGE_HOST: resolvedHost,
		KNOWLEDGE_PORT: String(resolvedPort),
	};

	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homeDir(), ".bun", "bin", "bun");
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
export function writeOpenCodeMcpEntry(opencodeConfigDir: string): void {
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
	replaceConfigFile(configPath, `${JSON.stringify(parsed, null, 2)}\n`);
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

/**
 * Download a release asset and install it at dst, verified against the
 * release's SHA256SUMS-<platform>. Used by binary installs to fetch plugin and
 * command files that aren't bundled. Fails closed: a missing manifest entry or
 * a hash mismatch throws and leaves dst untouched.
 * Synchronous (curl via spawnSync) to match the setup flow's control flow.
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
	const version = `v${pkg.version}`;
	const checksums = parseChecksums(
		fetchBufferSync(
			`${GITHUB_RELEASES}/${version}/SHA256SUMS-${detectPlatform()}`,
		).toString("utf8"),
	);
	const expected = requireExpectedSha(checksums, name);
	const data = fetchBufferSync(releaseAssetUrl(version, name));
	if (sha256Hex(data) !== expected) {
		throw new Error(`Checksum mismatch for ${name} — refusing to install.`);
	}
	const tmp = `${dst}.tmp`;
	try {
		writeFileSync(tmp, data, { mode: 0o644 });
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
	const configDir = join(homeDir(), ".config", "opencode");

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
					recordExtensionTarget(installDir, { asset: name, dst });
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
					recordExtensionTarget(installDir, { asset: name, dst });
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

	maybeSetupDaemon();

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
		KNOWLEDGE_HOST: resolvedHost,
		KNOWLEDGE_PORT: String(resolvedPort),
	};
	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		// In source mode, process.execPath is the bun binary itself.
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homeDir(), ".bun", "bin", "bun");
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
	return `http://${resolvedHost}:${resolvedPort}/hooks/claude-code/user-prompt`;
}

/** True when a settings handler points at this knowledge server's Claude hook URL. */
function isOurClaudeHandler(handler: unknown): boolean {
	return (
		typeof handler === "object" &&
		handler !== null &&
		(handler as Record<string, unknown>).url === claudeHookUrl()
	);
}

/**
 * The header carries the admin bearer. Claude Code expands `$KNOWLEDGE_ADMIN_TOKEN`
 * only when the variable is listed in allowedEnvVars, and an unset variable expands
 * to an empty string. So the token is sent when Claude Code has it set, and an empty
 * bearer otherwise, which is not a token.
 */
function makeClaudeHookEntry() {
	return {
		hooks: [
			{
				type: "http",
				url: claudeHookUrl(),
				timeout: 5,
				headers: { Authorization: "Bearer $KNOWLEDGE_ADMIN_TOKEN" },
				allowedEnvVars: ["KNOWLEDGE_ADMIN_TOKEN"],
			},
		],
	};
}

export function setupClaudeCode(): void {
	const claudeDir =
		process.env.CLAUDE_DB_PATH ??
		process.env.CLAUDE_CONFIG_DIR ??
		join(homeDir(), ".claude");

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
	const desired = makeClaudeHookEntry();

	// Drop every handler for this URL, in any shape, and append the current one. A
	// re-run therefore upgrades a handler written before the token header existed.
	let ours = 0;
	let upToDate = false;
	const kept: unknown[] = [];
	for (const entry of existingHooks) {
		if (typeof entry !== "object" || entry === null) {
			kept.push(entry);
			continue;
		}
		const group = entry as Record<string, unknown>;
		if (!Array.isArray(group.hooks)) {
			// Old format (pre-matcher): the handler sits at the top level.
			if (isOurClaudeHandler(group)) ours += 1;
			else kept.push(entry);
			continue;
		}
		const rest = group.hooks.filter((handler) => {
			if (!isOurClaudeHandler(handler)) return true;
			ours += 1;
			if (isDeepStrictEqual(handler, desired.hooks[0])) upToDate = true;
			return false;
		});
		if (rest.length === group.hooks.length) kept.push(entry);
		else if (rest.length > 0) kept.push({ ...group, hooks: rest });
	}

	if (ours === 1 && upToDate) {
		console.log("  ✓ UserPromptSubmit hook already configured (no change)");
	} else {
		hooks.UserPromptSubmit = [...kept, desired];
		console.log(
			`  ✓ UserPromptSubmit hook ${ours > 0 ? "updated" : "added"} (${claudeHookUrl()})`,
		);
	}
	settings.hooks = hooks;

	replaceConfigFile(settingsPath, `${JSON.stringify(settings, null, 2)}\n`);
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

	maybeSetupDaemon();

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
		KNOWLEDGE_HOST: resolvedHost,
		KNOWLEDGE_PORT: String(resolvedPort),
	};

	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homeDir(), ".bun", "bin", "bun");
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
	const cursorDir = process.env.CURSOR_HOME ?? join(homeDir(), ".cursor");
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
		replaceConfigFile(mcpPath, `${JSON.stringify(mcpConfig, null, 2)}\n`);
		console.log(`  ✓ Wrote ${mcpPath}`);
	}

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	maybeSetupDaemon();

	console.log(`
Start the knowledge server before using Cursor:
  ${startHint}

Setup complete!`);
}

// ── Codex setup ─────────────────────────────────────────────────────────────────

// Marks knowledge-server's UserPromptSubmit handler in ~/.codex/hooks.json. A re-run
// finds it by this path, so a port change replaces the old handler instead of adding one.
const CODEX_HOOK_PATH_MARKER = "/hooks/claude-code/user-prompt?client=codex";

/**
 * Shell command for Codex's UserPromptSubmit hook. Codex writes the hook input
 * ({prompt, session_id, cwd}) to stdin and reads hookSpecificOutput from stdout,
 * which is the same JSON the Claude Code endpoint returns.
 *
 * Budgets: the hook's own `timeout` in hooks.json is 5 s, and curl gives up after
 * 4 s, which leaves Codex time to start the process. Local rerank has a 3 s budget
 * (RERANK_TIMEOUT_MS) and falls back to unreranked results on expiry. Remote rerank
 * has a 2.5 s budget (RERANK_REMOTE_TIMEOUT_MS), but that budget starts after the
 * query embedding and store reads, so the reply takes the embedding time plus up to
 * 2.5 s. The 4 s limit leaves about 1.5 s for the embedding and the reads. A slower
 * embedding (EMBEDDING_TIMEOUT_MS, 20 s default) goes past the limit, and the prompt
 * then goes through without injected context. A longer limit needs a longer
 * hooks.json timeout to match.
 *
 * Auth: the admin bearer is sent only when KNOWLEDGE_ADMIN_TOKEN is set in the hook's
 * environment. The header is a here-document on fd 3, which the shell expands into a
 * pipe; curl reads it with `-H @/dev/fd/3`. The token is therefore never in curl's
 * argv, where other local users can read it with `ps`.
 *
 * `--noproxy '*'`: loopback must never go through an HTTP proxy from the
 * environment (the prompt would leave the machine). `--fail`: an error body is
 * not handed to Codex as hook output. `|| true` matters: a command hook that exits
 * 2 blocks the prompt, so a stopped or slow server must never fail the hook.
 */
function codexHookCommand(): string {
	const url = `http://${resolvedHost}:${resolvedPort}${CODEX_HOOK_PATH_MARKER}`;
	// POSIX single-quoting: host and port come from config, so the URL is always quoted.
	const quoted = `'${url.replace(/'/g, "'\\''")}'`;
	// `${…:+…}` adds the header option only when the token is set. The here-document
	// is unused otherwise.
	const auth = "${KNOWLEDGE_ADMIN_TOKEN:+-H @/dev/fd/3}";
	return [
		`curl -s --noproxy '*' --fail --max-time 4 ${auth} --data-binary @- ${quoted} 3<<KS_AUTH || true`,
		"Authorization: Bearer $KNOWLEDGE_ADMIN_TOKEN",
		"KS_AUTH",
	].join("\n");
}

/**
 * Write the UserPromptSubmit hook into ~/.codex/hooks.json. Other hooks and
 * groups in that file are kept. Groups that held only a previous knowledge-server
 * handler are dropped, so the file never ends up with two handlers.
 */
function setupCodexHook(codexHome: string): void {
	const hooksPath = join(codexHome, "hooks.json");
	let file: Record<string, unknown> = {};
	if (existsSync(hooksPath)) {
		try {
			file = JSON.parse(readFileSync(hooksPath, "utf8")) as Record<
				string,
				unknown
			>;
		} catch (e) {
			console.error(`  ✗ Failed to parse ${hooksPath}: ${e}`);
			console.error(
				"    Fix the JSON syntax error and re-run setup-tool codex to add the hook.",
			);
			return;
		}
	}

	const hooks = (file.hooks ?? {}) as Record<string, unknown>;
	const groups = Array.isArray(hooks.UserPromptSubmit)
		? (hooks.UserPromptSubmit as Array<Record<string, unknown>>)
		: [];
	const command = codexHookCommand();

	let ourHandlers = 0;
	let alreadyCurrent = false;
	const kept: unknown[] = [];
	for (const group of groups) {
		if (!Array.isArray(group.hooks)) {
			kept.push(group);
			continue;
		}
		const handlers = group.hooks as Array<Record<string, unknown>>;
		const rest = handlers.filter((h) => {
			if (
				typeof h.command !== "string" ||
				!h.command.includes(CODEX_HOOK_PATH_MARKER)
			) {
				return true;
			}
			ourHandlers += 1;
			if (h.command === command) alreadyCurrent = true;
			return false;
		});
		if (rest.length === handlers.length) kept.push(group);
		else if (rest.length > 0) kept.push({ ...group, hooks: rest });
	}
	if (alreadyCurrent && ourHandlers === 1) {
		console.log("  ✓ UserPromptSubmit hook already configured (no change)");
		return;
	}
	kept.push({ hooks: [{ type: "command", command, timeout: 5 }] });
	hooks.UserPromptSubmit = kept;
	file.hooks = hooks;

	replaceConfigFile(hooksPath, `${JSON.stringify(file, null, 2)}\n`);
	console.log(`  ✓ UserPromptSubmit hook written to ${hooksPath}`);
	console.log(
		"  ⚠ Codex does not run a new or changed hook until you trust it:",
	);
	console.log(
		"    open Codex, run /hooks, and trust the knowledge-server UserPromptSubmit hook.",
	);
	console.log(
		"    Trust is tied to the hook's contents, so re-run this after a port change and trust it again.",
	);
}

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
	const codexHome = process.env.CODEX_HOME ?? join(homeDir(), ".codex");
	const configPath = join(codexHome, "config.toml");

	console.log("Setting up Codex CLI integration...\n");

	mkdirSync(codexHome, { recursive: true });

	// Build the MCP command string for TOML.
	let commandToml: string;
	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homeDir(), ".bun", "bin", "bun");
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
env = { KNOWLEDGE_HOST = "${resolvedHost}", KNOWLEDGE_PORT = "${resolvedPort}" }
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
		replaceConfigFile(configPath, updated);
		console.log("  ✓ MCP server 'knowledge' added to ~/.codex/config.toml");
		console.log(`  ✓ Wrote ${configPath}`);
	}

	setupCodexHook(codexHome);

	const startHint = isSourceInstall()
		? `bun run ${join(getProjectDir(), "src", "index.ts")}`
		: "knowledge-server";

	maybeSetupDaemon();

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
		KNOWLEDGE_HOST: resolvedHost,
		KNOWLEDGE_PORT: String(resolvedPort),
	};

	if (isSourceInstall()) {
		const projectDir = getProjectDir();
		const bunBin = process.execPath.endsWith("bun")
			? process.execPath
			: join(homeDir(), ".bun", "bin", "bun");
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

function setupVSCode(): void {
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

// ── pi setup ─────────────────────────────────────────────────────────────────

/**
 * pi (https://github.com/earendil-works/pi-mono) has no MCP support, so the
 * integration is extension-only: plugin/pi-knowledge.ts hooks pi's
 * `before_agent_start` event and queries GET /activate over HTTP — the same
 * endpoint the OpenCode plugin uses. There is no MCP entry to register.
 *
 * What this setup does:
 *
 * 1. Places the extension file:
 *    - If ~/.pi/shared/extensions/ exists (layered setups that share extensions
 *      across PI_CODING_AGENT_DIR layers — pi's agent dir is a swap, not a
 *      merge), the file is placed there as knowledge.ts.
 *    - Otherwise it goes to ~/.pi/agent/extensions/knowledge.ts — pi's
 *      canonical global location, auto-discovered by the default layer with
 *      zero config edits.
 *    Source install: symlink (repo edits go live immediately).
 *    Binary install: copy from the install dir, or download the release asset.
 *
 * 2. Registers the extension path in the `extensions` array of every pi layer
 *    settings found at ~/.pi/<layer>/settings.json. Skipped for the default
 *    `agent` layer when the file sits in ~/.pi/agent/extensions/ (pi
 *    auto-discovers that directory — explicit registration would double-load).
 *    pi settings arrays support absolute paths (documented).
 */
function setupPi(): void {
	const piRoot = join(homeDir(), ".pi");

	console.log("Setting up pi integration...\n");

	if (!existsSync(piRoot)) {
		console.error(
			"  ✗ ~/.pi not found — pi does not appear to be installed (or has never run).",
		);
		console.error(
			"    Install pi (npm i -g @earendil-works/pi-coding-agent), run it once, then re-run this setup.",
		);
		process.exit(1);
	}

	const sourceInstall = isSourceInstall();
	const projectDir = sourceInstall ? getProjectDir() : "";

	// ── 1. Place the extension file ──
	const sharedExtDir = join(piRoot, "shared", "extensions");
	const useSharedDir = existsSync(sharedExtDir);
	const extDst = useSharedDir
		? join(sharedExtDir, "knowledge.ts")
		: join(piRoot, "agent", "extensions", "knowledge.ts");

	if (sourceInstall) {
		const extSrc = join(projectDir, "plugin", "pi-knowledge.ts");
		if (!existsSync(extSrc)) {
			console.error(`  ✗ Extension source not found: ${extSrc}`);
			console.error(
				"    Make sure you are running from the knowledge-server project directory.",
			);
			process.exit(1);
		}
		mkdirSync(dirname(extDst), { recursive: true });
		forceSymlink(extSrc, extDst);
		console.log(`  ✓ Extension: ${extDst}`);
		console.log(`       → ${extSrc}`);
	} else {
		// Binary install: copy from <install-dir>/ if present, otherwise download
		// from the GitHub release (same asset flow as the OpenCode plugin).
		mkdirSync(dirname(extDst), { recursive: true });
		const cached = join(getBinaryInstallDir(), "pi-knowledge.ts");
		if (existsSync(cached)) {
			try {
				copyFileSync(cached, extDst);
				recordExtensionTarget(getBinaryInstallDir(), {
					asset: "pi-knowledge.ts",
					dst: extDst,
				});
				console.log(`  ✓ Extension: ${extDst}`);
			} catch (err) {
				console.error(`  ✗ Failed to copy pi-knowledge.ts: ${err}`);
				process.exit(1);
			}
		} else {
			process.stdout.write("  Downloading pi-knowledge.ts... ");
			try {
				downloadAssetSync("pi-knowledge.ts", extDst);
				recordExtensionTarget(getBinaryInstallDir(), {
					asset: "pi-knowledge.ts",
					dst: extDst,
				});
				console.log("done");
			} catch (err) {
				console.error(`failed: ${err instanceof Error ? err.message : err}`);
				console.error(
					"    Extension is required for pi integration — aborting.",
				);
				process.exit(1);
			}
		}
	}

	// ── 2. Register in every layer settings.json under ~/.pi ──
	// The default agent layer auto-discovers ~/.pi/agent/extensions/ — skip
	// explicit registration there to avoid double-loading. Every other layer
	// (and the shared-dir case) needs the explicit path.
	const agentExtDir = join(piRoot, "agent", "extensions");
	const inAgentExtDir = extDst.startsWith(`${agentExtDir}/`);

	let settingsFiles: string[] = [];
	try {
		settingsFiles = readdirSync(piRoot, { withFileTypes: true })
			.filter((d) => d.isDirectory())
			.map((d) => join(piRoot, d.name, "settings.json"))
			.filter((p) => existsSync(p));
	} catch {
		// unreadable — handled below as "no settings found"
	}

	let registered = 0;
	for (const settingsPath of settingsFiles) {
		const layer = basename(dirname(settingsPath));
		if (inAgentExtDir && layer === "agent") {
			console.log(
				`  ✓ ${settingsPath}: auto-discovered from agent/extensions (no edit needed)`,
			);
			continue;
		}
		if (registerPiExtension(settingsPath, extDst)) registered += 1;
	}

	if (settingsFiles.length === 0) {
		if (inAgentExtDir) {
			console.log(
				"  ✓ No layer settings found — relying on agent/extensions auto-discovery",
			);
		} else {
			// Shared-dir layout but no settings anywhere: create a minimal default.
			const fresh = join(piRoot, "agent", "settings.json");
			mkdirSync(dirname(fresh), { recursive: true });
			writeFileSync(
				fresh,
				`${JSON.stringify({ extensions: [extDst] }, null, 2)}\n`,
			);
			console.log(`  ✓ Created ${fresh} with the extension registered`);
			registered = 1;
		}
	}

	maybeSetupDaemon();

	const startHint = sourceInstall
		? `bun run ${join(projectDir, "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using pi:
  ${startHint}

Setup complete!${registered > 0 ? ` (${registered} layer settings updated)` : ""}`);
}

// ── oh-my-pi setup ───────────────────────────────────────────────────────────

/**
 * Agent layers of an oh-my-pi install: the shared `agent` dir plus every
 * `profiles/<mode>/agent`, the same layers the oh-my-pi session reader scans.
 * A layer without an agent dir is skipped.
 */
function ompAgentLayers(ompRoot: string): string[] {
	const layers: string[] = [];
	const shared = join(ompRoot, "agent");
	if (existsSync(shared)) layers.push(shared);
	let profiles: string[] = [];
	try {
		profiles = readdirSync(join(ompRoot, "profiles"), { withFileTypes: true })
			.filter((d) => d.isDirectory())
			.map((d) => d.name);
	} catch {
		// no profiles dir — the shared layer alone may be the install
	}
	for (const mode of profiles) {
		const agent = join(ompRoot, "profiles", mode, "agent");
		if (existsSync(agent)) layers.push(agent);
	}
	return layers;
}

/**
 * `knowledge-server setup-tool oh-my-pi`
 *
 * oh-my-pi (`omp`, @oh-my-pi/pi-coding-agent) is a pi fork with its own extension
 * API, so it gets its own file, plugin/omp-knowledge.ts. The extension hooks
 * `before_agent_start` (once per prompt, before the model sees it) and queries
 * GET /activate with client tag `omp`.
 *
 * oh-my-pi auto-discovers `<agent-dir>/extensions/*.ts`, so the file is placed as
 * extensions/knowledge.ts in every agent layer and no settings entry is needed.
 * Source installs symlink the repo file; binary installs copy it and register the
 * copies so `knowledge-server update` refreshes them.
 */
export function setupOhMyPi(ompRoot: string = config.ompSessionsRoot): void {
	console.log("Setting up oh-my-pi integration...\n");

	const layers = ompAgentLayers(ompRoot);
	if (layers.length === 0) {
		console.error(
			`  ✗ No oh-my-pi agent directory under ${ompRoot} — oh-my-pi does not appear to be installed (or has never run).`,
		);
		console.error(
			"    Install oh-my-pi (bun add -g @oh-my-pi/pi-coding-agent), run it once, then re-run this setup.",
		);
		process.exit(1);
	}

	const sourceInstall = isSourceInstall();
	const projectDir = sourceInstall ? getProjectDir() : "";
	const extSrc = join(projectDir, "plugin", "omp-knowledge.ts");
	if (sourceInstall && !existsSync(extSrc)) {
		console.error(`  ✗ Extension source not found: ${extSrc}`);
		console.error(
			"    Make sure you are running from the knowledge-server project directory.",
		);
		process.exit(1);
	}
	const installDir = getBinaryInstallDir();

	for (const layer of layers) {
		const extDir = join(layer, "extensions");
		const dst = join(extDir, "knowledge.ts");
		mkdirSync(extDir, { recursive: true });

		if (sourceInstall) {
			forceSymlink(extSrc, dst);
			console.log(`  ✓ Extension: ${dst}`);
			console.log(`       → ${extSrc}`);
			continue;
		}

		// Binary install: copy <install-dir>/omp-knowledge.ts when present,
		// otherwise download the verified release asset. A leftover symlink from a
		// source install is removed first so the copy never writes into the repo.
		if (existsSync(dst) && lstatSync(dst).isSymbolicLink()) unlinkSync(dst);
		const cached = join(installDir, "omp-knowledge.ts");
		try {
			if (existsSync(cached)) {
				copyFileSync(cached, dst);
				console.log(`  ✓ Extension: ${dst}`);
			} else {
				process.stdout.write("  Downloading omp-knowledge.ts... ");
				downloadAssetSync("omp-knowledge.ts", dst);
				console.log("done");
			}
		} catch (err) {
			console.error(`failed: ${err instanceof Error ? err.message : err}`);
			console.error(
				"    Extension is required for oh-my-pi integration — aborting.",
			);
			process.exit(1);
		}
		recordExtensionTarget(installDir, { asset: "omp-knowledge.ts", dst });
	}

	maybeSetupDaemon();

	const startHint = sourceInstall
		? `bun run ${join(projectDir, "src", "index.ts")}`
		: "knowledge-server";

	console.log(`
Start the knowledge server before using oh-my-pi:
  ${startHint}

Open a new oh-my-pi session to load the extension.
Setup complete! (${layers.length} agent layer${layers.length === 1 ? "" : "s"} updated)`);
}

/**
 * Ensure `extensionPath` is present in the `extensions` array of a pi
 * settings.json. Returns true if the file was modified or already contained
 * the path; false on parse errors (warns and skips — manual fix is trivial).
 */
function registerPiExtension(
	settingsPath: string,
	extensionPath: string,
): boolean {
	let raw: string;
	try {
		raw = readFileSync(settingsPath, "utf8");
	} catch (err) {
		console.log(`  ⚠ Cannot read ${settingsPath}: ${err} — skipping`);
		return false;
	}

	let settings: Record<string, unknown>;
	try {
		settings = JSON.parse(raw) as Record<string, unknown>;
	} catch {
		console.log(
			`  ⚠ ${settingsPath} is not plain JSON — skipping. Add this to its "extensions" array manually:`,
		);
		console.log(`      "${extensionPath}"`);
		return false;
	}

	const existing = Array.isArray(settings.extensions)
		? (settings.extensions as unknown[]).filter((e) => typeof e === "string")
		: [];

	// pi settings support "~" — normalize both sides before comparing.
	const normalize = (p: string) =>
		p.startsWith("~/") ? join(homeDir(), p.slice(2)) : p;
	if (existing.map((e) => normalize(e as string)).includes(extensionPath)) {
		console.log(`  ✓ ${settingsPath}: already registered (no change)`);
		return true;
	}

	settings.extensions = [...existing, extensionPath];
	writeFileSync(settingsPath, `${JSON.stringify(settings, null, 2)}\n`);
	console.log(`  ✓ ${settingsPath}: extension registered`);
	return true;
}

// ── Daemon auto-registration ──────────────────────────────────────────────────

/**
 * Called at the end of each tool setup (opencode, claude-code, cursor, codex).
 *
 * If the daemon is already registered as a system service, prints a quiet
 * confirmation and returns — setup is idempotent.
 *
 * If not yet registered, calls setupDaemon() to register it. This ensures the
 * daemon survives reboots on the developer machine without requiring a separate
 * manual step.
 *
 * Rationale: setup-tool is always run on a developer machine (the machine that
 * produces AI tool sessions). The daemon always belongs on that machine — it
 * reads local session files and writes to stateDb (local SQLite or remote
 * Postgres depending on config). Registration is safe regardless of stateDb
 * config because:
 *   - SQLite (default): stateDb is always reachable, daemon starts successfully.
 *   - Postgres: if unreachable at startup, launchd/systemd crash-loops and
 *     retries — standard service behaviour for infrastructure dependencies.
 */
function maybeSetupDaemon(): void {
	// `bun test` sets NODE_ENV=test from any cwd. Never register a launchd or
	// systemd service from a test run: it would run this checkout's daemon.
	if (process.env.NODE_ENV === "test") return;
	const platform = process.platform;
	const home = homeDir();

	let serviceFilePath: string;
	if (platform === "darwin") {
		serviceFilePath = join(
			home,
			"Library",
			"LaunchAgents",
			"com.knowledge-server.daemon.plist",
		);
	} else if (platform === "linux") {
		serviceFilePath = join(
			home,
			".config",
			"systemd",
			"user",
			"knowledge-daemon.service",
		);
	} else {
		// Unsupported platform — skip silently (setupDaemon will warn if called directly).
		return;
	}

	if (existsSync(serviceFilePath)) {
		console.log(
			"  ✓ Daemon already registered as background service — skipping re-registration.",
		);
		return;
	}

	console.log("");
	console.log("Registering daemon as background service...");
	setupDaemon();
}

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
	const home = homeDir();

	// Shown on successful service registration — tells the user to disable auto-spawn.
	const autoSpawnNoteSuccess =
		"  Note: knowledge-server auto-spawns the daemon by default.\n" +
		"  Now that it runs as a system service, disable auto-spawn to avoid\n" +
		"  running two daemon instances:\n" +
		"    Set DAEMON_AUTO_SPAWN=false in your .env\n" +
		'    or add "daemonAutoSpawn": false to config.jsonc';

	// Shown on failed registration — service file is on disk but not yet running,
	// so the user should NOT disable auto-spawn until they fix registration.
	const autoSpawnNoteFailure =
		"  Note: the service file is on disk but the daemon is not running yet.\n" +
		"  Keep the server running so auto-spawn stays active until you fix\n" +
		"  the registration error above.";

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
  <dict>
    <!-- Restart after crashes only. A clean exit (e.g. "another daemon holds the lock") must not respawn. -->
    <key>SuccessfulExit</key>
    <false/>
  </dict>

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
			console.log("");
			console.log(autoSpawnNoteFailure);
		} else {
			console.log("  ✓ Daemon registered as macOS launchd service");
			console.log(`    Plist: ${plistPath}`);
			console.log(
				`    Log:   ${join(home, ".local", "share", "knowledge-server", "daemon.log")}`,
			);
			console.log("");
			console.log(autoSpawnNoteSuccess);
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
			console.log("");
			console.log(autoSpawnNoteFailure);
		} else {
			console.log("  ✓ Daemon registered as systemd user service");
			console.log(`    Service: ${servicePath}`);
			console.log(
				`    Log:     ${join(home, ".local", "share", "knowledge-server", "daemon.log")}`,
			);
			console.log("");
			console.log(autoSpawnNoteSuccess);
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
  oh-my-pi      Place extension in each oh-my-pi agent layer (no MCP)
  pi            Place extension in ~/.pi/shared/extensions or ~/.pi/agent/extensions (no MCP)
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
		case "pi":
			setupPi();
			break;
		case "oh-my-pi":
			setupOhMyPi();
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
				"Valid options: opencode, claude-code, pi, oh-my-pi, cursor, codex, vscode, daemon",
			);
			process.exit(1);
	}
}
