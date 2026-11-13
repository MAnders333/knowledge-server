import {
	chmodSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";

/**
 * Env vars that point setup and reader code at a home-like directory. Each is
 * redirected under the temp HOME so no run can reach the real user config.
 */
const HOME_ENV_VARS = [
	"HOME",
	"CURSOR_HOME",
	"CODEX_HOME",
	"CLAUDE_CONFIG_DIR",
	"CLAUDE_DB_PATH",
	"XDG_CONFIG_HOME",
	"KNOWLEDGE_CONFIG_HOME",
] as const;

export interface SetupIsolation {
	/** Temp HOME that setup-tool resolves its service and config paths under. */
	home: string;
	/** Written by the launchctl, systemctl and claude stubs: one line per invocation. */
	callsLog: string;
	restore(): void;
}

/**
 * Points HOME and every other home-like env var (CURSOR_HOME, CODEX_HOME,
 * CLAUDE_CONFIG_DIR, CLAUDE_DB_PATH, XDG_CONFIG_HOME, KNOWLEDGE_CONFIG_HOME) at a
 * temp dir, and prepends a PATH dir whose `launchctl`, `systemctl` and `claude`
 * stubs only record their argv. Setup runs that try to register a background
 * service, or the MCP server with Claude Code, then leave a call record instead of
 * touching launchd, systemd or the real CLI. Works from any cwd: it needs no
 * bunfig.toml or preload.
 */
export function isolateSetupTool(): SetupIsolation {
	const root = mkdtempSync(join(tmpdir(), "setup-isolation-"));
	const home = join(root, "home");
	const bin = join(root, "bin");
	const callsLog = join(root, "service-calls.log");
	mkdirSync(home);
	mkdirSync(bin);
	for (const name of ["launchctl", "systemctl", "claude"]) {
		const stub = join(bin, name);
		writeFileSync(stub, `#!/bin/sh\necho "${name} $*" >> "${callsLog}"\n`);
		chmodSync(stub, 0o755);
	}

	const saved = new Map(
		HOME_ENV_VARS.map((name) => [name, process.env[name]] as const),
	);
	const savedPath = process.env.PATH;
	process.env.HOME = home;
	process.env.CURSOR_HOME = join(home, ".cursor");
	process.env.CODEX_HOME = join(home, ".codex");
	process.env.CLAUDE_CONFIG_DIR = join(home, ".claude");
	process.env.CLAUDE_DB_PATH = join(home, ".claude");
	process.env.XDG_CONFIG_HOME = join(home, ".config");
	process.env.KNOWLEDGE_CONFIG_HOME = join(home, ".config", "knowledge-server");
	process.env.PATH = `${bin}${delimiter}${savedPath ?? ""}`;

	return {
		home,
		callsLog,
		restore() {
			for (const [name, value] of saved) {
				if (value === undefined) Reflect.deleteProperty(process.env, name);
				else process.env[name] = value;
			}
			process.env.PATH = savedPath;
			rmSync(root, { recursive: true, force: true });
		},
	};
}
