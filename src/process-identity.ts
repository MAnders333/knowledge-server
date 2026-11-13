import { spawnSync } from "node:child_process";
import { basename } from "node:path";
import { isProcessAlive } from "./process-lock.js";

/**
 * Process identity checks for PID files. A PID is only trusted when the process
 * it names still runs the expected program: PIDs are reused after a crash or
 * reboot, and a stale PID file must never make us signal or block on an
 * unrelated process.
 */

/** Process-table access. Injected in tests; the system table reads the live OS. */
export interface ProcessTable {
	alive(pid: number): boolean;
	/** Full command line, or null when it cannot be read. */
	command(pid: number): string | null;
}

/** Full command line of `pid` via `ps` (macOS and Linux), or null. */
export function readCommand(pid: number): string | null {
	const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
		encoding: "utf8",
		stdio: ["ignore", "pipe", "ignore"],
	});
	const out = result.status === 0 ? result.stdout.trim() : "";
	return out === "" ? null : out;
}

export const systemProcessTable: ProcessTable = {
	alive: isProcessAlive,
	command: readCommand,
};

/** Parse a PID file body. Null unless it is a positive decimal integer. */
export function parsePid(raw: string): number | null {
	const text = raw.trim();
	if (!/^\d+$/.test(text)) return null;
	const pid = Number(text);
	return Number.isSafeInteger(pid) && pid > 0 ? pid : null;
}

/**
 * True when a command line runs the compiled `binary`, or runs `bun` on the
 * source `sourceEntry` (for example `src/index.ts`).
 */
export function isCommandOf(
	command: string,
	binary: string,
	sourceEntry: string,
): boolean {
	const tokens = command.trim().split(/\s+/);
	const program = basename(tokens[0] ?? "");
	if (program === binary) return true;
	return (
		program.startsWith("bun") &&
		tokens.some((t) => t === sourceEntry || t.endsWith(`/${sourceEntry}`))
	);
}

export function isServerCommand(command: string): boolean {
	return isCommandOf(command, "knowledge-server", "src/index.ts");
}

export function isDaemonCommand(command: string): boolean {
	return isCommandOf(command, "knowledge-daemon", "src/daemon/index.ts");
}

export type PidFileState =
	/** The file does not hold a positive PID. */
	| { kind: "invalid" }
	/** The PID is gone, or it now belongs to another program. */
	| { kind: "stale"; pid: number; reason: "dead" | "other-program" }
	/** The PID is alive but its command line cannot be read, so identity is unknown. */
	| { kind: "unverified"; pid: number }
	/** The PID is alive and runs the expected program. */
	| { kind: "running"; pid: number };

/** Classify the contents of a PID file against a process table. */
export function classifyPidFile(
	raw: string,
	table: ProcessTable,
	isExpected: (command: string) => boolean,
): PidFileState {
	const pid = parsePid(raw);
	if (pid === null) return { kind: "invalid" };
	if (!table.alive(pid)) return { kind: "stale", pid, reason: "dead" };
	const command = table.command(pid);
	if (command === null) return { kind: "unverified", pid };
	return isExpected(command)
		? { kind: "running", pid }
		: { kind: "stale", pid, reason: "other-program" };
}
