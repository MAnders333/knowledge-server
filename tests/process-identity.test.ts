import { describe, expect, it } from "bun:test";
import {
	type ProcessTable,
	classifyPidFile,
	isDaemonCommand,
	isServerCommand,
	parsePid,
	startupPidAction,
} from "../src/process-identity";

function table(procs: Record<number, string | null>): ProcessTable {
	return {
		alive: (pid) => pid in procs,
		command: (pid) => procs[pid] ?? null,
	};
}

describe("parsePid", () => {
	it("accepts positive decimal integers, with surrounding whitespace", () => {
		expect(parsePid("4242\n")).toBe(4242);
		expect(parsePid(" 7 ")).toBe(7);
	});

	it("rejects zero, negative, non-numeric and partially numeric values", () => {
		for (const raw of ["0", "-1", "", "abc", "12abc", "1.5", "NaN"]) {
			expect(parsePid(raw)).toBeNull();
		}
	});
});

describe("isServerCommand and isDaemonCommand", () => {
	it("recognises the compiled binaries", () => {
		expect(isServerCommand("/usr/local/libexec/knowledge-server")).toBe(true);
		expect(isDaemonCommand("/usr/local/libexec/knowledge-daemon")).toBe(true);
	});

	it("recognises bun running the source entry points", () => {
		expect(isServerCommand("bun run src/index.ts")).toBe(true);
		expect(isServerCommand("/opt/bun run /work/repo/src/index.ts")).toBe(true);
		expect(isDaemonCommand("bun run /work/repo/src/daemon/index.ts")).toBe(
			true,
		);
	});

	it("matches an install path that contains spaces", () => {
		expect(isServerCommand("/Users/me/My Apps/libexec/knowledge-server")).toBe(
			true,
		);
		expect(
			isServerCommand("/opt/My Tools/bun run /Users/me/My Repo/src/index.ts"),
		).toBe(true);
		expect(
			isDaemonCommand(
				"/Users/me/My Apps/libexec/knowledge-daemon --interval=300",
			),
		).toBe(true);
	});

	it("keeps the server and the daemon apart", () => {
		expect(isServerCommand("bun run /work/repo/src/daemon/index.ts")).toBe(
			false,
		);
		expect(isDaemonCommand("bun run /work/repo/src/index.ts")).toBe(false);
		expect(isServerCommand("/usr/local/libexec/knowledge-daemon")).toBe(false);
	});

	it("rejects unrelated programs and a lookalike path", () => {
		expect(isServerCommand("/usr/bin/some-other-tool --serve")).toBe(false);
		expect(isServerCommand("node /tmp/myapp/src/index.ts")).toBe(false);
		expect(isServerCommand("bun run /tmp/index.ts")).toBe(false);
	});
});

describe("classifyPidFile", () => {
	const isServer = isServerCommand;

	it("is invalid for a non-positive or malformed PID", () => {
		expect(classifyPidFile("-1", table({}), isServer)).toEqual({
			kind: "invalid",
		});
		expect(classifyPidFile("0", table({}), isServer)).toEqual({
			kind: "invalid",
		});
	});

	it("is stale when the process is gone", () => {
		expect(classifyPidFile("9", table({}), isServer)).toEqual({
			kind: "stale",
			pid: 9,
			reason: "dead",
		});
	});

	it("is stale when the PID now belongs to another program", () => {
		expect(
			classifyPidFile("9", table({ 9: "/usr/bin/other --x" }), isServer),
		).toEqual({ kind: "stale", pid: 9, reason: "other-program" });
	});

	it("is unverified when the live process command cannot be read", () => {
		expect(classifyPidFile("9", table({ 9: null }), isServer)).toEqual({
			kind: "unverified",
			pid: 9,
		});
	});

	it("is running when the PID runs the expected program", () => {
		expect(
			classifyPidFile(
				"9\n",
				table({ 9: "/usr/local/libexec/knowledge-server" }),
				isServer,
			),
		).toEqual({ kind: "running", pid: 9 });
	});
});

describe("server identity with subcommands", () => {
	it("does not treat a process started with a subcommand as the server", () => {
		for (const command of [
			"/usr/local/libexec/knowledge-server mcp",
			"/usr/local/libexec/knowledge-server status",
			"/usr/local/libexec/knowledge-server consolidate --retry-dead",
			"/Users/me/My Apps/knowledge-server reinitialize --confirm",
			"bun run /repo/src/index.ts mcp",
			"bun run /repo/src/index.ts consolidate",
		]) {
			expect(isServerCommand(command), command).toBe(false);
		}
	});

	it("still recognises the server started with no arguments", () => {
		expect(isServerCommand("bun run --watch src/index.ts")).toBe(true);
		expect(isServerCommand("/usr/local/libexec/knowledge-server")).toBe(true);
	});

	it("keeps accepting daemon flags", () => {
		expect(
			isDaemonCommand("/usr/local/libexec/knowledge-daemon --interval=300"),
		).toBe(true);
	});
});

describe("startupPidAction", () => {
	const SERVER = "/usr/local/libexec/knowledge-server";

	it("refuses to start while the PID file names a running server", () => {
		expect(startupPidAction("9", table({ 9: SERVER }))).toBe("refuse");
	});

	it("keeps the file when the PID is alive but its command line is unreadable", () => {
		expect(startupPidAction("9", table({ 9: null }))).toBe("keep");
	});

	it("removes the file when the PID is gone", () => {
		expect(startupPidAction("9", table({}))).toBe("remove");
	});

	it("removes the file when the PID now runs another program or a server subcommand", () => {
		expect(
			startupPidAction("9", table({ 9: "/usr/bin/some-other-tool" })),
		).toBe("remove");
		expect(startupPidAction("9", table({ 9: `${SERVER} status` }))).toBe(
			"remove",
		);
	});

	it("removes a malformed file", () => {
		expect(startupPidAction("-1", table({ 9: SERVER }))).toBe("remove");
	});
});
