import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type StopDeps, runStop } from "../src/commands/stop";

const SERVER = "/usr/local/libexec/knowledge-server --port 3179";
const SOURCE_SERVER = "/opt/homebrew/bin/bun run /work/repo/src/index.ts";
const DAEMON = "/usr/local/libexec/knowledge-daemon --interval=300";
const UNRELATED = "/usr/bin/some-other-tool --serve";
const PID = 4242;

interface Proc {
	command: string | null;
	alive: boolean;
	/** Process exits when it receives SIGTERM. */
	exitsOnTerm: boolean;
}

/** Fake process table. Signals are recorded instead of sent. */
function fakeOs(procs: Record<number, Proc>) {
	const signals: Array<[number, string]> = [];
	const deps: StopDeps = {
		table: {
			alive: (pid) => procs[pid]?.alive ?? false,
			command: (pid) => procs[pid]?.command ?? null,
		},
		signal: (pid, sig) => {
			signals.push([pid, sig]);
			const proc = procs[pid];
			if (!proc?.alive)
				throw Object.assign(new Error("ESRCH"), { code: "ESRCH" });
			if (proc.exitsOnTerm) proc.alive = false;
		},
		sleep: async () => {},
	};
	return { deps, signals };
}

/** Runs `runStop` and captures what it prints. */
async function stop(
	pidPath: string,
	deps: StopDeps,
	legacyPidPath: string | null = null,
) {
	const out: string[] = [];
	const log = console.log;
	const error = console.error;
	console.log = (...args: unknown[]) => out.push(args.join(" "));
	console.error = (...args: unknown[]) => out.push(args.join(" "));
	try {
		const code = await runStop(pidPath, legacyPidPath, deps);
		return { code, out: out.join("\n") };
	} finally {
		console.log = log;
		console.error = error;
	}
}

describe("runStop: PID file identity", () => {
	let dir: string;
	let pidPath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-stop-"));
		pidPath = join(dir, "server.pid");
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("uses the legacy PID file when the configured one is missing", async () => {
		const legacy = join(dir, "legacy.pid");
		writeFileSync(legacy, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: SERVER, alive: true, exitsOnTerm: true },
		});
		const { code } = await stop(pidPath, deps, legacy);
		expect(code).toBe(0);
		expect(signals).toEqual([[PID, "SIGTERM"]]);
		expect(existsSync(legacy)).toBe(false);
	});

	it("reports not running when there is no PID file", async () => {
		const { deps, signals } = fakeOs({});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([]);
	});

	for (const raw of ["-1", "0", "abc", ""]) {
		it(`rejects the PID value ${JSON.stringify(raw)} without signalling`, async () => {
			writeFileSync(pidPath, raw);
			const { deps, signals } = fakeOs({
				[-1]: { command: SERVER, alive: true, exitsOnTerm: true },
			});
			const { code } = await stop(pidPath, deps);
			expect(code).toBe(1);
			expect(signals).toEqual([]);
			expect(existsSync(pidPath)).toBe(false);
		});
	}

	it("removes a stale PID file whose process is gone", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([]);
		expect(existsSync(pidPath)).toBe(false);
	});

	it("does not signal a reused PID that now runs another program", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: UNRELATED, alive: true, exitsOnTerm: true },
		});
		const { code, out } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([]);
		expect(out).toContain("not a knowledge-server");
		expect(existsSync(pidPath)).toBe(false);
	});

	it("does not signal a reused PID that runs the knowledge-daemon", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: DAEMON, alive: true, exitsOnTerm: true },
		});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([]);
	});

	it("fails closed when the live PID's command line cannot be read", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: null, alive: true, exitsOnTerm: true },
		});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(1);
		expect(signals).toEqual([]);
		expect(existsSync(pidPath)).toBe(true);
	});

	it("signals a PID that runs the compiled knowledge-server and waits for exit", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: SERVER, alive: true, exitsOnTerm: true },
		});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([[PID, "SIGTERM"]]);
		expect(existsSync(pidPath)).toBe(false);
	});

	it("signals a PID that runs the server from source", async () => {
		writeFileSync(pidPath, String(PID));
		const { deps, signals } = fakeOs({
			[PID]: { command: SOURCE_SERVER, alive: true, exitsOnTerm: true },
		});
		const { code } = await stop(pidPath, deps);
		expect(code).toBe(0);
		expect(signals).toEqual([[PID, "SIGTERM"]]);
	});
});
