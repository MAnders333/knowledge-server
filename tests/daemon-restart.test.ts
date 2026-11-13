import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { restartDaemonAfterUpdate } from "../src/commands/daemon-restart";
import { tryAcquireProcessLock } from "../src/process-lock";
import { daemonLockPath, daemonStatusPath } from "../src/runtime-files";

const COMPILED_DAEMON = "/usr/local/libexec/knowledge-daemon --interval=300";
const SOURCE_DAEMON =
	"/opt/homebrew/bin/bun run /work/repo/src/daemon/index.ts --interval=300";
const UNRELATED = "/usr/bin/some-other-tool --serve";
const STALE_PID = 4242;

describe("restartDaemonAfterUpdate: daemon identity before signalling", () => {
	let home: string;
	let dataDir: string;

	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "ks-restart-home-"));
		dataDir = mkdtempSync(join(tmpdir(), "ks-restart-data-"));
	});

	afterEach(() => {
		rmSync(home, { recursive: true, force: true });
		rmSync(dataDir, { recursive: true, force: true });
	});

	function writeStatus(pid: number): void {
		writeFileSync(
			daemonStatusPath(dataDir),
			JSON.stringify({
				pid,
				startedAt: 1,
				intervalMs: 0,
				lastCycleAt: null,
				consecutiveFailures: 0,
				lastError: null,
				lastUploadAt: null,
			}),
		);
	}

	/** Runs the restart with a process table stub; `alive` follows the signal. */
	async function restart(opts: {
		command: (pid: number) => string | null;
		alwaysAlive?: boolean;
	}) {
		const signals: Array<[number, string]> = [];
		let stopped = false;
		const logs: string[] = [];
		const outcome = await restartDaemonAfterUpdate({
			platform: "linux",
			home,
			uid: 1000,
			dataDir,
			command: opts.command,
			signal: (pid, sig) => {
				signals.push([pid, sig]);
				stopped = true;
			},
			alive: () => opts.alwaysAlive === true || !stopped,
			sleep: async () => {},
			log: (line) => logs.push(line),
			warn: (line) => logs.push(line),
		});
		return { outcome, signals };
	}

	it("does not signal a live PID from a stale status file that now belongs to another process", async () => {
		writeStatus(STALE_PID);
		const asked: number[] = [];
		const { outcome, signals } = await restart({
			command: (pid) => {
				asked.push(pid);
				return UNRELATED;
			},
			alwaysAlive: true,
		});
		expect(asked).toEqual([STALE_PID]);
		expect(signals).toEqual([]);
		expect(outcome).toBe("not-running");
	});

	it("does not signal a status-file PID whose command cannot be read", async () => {
		writeStatus(STALE_PID);
		const { outcome, signals } = await restart({
			command: () => null,
			alwaysAlive: true,
		});
		expect(signals).toEqual([]);
		expect(outcome).toBe("not-running");
	});

	it("signals a status-file PID that runs the compiled knowledge-daemon", async () => {
		writeStatus(STALE_PID);
		const { outcome, signals } = await restart({
			command: () => COMPILED_DAEMON,
		});
		expect(signals).toEqual([[STALE_PID, "SIGTERM"]]);
		expect(outcome).toBe("daemon-stopped");
	});

	it("signals a status-file PID that runs the source daemon entry point", async () => {
		writeStatus(STALE_PID);
		const { outcome, signals } = await restart({
			command: () => SOURCE_DAEMON,
		});
		expect(signals).toEqual([[STALE_PID, "SIGTERM"]]);
		expect(outcome).toBe("daemon-stopped");
	});

	it("does not signal a lock holder whose PID now belongs to another process", async () => {
		const lock = tryAcquireProcessLock(daemonLockPath(dataDir));
		expect(lock).not.toBeNull();
		try {
			const { outcome, signals } = await restart({
				command: () => UNRELATED,
				alwaysAlive: true,
			});
			expect(signals).toEqual([]);
			expect(outcome).toBe("not-running");
		} finally {
			lock?.release();
		}
	});

	it("signals a lock holder that runs the knowledge-daemon", async () => {
		const lock = tryAcquireProcessLock(daemonLockPath(dataDir));
		expect(lock).not.toBeNull();
		try {
			const { outcome, signals } = await restart({
				command: () => COMPILED_DAEMON,
			});
			expect(signals).toEqual([[process.pid, "SIGTERM"]]);
			expect(outcome).toBe("daemon-stopped");
		} finally {
			lock?.release();
		}
	});
});
