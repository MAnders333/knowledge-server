import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { liveDaemonRefusal } from "../src/commands/reinitialize";
import { tryAcquireProcessLock } from "../src/process-lock";
import { daemonLockPath } from "../src/runtime-files";

let dataDir: string;

beforeEach(() => {
	dataDir = mkdtempSync(join(tmpdir(), "ks-reinit-"));
});

afterEach(() => {
	rmSync(dataDir, { recursive: true, force: true });
});

describe("reinitialize and a live daemon", () => {
	it("refuses, naming the daemon and the in-flight cursor write, while a daemon holds the lock", () => {
		const lock = tryAcquireProcessLock(daemonLockPath(dataDir));
		expect(lock).not.toBeNull();
		try {
			const message = liveDaemonRefusal(dataDir);
			expect(message).not.toBeNull();
			expect(message).toContain(`PID ${process.pid}`);
			expect(message).toContain("daemon_cursor");
			expect(message).toContain("Stop the daemon");
		} finally {
			lock?.release();
		}
	});

	it("does not refuse when no daemon holds the lock", () => {
		expect(liveDaemonRefusal(dataDir)).toBeNull();
	});
});
