/**
 * Single-instance daemon lock across processes. A daemon that finds the lock held
 * must idle (not exit, which launchd/systemd would restart in a loop) and take
 * over once the holder is gone.
 */
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireDaemonLock } from "../src/daemon/single-instance";
import { startLockHolder } from "./support/process-lock-holder";

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
	for (const fn of cleanups.splice(0)) await fn();
});

function tempLockPath(): { dir: string; lockPath: string } {
	const dir = mkdtempSync(join(tmpdir(), "ks-daemon-lock-test-"));
	cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
	return { dir, lockPath: join(dir, "daemon.lock") };
}

describe("acquireDaemonLock", () => {
	it("takes a free lock without waiting", async () => {
		const { lockPath } = tempLockPath();
		const { lock, waited } = await acquireDaemonLock(lockPath, {
			retryMs: 10,
			jitterMs: 0,
		});
		expect(waited).toBe(false);
		lock.release();
	});

	it("a second daemon waits, logs once, and takes over when the holder dies", async () => {
		const { lockPath } = tempLockPath();
		const holder = await startLockHolder(lockPath);
		const waiter = Bun.spawn(
			[
				process.execPath,
				join(import.meta.dir, "support", "daemon-lock-waiter.ts"),
				lockPath,
				"50",
			],
			{ stdin: "pipe", stdout: "pipe", stderr: "pipe" },
		);
		cleanups.push(() => {
			waiter.kill();
		});
		const reader = waiter.stdout.getReader();
		const decoder = new TextDecoder();
		let output = "";
		const readUntil = async (needle: string): Promise<void> => {
			while (!output.includes(needle)) {
				const { value, done } = await reader.read();
				if (done) throw new Error(`waiter exited early: ${output}`);
				output += decoder.decode(value, { stream: true });
			}
		};

		await readUntil("WAITING");
		// The waiter does not exit while the lock is held; it is still alive after a retry.
		await new Promise((r) => setTimeout(r, 200));
		expect(waiter.exitCode).toBeNull();

		await holder.kill();
		await readUntil("ACQUIRED waited=true");

		expect(output.match(/WAITING/g)).toHaveLength(1);
		waiter.stdin.end();
		expect(await waiter.exited).toBe(0);
	});
});
