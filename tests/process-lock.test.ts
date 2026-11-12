import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
	lockHolder,
	readLockRecord,
	tryAcquireProcessLock,
} from "../src/process-lock";
import { startLockHolder } from "./support/process-lock-holder";

/** A pid that is guaranteed dead: spawn a short-lived child and wait for it. */
async function deadPid(): Promise<number> {
	const child = Bun.spawn([process.execPath, "-e", "0"], {
		stdout: "ignore",
		stderr: "ignore",
	});
	await child.exited;
	return child.pid;
}

describe("tryAcquireProcessLock", () => {
	let dir: string;
	let lockPath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-process-lock-"));
		lockPath = join(dir, "nested", "test.lock");
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("creates the lockfile with this pid and releases it", () => {
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		expect(readLockRecord(lockPath)?.pid).toBe(process.pid);
		handle?.release();
		expect(existsSync(lockPath)).toBe(false);
	});

	it("refuses a second acquire while the first is held (same process too)", () => {
		const first = tryAcquireProcessLock(lockPath);
		expect(first).not.toBeNull();
		expect(tryAcquireProcessLock(lockPath)).toBeNull();
		first?.release();
		const again = tryAcquireProcessLock(lockPath);
		expect(again).not.toBeNull();
		again?.release();
	});

	it("release does not remove a lock another process took over", () => {
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		writeFileSync(
			lockPath,
			JSON.stringify({ pid: 1, acquiredAt: 0, heartbeatAt: Date.now() }),
		);
		handle?.release();
		expect(readLockRecord(lockPath)?.pid).toBe(1);
	});

	it("takes over a lock whose holder is dead", async () => {
		const pid = await deadPid();
		const now = Date.now();
		mkdirSync(dirname(lockPath), { recursive: true });
		writeFileSync(
			lockPath,
			JSON.stringify({ pid, acquiredAt: now, heartbeatAt: now }),
		);
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		expect(readLockRecord(lockPath)?.pid).toBe(process.pid);
		handle?.release();
	});

	it("takes over a lock whose heartbeat expired even if the pid is alive", () => {
		mkdirSync(dirname(lockPath), { recursive: true });
		const old = Date.now() - 60 * 60_000;
		writeFileSync(
			lockPath,
			JSON.stringify({ pid: process.pid, acquiredAt: old, heartbeatAt: old }),
		);
		expect(lockHolder(lockPath)).toBeNull();
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		handle?.release();
	});

	it("treats a live holder with a fresh heartbeat as held", () => {
		mkdirSync(dirname(lockPath), { recursive: true });
		const now = Date.now();
		writeFileSync(
			lockPath,
			JSON.stringify({ pid: process.pid, acquiredAt: now, heartbeatAt: now }),
		);
		expect(lockHolder(lockPath)?.pid).toBe(process.pid);
		expect(tryAcquireProcessLock(lockPath)).toBeNull();
	});

	it("treats a freshly created but unreadable lockfile as held, a stale one as free", () => {
		mkdirSync(dirname(lockPath), { recursive: true });
		writeFileSync(lockPath, "");
		expect(lockHolder(lockPath)?.pid).toBe(0);
		expect(tryAcquireProcessLock(lockPath)).toBeNull();

		const longAgo = new Date(Date.now() - 10 * 60_000);
		utimesSync(lockPath, longAgo, longAgo);
		expect(lockHolder(lockPath)).toBeNull();
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		handle?.release();
	});

	it("keeps the heartbeat fresh while held", async () => {
		// Real timer on purpose: the heartbeat is a setInterval, so only the platform
		// clock exercises it. The interval is 20 ms, so the wait stays short.
		const handle = tryAcquireProcessLock(lockPath, {
			heartbeatMs: 20,
		});
		expect(handle).not.toBeNull();
		const before = readLockRecord(lockPath)?.heartbeatAt ?? 0;
		await Bun.sleep(80);
		const after = readLockRecord(lockPath)?.heartbeatAt ?? 0;
		expect(after).toBeGreaterThan(before);
		handle?.release();
	});
});

/** First line a child printed; the child keeps running, so stdout never reaches EOF. */
async function readFirstLine(
	stream: ReadableStream<Uint8Array>,
): Promise<string> {
	const reader = stream.getReader();
	const decoder = new TextDecoder();
	let text = "";
	while (!text.includes("\n")) {
		const { value, done } = await reader.read();
		if (done) break;
		text += decoder.decode(value, { stream: true });
	}
	reader.releaseLock();
	return text.split("\n")[0].trim();
}

describe("process lock across processes", () => {
	let dir: string;
	let lockPath: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-process-lock-xp-"));
		lockPath = join(dir, "shared.lock");
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("a lock held by another process blocks this one and names its pid", async () => {
		const holder = await startLockHolder(lockPath);
		try {
			expect(tryAcquireProcessLock(lockPath)).toBeNull();
			expect(lockHolder(lockPath)?.pid).toBe(holder.pid);
		} finally {
			await holder.release();
		}
		const after = tryAcquireProcessLock(lockPath);
		expect(after).not.toBeNull();
		after?.release();
	});

	it("a crashed holder's lock is taken over", async () => {
		const holder = await startLockHolder(lockPath);
		await holder.kill();
		expect(readLockRecord(lockPath)?.pid).toBe(holder.pid);
		const handle = tryAcquireProcessLock(lockPath);
		expect(handle).not.toBeNull();
		expect(readLockRecord(lockPath)?.pid).toBe(process.pid);
		handle?.release();
	});

	it("two concurrent starts: exactly one wins", async () => {
		const runs = await Promise.all(
			Array.from({ length: 4 }, () =>
				Bun.spawn(
					[
						process.execPath,
						join(import.meta.dir, "support", "process-lock-holder.ts"),
						lockPath,
					],
					{ stdin: "pipe", stdout: "pipe", stderr: "ignore" },
				),
			),
		);
		const outputs = await Promise.all(
			runs.map((p) => readFirstLine(p.stdout as ReadableStream<Uint8Array>)),
		);
		// Each child prints "held" or "busy" immediately; the winner keeps the lock
		// until its stdin closes, so all four race before anyone releases.
		const held = outputs.filter((o) => o === "held").length;
		expect(held).toBe(1);
		for (const p of runs) {
			p.stdin.end();
			await p.exited;
		}
	});
});
