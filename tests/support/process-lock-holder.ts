/**
 * Cross-process test helper: a child process that holds a process lock (see
 * src/process-lock.ts) until its stdin closes or it is killed.
 *
 * Run as a script (`bun tests/support/process-lock-holder.ts <lockPath>`) it
 * acquires the lock, prints "held", and waits. Exit code 3 and "busy" mean the
 * lock was already held by someone else.
 */
import { tryAcquireProcessLock } from "../../src/process-lock";

export interface LockHolderChild {
	pid: number;
	/** Close the child's stdin so it releases the lock and exits cleanly. */
	release(): Promise<void>;
	/** SIGKILL: simulates a crash, so the lockfile is left behind. */
	kill(): Promise<void>;
}

/** Start a child that holds `lockPath`; resolves once it reports "held". */
export async function startLockHolder(
	lockPath: string,
): Promise<LockHolderChild> {
	const child = Bun.spawn([process.execPath, import.meta.path, lockPath], {
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	const reader = child.stdout.getReader();
	const decoder = new TextDecoder();
	let text = "";
	while (!text.includes("\n")) {
		const { value, done } = await reader.read();
		if (done) break;
		text += decoder.decode(value, { stream: true });
	}
	reader.releaseLock();
	if (!text.startsWith("held")) {
		throw new Error(`lock holder did not acquire ${lockPath}: ${text.trim()}`);
	}
	return {
		pid: child.pid,
		release: async () => {
			child.stdin.end();
			await child.exited;
		},
		kill: async () => {
			child.kill("SIGKILL");
			await child.exited;
		},
	};
}

if (import.meta.main) {
	const lockPath = process.argv[2];
	const lock = tryAcquireProcessLock(lockPath);
	if (!lock) {
		console.log("busy");
		process.exit(3);
	}
	console.log("held");
	// Block until the parent closes our stdin.
	for await (const _chunk of Bun.stdin.stream()) {
		// drain
	}
	lock.release();
}
