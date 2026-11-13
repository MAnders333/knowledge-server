import {
	type LockHandle,
	lockHolder,
	tryAcquireProcessLock,
} from "../process-lock.js";

/** How often a daemon that found the lock held retries it. */
export const DAEMON_LOCK_RETRY_MS = 30_000;
/** Random extra delay per retry, so several waiting daemons do not retry in step. */
const DAEMON_LOCK_JITTER_MS = 5_000;

interface DaemonLockWaitOptions {
	retryMs?: number;
	jitterMs?: number;
	/** Called once, with the holder's PID (0 when unknown), before the first wait. */
	onWaiting?: (holderPid: number) => void;
	/** Called if this daemon later finds its lock taken over (see LockOptions.onLost). */
	onLost?: () => void;
}

/**
 * Take the daemon's single-instance lock, waiting for it when another daemon holds it.
 *
 * A daemon that loses the lock does not exit. Under launchd (KeepAlive) or systemd
 * (Restart=) an exiting daemon is restarted over and over, one restart per throttle
 * interval. So the loser idles here, retrying with jitter, and takes over as soon
 * as the holder exits or its lock goes stale. `waited` tells the caller whether it
 * had to wait.
 */
export async function acquireDaemonLock(
	lockPath: string,
	options: DaemonLockWaitOptions = {},
): Promise<{ lock: LockHandle; waited: boolean }> {
	const lockOptions = { onLost: options.onLost };
	const direct = tryAcquireProcessLock(lockPath, lockOptions);
	if (direct) return { lock: direct, waited: false };

	options.onWaiting?.(lockHolder(lockPath)?.pid ?? 0);
	const retryMs = options.retryMs ?? DAEMON_LOCK_RETRY_MS;
	const jitterMs = options.jitterMs ?? DAEMON_LOCK_JITTER_MS;
	for (;;) {
		await new Promise((resolve) =>
			setTimeout(resolve, retryMs + Math.random() * jitterMs),
		);
		const lock = tryAcquireProcessLock(lockPath, lockOptions);
		if (lock) return { lock, waited: true };
	}
}
