/**
 * Cross-process test helper: a daemon-style waiter (see src/daemon/single-instance.ts).
 * Run as `bun tests/support/daemon-lock-waiter.ts <lockPath> <retryMs>`. It prints
 * "WAITING <pid>" once when the lock is held, then "ACQUIRED waited=<bool>" when it
 * takes the lock, and holds it until its stdin closes.
 */
import { acquireDaemonLock } from "../../src/daemon/single-instance";

const [lockPath, retryMs] = process.argv.slice(2);
const { lock, waited } = await acquireDaemonLock(lockPath, {
	retryMs: Number(retryMs),
	jitterMs: 0,
	onWaiting: (pid) => console.log(`WAITING ${pid}`),
});
console.log(`ACQUIRED waited=${waited}`);
process.stdin.resume();
process.stdin.on("end", () => {
	lock.release();
	process.exit(0);
});
