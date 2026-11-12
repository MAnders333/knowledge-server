import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { readDaemonStatus } from "../daemon/status.js";
import { isProcessAlive, lockHolder } from "../process-lock.js";
import { daemonLockPath, daemonStatusPath } from "../runtime-files.js";

const LAUNCHD_LABEL = "com.knowledge-server.daemon";
const SYSTEMD_UNIT = "knowledge-daemon";
/** How long a stopped daemon may take to exit before it is reported as still running. */
const STOP_WAIT_MS = 10_000;
const STOP_POLL_MS = 100;

export type DaemonRestartOutcome =
	| "service-restarted"
	| "daemon-stopped"
	| "not-running"
	| "failed";

export interface DaemonRestartEnv {
	platform: NodeJS.Platform;
	home: string;
	uid: number;
	dataDir: string;
	/** Runs a command and returns its exit status (null when it cannot start). */
	run?: (cmd: string, args: string[]) => number | null;
	/** Sends a signal to a process. Throws when the process is gone. */
	signal?: (pid: number, signal: NodeJS.Signals) => void;
	alive?: (pid: number) => boolean;
	sleep?: (ms: number) => Promise<void>;
	log: (line: string) => void;
	warn: (line: string) => void;
}

function defaultRun(cmd: string, args: string[]): number | null {
	return spawnSync(cmd, args, { stdio: "ignore" }).status;
}

/**
 * After an update the running daemon still executes the old binary. Restart it so the
 * new one takes over:
 *  - a registered launchd or systemd (user) service is restarted in place;
 *  - otherwise the running daemon (lock holder) is stopped with SIGTERM. The server's
 *    auto-spawn, or a manual start, then runs the new binary.
 * Best effort: problems are logged with the manual command, never thrown.
 */
export async function restartDaemonAfterUpdate(
	env: DaemonRestartEnv,
): Promise<DaemonRestartOutcome> {
	const run = env.run ?? defaultRun;

	if (env.platform === "darwin") {
		const plist = join(
			env.home,
			"Library",
			"LaunchAgents",
			`${LAUNCHD_LABEL}.plist`,
		);
		if (existsSync(plist)) {
			const target = `gui/${env.uid}/${LAUNCHD_LABEL}`;
			const status = run("launchctl", ["kickstart", "-k", target]);
			if (status === 0) {
				env.log("  ✓ Restarted the launchd daemon service");
				return "service-restarted";
			}
			env.warn(
				`  ⚠ Could not restart the launchd daemon (exit ${status}). Run: launchctl kickstart -k ${target}`,
			);
			return "failed";
		}
	} else if (env.platform === "linux") {
		const unit = join(
			env.home,
			".config",
			"systemd",
			"user",
			`${SYSTEMD_UNIT}.service`,
		);
		if (existsSync(unit)) {
			const status = run("systemctl", ["--user", "restart", SYSTEMD_UNIT]);
			if (status === 0) {
				env.log("  ✓ Restarted the systemd daemon service");
				return "service-restarted";
			}
			env.warn(
				`  ⚠ Could not restart the systemd daemon (exit ${status}). Run: systemctl --user restart ${SYSTEMD_UNIT}`,
			);
			return "failed";
		}
	}

	return stopRunningDaemon(env);
}

async function stopRunningDaemon(
	env: DaemonRestartEnv,
): Promise<DaemonRestartOutcome> {
	const alive = env.alive ?? isProcessAlive;
	const holder = lockHolder(daemonLockPath(env.dataDir));
	if (holder && holder.pid === 0) {
		env.warn(
			"  ⚠ A knowledge-daemon is starting but has not written its PID yet. Stop it manually once it is running, then start it again.",
		);
		return "failed";
	}
	let pid = holder?.pid ?? null;
	if (pid === null) {
		const status = readDaemonStatus(daemonStatusPath(env.dataDir));
		if (status && status.pid > 0 && alive(status.pid)) pid = status.pid;
	}
	if (pid === null) {
		env.log(
			"  No knowledge-daemon is running; the next start uses the new binary.",
		);
		return "not-running";
	}

	const signal =
		env.signal ??
		((target: number, sig: NodeJS.Signals) => process.kill(target, sig));
	try {
		signal(pid, "SIGTERM");
	} catch (err) {
		// ESRCH: it exited between the check and the signal.
		if (alive(pid)) {
			env.warn(
				`  ⚠ Could not stop the old knowledge-daemon (PID ${pid}): ${err instanceof Error ? err.message : String(err)}. Stop it manually: kill ${pid}`,
			);
			return "failed";
		}
	}

	const sleep =
		env.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
	for (let waited = 0; alive(pid) && waited < STOP_WAIT_MS; ) {
		await sleep(STOP_POLL_MS);
		waited += STOP_POLL_MS;
	}
	if (alive(pid)) {
		env.warn(
			`  ⚠ The old knowledge-daemon (PID ${pid}) is still running after ${STOP_WAIT_MS / 1000} s. Stop it manually: kill ${pid}`,
		);
		return "failed";
	}
	env.log(
		`  ✓ Stopped the old knowledge-daemon (PID ${pid}). The server's auto-spawn, or knowledge-daemon, starts the new one.`,
	);
	return "daemon-stopped";
}
