import { existsSync, readFileSync, unlinkSync } from "node:fs";
import {
	type ProcessTable,
	classifyPidFile,
	isServerCommand,
	systemProcessTable,
} from "../process-identity.js";
import { errCode } from "../utils.js";

/**
 * `knowledge-server stop`
 *
 * Reads the PID file, sends SIGTERM to the running server, waits for it to
 * exit, then removes the PID file. The PID is signalled only when its command
 * line still shows a knowledge-server: a stale file whose PID was reused by
 * another program is removed, never signalled.
 *
 * Returns the exit code:
 *   0 — server stopped (or was not running)
 *   1 — unexpected error, or the PID could not be verified (not signalled)
 */

export interface StopDeps {
	table: ProcessTable;
	signal: (pid: number, sig: NodeJS.Signals) => void;
	sleep: (ms: number) => Promise<void>;
}

const defaultDeps: StopDeps = {
	table: systemProcessTable,
	signal: (pid, sig) => process.kill(pid, sig),
	sleep: (ms) => {
		const { promise, resolve } = Promise.withResolvers<void>();
		setTimeout(resolve, ms);
		return promise;
	},
};

export async function runStop(
	pidPath: string,
	deps: StopDeps = defaultDeps,
): Promise<number> {
	if (!pidPath) {
		console.error(
			"PID file path is not configured (KNOWLEDGE_PID_PATH is empty). Cannot stop server.",
		);
		return 1;
	}

	if (!existsSync(pidPath)) {
		console.log("Knowledge server is not running (no PID file found).");
		return 0;
	}

	const raw = readFileSync(pidPath, "utf8");
	const state = classifyPidFile(raw, deps.table, isServerCommand);

	if (state.kind === "invalid") {
		console.error(
			`PID file contains invalid value: "${raw.trim()}". Removing it.`,
		);
		unlinkSync(pidPath);
		return 1;
	}

	if (state.kind === "stale") {
		console.log(
			state.reason === "dead"
				? `Knowledge server (PID ${state.pid}) is not running — removing stale PID file.`
				: `PID ${state.pid} in the PID file is not a knowledge-server process — removing stale PID file.`,
		);
		unlinkSync(pidPath);
		return 0;
	}

	const pid = state.pid;
	if (state.kind === "unverified") {
		console.error(
			`Cannot verify that PID ${pid} is the knowledge server (its command line is unreadable), so it was not signalled. ` +
				`If it is the server, stop it manually: kill ${pid}. Otherwise remove ${pidPath}.`,
		);
		return 1;
	}

	// Send SIGTERM and wait for the process to exit.
	console.log(`Stopping knowledge server (PID ${pid})...`);
	try {
		deps.signal(pid, "SIGTERM");
	} catch (err: unknown) {
		if (errCode(err) === "ESRCH") {
			// Raced — process exited between the liveness check and the kill.
			console.log("Server already stopped.");
			if (existsSync(pidPath)) unlinkSync(pidPath);
			return 0;
		}
		console.error(`Failed to send SIGTERM to PID ${pid}: ${err}`);
		return 1;
	}

	// Poll until the process is gone, up to STOP_TIMEOUT_MS.
	// Must exceed the server's own SHUTDOWN_TIMEOUT_MS (30 s) so we don't
	// declare failure while the server is still draining in-flight LLM calls.
	const STOP_TIMEOUT_MS = 35_000;
	const POLL_INTERVAL_MS = 100;
	// Print a notice if the server is still alive after this long. The server
	// drains in-flight LLM/consolidation work before exiting, so a slow shutdown
	// is normal and expected — this message tells the user to keep waiting rather
	// than Ctrl-C prematurely. Note: the notice fires based on elapsed time only;
	// we have no direct visibility into whether consolidation is actually running.
	const NOTICE_MS = 2_000;
	const deadline = Date.now() + STOP_TIMEOUT_MS;
	const start = Date.now();
	let noticePrinted = false;

	while (Date.now() < deadline) {
		await deps.sleep(POLL_INTERVAL_MS);
		if (deps.table.alive(pid)) {
			if (!noticePrinted && Date.now() - start >= NOTICE_MS) {
				console.log(
					`Server is taking a moment to shut down — draining any in-flight work (up to ${Math.round(STOP_TIMEOUT_MS / 1000)}s)...`,
				);
				noticePrinted = true;
			}
			continue;
		}
		console.log("Knowledge server stopped.");
		// The server cleans up its own PID file on graceful shutdown.
		// If it crashed before doing so, clean up here.
		if (existsSync(pidPath)) unlinkSync(pidPath);
		return 0;
	}

	console.error(
		`Server (PID ${pid}) did not exit within ${STOP_TIMEOUT_MS / 1000}s after SIGTERM.`,
	);
	console.error(`Try: kill -9 ${pid}`);
	return 1;
}
