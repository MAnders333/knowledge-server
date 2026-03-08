import { existsSync, readFileSync, unlinkSync } from "node:fs";

/**
 * `knowledge-server stop`
 *
 * Reads the PID file, sends SIGTERM to the running server, waits for it to
 * exit, then removes the PID file. Handles stale PID files (process already
 * dead) gracefully — removes the file and exits with a clear message.
 *
 * Exit codes:
 *   0 — server stopped (or was not running)
 *   1 — unexpected error (permission denied, etc.)
 */
export async function runStop(pidPath: string): Promise<void> {
	if (!pidPath) {
		console.error(
			"PID file path is not configured (KNOWLEDGE_PID_PATH is empty). Cannot stop server.",
		);
		process.exit(1);
	}

	if (!existsSync(pidPath)) {
		console.log("Knowledge server is not running (no PID file found).");
		return;
	}

	const raw = readFileSync(pidPath, "utf8").trim();
	const pid = Number.parseInt(raw, 10);

	if (Number.isNaN(pid) || pid <= 0) {
		console.error(`PID file contains invalid value: "${raw}". Removing it.`);
		unlinkSync(pidPath);
		process.exit(1);
	}

	// Check if the process is actually alive before signalling.
	const isAlive = (() => {
		try {
			process.kill(pid, 0);
			return true;
		} catch (e) {
			// EPERM = process exists but we can't signal it (still alive).
			// ESRCH = no such process (truly dead).
			return (e as NodeJS.ErrnoException).code === "EPERM";
		}
	})();

	if (!isAlive) {
		console.log(
			`Knowledge server (PID ${pid}) is not running — removing stale PID file.`,
		);
		unlinkSync(pidPath);
		return;
	}

	// Send SIGTERM and wait for the process to exit.
	console.log(`Stopping knowledge server (PID ${pid})...`);
	try {
		process.kill(pid, "SIGTERM");
	} catch (err: unknown) {
		const code = (err as NodeJS.ErrnoException).code;
		if (code === "ESRCH") {
			// Raced — process exited between the liveness check and the kill.
			console.log("Server already stopped.");
			if (existsSync(pidPath)) unlinkSync(pidPath);
			return;
		}
		console.error(`Failed to send SIGTERM to PID ${pid}: ${err}`);
		process.exit(1);
	}

	// Poll until the process is gone, up to STOP_TIMEOUT_MS.
	// Must exceed the server's own SHUTDOWN_TIMEOUT_MS (30 s) so we don't
	// declare failure while the server is still draining in-flight LLM calls.
	const STOP_TIMEOUT_MS = 35_000;
	const POLL_INTERVAL_MS = 100;
	const deadline = Date.now() + STOP_TIMEOUT_MS;

	while (Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
		try {
			process.kill(pid, 0); // still alive — keep waiting
		} catch (e) {
			if ((e as NodeJS.ErrnoException).code === "EPERM") continue; // alive, no permission
			// ESRCH or other — process is gone.
			console.log("Knowledge server stopped.");
			// The server cleans up its own PID file on graceful shutdown.
			// If it crashed before doing so, clean up here.
			if (existsSync(pidPath)) unlinkSync(pidPath);
			return;
		}
	}

	console.error(
		`Server (PID ${pid}) did not exit within ${STOP_TIMEOUT_MS / 1000}s after SIGTERM.`,
	);
	console.error(`Try: kill -9 ${pid}`);
	process.exit(1);
}
