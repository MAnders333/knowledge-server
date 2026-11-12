/**
 * Single-instance daemon: a second knowledge-daemon for the same data directory
 * exits cleanly and leaves the first one running; the lock frees when it stops.
 *
 * Runs the real daemon entry point with HOME pointed at a temp dir, so the state
 * DB, lockfiles and status file all live under that dir.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { readDaemonStatus } from "../src/daemon/status";
import { lockHolder, readLockRecord } from "../src/process-lock";
import { daemonLockPath, daemonStatusPath } from "../src/runtime-files";

const ENTRY = resolve(import.meta.dir, "../src/daemon/index.ts");

/** Read a child's stdout until `pattern` appears; returns everything read so far. */
async function readUntil(
	stream: ReadableStream<Uint8Array>,
	pattern: string,
): Promise<string> {
	const reader = stream.getReader();
	const decoder = new TextDecoder();
	let text = "";
	while (!text.includes(pattern)) {
		const { value, done } = await reader.read();
		if (done) break;
		text += decoder.decode(value, { stream: true });
	}
	reader.releaseLock();
	return text;
}

describe("knowledge-daemon single instance", () => {
	let home: string;
	let env: Record<string, string>;
	let dataDir: string;

	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "ks-daemon-lock-"));
		dataDir = join(home, ".local", "share", "knowledge-server");
		const inherited = Object.fromEntries(
			Object.entries(process.env).filter(
				(e): e is [string, string] => e[1] !== undefined,
			),
		);
		env = {
			...inherited,
			HOME: home,
			XDG_CONFIG_HOME: join(home, ".config"),
			LOG_LEVEL: "info",
		};
		for (const key of [
			"KNOWLEDGE_CONFIG_HOME",
			"STATE_DB_URI",
			"KNOWLEDGE_DAEMON_LOG_PATH",
			"DAEMON_UPLOAD_INTERVAL_SECONDS",
		]) {
			delete env[key];
		}
	});

	afterEach(() => {
		rmSync(home, { recursive: true, force: true });
	});

	function startDaemon(args: string[]) {
		return Bun.spawn([process.execPath, ENTRY, ...args], {
			env,
			stdout: "pipe",
			stderr: "pipe",
		});
	}

	it("a second start exits cleanly while the first keeps the lock", async () => {
		const first = startDaemon(["--interval=3600"]);
		try {
			const started = await readUntil(
				first.stdout as ReadableStream,
				"Starting.",
			);
			expect(started).toContain("Starting.");
			expect(lockHolder(daemonLockPath(dataDir))?.pid).toBe(first.pid);

			const second = startDaemon(["--interval=3600"]);
			const [out, code] = await Promise.all([
				new Response(second.stdout).text(),
				second.exited,
			]);
			expect(code).toBe(0);
			expect(out).toContain(`already runs for ${dataDir}`);
			expect(out).toContain(`(PID ${first.pid})`);
			expect(out).not.toContain("Starting.");

			// The first daemon still owns the lock and its status file.
			expect(readLockRecord(daemonLockPath(dataDir))?.pid).toBe(first.pid);
			expect(readDaemonStatus(daemonStatusPath(dataDir))?.pid).toBe(first.pid);
		} finally {
			first.kill("SIGTERM");
			await first.exited;
		}
	}, 60_000);

	it("the lock frees when the daemon stops, so a later run can start", async () => {
		const first = startDaemon(["--interval=3600"]);
		await readUntil(first.stdout as ReadableStream, "Starting.");
		first.kill("SIGTERM");
		await first.exited;
		expect(existsSync(daemonLockPath(dataDir))).toBe(false);

		const once = startDaemon(["--once"]);
		const [out, code] = await Promise.all([
			new Response(once.stdout).text(),
			once.exited,
		]);
		expect(code).toBe(0);
		expect(out).toContain("Done.");
		expect(existsSync(daemonLockPath(dataDir))).toBe(false);
	}, 60_000);
});
