import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serverRefusal } from "../src/commands/reinitialize";
import { serverStateLine } from "../src/commands/status";
import {
	type ProcessTable,
	locatePidFile,
	pidFileCandidates,
} from "../src/process-identity";

const SERVER = "/usr/local/libexec/knowledge-server --port 3179";
const OTHER = "/usr/bin/some-other-tool --serve";
const PID = "4242";

/** Process table with one live PID whose command line is `command`. */
function table(command: string | null): ProcessTable {
	return {
		alive: () => true,
		command: () => command,
	};
}

describe("pidFileCandidates and locatePidFile", () => {
	let dir: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-pid-paths-"));
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("lists the configured path, then a distinct legacy path", () => {
		expect(pidFileCandidates("/a/server.pid", "/b/server.pid")).toEqual([
			"/a/server.pid",
			"/b/server.pid",
		]);
	});

	it("drops a legacy path equal to the configured one, and empty paths", () => {
		expect(pidFileCandidates("/a/server.pid", "/a/server.pid")).toEqual([
			"/a/server.pid",
		]);
		expect(pidFileCandidates("/a/server.pid", null)).toEqual(["/a/server.pid"]);
		expect(pidFileCandidates("", null)).toEqual([]);
	});

	it("falls back to the legacy file when the configured one is missing", () => {
		const primary = join(dir, "server.pid");
		const legacy = join(dir, "legacy.pid");
		writeFileSync(legacy, PID);
		expect(locatePidFile(primary, legacy)).toBe(legacy);
	});

	it("prefers the configured file when both exist", () => {
		const primary = join(dir, "server.pid");
		const legacy = join(dir, "legacy.pid");
		writeFileSync(primary, PID);
		writeFileSync(legacy, PID);
		expect(locatePidFile(primary, legacy)).toBe(primary);
	});

	it("returns null when neither file exists", () => {
		expect(
			locatePidFile(join(dir, "server.pid"), join(dir, "legacy.pid")),
		).toBeNull();
	});
});

describe("status server line", () => {
	it("reports a running server", () => {
		expect(serverStateLine(PID, table(SERVER))).toBe("running (PID 4242)");
	});

	it("reports a reused PID as a stale file, not a running server", () => {
		expect(serverStateLine(PID, table(OTHER))).toBe(
			"stopped (stale PID file; PID 4242 belongs to another program)",
		);
	});

	it("reports an unreadable command line as unknown, not stopped", () => {
		expect(serverStateLine(PID, table(null))).toBe(
			"unknown (PID 4242 is alive; its command line is unreadable)",
		);
	});

	it("reports a malformed PID file", () => {
		expect(serverStateLine("-1", table(SERVER))).toBe(
			"unknown (malformed PID file)",
		);
	});
});

describe("reinitialize server refusal", () => {
	it("refuses while a server runs", () => {
		expect(serverRefusal(PID, table(SERVER))).toContain("Server is running");
	});

	it("refuses while a live PID's command line is unreadable", () => {
		expect(serverRefusal(PID, table(null))).toContain("cannot be ruled out");
	});

	it("does not refuse for a reused PID that runs another program", () => {
		expect(serverRefusal(PID, table(OTHER))).toBeNull();
	});

	it("does not refuse for a malformed PID file", () => {
		expect(serverRefusal("abc", table(SERVER))).toBeNull();
	});
});
