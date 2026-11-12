import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { restartDaemonAfterUpdate } from "../src/commands/daemon-restart";
import {
	GITHUB_RELEASES,
	detectPlatform,
	parseChecksums,
	requireExpectedSha,
} from "../src/commands/release-download";
import {
	type ReleaseAsset,
	downloadAndInstallDaemon,
	installVerifiedAssets,
	isDowngrade,
} from "../src/commands/update";
import { tryAcquireProcessLock } from "../src/process-lock";
import { daemonLockPath } from "../src/runtime-files";

const VERSION = "v9.9.9";

function sha256(data: Buffer | string): string {
	return createHash("sha256").update(data).digest("hex");
}

/** Serve a fixed map of URL → body. Unknown URLs get 404. */
function routedFetch(routes: Map<string, Buffer>): typeof fetch {
	return (async (input: string | URL | Request) => {
		const url = typeof input === "string" ? input : input.toString();
		const body = routes.get(url);
		if (!body) return new Response("not found", { status: 404 });
		return new Response(body, { status: 200 });
	}) as unknown as typeof fetch;
}

function assetUrl(name: string): string {
	return `${GITHUB_RELEASES}/${VERSION}/${name}`;
}

let root: string;
let libexec: string;

beforeEach(() => {
	root = mkdtempSync(join(tmpdir(), "ks-update-test-"));
	libexec = join(root, "libexec");
	mkdirSync(libexec, { recursive: true });
});

afterEach(() => {
	rmSync(root, { recursive: true, force: true });
});

function read(path: string): string {
	return readFileSync(path, "utf8");
}

/** Files left behind by a failed staging step (dot-prefixed partials). */
function leftovers(dir: string): string[] {
	return readdirSync(dir).filter((f) => f.startsWith("."));
}

interface Fixture {
	assets: ReleaseAsset[];
	routes: Map<string, Buffer>;
	checksums: Map<string, string>;
}

/**
 * A release with a server binary, a daemon binary and the knowledge.ts plugin.
 * Installed files are seeded with OLD-* contents; the release ships NEW-*.
 */
function fixture(opts: { withDaemon?: boolean } = {}): Fixture {
	const withDaemon = opts.withDaemon ?? true;
	const serverName = "knowledge-server-linux-x64";
	const daemonName = "knowledge-daemon-linux-x64";
	const pluginName = "knowledge.ts";

	const serverBin = Buffer.from("NEW-SERVER");
	const daemonBin = Buffer.from("NEW-DAEMON");
	const plugin = Buffer.from("NEW-PLUGIN");

	const routes = new Map<string, Buffer>([
		[assetUrl(`${serverName}.gz`), Bun.gzipSync(serverBin)],
		[assetUrl(`${daemonName}.gz`), Bun.gzipSync(daemonBin)],
		[assetUrl(pluginName), plugin],
	]);
	const sums = [
		`${sha256(serverBin)}  ${serverName}`,
		...(withDaemon ? [`${sha256(daemonBin)}  ${daemonName}`] : []),
		`${sha256(plugin)}  ${pluginName}`,
	].join("\n");

	writeFileSync(join(libexec, "knowledge-server"), "OLD-SERVER");
	if (withDaemon)
		writeFileSync(join(libexec, "knowledge-daemon"), "OLD-DAEMON");
	writeFileSync(join(root, "knowledge.ts"), "OLD-PLUGIN");

	const assets: ReleaseAsset[] = [
		{
			name: serverName,
			url: assetUrl(`${serverName}.gz`),
			dest: join(libexec, "knowledge-server"),
			gzip: true,
			smokeArgs: ["--help"],
		},
		{
			name: daemonName,
			url: assetUrl(`${daemonName}.gz`),
			dest: join(libexec, "knowledge-daemon"),
			gzip: true,
			smokeArgs: ["--version"],
		},
		{
			name: pluginName,
			url: assetUrl(pluginName),
			dest: join(root, "knowledge.ts"),
			gzip: false,
		},
	];
	return { assets, routes, checksums: parseChecksums(`${sums}\n`) };
}

/** Smoke stub that always passes. */
const smokeAll = async () => true;

describe("installVerifiedAssets", () => {
	it("installs verified assets and keeps the previous files as .prev", async () => {
		const f = fixture();
		await installVerifiedAssets(f.assets, f.checksums, {
			fetchFn: routedFetch(f.routes),
			smoke: smokeAll,
		});
		expect(read(join(libexec, "knowledge-server"))).toBe("NEW-SERVER");
		expect(read(join(libexec, "knowledge-daemon"))).toBe("NEW-DAEMON");
		expect(read(join(root, "knowledge.ts"))).toBe("NEW-PLUGIN");
		expect(read(join(libexec, "knowledge-server.prev"))).toBe("OLD-SERVER");
		expect(read(join(libexec, "knowledge-daemon.prev"))).toBe("OLD-DAEMON");
		expect(read(join(root, "knowledge.ts.prev"))).toBe("OLD-PLUGIN");
		expect(leftovers(libexec)).toEqual([]);
	});

	it("refuses when SHA256SUMS has no entry for a binary (no install)", async () => {
		const f = fixture({ withDaemon: false });
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke: smokeAll,
			}),
		).rejects.toThrow(/no entry for knowledge-daemon/);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(root, "knowledge.ts"))).toBe("OLD-PLUGIN");
		expect(existsSync(join(libexec, "knowledge-server.prev"))).toBe(false);
		expect(leftovers(libexec)).toEqual([]);
	});

	it("refuses when a plugin file has no SHA256SUMS entry (no partial install)", async () => {
		const f = fixture();
		f.checksums.delete("knowledge.ts");
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke: smokeAll,
			}),
		).rejects.toThrow(/no entry for knowledge.ts/);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(libexec, "knowledge-daemon"))).toBe("OLD-DAEMON");
	});

	it("refuses on checksum mismatch and leaves nothing staged", async () => {
		const f = fixture();
		f.checksums.set("knowledge-server-linux-x64", "0".repeat(64));
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke: smokeAll,
			}),
		).rejects.toThrow(/Checksum mismatch for knowledge-server-linux-x64/);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(libexec, "knowledge-daemon"))).toBe("OLD-DAEMON");
		expect(leftovers(libexec)).toEqual([]);
	});

	it("refuses when a plugin file mismatches, even if binaries verify", async () => {
		const f = fixture();
		f.checksums.set("knowledge.ts", "f".repeat(64));
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke: smokeAll,
			}),
		).rejects.toThrow(/Checksum mismatch for knowledge.ts/);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(root, "knowledge.ts"))).toBe("OLD-PLUGIN");
		expect(leftovers(libexec)).toEqual([]);
	});

	it("aborts before swapping when a staged binary fails its smoke test", async () => {
		const f = fixture();
		const smoke = async (path: string) => !path.includes(".partial");
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke,
			}),
		).rejects.toThrow(/failed smoke test/);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(libexec, "knowledge-daemon"))).toBe("OLD-DAEMON");
		expect(existsSync(join(libexec, "knowledge-server.prev"))).toBe(false);
		expect(leftovers(libexec)).toEqual([]);
	});

	it("rolls back both binaries and the plugin when the post-swap smoke test fails", async () => {
		const f = fixture();
		// Staged files pass; the installed server fails (it is the final path).
		const smoke = async (path: string) =>
			path.includes(".partial") || !path.endsWith("knowledge-server");
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke,
			}),
		).rejects.toThrow(
			/post-install smoke test failed.*Previous files restored/,
		);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
		expect(read(join(libexec, "knowledge-daemon"))).toBe("OLD-DAEMON");
		expect(read(join(root, "knowledge.ts"))).toBe("OLD-PLUGIN");
		expect(existsSync(join(libexec, "knowledge-server.prev"))).toBe(false);
		expect(existsSync(join(libexec, "knowledge-daemon.prev"))).toBe(false);
		expect(leftovers(libexec)).toEqual([]);
	});

	it("removes a newly installed daemon on rollback when none existed before", async () => {
		const f = fixture({ withDaemon: false });
		f.checksums.set(
			"knowledge-daemon-linux-x64",
			sha256(Buffer.from("NEW-DAEMON")),
		);
		const smoke = async (path: string) =>
			path.includes(".partial") || !path.endsWith("knowledge-daemon");
		await expect(
			installVerifiedAssets(f.assets, f.checksums, {
				fetchFn: routedFetch(f.routes),
				smoke,
			}),
		).rejects.toThrow(/Previous files restored/);
		expect(existsSync(join(libexec, "knowledge-daemon"))).toBe(false);
		expect(read(join(libexec, "knowledge-server"))).toBe("OLD-SERVER");
	});
});

describe("downloadAndInstallDaemon (startup self-heal)", () => {
	it("refuses to install when SHA256SUMS lacks the daemon entry", async () => {
		const platform = detectPlatform();
		const name = `knowledge-daemon-${platform}`;
		const bin = Buffer.from("DAEMON");
		const routes = new Map<string, Buffer>([
			[`${GITHUB_RELEASES}/${VERSION}/SHA256SUMS-${platform}`, Buffer.from("")],
			[`${GITHUB_RELEASES}/${VERSION}/${name}.gz`, Bun.gzipSync(bin)],
		]);
		const dest = join(libexec, "knowledge-daemon");
		await expect(
			downloadAndInstallDaemon(VERSION, dest, {
				fetchFn: routedFetch(routes),
				smoke: smokeAll,
			}),
		).rejects.toThrow(/no entry for knowledge-daemon/);
		expect(existsSync(dest)).toBe(false);
	});

	it("installs a verified daemon", async () => {
		const platform = detectPlatform();
		const name = `knowledge-daemon-${platform}`;
		const bin = Buffer.from("DAEMON");
		const routes = new Map<string, Buffer>([
			[
				`${GITHUB_RELEASES}/${VERSION}/SHA256SUMS-${platform}`,
				Buffer.from(`${sha256(bin)}  ${name}\n`),
			],
			[`${GITHUB_RELEASES}/${VERSION}/${name}.gz`, Bun.gzipSync(bin)],
		]);
		const dest = join(libexec, "knowledge-daemon");
		await downloadAndInstallDaemon(VERSION, dest, {
			fetchFn: routedFetch(routes),
			smoke: smokeAll,
		});
		expect(read(dest)).toBe("DAEMON");
	});
});

describe("parseChecksums / requireExpectedSha", () => {
	const h = "a".repeat(64);

	it("accepts text and binary-mode entries", () => {
		const map = parseChecksums(`${h}  one.bin\n${"B".repeat(64)} *two.bin\n`);
		expect(map.get("one.bin")).toBe(h);
		expect(map.get("two.bin")).toBe("b".repeat(64));
	});

	it("rejects malformed lines instead of skipping them", () => {
		expect(() => parseChecksums(`${h}  ok.bin\nnot-a-hash file\n`)).toThrow(
			/Malformed SHA256SUMS line/,
		);
	});

	it("rejects conflicting duplicate entries", () => {
		expect(() => parseChecksums(`${h}  x\n${"c".repeat(64)}  x\n`)).toThrow(
			/Conflicting/,
		);
	});

	it("requireExpectedSha throws for a missing entry", () => {
		expect(() => requireExpectedSha(new Map(), "knowledge.ts")).toThrow(
			/no entry for knowledge.ts/,
		);
	});
});

describe("isDowngrade", () => {
	it("detects older targets only", () => {
		expect(isDowngrade("v3.10.3", "v3.10.2")).toBe(true);
		expect(isDowngrade("v3.10.3", "v2.99.0")).toBe(true);
		expect(isDowngrade("v3.10.3", "v3.10.3")).toBe(false);
		expect(isDowngrade("v3.10.3", "v3.11.0")).toBe(false);
		expect(isDowngrade("v3.10.3", "v4.0.0")).toBe(false);
	});
});

describe("restartDaemonAfterUpdate", () => {
	const noSleep = async () => {};
	let home: string;
	let dataDir: string;
	const log = () => {};
	const warn = () => {};

	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "ks-restart-home-"));
		dataDir = mkdtempSync(join(tmpdir(), "ks-restart-data-"));
	});

	afterEach(() => {
		rmSync(home, { recursive: true, force: true });
		rmSync(dataDir, { recursive: true, force: true });
	});

	it("kickstarts a registered launchd service instead of signalling the process", async () => {
		const plistDir = join(home, "Library", "LaunchAgents");
		mkdirSync(plistDir, { recursive: true });
		writeFileSync(
			join(plistDir, "com.knowledge-server.daemon.plist"),
			"<plist/>",
		);
		const calls: Array<[string, string[]]> = [];
		const signal = () => {
			throw new Error("must not signal a service-managed daemon");
		};

		const outcome = await restartDaemonAfterUpdate({
			platform: "darwin",
			home,
			uid: 501,
			dataDir,
			run: (cmd, args) => {
				calls.push([cmd, args]);
				return 0;
			},
			signal,
			log,
			warn,
		});

		expect(outcome).toBe("service-restarted");
		expect(calls).toEqual([
			["launchctl", ["kickstart", "-k", "gui/501/com.knowledge-server.daemon"]],
		]);
	});

	it("restarts a systemd user unit on Linux and reports a failed restart", async () => {
		const unitDir = join(home, ".config", "systemd", "user");
		mkdirSync(unitDir, { recursive: true });
		writeFileSync(join(unitDir, "knowledge-daemon.service"), "[Unit]");
		const calls: string[][] = [];
		const ok = await restartDaemonAfterUpdate({
			platform: "linux",
			home,
			uid: 1000,
			dataDir,
			run: (cmd, args) => {
				calls.push([cmd, ...args]);
				return 0;
			},
			log,
			warn,
		});
		expect(ok).toBe("service-restarted");
		expect(calls).toEqual([
			["systemctl", "--user", "restart", "knowledge-daemon"],
		]);

		const failed = await restartDaemonAfterUpdate({
			platform: "linux",
			home,
			uid: 1000,
			dataDir,
			run: () => 1,
			log,
			warn,
		});
		expect(failed).toBe("failed");
	});

	it("stops a running auto-spawned daemon with SIGTERM and waits for it to exit", async () => {
		const lock = tryAcquireProcessLock(daemonLockPath(dataDir));
		expect(lock).not.toBeNull();
		const signals: Array<[number, string]> = [];
		let stopped = false;
		try {
			const outcome = await restartDaemonAfterUpdate({
				platform: "darwin",
				home,
				uid: 501,
				dataDir,
				run: () => {
					throw new Error("no service registered");
				},
				signal: (pid, sig) => {
					signals.push([pid, sig]);
					stopped = true;
				},
				alive: () => !stopped,
				command: () => "/x/libexec/knowledge-daemon --interval=300",
				sleep: noSleep,
				log,
				warn,
			});
			expect(outcome).toBe("daemon-stopped");
			expect(signals).toEqual([[process.pid, "SIGTERM"]]);
		} finally {
			lock?.release();
		}
	});

	it("does not signal a lock holder that is not a knowledge-daemon", async () => {
		const lock = tryAcquireProcessLock(daemonLockPath(dataDir));
		expect(lock).not.toBeNull();
		const signals: Array<[number, string]> = [];
		try {
			const outcome = await restartDaemonAfterUpdate({
				platform: "darwin",
				home,
				uid: 501,
				dataDir,
				run: () => {
					throw new Error("no service registered");
				},
				command: () => "/usr/bin/some-other-tool --serve",
				signal: (pid, sig) => {
					signals.push([pid, sig]);
				},
				alive: () => true,
				sleep: noSleep,
				log,
				warn,
			});
			expect(outcome).toBe("not-running");
			expect(signals).toEqual([]);
		} finally {
			lock?.release();
		}
	});

	it("reports not-running when no daemon holds the lock", async () => {
		const outcome = await restartDaemonAfterUpdate({
			platform: "linux",
			home,
			uid: 1000,
			dataDir,
			run: () => 1,
			signal: () => {
				throw new Error("nothing to signal");
			},
			log,
			warn,
		});
		expect(outcome).toBe("not-running");
	});
});
