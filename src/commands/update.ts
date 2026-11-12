import { existsSync } from "node:fs";
import { rename, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import {
	EXTENSION_ASSETS,
	readExtensionTargets,
	refreshExtensionTargets,
} from "./extension-targets.js";
import {
	REPO,
	detectPlatform,
	downloadVerified,
	fetchChecksums,
	releaseAssetUrl,
	requireExpectedSha,
	requireManifestCoverage,
} from "./release-download.js";

const GITHUB_API = "https://api.github.com";
const SMOKE_TIMEOUT_MS = 20_000;
const PREV_SUFFIX = ".prev";

export type FetchFn = typeof fetch;
export type SmokeFn = (path: string, args: string[]) => Promise<boolean>;

export interface InstallDeps {
	fetchFn?: FetchFn;
	/** Runs a staged or installed binary; true when it exits 0. Injectable for tests. */
	smoke?: SmokeFn;
}

/** One release asset to install. `dest` decides the staging directory. */
export interface ReleaseAsset {
	/** Name as listed in SHA256SUMS, also the installed file's basename. */
	name: string;
	/** URL of the release asset (gzipped for binaries, plain for text). */
	url: string;
	/** Final install path. Staged beside it so the rename is atomic. */
	dest: string;
	gzip: boolean;
	/** Binaries only: arguments that must make the file exit 0 before and after the swap. */
	smokeArgs?: string[];
}

/**
 * Run a binary with `args` and report whether it exits 0. Output is discarded.
 * Never throws: a binary that cannot be executed counts as a failed smoke test.
 */
export async function smokeTest(
	path: string,
	args: string[],
): Promise<boolean> {
	try {
		const proc = Bun.spawn([path, ...args], {
			stdout: "ignore",
			stderr: "ignore",
		});
		const timer = setTimeout(() => proc.kill(), SMOKE_TIMEOUT_MS);
		try {
			return (await proc.exited) === 0;
		} finally {
			clearTimeout(timer);
		}
	} catch {
		return false;
	}
}

/** Parse "vX.Y.Z" into numeric parts, or null if the format is not strict semver. */
export function parseVersion(version: string): [number, number, number] | null {
	const match = /^v(\d+)\.(\d+)\.(\d+)$/.exec(version);
	if (!match) return null;
	return [Number(match[1]), Number(match[2]), Number(match[3])];
}

/** True when `target` is an older release than `current`. */
export function isDowngrade(current: string, target: string): boolean {
	const cur = parseVersion(current);
	const tgt = parseVersion(target);
	if (!cur || !tgt) throw new Error(`Cannot compare ${current} and ${target}`);
	for (let i = 0; i < 3; i++) {
		if (tgt[i] !== cur[i]) return tgt[i] < cur[i];
	}
	return false;
}

/** Fetch the latest release tag from the GitHub API. Returns "vX.Y.Z". */
async function fetchLatestVersion(fetchFn: FetchFn): Promise<string> {
	const res = await fetchFn(`${GITHUB_API}/repos/${REPO}/releases/latest`, {
		headers: {
			Accept: "application/vnd.github+json",
			"User-Agent": "knowledge-server-updater",
		},
	});
	if (!res.ok) {
		throw new Error(`GitHub API error: ${res.status} ${res.statusText}`);
	}
	const data = (await res.json()) as { tag_name?: string };
	if (!data.tag_name || !parseVersion(data.tag_name)) {
		throw new Error(
			`Unexpected tag_name from GitHub API: ${JSON.stringify(data.tag_name)}`,
		);
	}
	return data.tag_name;
}

function prevPath(dest: string): string {
	return dest + PREV_SUFFIX;
}

async function removeQuietly(path: string): Promise<void> {
	await unlink(path).catch(() => {});
}

interface SwapRecord {
	dest: string;
	/** The previous file was renamed to `dest.prev`. */
	movedAside: boolean;
	/** The staged file was renamed into `dest`. */
	installed: boolean;
}

/**
 * Restore the pre-swap state of every record, in reverse order. Returns
 * human-readable failures; an empty array means the rollback fully succeeded.
 */
async function rollbackSwap(
	records: SwapRecord[],
	staged: string[],
): Promise<string[]> {
	const failures: string[] = [];
	for (const rec of [...records].reverse()) {
		if (rec.installed) await removeQuietly(rec.dest);
		if (rec.movedAside) {
			try {
				await rename(prevPath(rec.dest), rec.dest);
			} catch (err) {
				failures.push(
					`could not restore ${rec.dest} from ${prevPath(rec.dest)}: ${err instanceof Error ? err.message : err}`,
				);
			}
		}
	}
	await Promise.all(staged.map(removeQuietly));
	return failures;
}

/**
 * Install staged files as one transaction. Each destination's current file is
 * moved to `<dest>.prev`, then the staged file is renamed into place. After all
 * renames, binaries with smoke arguments are run from their final paths. Any
 * failure restores every destination to its pre-swap state, so binaries are
 * never left as a mismatched pair.
 */
async function swapTransaction(
	entries: Array<{ tmp: string; asset: ReleaseAsset }>,
	smoke: SmokeFn,
): Promise<void> {
	const records: SwapRecord[] = [];
	const stagedPaths = entries.map((e) => e.tmp);
	try {
		for (const { tmp, asset } of entries) {
			const rec: SwapRecord = {
				dest: asset.dest,
				movedAside: false,
				installed: false,
			};
			records.push(rec);
			if (existsSync(asset.dest)) {
				await rename(asset.dest, prevPath(asset.dest));
				rec.movedAside = true;
			}
			await rename(tmp, asset.dest);
			rec.installed = true;
		}
		for (const { asset } of entries) {
			if (!asset.smokeArgs) continue;
			if (!(await smoke(asset.dest, asset.smokeArgs))) {
				throw new Error(
					`post-install smoke test failed: ${asset.dest} ${asset.smokeArgs.join(" ")}`,
				);
			}
		}
	} catch (err) {
		const failures = await rollbackSwap(records, stagedPaths);
		const base = err instanceof Error ? err.message : String(err);
		if (failures.length > 0) {
			throw new Error(
				`${base}. Rollback FAILED: ${failures.join("; ")}. Restore manually from the *.prev files.`,
			);
		}
		throw new Error(`${base}. Previous files restored.`);
	}
}

/**
 * Download, verify, smoke-test and install a set of release assets.
 *
 * Nothing touches the install directory until every asset has passed checksum
 * verification and every binary has passed its staged smoke test. Any missing
 * SHA256SUMS entry, hash mismatch or failed smoke test aborts with no change.
 */
export async function installVerifiedAssets(
	assets: ReleaseAsset[],
	checksums: Map<string, string>,
	deps: InstallDeps = {},
): Promise<void> {
	const fetchFn = deps.fetchFn ?? fetch;
	const smoke = deps.smoke ?? smokeTest;

	// Resolve every expected hash before any network I/O.
	const expected = assets.map((a) => requireExpectedSha(checksums, a.name));

	const staged: Array<{ tmp: string; asset: ReleaseAsset }> = [];
	try {
		for (let i = 0; i < assets.length; i++) {
			const asset = assets[i];
			const tmp = await downloadVerified({
				url: asset.url,
				stageDir: dirname(asset.dest),
				name: asset.name,
				expectedSha: expected[i],
				gzip: asset.gzip,
				mode: asset.smokeArgs ? 0o755 : 0o644,
				fetchFn,
			});
			staged.push({ tmp, asset });
		}
		for (const { tmp, asset } of staged) {
			if (!asset.smokeArgs) continue;
			if (!(await smoke(tmp, asset.smokeArgs))) {
				throw new Error(
					`staged ${asset.name} failed smoke test (${asset.smokeArgs.join(" ")}); nothing was installed`,
				);
			}
		}
	} catch (err) {
		await Promise.all(staged.map((s) => removeQuietly(s.tmp)));
		throw err;
	}

	await swapTransaction(staged, smoke);
}

/**
 * Download and install the knowledge-daemon binary at `destPath`.
 *
 * Used by the server startup path to self-heal a missing daemon binary —
 * e.g. after upgrading from v2 where only knowledge-server was downloaded.
 *
 * @param version  The version to download (e.g. "v3.0.0"). Fetches the latest
 *                 release from GitHub when omitted.
 * @param destPath The full path where knowledge-daemon should be installed.
 */
export async function downloadAndInstallDaemon(
	version: string | undefined,
	destPath: string,
	deps: InstallDeps = {},
): Promise<void> {
	const fetchFn = deps.fetchFn ?? fetch;
	const platform = detectPlatform();
	const targetVersion = version ?? (await fetchLatestVersion(fetchFn));
	const checksums = await fetchChecksums(targetVersion, platform, fetchFn);
	const asset = `knowledge-daemon-${platform}`;
	await installVerifiedAssets(
		[
			{
				name: asset,
				url: releaseAssetUrl(targetVersion, `${asset}.gz`),
				dest: destPath,
				gzip: true,
				smokeArgs: ["--version"],
			},
		],
		checksums,
		{ fetchFn, smoke: deps.smoke },
	);
}

/**
 * Main update routine.
 *
 * Usage: knowledge-server update [--version v1.2.3] [--force]
 *
 * Checks for a newer release on GitHub, verifies and smoke-tests the
 * platform binaries and plugin files, then installs them as one transaction.
 * The previous files are kept as `<name>.prev`. The server must be restarted
 * to pick up the new binary.
 *
 * @param argv - process.argv slice after "update" (e.g. ["--version", "v1.2.3"])
 * @param currentVersion - the version string of the currently running binary (e.g. "v1.0.1")
 * @param deps - injectable I/O for tests
 */
export async function runUpdate(
	argv: string[],
	currentVersion: string,
	deps: InstallDeps = {},
): Promise<void> {
	const fetchFn = deps.fetchFn ?? fetch;

	let targetVersion = "";
	const versionIdx = argv.indexOf("--version");
	if (versionIdx !== -1) {
		targetVersion = argv[versionIdx + 1] ?? "";
		if (!targetVersion || !parseVersion(targetVersion)) {
			console.error(
				`Invalid version: ${targetVersion || "(missing)"}. Expected format: v1.2.3`,
			);
			process.exit(1);
		}
	}
	const force = argv.includes("--force");

	if (!parseVersion(currentVersion)) {
		console.error(`  ✗ Unexpected current version: ${currentVersion}`);
		process.exit(1);
	}

	const execPath = process.execPath;

	// Fail fast: running via `bun run src/index.ts` sets process.execPath to the
	// Bun runtime binary (e.g. ~/.bun/bin/bun), not the compiled knowledge-server binary.
	// Replacing that path would overwrite the Bun runtime. Check the binary name
	// before doing any network I/O so the error is immediate and clear.
	if (basename(execPath) !== "knowledge-server") {
		console.error(`  ✗ Unexpected executable path: ${execPath}`);
		console.error(
			"    'knowledge-server update' only works with the compiled binary, not 'bun run'.",
		);
		process.exit(1);
	}
	if (!existsSync(execPath)) {
		console.error(`  ✗ Compiled binary not found at: ${execPath}`);
		process.exit(1);
	}

	let platform: string;
	try {
		platform = detectPlatform();
	} catch (err) {
		console.error(`  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	console.log(`  Current version:  ${currentVersion}`);
	console.log(`  Executable:       ${execPath}`);
	console.log(`  Platform:         ${platform}`);

	if (!targetVersion) {
		process.stdout.write("  Fetching latest release... ");
		try {
			targetVersion = await fetchLatestVersion(fetchFn);
			console.log(targetVersion);
		} catch (err) {
			console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
			process.exit(1);
		}
	}

	if (targetVersion === currentVersion) {
		console.log(`\n  Already at ${currentVersion} — nothing to do.`);
		return;
	}
	if (isDowngrade(currentVersion, targetVersion) && !force) {
		console.error(
			`\n  ✗ Refusing to downgrade ${currentVersion} → ${targetVersion}. Pass --force to install an older release.`,
		);
		process.exit(1);
	}

	console.log(`\n  Updating ${currentVersion} → ${targetVersion}`);

	process.stdout.write("  Fetching checksums... ");
	let checksums: Map<string, string>;
	try {
		checksums = await fetchChecksums(targetVersion, platform, fetchFn);
		console.log("done");
	} catch (err) {
		console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	// knowledge-server (HTTP/MCP server) and knowledge-daemon (episode uploader,
	// auto-spawned by the server in single-machine mode) live side by side.
	const libexecDir = dirname(execPath);
	const daemonPath = join(libexecDir, "knowledge-daemon");

	// Plugin and command files live one level above libexec/ (install.sh layout).
	const inferredInstallDir = dirname(libexecDir);
	const assets: ReleaseAsset[] = [
		{
			name: `knowledge-server-${platform}`,
			url: releaseAssetUrl(targetVersion, `knowledge-server-${platform}.gz`),
			dest: execPath,
			gzip: true,
			smokeArgs: ["--help"],
		},
		{
			name: `knowledge-daemon-${platform}`,
			url: releaseAssetUrl(targetVersion, `knowledge-daemon-${platform}.gz`),
			dest: daemonPath,
			gzip: true,
			smokeArgs: ["--version"],
		},
	];
	// Plugin and command files are refreshed when they exist in <install-dir>/
	// (install.sh and earlier updates place them there). Every copy that setup-tool
	// made in a tool's config dir is registered and refreshed too, so registered
	// assets are fetched even when they are not yet on disk.
	const targets = readExtensionTargets(inferredInstallDir);
	const refresh = new Set<string>(targets.map((t) => t.asset));
	for (const name of EXTENSION_ASSETS) {
		if (existsSync(join(inferredInstallDir, name))) refresh.add(name);
	}
	for (const name of refresh) {
		assets.push({
			name,
			url: releaseAssetUrl(targetVersion, name),
			dest: join(inferredInstallDir, name),
			gzip: false,
		});
	}

	try {
		requireManifestCoverage(
			checksums,
			targetVersion,
			assets.filter((a) => !a.smokeArgs).map((a) => a.name),
		);
	} catch (err) {
		console.error(`  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	process.stdout.write("  Verifying, staging and installing... ");
	try {
		await installVerifiedAssets(assets, checksums, {
			fetchFn,
			smoke: deps.smoke,
		});
		console.log("done");
	} catch (err) {
		console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	// Copy the verified plugin and command files over their registered copies.
	for (const outcome of refreshExtensionTargets(inferredInstallDir, targets)) {
		if (outcome.status === "refreshed") {
			console.log(`  ✓ Refreshed ${outcome.target.dst}`);
		} else {
			console.warn(
				`  ⚠ Not refreshed ${outcome.target.dst}: ${outcome.reason}`,
			);
		}
	}

	// Remove the now-obsolete knowledge-server-mcp binary if it exists alongside
	// the main binary. As of v1.7.0 the MCP stdio proxy is built into the main
	// binary as `knowledge-server mcp`; the separate binary is no longer
	// distributed or updated.
	const mcpBinaryPath = join(libexecDir, "knowledge-server-mcp");
	if (existsSync(mcpBinaryPath)) {
		try {
			await unlink(mcpBinaryPath);
			console.log("  ✓ Removed obsolete knowledge-server-mcp binary");
		} catch {
			console.warn(
				"  ⚠ Could not remove obsolete knowledge-server-mcp binary — delete it manually:",
			);
			console.warn(`    rm "${mcpBinaryPath}"`);
		}
	}

	console.log(`\n  Updated to ${targetVersion}.`);
	console.log(
		`  Previous binaries kept as ${basename(execPath)}${PREV_SUFFIX} and knowledge-daemon${PREV_SUFFIX} for rollback.`,
	);
	console.log("  Restart the server to pick up the new binary.");

	const current = parseVersion(currentVersion);
	const target = parseVersion(targetVersion);
	if (!current || !target) {
		throw new Error(
			`Unexpected version format: ${currentVersion} → ${targetVersion}`,
		);
	}
	const [currentMajor, currentMinor] = current;
	const [targetMajor] = target;

	// v1.x → v2.x: knowledge-server-mcp binary replaced by `knowledge-server mcp`.
	if (currentMajor === 1 && currentMinor < 7) {
		console.log(`
  ⚠ Breaking change: the separate knowledge-server-mcp binary has been
    replaced by \`knowledge-server mcp\`. Re-run setup-tool for each of your
    tools to update your MCP config:
      knowledge-server setup-tool opencode
      knowledge-server setup-tool claude-code
      knowledge-server setup-tool cursor
      knowledge-server setup-tool codex`);
	}

	// v2.x → v3.x: consolidation now requires knowledge-daemon.
	// The server auto-spawns it automatically, so no manual action is needed
	// for single-machine setups. For remote setups, install the daemon separately.
	if (currentMajor < 3 && targetMajor >= 3) {
		console.log(`
  ⚠ Breaking change (v3): consolidation now runs through knowledge-daemon.

  Single-machine users: no action needed — the server auto-spawns the
  daemon automatically (just installed alongside this binary).

  Remote server setup (daemon on local machine, server elsewhere):
    - Set DAEMON_AUTO_SPAWN=false on the remote server
    - Run the daemon on each local machine:
        knowledge-daemon --interval=300
      or register it as a background service:
        knowledge-server setup-tool daemon

  If you had a custom POSTGRES_CONNECTION_URI or KNOWLEDGE_DB_PATH,
  run once to migrate your config:
    knowledge-server migrate-config`);
	}
}
