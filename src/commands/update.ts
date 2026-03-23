import { createHash } from "node:crypto";
import { createWriteStream, existsSync } from "node:fs";
import { chmod, readFile, rename, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";

const REPO = "MAnders333/knowledge-server";
const GITHUB_API = "https://api.github.com";
const GITHUB_RELEASES = `https://github.com/${REPO}/releases/download`;

/**
 * Detect the platform suffix used in release asset names.
 * Matches the suffixes produced by the CI build matrix.
 */
function detectPlatform(): string {
	const os = process.platform;
	const arch = process.arch;

	if (os === "linux" && arch === "x64") return "linux-x64";
	if (os === "darwin" && arch === "arm64") return "darwin-arm64";

	throw new Error(
		`Unsupported platform: ${os}/${arch}. Supported: linux/x64, darwin/arm64. To update manually, download from https://github.com/${REPO}/releases`,
	);
}

/**
 * Fetch the latest release tag from the GitHub API.
 * Returns a version string like "v1.2.3".
 */
async function fetchLatestVersion(): Promise<string> {
	const res = await fetch(`${GITHUB_API}/repos/${REPO}/releases/latest`, {
		headers: {
			Accept: "application/vnd.github+json",
			"User-Agent": "knowledge-server-updater",
		},
	});
	if (!res.ok) {
		throw new Error(`GitHub API error: ${res.status} ${res.statusText}`);
	}
	const data = (await res.json()) as { tag_name?: string };
	if (!data.tag_name || !/^v\d+\.\d+\.\d+$/.test(data.tag_name)) {
		throw new Error(
			`Unexpected tag_name from GitHub API: ${JSON.stringify(data.tag_name)}`,
		);
	}
	return data.tag_name;
}

/**
 * Fetch the SHA256SUMS file for a given platform from the release and return
 * a map of { assetFilename → expectedHex }.
 */
async function fetchChecksums(
	targetVersion: string,
	platform: string,
): Promise<Map<string, string>> {
	const url = `${GITHUB_RELEASES}/${targetVersion}/SHA256SUMS-${platform}`;
	const res = await fetch(url, {
		headers: { "User-Agent": "knowledge-server-updater" },
		redirect: "follow",
	});
	if (!res.ok) {
		throw new Error(
			`Failed to fetch checksums: ${res.status} ${res.statusText} (${url})`,
		);
	}
	const text = await res.text();
	const map = new Map<string, string>();
	for (const line of text.split("\n")) {
		const trimmed = line.trim();
		if (!trimmed) continue;
		// sha256sum format: "<hash>  <filename>" (two spaces) or "<hash> <filename>"
		const spaceIdx = trimmed.indexOf(" ");
		if (spaceIdx < 0) continue;
		const hash = trimmed.slice(0, spaceIdx).trim();
		const filename = trimmed.slice(spaceIdx).trim().replace(/^\*/, ""); // strip leading * (binary mode)
		map.set(filename, hash);
	}
	return map;
}

/**
 * Verify the SHA256 checksum of a downloaded file against an expected hex string.
 * Throws if the checksum does not match.
 */
async function verifyChecksum(
	filePath: string,
	expectedHex: string,
	label: string,
): Promise<void> {
	const data = await readFile(filePath);
	const actual = createHash("sha256").update(data).digest("hex");
	if (actual !== expectedHex) {
		throw new Error(
			`Checksum mismatch for ${label}:\n  expected: ${expectedHex}\n  actual:   ${actual}\n  Aborting — the download may be corrupt or tampered.`,
		);
	}
}

/**
 * Download a gzip-compressed binary URL, decompress on the fly, and write to a
 * temp file beside the target path. Streams the response body directly to disk
 * so the full binary is never held in memory.
 *
 * Placing the temp file in the SAME DIRECTORY as the target guarantees the
 * subsequent rename is within the same filesystem — cross-device renames (e.g.
 * /tmp → /home) are not atomic and can corrupt the binary if the process dies
 * mid-copy. A same-directory rename is a single atomic syscall on POSIX.
 *
 * Prints a live progress indicator showing MB received.
 *
 * Returns the temp file path (caller is responsible for cleanup on error).
 */
async function downloadBinary(
	url: string,
	targetPath: string,
	label: string,
): Promise<string> {
	const tmpPath = join(
		dirname(targetPath),
		`.knowledge-server-update-${Date.now()}-${Math.random().toString(36).slice(2)}`,
	);

	const res = await fetch(url, {
		headers: { "User-Agent": "knowledge-server-updater" },
		redirect: "follow",
	});
	if (!res.ok) {
		throw new Error(
			`Download failed: ${res.status} ${res.statusText} (${url})`,
		);
	}
	if (!res.body) {
		throw new Error(`No response body from ${url}`);
	}

	// Stream: HTTP response → gunzip → disk.
	// Node's pipeline() handles backpressure and propagates errors.
	// We wrap res.body (Web ReadableStream) in a Node Readable via Bun's
	// compatibility layer so we can pipe through node:zlib.createGunzip().
	const fileStream = createWriteStream(tmpPath);
	let bytesWritten = 0;
	process.stdout.write(`  Downloading ${label}... `);

	try {
		// Bun supports piping Web ReadableStream to Node streams directly.
		// We interpose a transform to count decompressed bytes for the progress display.
		const gunzip = createGunzip();
		gunzip.on("data", (chunk: Buffer) => {
			bytesWritten += chunk.length;
			// Overwrite the same line: \r moves to start of line without newline.
			const mb = (bytesWritten / 1_048_576).toFixed(1);
			process.stdout.write(`\r  Downloading ${label}... ${mb} MB`);
		});

		// Convert Web ReadableStream to Node Readable for pipeline()
		// res.body null case is already guarded above. Double cast (through unknown) is required
		// because tsc sees ReadableStream<Uint8Array<ArrayBuffer>> which doesn't structurally
		// overlap with Node's ReadableStream<any> — the two ReadableStream types come from
		// different @types packages (lib.dom vs @types/node).
		const nodeReadable = Readable.fromWeb(
			res.body as unknown as Parameters<typeof Readable.fromWeb>[0],
		);
		await pipeline(nodeReadable, gunzip, fileStream);

		const mb = (bytesWritten / 1_048_576).toFixed(1);
		process.stdout.write(`\r  Downloading ${label}... ${mb} MB — done\n`);
	} catch (err) {
		await unlink(tmpPath).catch(() => {});
		throw err;
	}

	return tmpPath;
}

/**
 * Atomically install a downloaded binary temp file at the target path.
 * chmod 0o755 is applied before rename so the binary is never non-executable
 * during the swap (chmod is on the temp file, not the live binary).
 */
async function installBinary(
	tmpPath: string,
	targetPath: string,
): Promise<void> {
	try {
		await chmod(tmpPath, 0o755);
		await rename(tmpPath, targetPath);
	} catch (err) {
		await unlink(tmpPath).catch(() => {});
		throw err;
	}
}

/**
 * Download and install the knowledge-daemon binary for the current platform
 * at the given destination path.
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
): Promise<void> {
	const platform = detectPlatform();
	const targetVersion = version ?? (await fetchLatestVersion());
	const checksums = await fetchChecksums(targetVersion, platform);

	const asset = `knowledge-daemon-${platform}`;
	const url = `${GITHUB_RELEASES}/${targetVersion}/${asset}.gz`;

	const tmpPath = await downloadBinary(url, destPath, asset);
	// Note: downloadBinary and installBinary each clean up tmpPath on their own
	// failure paths. The only gap is a checksum mismatch after a successful
	// download — handle that explicitly here.
	const expectedHash = checksums.get(asset);
	if (expectedHash) {
		try {
			await verifyChecksum(tmpPath, expectedHash, asset);
		} catch (err) {
			await unlink(tmpPath).catch(() => {});
			throw err;
		}
	} else {
		console.warn(
			`  ⚠ No checksum found for ${asset} in release ${targetVersion} — installing without verification.`,
		);
	}
	await installBinary(tmpPath, destPath);
}

/**
 * Main update routine.
 *
 * Usage: knowledge-server update [--version v1.2.3]
 *
 * Checks for a newer release on GitHub, downloads the platform-appropriate
 * binary (gzip-compressed, streamed directly to disk), verifies SHA-256, and
 * atomically replaces the current executable in place.
 * The server must be restarted to pick up the new binary.
 *
 * @param argv - process.argv slice after "update" (e.g. ["--version", "v1.2.3"])
 * @param currentVersion - the version string of the currently running binary (e.g. "v1.0.1")
 */
export async function runUpdate(
	argv: string[],
	currentVersion: string,
): Promise<void> {
	// Parse optional --version flag
	let targetVersion = "";
	const versionIdx = argv.indexOf("--version");
	if (versionIdx !== -1) {
		targetVersion = argv[versionIdx + 1] ?? "";
		if (!targetVersion || !/^v\d+\.\d+\.\d+$/.test(targetVersion)) {
			console.error(
				`Invalid version: ${targetVersion || "(missing)"}. Expected format: v1.2.3`,
			);
			process.exit(1);
		}
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

	// Detect platform
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

	// Resolve target version
	if (!targetVersion) {
		process.stdout.write("  Fetching latest release... ");
		try {
			targetVersion = await fetchLatestVersion();
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

	console.log(`\n  Updating ${currentVersion} → ${targetVersion}`);

	// Fetch checksums before downloading — fail fast if the checksums file is unavailable.
	// The checksum file contains hashes of the uncompressed binaries; we verify after gunzip.
	process.stdout.write("  Fetching checksums... ");
	let checksums: Map<string, string>;
	try {
		checksums = await fetchChecksums(targetVersion, platform);
		console.log("done");
	} catch (err) {
		console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	// Download both binaries: knowledge-server (HTTP/MCP server) and knowledge-daemon
	// (episode uploader, auto-spawned by the server in single-machine mode).
	const libexecDir = dirname(execPath);
	const daemonPath = join(libexecDir, "knowledge-daemon");

	const binariesToUpdate: Array<{ asset: string; destPath: string }> = [
		{ asset: `knowledge-server-${platform}`, destPath: execPath },
		{ asset: `knowledge-daemon-${platform}`, destPath: daemonPath },
	];

	// Track temp paths so all can be cleaned up if any step fails.
	// downloadBinary places temp files next to the target binary for atomic rename.
	const tmpPaths: string[] = [];
	try {
		for (const { asset, destPath } of binariesToUpdate) {
			const url = `${GITHUB_RELEASES}/${targetVersion}/${asset}.gz`;
			// Push the path only after downloadBinary succeeds so the outer catch
			// only tries to unlink files that actually exist.
			const tmpPath = await downloadBinary(url, destPath, asset);
			tmpPaths.push(tmpPath);
			const expectedHash = checksums.get(asset);
			if (expectedHash) {
				process.stdout.write("  Verifying checksum... ");
				await verifyChecksum(tmpPath, expectedHash, asset);
				console.log("ok");
			} else {
				console.warn(
					`  ⚠ No checksum found for ${asset} — proceeding without verification`,
				);
			}
		}
	} catch (err) {
		// Clean up temp files; outer catch is the single cleanup point.
		await Promise.all(tmpPaths.map((p) => unlink(p).catch(() => {})));
		console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
	}

	// Pair each temp path with its target and install sequentially.
	// Sequential (not parallel) avoids two concurrent overwrites of files in the
	// same directory, one of which is the running executable.
	process.stdout.write("  Installing binaries... ");
	try {
		for (let i = 0; i < binariesToUpdate.length; i++) {
			await installBinary(tmpPaths[i], binariesToUpdate[i].destPath);
		}
		console.log("done");
	} catch (err) {
		console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
		process.exit(1);
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

	// Update plugin and command files if the install dir can be inferred.
	// Convention from install.sh: binary lives at <install-dir>/libexec/knowledge-server,
	// so the install dir is two levels up from process.execPath.
	const inferredInstallDir = dirname(libexecDir);
	const pluginDest = join(inferredInstallDir, "knowledge.ts");
	const commandFiles: Array<[string, string]> = [
		["consolidate.md", join(inferredInstallDir, "consolidate.md")],
		["knowledge-review.md", join(inferredInstallDir, "knowledge-review.md")],
	];

	if (existsSync(pluginDest)) {
		process.stdout.write("  Updating plugin and commands... ");
		try {
			const assets = [
				["knowledge.ts", pluginDest],
				...commandFiles.filter(([, dest]) => existsSync(dest)),
			] as Array<[string, string]>;

			// Asset names are hardcoded constants in this function — not user-controlled.
			await Promise.all(
				assets.map(async ([name, dest]) => {
					const url = `${GITHUB_RELEASES}/${targetVersion}/${name}`;
					const res = await fetch(url, {
						headers: { "User-Agent": "knowledge-server-updater" },
						redirect: "follow",
					});
					if (!res.ok)
						throw new Error(`${res.status} ${res.statusText} (${name})`);
					const tmpDest = join(
						dirname(dest),
						`.knowledge-server-update-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
					);
					try {
						await Bun.write(tmpDest, await res.text());
						await rename(tmpDest, dest);
					} catch (err) {
						await unlink(tmpDest).catch(() => {});
						throw err;
					}
				}),
			);
			console.log("done");
		} catch (err) {
			console.error(
				`\n  ⚠ Plugin/command update failed: ${err instanceof Error ? err.message : err}`,
			);
			console.error(
				"    Binary was updated. Re-run install.sh to refresh plugin files.",
			);
		}
	}

	console.log(`\n  Updated to ${targetVersion}.`);
	console.log("  Restart the server to pick up the new binary.");

	const currentMatch = /^v(\d+)\.(\d+)/.exec(currentVersion);
	if (!currentMatch)
		throw new Error(`Unexpected currentVersion format: ${currentVersion}`);
	const currentMajor = Number(currentMatch[1]);
	const currentMinor = Number(currentMatch[2]);

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
	const targetMatch = /^v(\d+)/.exec(targetVersion);
	if (!targetMatch)
		throw new Error(`Unexpected targetVersion format: ${targetVersion}`);
	const targetMajor = Number(targetMatch[1]);
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
