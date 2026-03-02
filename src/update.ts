import { rename, chmod, unlink, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join, dirname, basename } from "node:path";

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
    `Unsupported platform: ${os}/${arch}. Supported: linux/x64, darwin/arm64. To update manually, download from https://github.com/${REPO}/releases`
  );
}

/**
 * Fetch the latest release tag from the GitHub API.
 * Returns a version string like "v1.2.3".
 */
async function fetchLatestVersion(): Promise<string> {
  const res = await fetch(`${GITHUB_API}/repos/${REPO}/releases/latest`, {
    headers: { "Accept": "application/vnd.github+json", "User-Agent": "knowledge-server-updater" },
  });
  if (!res.ok) {
    throw new Error(`GitHub API error: ${res.status} ${res.statusText}`);
  }
  const data = (await res.json()) as { tag_name?: string };
  if (!data.tag_name || !/^v\d+\.\d+\.\d+$/.test(data.tag_name)) {
    throw new Error(`Unexpected tag_name from GitHub API: ${JSON.stringify(data.tag_name)}`);
  }
  return data.tag_name;
}

/**
 * Download a binary URL to a temp file beside the target path.
 *
 * Placing the temp file in the SAME DIRECTORY as the target guarantees the
 * subsequent rename is within the same filesystem — cross-device renames (e.g.
 * /tmp → /home) are not atomic and can corrupt the binary if the process dies
 * mid-copy. A same-directory rename is a single atomic syscall on POSIX.
 *
 * Returns the temp file path (caller is responsible for cleanup on error).
 */
async function downloadBinary(url: string, targetPath: string): Promise<string> {
  const tmpPath = join(
    dirname(targetPath),
    `.knowledge-server-update-${Date.now()}-${Math.random().toString(36).slice(2)}`
  );

  const res = await fetch(url, {
    headers: { "User-Agent": "knowledge-server-updater" },
    redirect: "follow",
  });
  if (!res.ok) {
    throw new Error(`Download failed: ${res.status} ${res.statusText} (${url})`);
  }
  if (!res.body) {
    throw new Error(`No response body from ${url}`);
  }

  try {
    await Bun.write(tmpPath, res);
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
async function installBinary(tmpPath: string, targetPath: string): Promise<void> {
  try {
    await chmod(tmpPath, 0o755);
    await rename(tmpPath, targetPath);
  } catch (err) {
    await unlink(tmpPath).catch(() => {});
    throw err;
  }
}

/**
 * Main update routine.
 *
 * Usage: knowledge-server update [--version v1.2.3]
 *
 * Checks for a newer release on GitHub, downloads the platform-appropriate
 * binary, and atomically replaces the current executable in place.
 * The server must be restarted to pick up the new binary.
 *
 * @param argv - process.argv slice after "update" (e.g. ["--version", "v1.2.3"])
 * @param currentVersion - the version string of the currently running binary (e.g. "v1.0.1")
 */
export async function runUpdate(argv: string[], currentVersion: string): Promise<void> {
  // Parse optional --version flag
  let targetVersion = "";
  const versionIdx = argv.indexOf("--version");
  if (versionIdx !== -1) {
    targetVersion = argv[versionIdx + 1] ?? "";
    if (!targetVersion || !/^v\d+\.\d+\.\d+$/.test(targetVersion)) {
      console.error(`Invalid version: ${targetVersion || "(missing)"}. Expected format: v1.2.3`);
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
    console.error("    'knowledge-server update' only works with the compiled binary, not 'bun run'.");
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

  // Download both binaries to temp files BEFORE replacing either.
  // This prevents a split-version install if the second download fails after
  // the first binary has already been replaced.
  const binaryUrl = `${GITHUB_RELEASES}/${targetVersion}/knowledge-server-${platform}`;
  const mcpPath = join(dirname(execPath), "knowledge-server-mcp");
  const mcpUrl = `${GITHUB_RELEASES}/${targetVersion}/knowledge-server-mcp-${platform}`;
  const hasMcp = existsSync(mcpPath);

  process.stdout.write(`  Downloading knowledge-server-${platform}... `);
  let mainTmpPath = "";
  try {
    mainTmpPath = await downloadBinary(binaryUrl, execPath);
    console.log("done");
  } catch (err) {
    console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  let mcpTmpPath = "";
  if (hasMcp) {
    process.stdout.write(`  Downloading knowledge-server-mcp-${platform}... `);
    try {
      mcpTmpPath = await downloadBinary(mcpUrl, mcpPath);
      console.log("done");
    } catch (err) {
      // Clean up the already-downloaded main binary temp file before exiting
      await unlink(mainTmpPath).catch(() => {});
      console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
      process.exit(1);
    }
  }

  // Both downloads succeeded — now atomically replace the binaries.
  // Main and MCP installs are kept in separate try/catch blocks so that a failure
  // on the MCP rename (after the main binary is already replaced) produces a clear
  // recovery message rather than a generic error with no indication of partial success.
  process.stdout.write("  Installing main binary... ");
  try {
    await installBinary(mainTmpPath, execPath);
    console.log("done");
  } catch (err) {
    // Main install failed — clean up both temp files and exit
    if (mcpTmpPath) await unlink(mcpTmpPath).catch(() => {});
    console.error(`\n  ✗ ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  if (mcpTmpPath) {
    process.stdout.write("  Installing MCP binary... ");
    try {
      await installBinary(mcpTmpPath, mcpPath);
      console.log("done");
    } catch (err) {
      // Main binary was already replaced. MCP is still the old version.
      // NOTE: Re-running `knowledge-server update` (with or without --version) will NOT
      // fix this — the main binary now reports the new version, so the equality check
      // exits early with "nothing to do". Recovery requires re-running install.sh, which
      // has no version equality check and will replace both binaries unconditionally.
      console.error(`\n  ⚠ MCP binary update failed: ${err instanceof Error ? err.message : err}`);
      console.error(`    Main binary was updated to ${targetVersion}.`);
      console.error("    To finish updating the MCP binary, re-run the installer: curl … | bash");
    }
  }

  // Update plugin and command files if the install dir can be inferred.
  // Convention from install.sh: binary lives at <install-dir>/libexec/knowledge-server,
  // so the install dir is two levels up from process.execPath.
  const inferredInstallDir = dirname(dirname(execPath));
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
      await Promise.all(assets.map(async ([name, dest]) => {
        const url = `${GITHUB_RELEASES}/${targetVersion}/${name}`;
        const res = await fetch(url, {
          headers: { "User-Agent": "knowledge-server-updater" },
          redirect: "follow",
        });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText} (${name})`);
        // Write to a temp file beside the destination then rename atomically,
        // matching the binary install pattern so a crash mid-write doesn't corrupt
        // the live plugin file.
        // Include `name` in the temp path so concurrent writes in Promise.all
        // can never share the same temp file (Date.now() is the same millisecond
        // for all promises launched in the same tick).
        const tmpDest = join(
          dirname(dest),
          `.knowledge-server-update-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}`
        );
        try {
          await writeFile(tmpDest, await res.text(), "utf8");
          await rename(tmpDest, dest);
        } catch (err) {
          await unlink(tmpDest).catch(() => {});
          throw err;
        }
      }));
      console.log("done");
    } catch (err) {
      console.error(`\n  ⚠ Plugin/command update failed: ${err instanceof Error ? err.message : err}`);
      console.error("    Binaries were updated. Re-run install.sh to refresh plugin files.");
    }
  }

  console.log(`\n  Updated to ${targetVersion}.`);
  console.log("  Restart the server to pick up the new binary.");
}
