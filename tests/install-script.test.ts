import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import {
	chmodSync,
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

/**
 * Runs scripts/install.sh end to end. `curl` and `uname` are shims on PATH:
 * curl serves fixture files by URL basename (404 otherwise, like --fail) and
 * uname reports linux/x86_64, so the run is the same on every host.
 */

const INSTALLER = join(import.meta.dir, "..", "scripts", "install.sh");
const VERSION = "v9.9.9";
const PLUGIN_FILES = [
	"knowledge.ts",
	"pi-knowledge.ts",
	"omp-knowledge.ts",
	"consolidate.md",
	"knowledge-review.md",
];
const BINARIES = ["knowledge-server-linux-x64", "knowledge-daemon-linux-x64"];

const CURL_SHIM = `#!/bin/sh
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    --output) out="$2"; shift 2 ;;
    https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
src="$FIXTURE_DIR/\${url##*/}"
if [ -f "$src" ]; then cp "$src" "$out"; exit 0; fi
echo "curl: (22) The requested URL returned error: 404" >&2
exit 22
`;

const UNAME_SHIM = `#!/bin/sh
case "$1" in
  -s) echo Linux ;;
  -m) echo x86_64 ;;
  *) echo Linux ;;
esac
`;

function sha256(data: Buffer | string): string {
	return createHash("sha256").update(data).digest("hex");
}

let root: string;
let fixtures: string;
/** Every fixture's manifest line, binaries first. Written as SHA256SUMS per test. */
let manifestLines: string[];
let shims: string;
let home: string;
let installDir: string;

beforeEach(() => {
	root = mkdtempSync(join(tmpdir(), "ks-install-test-"));
	fixtures = join(root, "fixtures");
	shims = join(root, "bin");
	home = join(root, "home");
	installDir = join(root, "install");
	for (const dir of [fixtures, shims, home])
		mkdirSync(dir, { recursive: true });
	writeFileSync(join(shims, "curl"), CURL_SHIM);
	writeFileSync(join(shims, "uname"), UNAME_SHIM);
	chmodSync(join(shims, "curl"), 0o755);
	chmodSync(join(shims, "uname"), 0o755);

	manifestLines = [];
	for (const name of BINARIES) {
		const body = Buffer.from("#!/bin/sh\nexit 0\n");
		writeFileSync(join(fixtures, `${name}.gz`), gzipSync(body));
		manifestLines.push(`${sha256(body)}  ${name}`);
	}
	for (const name of PLUGIN_FILES) {
		const body = `// fixture ${name}\n`;
		writeFileSync(join(fixtures, name), body);
		manifestLines.push(`${sha256(body)}  ${name}`);
	}
});

afterEach(() => {
	rmSync(root, { recursive: true, force: true });
});

/** A v3.10.3-style manifest: binaries only, no plugin/command entries. */
function writeBinaryOnlyManifest(): void {
	const binaryLines = manifestLines.slice(0, BINARIES.length);
	writeFileSync(
		join(fixtures, "SHA256SUMS-linux-x64"),
		`${binaryLines.join("\n")}\n`,
	);
}

function writeFullManifest(): void {
	writeFileSync(
		join(fixtures, "SHA256SUMS-linux-x64"),
		`${manifestLines.join("\n")}\n`,
	);
}

function runInstaller(args: string[]) {
	const env: Record<string, string> = {
		PATH: `${shims}:${process.env.PATH ?? ""}`,
		HOME: home,
		XDG_CONFIG_HOME: join(home, ".config"),
		KNOWLEDGE_SERVER_DIR: installDir,
		OPENCODE_PLUGIN_DIR: join(home, "opencode", "plugins"),
		OPENCODE_COMMAND_DIR: join(home, "opencode", "command"),
		FIXTURE_DIR: fixtures,
	};
	const proc = Bun.spawnSync(["bash", INSTALLER, ...args], {
		env,
		stdout: "pipe",
		stderr: "pipe",
	});
	return {
		exitCode: proc.exitCode,
		output: proc.stdout.toString() + proc.stderr.toString(),
	};
}

describe("install.sh --version", () => {
	it("installs a release whose manifest covers binaries and plugin files", () => {
		writeFullManifest();
		const result = runInstaller(["--version", VERSION]);
		expect(result.output).toContain("verified knowledge-server-linux-x64");
		expect(result.exitCode).toBe(0);
		expect(readFileSync(join(installDir, "version"), "utf8").trim()).toBe(
			VERSION,
		);
		expect(existsSync(join(installDir, "libexec", "knowledge-server"))).toBe(
			true,
		);
	});

	it("refuses a release whose manifest predates plugin checksums, with a clear message", () => {
		writeBinaryOnlyManifest();
		const result = runInstaller(["--version", "v3.10.3"]);
		expect(result.exitCode).toBe(1);
		expect(result.output).toContain("predates checksummed plugin files");
		expect(result.output).toContain("knowledge.ts");
		expect(result.output).toContain("use a newer release");
		// Nothing was installed.
		expect(existsSync(join(installDir, "libexec", "knowledge-server"))).toBe(
			false,
		);
		expect(existsSync(join(installDir, "version"))).toBe(false);
	});
});

describe("README installer one-liner", () => {
	it("fetches install.sh from the latest release asset, not from main", () => {
		const readme = readFileSync(
			join(import.meta.dir, "..", "README.md"),
			"utf8",
		);
		expect(readme).toContain(
			"curl -fsSL https://github.com/MAnders333/knowledge-server/releases/latest/download/install.sh | bash",
		);
		expect(readme).not.toContain(
			"raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh",
		);
	});
});
