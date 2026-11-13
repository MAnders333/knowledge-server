import { describe, expect, it } from "bun:test";
import pkg from "../package.json" with { type: "json" };

// The daemon binary is smoke-tested by `knowledge-server update` before it is
// swapped in. These flags must exit 0 without connecting to any state DB.
const ENTRY = new URL("../src/daemon/index.ts", import.meta.url).pathname;

async function runDaemon(args: string[]) {
	const proc = Bun.spawn(["bun", ENTRY, ...args], {
		env: {
			...process.env,
			// An unreachable Postgres URI: any DB connect attempt would exit 1.
			STATE_DB_URI: "postgres://nobody:" + "nothing" + "@127.0.0.1:1/none",
		},
		stdout: "pipe",
		stderr: "pipe",
	});
	const timer = setTimeout(() => proc.kill(), 15_000);
	const [stdout, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		proc.exited,
	]);
	clearTimeout(timer);
	return { stdout, exitCode };
}

describe("knowledge-daemon CLI", () => {
	it("--version prints the package version and exits 0", async () => {
		const { stdout, exitCode } = await runDaemon(["--version"]);
		expect(exitCode).toBe(0);
		expect(stdout.trim()).toBe(`knowledge-daemon v${pkg.version}`);
	});

	it("--help prints usage and exits 0 without starting the uploader", async () => {
		const { stdout, exitCode } = await runDaemon(["--help"]);
		expect(exitCode).toBe(0);
		expect(stdout).toContain("Usage: knowledge-daemon");
	});
});
