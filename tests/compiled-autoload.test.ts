import { describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const KEY = "KS_AUTOLOAD_PROBE";
const CWD_VALUE = "from-cwd-env";

/**
 * Compile a one-line probe that prints KEY, run it from a directory holding a
 * `.env` that sets KEY, and return what it printed. Bun's standalone-executable
 * autoload would load that `.env` unless the build disables it.
 */
function probe(buildFlags: string[]): string {
	const dir = mkdtempSync(join(tmpdir(), "ks-autoload-"));
	try {
		const entry = join(dir, "probe.ts");
		writeFileSync(
			entry,
			`console.log("probe=" + (process.env.${KEY} ?? "unset"));\n`,
		);
		const work = join(dir, "work");
		mkdirSync(work);
		writeFileSync(join(work, ".env"), `${KEY}=${CWD_VALUE}\n`);
		const bin = join(dir, "probe-bin");
		const build = Bun.spawnSync(
			[
				process.execPath,
				"build",
				"--compile",
				...buildFlags,
				entry,
				"--outfile",
				bin,
			],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(build.stderr.toString()).toBe("");
		expect(build.exitCode).toBe(0);
		const { [KEY]: _inherited, ...env } = process.env;
		const run = Bun.spawnSync([bin], { cwd: work, env, stdout: "pipe" });
		return run.stdout.toString().trim();
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
}

describe("standalone binaries and the working directory", () => {
	it("do not load ./.env when built with --no-compile-autoload-dotenv", () => {
		expect(
			probe(["--no-compile-autoload-dotenv", "--no-compile-autoload-bunfig"]),
		).toBe("probe=unset");
	}, 120_000);

	it("control: the default build does load ./.env (the flag is what prevents it)", () => {
		expect(probe([])).toBe(`probe=${CWD_VALUE}`);
	}, 120_000);
});
