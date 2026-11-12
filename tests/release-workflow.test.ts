import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "..");
const read = (rel: string): string => readFileSync(join(ROOT, rel), "utf8");

/** Standalone builds must not autoload .env / bunfig.toml from the cwd. */
const NO_AUTOLOAD_FLAGS = [
	"--no-compile-autoload-dotenv",
	"--no-compile-autoload-bunfig",
];

describe("standalone build commands", () => {
	it("every `bun build --compile` passes both no-autoload flags", () => {
		const files = [
			"package.json",
			".github/workflows/ci.yml",
			".github/workflows/release-please.yml",
		];
		let found = 0;
		for (const file of files) {
			for (const line of read(file).split("\n")) {
				// A package.json script line may chain several builds with `&&`.
				for (const segment of line.split("&&")) {
					if (!segment.includes("bun build --compile")) continue;
					found++;
					for (const flag of NO_AUTOLOAD_FLAGS) {
						expect(segment, `${file}: ${flag}`).toContain(flag);
					}
				}
			}
		}
		expect(found).toBeGreaterThanOrEqual(6);
	});
});

describe("release workflow publish job", () => {
	const workflow = read(".github/workflows/release-please.yml");
	const publish = workflow.slice(workflow.indexOf("\n  publish:"));

	it("gives gh the repository explicitly (the job does not check out the repo)", () => {
		expect(publish).not.toContain("actions/checkout");
		expect(publish).toContain("GH_REPO: ${{ github.repository }}");
	});

	it("uploads and verifies install.sh alongside the other release assets", () => {
		expect(publish).toContain("release-linux-x64/install.sh");
		expect(publish).toMatch(/\binstall\.sh\b/);
		const stage = workflow.slice(
			workflow.indexOf("name: Stage release files"),
			workflow.indexOf("\n  publish:"),
		);
		expect(stage).toContain("dist/install.sh");
	});
});

describe("release workflow concurrency", () => {
	const workflow = read(".github/workflows/release-please.yml");
	const header = workflow.slice(0, workflow.indexOf("\njobs:"));
	const releaseJob = workflow.slice(
		workflow.indexOf("\n  release-please:"),
		workflow.indexOf("\n  build-binaries:"),
	);

	it("is not set at workflow level, where skipped runs would queue", () => {
		expect(header).not.toMatch(/^concurrency:/m);
	});

	it("is set on the release-please job and never cancels an in-flight release", () => {
		expect(releaseJob).toContain(
			"concurrency:\n      group: release-please\n      cancel-in-progress: false",
		);
	});
});
