import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { EXTENSION_ASSETS } from "../src/commands/extension-targets";

const ROOT = join(import.meta.dir, "..");
const read = (rel: string): string => readFileSync(join(ROOT, rel), "utf8");

interface Step {
	name?: string;
	id?: string;
	uses?: string;
	run?: string;
	with?: Record<string, string>;
}
interface Job {
	needs?: string | string[];
	if?: string;
	steps: Step[];
	strategy?: { matrix: { include: { suffix: string }[] } };
}
interface Workflow {
	jobs: Record<string, Job>;
}
const workflow = (rel: string): Workflow =>
	Bun.YAML.parse(read(rel)) as Workflow;

/** Standalone builds must not autoload .env / bunfig.toml from the cwd. */
const NO_AUTOLOAD_FLAGS = [
	"--no-compile-autoload-dotenv",
	"--no-compile-autoload-bunfig",
];

/** Binaries are built and published for these asset suffixes only. */
const PLATFORMS = ["linux-x64", "darwin-arm64"];

describe("standalone build command", () => {
	it("the compile script passes both no-autoload flags", () => {
		const { scripts } = JSON.parse(read("package.json")) as {
			scripts: Record<string, string>;
		};
		expect(scripts.compile).toContain("bun build --compile");
		for (const flag of NO_AUTOLOAD_FLAGS) {
			expect(scripts.compile, flag).toContain(flag);
		}
	});

	it("CI and the release workflow compile only through that script", () => {
		for (const file of [
			".github/workflows/ci.yml",
			".github/workflows/release-please.yml",
		]) {
			expect(read(file), file).not.toContain("bun build --compile");
			expect(read(file), file).toContain("bun run compile ");
		}
	});
});

describe("release-please configuration", () => {
	const releaseJob = workflow(".github/workflows/release-please.yml").jobs[
		"release-please"
	] as Job;
	const action = releaseJob.steps.find((s) =>
		s.uses?.startsWith("googleapis/release-please-action@"),
	);

	it("the action step does not pass release-type, so the config file applies", () => {
		expect(action).toBeDefined();
		expect(action?.with ?? {}).not.toHaveProperty("release-type");
	});

	it("the config makes releases drafts and creates their tags", () => {
		const config = JSON.parse(read("release-please-config.json"));
		expect(config.draft).toBe(true);
		expect(config["force-tag-creation"]).toBe(true);
		expect(config["release-type"]).toBe("node");
		// Bun's toHaveProperty reads "." as a path separator, so check the keys.
		expect(Object.keys(config.packages)).toContain(".");
	});

	it("the manifest tracks the package.json version", () => {
		const manifest = JSON.parse(read(".release-please-manifest.json"));
		const pkg = JSON.parse(read("package.json"));
		expect(manifest["."]).toBe(pkg.version);
	});
});

describe("release asset names", () => {
	const wf = workflow(".github/workflows/release-please.yml");
	const verify = wf.jobs.publish?.steps.find(
		(s) => s.name === "Verify every expected asset is present",
	);
	const expected = /expected="([^"]+)"/
		.exec(verify?.run ?? "")?.[1]
		?.split(" ");
	const sumsRun = wf.jobs["build-binaries"]?.steps.find(
		(s) => s.name === "Generate SHA256SUMS",
	)?.run;
	const sumsFiles = /files=\(([\s\S]*?)\)/
		.exec(sumsRun ?? "")?.[1]
		?.replaceAll("${{ matrix.suffix }}", "linux-x64")
		.trim()
		.split(/\s+/);

	const perPlatform = PLATFORMS.flatMap((p) => [
		`knowledge-server-${p}`,
		`knowledge-server-${p}.gz`,
		`knowledge-daemon-${p}`,
		`knowledge-daemon-${p}.gz`,
		`SHA256SUMS-${p}`,
	]);
	const shared = [...EXTENSION_ASSETS, "install.sh"];

	it("the build matrix produces exactly the platforms in this contract", () => {
		const suffixes = wf.jobs["build-binaries"]?.strategy?.matrix.include.map(
			(i) => i.suffix,
		);
		expect(suffixes?.sort()).toEqual([...PLATFORMS].sort());
	});

	it("the publish check expects exactly the assets the builds produce", () => {
		expect(expected).toBeDefined();
		expect([...(expected ?? [])].sort()).toEqual(
			[...perPlatform, ...shared].sort(),
		);
	});

	it("SHA256SUMS lists every binary and every shared file", () => {
		expect(sumsFiles?.sort()).toEqual(
			[
				"knowledge-server-linux-x64",
				"knowledge-daemon-linux-x64",
				...shared,
			].sort(),
		);
	});

	it("install.sh downloads the plugin and command files the release ships", () => {
		const match = /plugin_assets=\(([^)]*)\)/.exec(read("scripts/install.sh"));
		expect(match?.[1]?.trim().split(/\s+/).sort()).toEqual(
			[...EXTENSION_ASSETS].sort(),
		);
	});

	it("install.sh and update name the binaries the workflow publishes", () => {
		const installer = read("scripts/install.sh");
		expect(installer).toContain('"knowledge-server-$platform"');
		expect(installer).toContain('"knowledge-daemon-$platform"');
		const update = read("src/commands/update.ts");
		expect(update).toContain("`knowledge-server-${platform}`");
		expect(update).toContain("`knowledge-daemon-${platform}`");
		const download = read("src/commands/release-download.ts");
		for (const p of PLATFORMS) expect(download).toContain(`"${p}"`);
	});
});

describe("release workflow publish job", () => {
	const workflowText = read(".github/workflows/release-please.yml");
	const publish = workflowText.slice(workflowText.indexOf("\n  publish:"));

	it("waits for every build leg before publishing", () => {
		expect(
			workflow(".github/workflows/release-please.yml").jobs.publish?.needs,
		).toEqual(["release-please", "build-binaries"]);
	});

	it("gives gh the repository explicitly (the job does not check out the repo)", () => {
		expect(publish).not.toContain("actions/checkout");
		expect(publish).toContain("GH_REPO: ${{ github.repository }}");
	});

	it("uploads and verifies install.sh alongside the other release assets", () => {
		expect(publish).toContain("release-linux-x64/install.sh");
		expect(publish).toMatch(/\binstall\.sh\b/);
		const stage = workflowText.slice(
			workflowText.indexOf("name: Stage release files"),
			workflowText.indexOf("\n  publish:"),
		);
		expect(stage).toContain("dist/install.sh");
	});
});

describe("release workflow concurrency", () => {
	const workflowText = read(".github/workflows/release-please.yml");
	const header = workflowText.slice(0, workflowText.indexOf("\njobs:"));
	const releaseJob = workflowText.slice(
		workflowText.indexOf("\n  release-please:"),
		workflowText.indexOf("\n  build-binaries:"),
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
