import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { EXTENSION_ASSETS } from "../src/commands/extension-targets";
import { parseVersion } from "../src/commands/update";

const ROOT = join(import.meta.dir, "..");
const read = (rel: string): string => readFileSync(join(ROOT, rel), "utf8");

interface Step {
	name?: string;
	id?: string;
	uses?: string;
	run?: string;
	env?: Record<string, string>;
	with?: Record<string, string>;
}
interface Job {
	needs?: string | string[];
	if?: string;
	permissions?: Record<string, string>;
	steps: Step[];
	strategy?: { matrix: { include: { suffix: string }[] } };
}
interface Workflow {
	permissions?: Record<string, string>;
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

	it("the config keeps tags unprefixed by the package name, with a v prefix", () => {
		const config = JSON.parse(read("release-please-config.json"));
		// Release-please defaults include-component-in-tag to true, which would tag
		// releases knowledge-server-vX.Y.Z; install.sh and update reject that form.
		expect(config["include-component-in-tag"]).toBe(false);
		expect(config["include-v-in-tag"]).toBe(true);
	});

	it("the tag the config yields for the next version is one install.sh and update accept", () => {
		const config = JSON.parse(read("release-please-config.json"));
		const pkg = JSON.parse(read("package.json"));
		const componentPrefix = config["include-component-in-tag"]
			? `${pkg.name}-`
			: "";
		const tag = `${componentPrefix}${config["include-v-in-tag"] ? "v" : ""}3.10.4`;
		const installerPattern = /\[\[ ! "\$VERSION" =~ (\^[^ ]+\$) \]\]/.exec(
			read("scripts/install.sh"),
		)?.[1];
		expect(installerPattern).toBeDefined();
		expect(new RegExp(installerPattern ?? "").test(tag)).toBe(true);
		expect(parseVersion(tag)).toEqual([3, 10, 4]);
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

describe("workflow job timeouts", () => {
	// Expected limits per job. Every job in every workflow must set one.
	const expected: Record<string, Record<string, number>> = {
		"ci.yml": { check: 30 },
		"bun-advisories.yml": { check: 10 },
		"release-please.yml": {
			"release-please": 15,
			"build-binaries": 30,
			publish: 15,
		},
	};

	for (const [file, jobs] of Object.entries(expected)) {
		it(`${file}: every job sets timeout-minutes to its limit`, () => {
			const wf = Bun.YAML.parse(read(`.github/workflows/${file}`)) as Record<
				string,
				Record<string, { "timeout-minutes"?: number }>
			>;
			const found = Object.fromEntries(
				Object.entries(wf.jobs).map(([name, job]) => [
					name,
					job["timeout-minutes"],
				]),
			);
			expect(found).toEqual(jobs);
		});
	}
});

describe("release build checks CI for the commit it builds", () => {
	const wf = workflow(".github/workflows/release-please.yml");
	const build = wf.jobs["build-binaries"] as Job;
	const guardIndex = build.steps.findIndex(
		(s) => s.name === "Build only a commit whose CI run succeeded",
	);
	const checkoutIndex = build.steps.findIndex((s) =>
		s.uses?.startsWith("actions/checkout@"),
	);
	const guard = build.steps[guardIndex];

	it("runs the guard before the checkout of the release commit", () => {
		expect(guardIndex).toBeGreaterThanOrEqual(0);
		expect(guardIndex).toBeLessThan(checkoutIndex);
	});

	it("asks for the successful push CI run of the release commit, and fails without one", () => {
		expect(guard?.env).toEqual({
			GH_TOKEN: "${{ secrets.GITHUB_TOKEN }}",
			RELEASE_SHA: "${{ needs.release-please.outputs.sha }}",
		});
		expect(guard?.run).toContain("gh run list");
		expect(guard?.run).toContain('--commit "$RELEASE_SHA"');
		expect(guard?.run).toContain("--event push");
		expect(guard?.run).toContain("--status success");
		expect(guard?.run).toContain('[ "$green" -lt 1 ]');
		expect(guard?.run).toContain("exit 1");
	});

	it("does not compare the release commit with the validated head, which main can move past", () => {
		expect(guard?.run).not.toContain("VALIDATED_SHA");
		expect(guard?.run).not.toContain("workflow_run.head_sha");
	});

	it("grants actions: read on the build job only, and nothing more", () => {
		expect(build.permissions).toMatchObject({ actions: "read" });
		expect(wf.permissions).toEqual({});
		for (const [name, job] of Object.entries(wf.jobs)) {
			if (name !== "build-binaries")
				expect(job.permissions?.actions).toBeUndefined();
		}
	});

	it("checks out the release commit the guard checked", () => {
		const checkout = build.steps[checkoutIndex];
		expect(checkout?.with?.ref).toBe("${{ needs.release-please.outputs.sha }}");
	});

	it("the publish comment says a re-run works once CI is green for the commit", () => {
		const text = read(".github/workflows/release-please.yml");
		const comment = text.slice(
			text.indexOf("\n  publish:"),
			text.indexOf("name: Publish release"),
		);
		expect(comment).toMatch(/CI is green for that\s+#?\s*commit/);
	});
});
