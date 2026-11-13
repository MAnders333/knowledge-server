/**
 * Tests for the extension-target registry that setup-tool writes and
 * `knowledge-server update` reads: a copy made in a tool's config dir must be
 * refreshed when a new verified release is installed.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	EXTENSION_TARGETS_FILE,
	readExtensionTargets,
	recordExtensionTarget,
	refreshExtensionTargets,
} from "../src/commands/extension-targets";

let installDir: string;
let toolDir: string;

beforeEach(() => {
	const root = mkdtempSync(join(tmpdir(), "ks-ext-targets-"));
	installDir = join(root, "install");
	toolDir = join(root, "tool", "extensions");
	mkdirSync(installDir, { recursive: true });
	mkdirSync(toolDir, { recursive: true });
});

afterEach(() => {
	rmSync(join(installDir, ".."), { recursive: true, force: true });
});

describe("readExtensionTargets", () => {
	it("returns no targets when nothing was recorded", () => {
		expect(readExtensionTargets(installDir)).toEqual([]);
	});

	it("drops entries with an unknown asset or a relative destination", () => {
		writeFileSync(
			join(installDir, EXTENSION_TARGETS_FILE),
			JSON.stringify({
				targets: [
					{ asset: "knowledge.ts", dst: join(toolDir, "a.ts") },
					{ asset: "../../evil.sh", dst: join(toolDir, "b.ts") },
					{ asset: "knowledge.ts", dst: "relative/c.ts" },
				],
			}),
		);
		expect(readExtensionTargets(installDir)).toEqual([
			{ asset: "knowledge.ts", dst: join(toolDir, "a.ts") },
		]);
	});

	it("treats a corrupt registry as empty instead of failing update", () => {
		writeFileSync(join(installDir, EXTENSION_TARGETS_FILE), "{ not json");
		expect(readExtensionTargets(installDir)).toEqual([]);
	});
});

describe("recordExtensionTarget", () => {
	it("records a copy once, even when recorded again", () => {
		const target = { asset: "pi-knowledge.ts", dst: join(toolDir, "k.ts") };
		recordExtensionTarget(installDir, target);
		recordExtensionTarget(installDir, target);
		expect(readExtensionTargets(installDir)).toEqual([target]);
	});

	it("replaces the asset recorded for the same destination", () => {
		const dst = join(toolDir, "k.ts");
		recordExtensionTarget(installDir, { asset: "knowledge.ts", dst });
		recordExtensionTarget(installDir, { asset: "omp-knowledge.ts", dst });
		expect(readExtensionTargets(installDir)).toEqual([
			{ asset: "omp-knowledge.ts", dst },
		]);
	});

	it("keeps targets for other destinations", () => {
		const a = { asset: "knowledge.ts", dst: join(toolDir, "a.ts") };
		const b = { asset: "consolidate.md", dst: join(toolDir, "b.md") };
		recordExtensionTarget(installDir, a);
		recordExtensionTarget(installDir, b);
		expect(readExtensionTargets(installDir)).toEqual([a, b]);
	});
});

describe("refreshExtensionTargets", () => {
	it("overwrites an existing copy with the installed asset", () => {
		writeFileSync(join(installDir, "knowledge.ts"), "new release");
		const dst = join(toolDir, "knowledge.ts");
		writeFileSync(dst, "old release");

		const outcomes = refreshExtensionTargets(installDir, [
			{ asset: "knowledge.ts", dst },
		]);

		expect(outcomes.map((o) => o.status)).toEqual(["refreshed"]);
		expect(readFileSync(dst, "utf8")).toBe("new release");
		expect(existsSync(`${dst}.tmp`)).toBe(false);
	});

	it("does not re-create a copy the user removed", () => {
		writeFileSync(join(installDir, "knowledge.ts"), "new release");
		const dst = join(toolDir, "knowledge.ts");

		const outcomes = refreshExtensionTargets(installDir, [
			{ asset: "knowledge.ts", dst },
		]);

		expect(outcomes[0]).toMatchObject({
			status: "skipped",
			reason: "target removed",
		});
		expect(existsSync(dst)).toBe(false);
	});

	it("leaves a symlink alone (source installs own those)", () => {
		const source = join(toolDir, "source.ts");
		writeFileSync(source, "repo file");
		writeFileSync(join(installDir, "knowledge.ts"), "new release");
		const dst = join(toolDir, "knowledge.ts");
		symlinkSync(source, dst);

		const outcomes = refreshExtensionTargets(installDir, [
			{ asset: "knowledge.ts", dst },
		]);

		expect(outcomes[0]).toMatchObject({
			status: "skipped",
			reason: "target is a symlink",
		});
		expect(readFileSync(source, "utf8")).toBe("repo file");
	});

	it("skips a target whose asset is not in the install dir", () => {
		const dst = join(toolDir, "knowledge.ts");
		writeFileSync(dst, "old release");

		const outcomes = refreshExtensionTargets(installDir, [
			{ asset: "knowledge.ts", dst },
		]);

		expect(outcomes[0]).toMatchObject({
			status: "skipped",
			reason: "asset not on disk",
		});
		expect(readFileSync(dst, "utf8")).toBe("old release");
	});
});
