import { describe, expect, it } from "bun:test";
import {
	parseChecksums,
	requireManifestCoverage,
} from "../src/commands/release-download";

const HASH = "a".repeat(64);
const PLUGIN = ["knowledge.ts", "consolidate.md"];

describe("requireManifestCoverage", () => {
	it("passes when every requested file has a manifest entry", () => {
		const sums = parseChecksums(
			[
				`${HASH}  knowledge-server-linux-x64`,
				`${HASH}  knowledge.ts`,
				`${HASH}  consolidate.md`,
			].join("\n"),
		);
		expect(() =>
			requireManifestCoverage(sums, "v3.11.0", PLUGIN),
		).not.toThrow();
	});

	it("refuses a v3.10.3-style manifest (binaries only) with a message that names the release and the files", () => {
		const sums = parseChecksums(`${HASH}  knowledge-server-linux-x64\n`);
		let message = "";
		try {
			requireManifestCoverage(sums, "v3.10.3", PLUGIN);
		} catch (err) {
			message = err instanceof Error ? err.message : String(err);
		}
		expect(message).toContain("v3.10.3");
		expect(message).toContain("predates checksummed plugin files");
		expect(message).toContain("knowledge.ts, consolidate.md");
		expect(message).toContain("use a newer release");
	});
});
