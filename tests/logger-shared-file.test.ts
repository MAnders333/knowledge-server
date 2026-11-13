import { describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Logger } from "../src/logger";

// The server, `consolidate` and `process-inbox` all append to one server.log.
// Each process holds its own descriptor, so these tests use two loggers on one path.

describe("Logger sharing its file with another logger", () => {
	it("keeps writing to the active file after another logger rotated it", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-logger-shared-"));
		try {
			const path = join(dir, "server.log");
			const quiet = new Logger(path, "info", { mirror: false, maxBytes: 0 });
			const rotator = new Logger(path, "info", {
				mirror: false,
				maxBytes: 300,
			});
			quiet.log("before rotation");
			for (let i = 0; i < 20; i++) {
				rotator.log(`rotator line ${i} ${"x".repeat(30)}`);
			}
			expect(existsSync(`${path}.1`)).toBe(true);

			quiet.log("after rotation");
			quiet.close();
			rotator.close();

			expect(readFileSync(path, "utf8")).toContain("after rotation");
			expect(readFileSync(`${path}.1`, "utf8")).not.toContain("after rotation");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("recreates the active file when it was removed", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-logger-shared-"));
		try {
			const path = join(dir, "server.log");
			const log = new Logger(path, "info", { mirror: false });
			log.log("first");
			rmSync(path);
			log.log("second");
			log.close();

			const text = readFileSync(path, "utf8");
			expect(text).toContain("second");
			expect(text).not.toContain("first");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
