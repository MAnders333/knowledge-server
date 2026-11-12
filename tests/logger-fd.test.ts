import { describe, expect, it } from "bun:test";
import {
	mkdtempSync,
	openSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Logger, parseLogLevel } from "../src/logger";

function tempLog(): { dir: string; path: string } {
	const dir = mkdtempSync(join(tmpdir(), "ks-logger-test-"));
	return { dir, path: join(dir, "server.log") };
}

describe("parseLogLevel", () => {
	it("defaults to info for unset or unknown values", () => {
		expect(parseLogLevel(undefined)).toBe("info");
		expect(parseLogLevel("")).toBe("info");
		expect(parseLogLevel("verbose")).toBe("info");
	});

	it("accepts debug, info, warn, error case-insensitively", () => {
		expect(parseLogLevel("DEBUG")).toBe("debug");
		expect(parseLogLevel(" warn ")).toBe("warn");
		expect(parseLogLevel("error")).toBe("error");
	});
});

describe("Logger", () => {
	it("reuses one file descriptor across writes", () => {
		const { dir, path } = tempLog();
		try {
			const log = new Logger(path, "info", { mirror: false });
			for (let i = 0; i < 200; i++) log.log(`line ${i}`);
			log.warn("warned");
			expect(log.fileOpens).toBe(1);
			const lines = readFileSync(path, "utf8").trim().split("\n");
			expect(lines.length).toBe(201);
			expect(lines[0]).toMatch(/^\d{4}-\d{2}-\d{2}T[\d:.]+Z \[INFO\] line 0$/);
			expect(lines[200]).toMatch(/\[WARN\] warned$/);
			log.close();
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("drops debug lines below the configured level", () => {
		const { dir, path } = tempLog();
		try {
			const log = new Logger(path, "info", { mirror: false });
			log.debug("hidden");
			log.log("shown");
			expect(log.isEnabled("debug")).toBe(false);
			log.close();
			const text = readFileSync(path, "utf8");
			expect(text).not.toContain("hidden");
			expect(text).toContain("shown");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("writes debug lines at debug level", () => {
		const { dir, path } = tempLog();
		try {
			const log = new Logger(path, "debug", { mirror: false });
			log.debug("detail");
			log.close();
			expect(readFileSync(path, "utf8")).toContain("[DEBUG] detail");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("does not write a second copy when stdout is already the log file", async () => {
		// Reproduces launchd: StandardOutPath == log file. Each line must appear once.
		const { dir, path } = tempLog();
		try {
			writeFileSync(path, "");
			const loggerPath = resolve(import.meta.dir, "../src/logger.ts");
			const script = join(dir, "child.ts");
			writeFileSync(
				script,
				`import { logger } from ${JSON.stringify(loggerPath)};
logger.init(${JSON.stringify(path)}, "info");
for (let i = 0; i < 5; i++) logger.log("child line " + i);
logger.error("child error");
`,
			);
			const out = openSync(path, "a");
			const child = Bun.spawn([process.execPath, script], {
				stdout: out,
				stderr: out,
				stdin: "ignore",
			});
			await child.exited;
			const lines = readFileSync(path, "utf8")
				.split("\n")
				.filter((l) => l.includes("child "));
			expect(lines.length).toBe(6);
			expect(new Set(lines).size).toBe(6);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
