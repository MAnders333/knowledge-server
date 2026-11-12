import { describe, expect, it } from "bun:test";
import {
	existsSync,
	mkdtempSync,
	openSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { LOG_KEEP_FILES, Logger } from "../src/logger";

function tempDir(): string {
	return mkdtempSync(join(tmpdir(), "ks-logger-rotate-"));
}

function linesOf(path: string): string[] {
	return existsSync(path)
		? readFileSync(path, "utf8").split("\n").filter(Boolean)
		: [];
}

describe("Logger size rotation", () => {
	it("rotates the active file and keeps LOG_KEEP_FILES rotated copies", () => {
		const dir = tempDir();
		try {
			const path = join(dir, "server.log");
			const log = new Logger(path, "info", { mirror: false, maxBytes: 400 });
			for (let i = 0; i < 120; i++) log.log(`rotation line ${i}`);
			log.close();

			for (let i = 1; i <= LOG_KEEP_FILES; i++) {
				expect(existsSync(`${path}.${i}`)).toBe(true);
			}
			expect(existsSync(`${path}.${LOG_KEEP_FILES + 1}`)).toBe(false);
			for (const p of [path, `${path}.1`, `${path}.2`, `${path}.3`]) {
				expect(statSync(p).size).toBeLessThanOrEqual(400);
			}
			// Newest line is in the active file; the oldest retained copy is older.
			expect(readFileSync(path, "utf8")).toContain("rotation line 119");
			expect(readFileSync(`${path}.1`, "utf8")).not.toContain(
				"rotation line 119",
			);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("never rotates when maxBytes is 0", () => {
		const dir = tempDir();
		try {
			const path = join(dir, "server.log");
			const log = new Logger(path, "info", { mirror: false, maxBytes: 0 });
			for (let i = 0; i < 200; i++) log.log(`unbounded line ${i}`);
			log.close();
			expect(existsSync(`${path}.1`)).toBe(false);
			expect(linesOf(path).length).toBe(200);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("keeps the active file's size counter across reopen", () => {
		const dir = tempDir();
		try {
			const path = join(dir, "server.log");
			const first = new Logger(path, "info", { mirror: false, maxBytes: 300 });
			for (let i = 0; i < 4; i++) first.log(`first ${i} ${"x".repeat(40)}`);
			first.close();
			const second = new Logger(path, "info", { mirror: false, maxBytes: 300 });
			for (let i = 0; i < 4; i++) second.log(`second ${i} ${"x".repeat(40)}`);
			second.close();
			for (const p of [path, `${path}.1`]) {
				if (existsSync(p)) expect(statSync(p).size).toBeLessThanOrEqual(300);
			}
			expect(existsSync(`${path}.1`)).toBe(true);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe("Logger with stdout redirected to the log file", () => {
	it("writes each line once and survives rotation", async () => {
		const dir = tempDir();
		try {
			const path = join(dir, "server.log");
			writeFileSync(path, "");
			const loggerPath = resolve(import.meta.dir, "../src/logger.ts");
			const script = join(dir, "child.ts");
			writeFileSync(
				script,
				`import { Logger } from ${JSON.stringify(loggerPath)};
const log = new Logger(${JSON.stringify(path)}, "info", { maxBytes: 600 });
for (let i = 0; i < 80; i++) log.log("redirect line " + i + " " + "y".repeat(20));
log.close();
`,
			);
			const out = openSync(path, "a");
			const child = Bun.spawn([process.execPath, script], {
				stdout: out,
				stderr: out,
				stdin: "ignore",
			});
			await child.exited;

			const all = [
				...linesOf(`${path}.3`),
				...linesOf(`${path}.2`),
				...linesOf(`${path}.1`),
				...linesOf(path),
			].filter((l) => l.includes("redirect line"));
			// Rotation drops the oldest copies, so only a tail survives; within that
			// tail every line appears exactly once and in order.
			const nums = all.map((l) => Number(/redirect line (\d+)/.exec(l)?.[1]));
			expect(nums[nums.length - 1]).toBe(79);
			expect(new Set(nums).size).toBe(nums.length);
			expect(nums).toEqual([...nums].sort((a, b) => a - b));
			expect(existsSync(`${path}.1`)).toBe(true);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
