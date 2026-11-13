import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	chmodSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { ownedPrivatePaths, tightenPrivatePaths } from "../src/private-files";

const mode = (p: string): number => statSync(p).mode & 0o777;

describe("tightenPrivatePaths", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-private-files-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("tightens group/other-readable files to 0600 and directories to 0700", () => {
		const file = join(tempDir, "server.log");
		const dir = join(tempDir, "config");
		writeFileSync(file, "old log with a token");
		chmodSync(file, 0o644);
		mkdirSync(dir);
		chmodSync(dir, 0o755);

		tightenPrivatePaths({ files: [file], dirs: [dir] });

		expect(mode(file)).toBe(0o600);
		expect(mode(dir)).toBe(0o700);
	});

	it("leaves already-private paths alone and skips missing ones", () => {
		const file = join(tempDir, "state.db");
		writeFileSync(file, "");
		chmodSync(file, 0o600);

		expect(() =>
			tightenPrivatePaths({
				files: [file, join(tempDir, "missing.db-wal")],
				dirs: [join(tempDir, "missing-dir")],
			}),
		).not.toThrow();
		expect(mode(file)).toBe(0o600);
	});

	it("a chmod failure neither throws nor stops the remaining paths", () => {
		const first = join(tempDir, "first.log");
		const second = join(tempDir, "second.log");
		writeFileSync(first, "");
		writeFileSync(second, "");
		chmodSync(first, 0o644);
		chmodSync(second, 0o644);
		const attempted: string[] = [];
		const failingChmod = (path: string, m: number) => {
			attempted.push(path);
			if (path === first) throw new Error("EPERM: operation not permitted");
			chmodSync(path, m);
		};

		expect(() =>
			tightenPrivatePaths({ files: [first, second], dirs: [] }, failingChmod),
		).not.toThrow();
		expect(attempted).toEqual([first, second]);
		expect(mode(second)).toBe(0o600);
	});
});

describe("ownedPrivatePaths", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-owned-paths-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("covers configured SQLite stores and state DB with their WAL/SHM sidecars, and the config dir", () => {
		const configPath = join(tempDir, "config.jsonc");
		const storePath = join(tempDir, "work.db");
		const statePath = join(tempDir, "state.db");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "main", kind: "sqlite", writable: true, path: storePath },
				],
				stateDb: { kind: "sqlite", path: statePath },
			}),
		);

		const owned = ownedPrivatePaths(configPath);

		for (const p of [storePath, statePath]) {
			for (const suffix of ["", "-wal", "-shm"]) {
				expect(owned.files).toContain(`${p}${suffix}`);
			}
		}
		expect(owned.dirs).toContain(tempDir);
	});

	it("includes the server log, PID file, daemon log and both .env locations", () => {
		const owned = ownedPrivatePaths(join(tempDir, "missing.jsonc"));
		const home = homedir();
		expect(owned.files).toContain(
			join(home, ".local", "share", "knowledge-server", "daemon.log"),
		);
		expect(owned.files).toContain(
			join(home, ".local", "share", "knowledge-server", ".env"),
		);
		expect(owned.files.some((p) => p.endsWith("server.log"))).toBe(true);
		expect(owned.files.some((p) => p.endsWith("server.pid"))).toBe(true);
	});
});
