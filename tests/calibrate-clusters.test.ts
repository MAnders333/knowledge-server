/**
 * Tests for `knowledge-server calibrate --clusters` helpers, the read-only
 * SQLite/Postgres open paths it relies on, and an end-to-end run against a
 * temp store.
 */
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type SimEntry,
	parseThresholdList,
	runCalibrateClusters,
	summarizeClusters,
} from "../src/commands/calibrate-clusters";
import { config } from "../src/config";
import type { GreedyCluster } from "../src/consolidation/cluster";
import { PostgresKnowledgeDB } from "../src/db/postgres/index";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { fakeEmbedding, makeEntry } from "./fixtures";

function simEntry(id: string): SimEntry {
	return { ...makeEntry({ id, content: `content ${id}` }), embedding: [1, 0] };
}

/** Eight-dimensional vectors that point the same way, so they form one cluster. */
function nearAxisVec(i: number): number[] {
	const v = new Array<number>(8).fill(0);
	v[0] = 1;
	v[1] = 0.05 * i;
	return v;
}

describe("parseThresholdList", () => {
	it("returns de-duplicated ascending values", () => {
		expect(parseThresholdList("0.8, 0.7,0.8")).toEqual([0.7, 0.8]);
	});

	it("rejects empty, zero, out-of-range and non-numeric input", () => {
		for (const raw of ["", "0", "1.1", "a", "0.7abc", "0.7,,0.8"]) {
			expect(() => parseThresholdList(raw)).toThrow("--thresholds");
		}
	});

	it("names the flag it was given in the error", () => {
		expect(() => parseThresholdList("abc", "--samples")).toThrow(
			"--samples must be a comma-separated list",
		);
	});
});

describe("summarizeClusters", () => {
	it("reports sizes, the largest share and capped clusters", () => {
		const ids = {
			big: Array.from({ length: 6 }, (_, i) => `big${i}`),
			small: ["lonely"],
		};
		const entries = [...ids.big, ...ids.small].map(simEntry);
		const clusters: GreedyCluster[] = [
			{ centroid: [1, 0], memberIds: ids.big },
			{ centroid: [0, 1], memberIds: ids.small },
		];

		const s = summarizeClusters(entries, clusters, {
			minMembers: 5,
			maxPeers: 5,
			maxPromptTokens: 30_000,
		});

		expect(s.clusterCount).toBe(2);
		expect(s.singletons).toBe(1);
		expect(s.max).toBe(6);
		expect(s.eligible).toBe(1);
		expect(s.capped).toBe(1);
		expect(s.largestShare).toBeCloseTo(6 / 7, 10);
		expect(s.largestTokens).toBeGreaterThan(0);
	});

	it("counts an eligible cluster whose prompt exceeds maxPromptTokens as capped", () => {
		const ids = ["l0", "l1", "l2", "l3", "l4"];
		const entries = ids.map((id) => ({
			...makeEntry({ id, content: "word ".repeat(200) }),
			embedding: [1, 0],
		}));
		const clusters: GreedyCluster[] = [{ centroid: [1, 0], memberIds: ids }];

		const s = summarizeClusters(entries, clusters, {
			minMembers: 5,
			maxPeers: 50,
			maxPromptTokens: 10,
		});

		expect(s.eligible).toBe(1);
		expect(s.capped).toBe(1);
	});

	it("returns zeros when there are no clusters", () => {
		const s = summarizeClusters([], [], {
			minMembers: 5,
			maxPeers: 5,
			maxPromptTokens: 30_000,
		});
		expect(s).toEqual({
			clusterCount: 0,
			singletons: 0,
			p50: 0,
			p90: 0,
			max: 0,
			largestShare: 0,
			largestTokens: 0,
			eligible: 0,
			capped: 0,
		});
	});
});

describe("PostgresKnowledgeDB readOnly option", () => {
	// postgres.js connects lazily, so the constructor needs no server. The sql
	// handle is swapped for a recorder to see how the read is issued.
	function recordingSql(beginModes: unknown[]) {
		const tx = { unsafe: () => Promise.resolve([]) };
		return Object.assign(() => Promise.resolve([]), {
			unsafe: () => Promise.resolve([]),
			begin: (mode: unknown, fn: (t: unknown) => Promise<unknown>) => {
				beginModes.push(mode);
				return fn(tx);
			},
		});
	}

	it("runs reads inside a BEGIN READ ONLY transaction when readOnly", async () => {
		const db = new PostgresKnowledgeDB("postgres://localhost:1/none", 1, {
			readOnly: true,
		});
		const beginModes: unknown[] = [];
		(db as unknown as { sql: unknown }).sql = recordingSql(beginModes);
		await db.getActiveEntriesWithEmbeddings();
		expect(beginModes).toEqual(["read only"]);
	});

	it("does not open a read-only transaction by default", async () => {
		const db = new PostgresKnowledgeDB("postgres://localhost:1/none", 1);
		const beginModes: unknown[] = [];
		(db as unknown as { sql: unknown }).sql = recordingSql(beginModes);
		await db.getActiveEntriesWithEmbeddings();
		expect(beginModes).toEqual([]);
	});
});

describe("KnowledgeDB readOnly option", () => {
	it("reads existing data but rejects writes", async () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-calibrate-clusters-"));
		const path = join(dir, "ro.db");
		try {
			const writer = new KnowledgeDB(path);
			await writer.insertEntry({
				...makeEntry({ id: "a", content: "alpha" }),
				embedding: fakeEmbedding("alpha"),
			});
			await writer.insertEntry({
				...makeEntry({ id: "b", content: "beta" }),
				embedding: fakeEmbedding("beta"),
			});
			await writer.close();

			const reader = new KnowledgeDB(path, { readOnly: true });
			try {
				const rows = await reader.getActiveEntriesWithEmbeddings();
				expect(rows).toHaveLength(2);
				await expect(
					Promise.resolve().then(() =>
						reader.insertEntry({
							...makeEntry({ id: "x", content: "nope" }),
							embedding: fakeEmbedding("nope"),
						}),
					),
				).rejects.toThrow(/readonly/i);
			} finally {
				await reader.close();
			}
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("throws and does not create the file when the store is missing", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-calibrate-clusters-"));
		const path = join(dir, "missing.db");
		try {
			expect(() => new KnowledgeDB(path, { readOnly: true })).toThrow();
			expect(existsSync(path)).toBe(false);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe("runCalibrateClusters (end-to-end, temp store)", () => {
	let dir: string | undefined;
	let logSpy: ReturnType<typeof spyOn> | undefined;
	let errSpy: ReturnType<typeof spyOn> | undefined;
	let exitSpy: ReturnType<typeof spyOn> | undefined;

	afterEach(() => {
		logSpy?.mockRestore();
		errSpy?.mockRestore();
		exitSpy?.mockRestore();
		if (dir) rmSync(dir, { recursive: true, force: true });
		dir = undefined;
	});

	function captureOutput(): { lines: () => string } {
		const out: string[] = [];
		logSpy = spyOn(console, "log").mockImplementation((...a: unknown[]) => {
			out.push(a.map(String).join(" "));
		});
		errSpy = spyOn(console, "error").mockImplementation((...a: unknown[]) => {
			out.push(a.map(String).join(" "));
		});
		return { lines: () => out.join("\n") };
	}

	function writeConfig(storePath: string): string {
		const configPath = join(dir as string, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{ id: "test", kind: "sqlite", path: storePath, writable: true },
				],
				domains: [],
				projects: [],
				stateDb: { kind: "sqlite" },
			}),
		);
		return configPath;
	}

	it("runs the simulation, prints no paths, and leaves entries and cluster rows unchanged", async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-calibrate-e2e-"));
		const storePath = join(dir, "store.db");
		// Seven entries clear CLUSTER_MIN_MEMBERS (5), so the sweep runs. Their control
		// characters must not reach the output.
		const ids = ["a", "b", "c", "d", "e", "f", "g"];
		const writer = new KnowledgeDB(storePath);
		for (const [i, id] of ids.entries()) {
			await writer.insertEntry({
				...makeEntry({ id, content: `entry ${id}\u001b[31m red\u0007 text` }),
				embedding: nearAxisVec(i),
			});
		}
		await writer.persistClusters([
			{
				id: "c1",
				centroid: nearAxisVec(0),
				memberIds: ids,
				isNew: true,
				membershipChanged: true,
			},
		]);
		await writer.close();
		const configPath = writeConfig(storePath);

		const snapshot = async () => {
			const r = new KnowledgeDB(storePath, { readOnly: true });
			try {
				const clusters = await r.getClustersWithMembers();
				return {
					entries: (await r.getActiveEntriesWithEmbeddings()).map((e) => e.id),
					clusters: clusters.map((c) => ({
						...c,
						memberJoinedAt: [...c.memberJoinedAt.entries()],
					})),
				};
			} finally {
				await r.close();
			}
		};
		const before = await snapshot();

		const savedThreshold = config.consolidation.clusterAssignmentThreshold;
		config.consolidation.clusterAssignmentThreshold = 0.5;
		const out = captureOutput();
		let ok = false;
		try {
			ok = await runCalibrateClusters(
				["--thresholds", "0.5,0.7", "--samples", "0.5"],
				configPath,
			);
		} finally {
			config.consolidation.clusterAssignmentThreshold = savedThreshold;
		}

		const text = out.lines();
		expect(ok).toBe(true);
		expect(text).toContain(
			'Store "test" (sqlite): 7 active/conflicted entries',
		);
		// One cluster of all seven at threshold 0.50, marked as the current setting.
		expect(text).toMatch(
			/^\s*running\s+0\.50\s+1\s+0\s+7\s+7\s+7\s+100\.0%.*<- current$/m,
		);
		expect(text).toContain("red text");
		expect(text).not.toContain("\u001b");
		expect(text).not.toContain("\u0007");
		expect(text).not.toContain(dir);
		expect(text).not.toContain(storePath);
		expect(text).not.toContain("store.db");
		expect(text).not.toContain("config.jsonc");
		expect(await snapshot()).toEqual(before);
	});

	it("reports an unreadable config without printing its path", async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-calibrate-e2e-"));
		const configPath = join(dir, "config.jsonc");
		writeFileSync(configPath, "{ not json");
		const out = captureOutput();
		exitSpy = spyOn(process, "exit").mockImplementation((code?: number) => {
			throw new Error(`exit ${code}`);
		});

		await expect(
			runCalibrateClusters(["--thresholds", "0.6"], configPath),
		).rejects.toThrow("exit 1");
		expect(out.lines()).toContain("Failed to load config file");
		expect(out.lines()).not.toContain(dir);
		expect(out.lines()).not.toContain(configPath);
	});

	it("reports a missing store as unavailable without printing its path", async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-calibrate-e2e-"));
		const storePath = join(dir, "missing.db");
		const configPath = writeConfig(storePath);
		const out = captureOutput();

		expect(
			await runCalibrateClusters(["--thresholds", "0.6"], configPath),
		).toBe(false);

		expect(out.lines()).toContain('Store "test" (sqlite): unavailable');
		expect(out.lines()).toContain(
			"Stores: 0 analysed, 0 skipped, 1 unavailable.",
		);
		expect(out.lines()).not.toContain("Pick the lowest threshold");
		expect(out.lines()).not.toContain(dir);
		expect(existsSync(storePath)).toBe(false);
	});

	it("returns false and gives no threshold advice when no store has enough entries", async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-calibrate-e2e-"));
		const storePath = join(dir, "store.db");
		const writer = new KnowledgeDB(storePath);
		for (const id of ["a", "b"]) {
			await writer.insertEntry({
				...makeEntry({ id, content: id }),
				embedding: nearAxisVec(0),
			});
		}
		await writer.close();
		const configPath = writeConfig(storePath);
		const out = captureOutput();

		expect(
			await runCalibrateClusters(["--thresholds", "0.6"], configPath),
		).toBe(false);

		expect(out.lines()).toContain("too few entries for synthesis - skipped");
		expect(out.lines()).toContain(
			"Stores: 0 analysed, 1 skipped, 0 unavailable.",
		);
		expect(out.lines()).not.toContain("Pick the lowest threshold");
	});

	it("reports a store that never answers as unavailable (TIMEOUT)", async () => {
		dir = mkdtempSync(join(tmpdir(), "ks-calibrate-e2e-"));
		const configPath = join(dir, "config.jsonc");
		writeFileSync(
			configPath,
			JSON.stringify({
				stores: [
					{
						id: "test",
						kind: "postgres",
						uri: "postgres://localhost:1/none",
						writable: true,
					},
				],
				domains: [],
				projects: [],
				stateDb: { kind: "sqlite" },
			}),
		);
		// The read never settles, so only the store timeout can end it.
		const readSpy = spyOn(
			PostgresKnowledgeDB.prototype,
			"getActiveEntriesWithEmbeddings",
		).mockImplementation(() => new Promise<never>(() => {}));
		const savedTimeout = config.storeInitTimeoutMs;
		config.storeInitTimeoutMs = 10;
		const out = captureOutput();
		try {
			expect(
				await runCalibrateClusters(["--thresholds", "0.6"], configPath),
			).toBe(false);
		} finally {
			config.storeInitTimeoutMs = savedTimeout;
			readSpy.mockRestore();
		}
		expect(out.lines()).toContain(
			'Store "test" (postgres): unavailable - skipped (TIMEOUT)',
		);
	});
});
