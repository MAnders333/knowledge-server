/**
 * Schema-version safety: one ordered migration registry per backend, a refusal
 * (never a drop) for stores newer than this build, and a hard error naming the
 * missing version when a migration is absent.
 */
import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	assertContiguousMigrations,
	assertNotNewerThanCode,
	latestVersion,
	pendingMigrations,
} from "../src/db/migration-registry";
import { PG_MIGRATIONS } from "../src/db/postgres/migrations";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { MIGRATIONS } from "../src/db/sqlite/migrations";
import { SCHEMA_VERSION } from "../src/db/sqlite/schema";
import { ServerStateDB } from "../src/db/state/index";
import {
	SERVER_LOCAL_MIGRATIONS,
	SERVER_LOCAL_SCHEMA_VERSION,
} from "../src/db/state/schema";

describe("migration registries", () => {
	it("knowledge.db migrations are contiguous and end at SCHEMA_VERSION", () => {
		assertContiguousMigrations(MIGRATIONS);
		expect(latestVersion(MIGRATIONS)).toBe(SCHEMA_VERSION);
	});

	it("Postgres knowledge migrations are contiguous and end at SCHEMA_VERSION", () => {
		assertContiguousMigrations(PG_MIGRATIONS);
		expect(latestVersion(PG_MIGRATIONS)).toBe(SCHEMA_VERSION);
	});

	it("state migrations are contiguous and end at SERVER_LOCAL_SCHEMA_VERSION", () => {
		assertContiguousMigrations(SERVER_LOCAL_MIGRATIONS);
		expect(latestVersion(SERVER_LOCAL_MIGRATIONS)).toBe(
			SERVER_LOCAL_SCHEMA_VERSION,
		);
	});

	it("a gap in a registry is rejected", () => {
		expect(() =>
			assertContiguousMigrations([
				{ version: 1, label: "a" },
				{ version: 3, label: "b" },
			]),
		).toThrow(/v1 is followed by v3/);
	});

	it("pendingMigrations names the missing version", () => {
		const list = [
			{ version: 8, label: "a" },
			{ version: 9, label: "b" },
			{ version: 11, label: "c" },
		];
		expect(() => pendingMigrations(list, 7, 11, "test store")).toThrow(
			/no migration for v10/,
		);
		expect(
			pendingMigrations(
				[8, 9, 10, 11].map((version) => ({ version, label: "x" })),
				8,
				11,
				"test store",
			).map((m) => m.version),
		).toEqual([9, 10, 11]);
	});

	it("a store newer than the build is refused", () => {
		expect(() => assertNotNewerThanCode(5, 4, "test store")).toThrow(
			/newer than this build \(v4\)/,
		);
		expect(() => assertNotNewerThanCode(4, 4, "test store")).not.toThrow();
	});
});

describe("version guard on SQLite stores", () => {
	let dir: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "ks-schema-guard-"));
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	function setVersion(path: string, version: number): void {
		const raw = new Database(path);
		raw.exec("DELETE FROM schema_version");
		raw.exec(
			`INSERT INTO schema_version (version, applied_at) VALUES (${version}, 0)`,
		);
		raw.close();
	}

	function tableExists(path: string, name: string): boolean {
		const raw = new Database(path, { readonly: true });
		try {
			return (
				raw
					.prepare(
						"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
					)
					.get(name) !== null
			);
		} finally {
			raw.close();
		}
	}

	it("v18 drops the strength and last_accessed_at indexes from an existing store", async () => {
		const path = join(dir, "knowledge.db");
		await new KnowledgeDB(path).close();
		const indexExists = (name: string) => {
			const raw = new Database(path, { readonly: true });
			try {
				return (
					raw
						.prepare(
							"SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
						)
						.get(name) !== null
				);
			} finally {
				raw.close();
			}
		};
		expect(indexExists("idx_entry_strength")).toBe(false);
		expect(indexExists("idx_entry_accessed")).toBe(false);

		const raw = new Database(path);
		raw.exec("CREATE INDEX idx_entry_strength ON knowledge_entry(strength)");
		raw.exec(
			"CREATE INDEX idx_entry_accessed ON knowledge_entry(last_accessed_at)",
		);
		raw.close();
		setVersion(path, 17);

		await new KnowledgeDB(path).close();
		expect(indexExists("idx_entry_strength")).toBe(false);
		expect(indexExists("idx_entry_accessed")).toBe(false);
		expect(indexExists("idx_entry_created")).toBe(true);
	});

	it("refuses a knowledge.db newer than this build, without dropping tables", async () => {
		const path = join(dir, "knowledge.db");
		await new KnowledgeDB(path).close();
		setVersion(path, SCHEMA_VERSION + 1);

		expect(() => new KnowledgeDB(path)).toThrow(/newer than this build/);
		expect(tableExists(path, "knowledge_entry")).toBe(true);
	});

	it("refuses a knowledge.db with a missing migration, naming the version, without dropping", async () => {
		const path = join(dir, "knowledge.db");
		await new KnowledgeDB(path).close();
		setVersion(path, MIGRATIONS[0].version - 2);

		expect(() => new KnowledgeDB(path)).toThrow(
			new RegExp(`no migration for v${MIGRATIONS[0].version - 1}`),
		);
		expect(tableExists(path, "knowledge_entry")).toBe(true);
	});

	it("refuses a state.db newer than this build", async () => {
		const path = join(dir, "state.db");
		const state = new ServerStateDB(path);
		await state.close();
		setVersion(path, SERVER_LOCAL_SCHEMA_VERSION + 1);

		expect(() => new ServerStateDB(path)).toThrow(/newer than this build/);
		expect(tableExists(path, "pending_episodes")).toBe(true);
	});

	it("insertPendingEpisode stores the same episode range once", async () => {
		const state = new ServerStateDB(join(dir, "state.db"));
		try {
			const episode = {
				id: "row-a",
				userId: "alice",
				source: "opencode",
				sessionId: "s1",
				startMessageId: "m0",
				endMessageId: "m9",
				sessionTitle: "t",
				projectName: "p",
				directory: "/tmp",
				content: "c",
				contentType: "messages" as const,
				timeCreated: 1,
				maxMessageTime: 2,
				approxTokens: 3,
				uploadedAt: 4,
			};
			await state.insertPendingEpisode(episode);
			await state.insertPendingEpisode({ ...episode, id: "row-b" });
			expect((await state.getPendingEpisodes(0)).map((r) => r.id)).toEqual([
				"row-a",
			]);
		} finally {
			await state.close();
		}
	});
});
