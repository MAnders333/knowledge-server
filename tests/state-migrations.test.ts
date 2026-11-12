import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ServerStateDB } from "../src/db/state/index";
import type { PendingEpisode } from "../src/types";

/**
 * ServerStateDB runs the v3 staging-table migration against a legacy
 * knowledge.db. The only file it may read or modify is the one it is given.
 */
describe("ServerStateDB legacy staging migration", () => {
	let tempDir: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-state-migration-"));
	});

	afterEach(() => {
		rmSync(tempDir, { recursive: true, force: true });
	});

	/** A legacy knowledge.db that still holds one pending staging row. */
	async function makeLegacyDb(path: string): Promise<void> {
		const legacy = new ServerStateDB(path);
		await legacy.insertPendingEpisode({
			id: "ep-legacy",
			userId: "u1",
			source: "opencode",
			sessionId: "s1",
			startMessageId: "m1",
			endMessageId: "m2",
			sessionTitle: "t",
			projectName: "p",
			directory: "/d",
			content: "content",
			contentType: "messages",
			timeCreated: 1,
			maxMessageTime: 1,
			approxTokens: 1,
			uploadedAt: 1,
		} as PendingEpisode);
		await legacy.close();
	}

	function legacyTables(path: string): string[] {
		const db = new Database(path, { readonly: true });
		try {
			return db
				.query<{ name: string }, []>(
					"SELECT name FROM sqlite_master WHERE type = 'table'",
				)
				.all()
				.map((r) => r.name);
		} finally {
			db.close();
		}
	}

	it("copies staging rows from the legacy file it is given, then drops them there", async () => {
		const legacyPath = join(tempDir, "knowledge.db");
		await makeLegacyDb(legacyPath);

		const stateDb = new ServerStateDB(join(tempDir, "state.db"), legacyPath);
		const pending = await stateDb.getPendingEpisodes(0);
		await stateDb.close();

		expect(pending.map((e) => e.id)).toEqual(["ep-legacy"]);
		expect(legacyTables(legacyPath)).not.toContain("pending_episodes");
	});

	it("never reads or modifies a legacy file it was not given", async () => {
		const legacyPath = join(tempDir, "knowledge.db");
		await makeLegacyDb(legacyPath);

		const stateDb = new ServerStateDB(join(tempDir, "state.db"));
		expect(await stateDb.getPendingEpisodes(0)).toEqual([]);
		await stateDb.close();

		expect(legacyTables(legacyPath)).toContain("pending_episodes");
		const legacy = new Database(legacyPath, { readonly: true });
		const count = legacy
			.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM pending_episodes")
			.get();
		legacy.close();
		expect(count?.n).toBe(1);
	});

	it("creates the state DB file with mode 0600", async () => {
		const statePath = join(tempDir, "state.db");
		const stateDb = new ServerStateDB(statePath);
		await stateDb.close();

		expect(statSync(statePath).mode & 0o777).toBe(0o600);
	});
});
