/**
 * PostgresServerStateDB: activation_event migration (v3) and measurement methods.
 *
 * Runs against a real PostgreSQL instance. Skipped when PG_TEST_URI is not set.
 *
 * Usage:
 *   docker run -d --name ks-measurement-pg -p 55433:5432 \
 *     -e POSTGRES_USER=ks -e POSTGRES_PASSWORD=<password> -e POSTGRES_DB=ks pgvector/pgvector:pg16
 *   PG_TEST_URI=postgres://<user>:<password>@localhost:<port>/<db> bun test tests/state-postgres-activation.test.ts
 */

import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
} from "bun:test";
import { randomUUID } from "node:crypto";
import postgres from "postgres";
import { PostgresServerStateDB } from "../src/db/state/postgres";
import type { ActivationEventRow } from "../src/types";

const PG_URI = process.env.PG_TEST_URI;

function row(overrides: Partial<ActivationEventRow> = {}): ActivationEventRow {
	return {
		ts: 1_000,
		client: "claude-code",
		sessionHash: "abc123",
		promptChars: 42,
		cueCount: 3,
		nCandidates: 10,
		nReturned: 2,
		entryIds: ["e1", "e2"],
		rawSims: [0.81, 0.62],
		rerankScores: [0.9, 0.4],
		rerankModel: "remote/jev-1.14.0",
		gateMode: "rerank",
		latencyMs: 35,
		clientDeadlineExceeded: false,
		gateDropped: 0,
		budgetDropped: 0,
		deduped: 0,
		injectedTokens: 0,
		...overrides,
	};
}

// Each test gets its own database, so the fresh-install and upgrade paths run
// for real instead of against whatever the other PG suites left behind.
describe.skipIf(!PG_URI)("PostgresServerStateDB activation_event", () => {
	let admin: postgres.Sql;
	let dbName = "";
	let testUri = "";

	beforeAll(() => {
		admin = postgres(PG_URI as string, { max: 1 });
	});

	afterAll(async () => {
		await admin.end();
	});

	beforeEach(async () => {
		dbName = `ks_act_test_${randomUUID().replaceAll("-", "")}`;
		await admin.unsafe(`CREATE DATABASE "${dbName}"`);
		const url = new URL(PG_URI as string);
		url.pathname = `/${dbName}`;
		testUri = url.toString();
	});

	afterEach(async () => {
		await admin.unsafe(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
	});

	/** Run a check against the test database over its own short-lived handle. */
	async function withDb<T>(fn: (sql: postgres.Sql) => Promise<T>): Promise<T> {
		const sql = postgres(testUri, { max: 1 });
		try {
			return await fn(sql);
		} finally {
			await sql.end();
		}
	}

	async function schemaVersions(): Promise<number[]> {
		return withDb(async (sql) =>
			(
				await sql`SELECT version FROM state_schema_version ORDER BY version`
			).map((r) => Number(r.version)),
		);
	}

	async function activationTableExists(): Promise<boolean> {
		return withDb(async (sql) => {
			const [r] = await sql`
				SELECT to_regclass('public.activation_event') IS NOT NULL AS ok
			`;
			return r.ok === true;
		});
	}

	it("fresh init stamps schema v5 and creates activation_event", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
		} finally {
			await state.close();
		}
		expect(await schemaVersions()).toEqual([5]);
		expect(await activationTableExists()).toBe(true);
	});

	it("upgrades a v2 state DB by adding activation_event", async () => {
		const seed = new PostgresServerStateDB(testUri);
		try {
			await seed.initialize();
		} finally {
			await seed.close();
		}
		// Rewind to the v2 shape: pending_episode_attempts present, no
		// activation_event table, stamped v2 only.
		await withDb(async (sql) => {
			await sql.unsafe("DROP TABLE activation_event");
			await sql`DELETE FROM state_schema_version`;
			await sql`INSERT INTO state_schema_version (version, applied_at) VALUES (2, 0)`;
		});
		expect(await activationTableExists()).toBe(false);

		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await state.insertActivationEvents([row({ ts: 5_000 })]);
			expect(await state.countActivationsByClient(0)).toEqual({
				"claude-code": 1,
			});
		} finally {
			await state.close();
		}
		expect(await schemaVersions()).toEqual([2, 3, 4, 5]);
		expect(await activationTableExists()).toBe(true);
	});

	it("upgrades a v4 state DB by adding rerank_model and keeps its rows", async () => {
		const seed = new PostgresServerStateDB(testUri);
		try {
			await seed.initialize();
			await seed.insertActivationEvents([row({ ts: 1_000 })]);
		} finally {
			await seed.close();
		}
		// Rewind to v4: the column is absent and the schema is stamped v4 only.
		await withDb(async (sql) => {
			await sql.unsafe("ALTER TABLE activation_event DROP COLUMN rerank_model");
			await sql`DELETE FROM state_schema_version`;
			await sql`INSERT INTO state_schema_version (version, applied_at) VALUES (4, 0)`;
		});

		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await state.insertActivationEvents([
				row({ ts: 2_000, rerankModel: "local/Xenova/ms-marco-MiniLM-L-6-v2" }),
			]);
		} finally {
			await state.close();
		}
		expect(await schemaVersions()).toEqual([4, 5]);
		const stored = await withDb(
			(sql) => sql`SELECT ts, rerank_model FROM activation_event ORDER BY ts`,
		);
		expect(stored.map((r) => r.rerank_model)).toEqual([
			null,
			"local/Xenova/ms-marco-MiniLM-L-6-v2",
		]);
	});

	it("inserts a batch and counts activations by client", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await state.insertActivationEvents([]);
			await state.insertActivationEvents([
				row({ ts: 1_000, client: "claude-code" }),
				row({
					ts: 2_000,
					client: "claude-code",
					sessionHash: null,
					rerankScores: null,
					rerankModel: null,
					gateMode: "dense",
				}),
				row({
					ts: 3_000,
					client: "opencode",
					entryIds: [],
					rawSims: [],
					nReturned: 0,
				}),
			]);

			expect(await state.countActivationsByClient(0)).toEqual({
				"claude-code": 2,
				opencode: 1,
			});
			expect(await state.countActivationsByClient(1_500)).toEqual({
				"claude-code": 1,
				opencode: 1,
			});
			expect(await state.countActivationsByClient(4_000)).toEqual({});
		} finally {
			await state.close();
		}

		// Arrays are JSON text, the same encoding the SQLite backend uses.
		const stored = await withDb(
			(sql) => sql`
				SELECT ts, entry_ids, raw_sims, rerank_scores, rerank_model, session_hash,
				       gate_mode, client_deadline_exceeded
				FROM activation_event ORDER BY ts
			`,
		);
		expect(stored.map((r) => Number(r.ts))).toEqual([1_000, 2_000, 3_000]);
		expect(stored[0].entry_ids).toBe(JSON.stringify(["e1", "e2"]));
		expect(stored[0].raw_sims).toBe(JSON.stringify([0.81, 0.62]));
		expect(stored[0].rerank_scores).toBe(JSON.stringify([0.9, 0.4]));
		expect(stored[0].rerank_model).toBe("remote/jev-1.14.0");
		expect(stored[0].session_hash).toBe("abc123");
		expect(stored[0].client_deadline_exceeded).toBe(false);
		expect(stored[1].rerank_scores).toBeNull();
		expect(stored[1].rerank_model).toBeNull();
		expect(stored[1].session_hash).toBeNull();
		expect(stored[1].gate_mode).toBe("dense");
		expect(stored[2].entry_ids).toBe("[]");
	});

	it("prunes only rows older than the cutoff and returns the count", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await state.insertActivationEvents([
				row({ ts: 1_000 }),
				row({ ts: 2_000 }),
				row({ ts: 2_500 }),
				row({ ts: 3_000 }),
			]);

			// ts < cutoff: the row exactly at the cutoff is kept.
			expect(await state.pruneActivationEvents(2_500)).toBe(2);
			expect(await state.pruneActivationEvents(2_500)).toBe(0);
			expect(await state.countActivationsByClient(0)).toEqual({
				"claude-code": 2,
			});
		} finally {
			await state.close();
		}
		const remaining = await withDb(
			(sql) => sql`SELECT ts FROM activation_event ORDER BY ts`,
		);
		expect(remaining.map((r) => Number(r.ts))).toEqual([2_500, 3_000]);
	});

	it("counts captured episodes by source within the window", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await withDb(async (sql) => {
				const episodes: Array<[string, string, number]> = [
					["opencode", "s1", 100],
					["opencode", "s2", 500],
					["claude-code", "s3", 600],
					["opencode", "s4", 700],
				];
				for (const [source, sessionId, processedAt] of episodes) {
					await sql`
						INSERT INTO consolidated_episode
						(source, session_id, start_message_id, end_message_id,
						 content_type, processed_at, entries_created)
						VALUES (${source}, ${sessionId}, 'a', 'b', 'messages', ${processedAt}, 1)
					`;
				}
			});

			expect(await state.countEpisodesBySource(0)).toEqual({
				opencode: 3,
				"claude-code": 1,
			});
			expect(await state.countEpisodesBySource(400)).toEqual({
				opencode: 2,
				"claude-code": 1,
			});
			expect(await state.countEpisodesBySource(800)).toEqual({});
		} finally {
			await state.close();
		}
	});
});
