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
	spyOn,
} from "bun:test";
import { randomUUID } from "node:crypto";
import postgres from "postgres";
import { PostgresServerStateDB } from "../src/db/state/postgres";
import { logger } from "../src/logger";
import type { ActivationEventRow, PendingEpisode } from "../src/types";

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

/**
 * Reconstructed historical shape of the state DB at v1-v3. No release ever stamped
 * state.db as v3 or v4. This DDL is rebuilt from the git history of the v3 commit
 * (d0f468d) so that the upgrade has a realistic starting point. Frozen on purpose:
 * the upgrade under test is what brings it to the current version.
 */
const V3_STATE_SQL = `
  CREATE TABLE state_schema_version (
    version INTEGER NOT NULL UNIQUE,
    applied_at BIGINT NOT NULL
  );
  INSERT INTO state_schema_version (version, applied_at) VALUES (1, 0), (2, 0), (3, 0);

  CREATE TABLE pending_episodes (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT 'default',
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    start_message_id TEXT NOT NULL,
    end_message_id TEXT NOT NULL,
    session_title TEXT NOT NULL DEFAULT '',
    project_name TEXT NOT NULL DEFAULT '',
    directory TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK(content_type IN ('messages', 'compaction_summary', 'document')),
    session_timestamp BIGINT NOT NULL DEFAULT 0,
    max_message_time BIGINT NOT NULL DEFAULT 0,
    approx_tokens INTEGER NOT NULL DEFAULT 0,
    uploaded_at BIGINT NOT NULL
  );
  CREATE INDEX idx_pending_source_time ON pending_episodes(source, max_message_time);

  CREATE TABLE pending_episode_attempts (
    id TEXT PRIMARY KEY,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
  );

  CREATE TABLE activation_event (
    id BIGSERIAL PRIMARY KEY,
    ts BIGINT NOT NULL,
    client TEXT NOT NULL,
    session_hash TEXT,
    prompt_chars INTEGER NOT NULL,
    cue_count INTEGER NOT NULL,
    n_candidates INTEGER NOT NULL,
    n_returned INTEGER NOT NULL,
    entry_ids TEXT NOT NULL,
    raw_sims TEXT NOT NULL,
    rerank_scores TEXT,
    gate_mode TEXT NOT NULL CHECK (gate_mode IN ('rerank', 'dense', 'off')),
    latency_ms INTEGER NOT NULL,
    client_deadline_exceeded BOOLEAN NOT NULL DEFAULT FALSE
  );
  CREATE INDEX idx_activation_event_ts ON activation_event(ts);
  CREATE INDEX idx_activation_event_client_ts ON activation_event(client, ts);

  CREATE TABLE consolidated_episode (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    start_message_id TEXT NOT NULL,
    end_message_id TEXT NOT NULL,
    content_type TEXT NOT NULL,
    processed_at BIGINT NOT NULL,
    entries_created INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id, start_message_id, end_message_id)
  );

  CREATE TABLE consolidation_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    last_consolidated_at BIGINT NOT NULL DEFAULT 0,
    total_sessions_processed BIGINT NOT NULL DEFAULT 0,
    total_entries_created BIGINT NOT NULL DEFAULT 0,
    total_entries_updated BIGINT NOT NULL DEFAULT 0
  );
  INSERT INTO consolidation_state (id) VALUES (1);

  INSERT INTO pending_episodes (id, source, session_id, start_message_id, end_message_id, content, content_type, max_message_time, uploaded_at)
    VALUES ('ep-v3', 'opencode', 's1', 'a', 'b', 'staged text', 'messages', 1, 1);
  INSERT INTO activation_event (ts, client, prompt_chars, cue_count, n_candidates, n_returned, entry_ids, raw_sims, gate_mode, latency_ms)
    VALUES (1000, 'opencode', 42, 2, 5, 1, '["entry-1"]', '[0.5]', 'off', 12);
`;

/** The v4 step, reconstructed like the v3 fixture above (commit 8096c1a): four activation_event columns, stamped v4. */
const V4_STATE_SQL = `
  ALTER TABLE activation_event ADD COLUMN gate_dropped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN budget_dropped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN deduped INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE activation_event ADD COLUMN injected_tokens INTEGER NOT NULL DEFAULT 0;
  INSERT INTO state_schema_version (version, applied_at) VALUES (4, 0);
`;

const LEGACY_FIXTURES = [
	{ label: "reconstructed v3", sql: V3_STATE_SQL },
	{ label: "reconstructed v4", sql: V3_STATE_SQL + V4_STATE_SQL },
] as const;

// Each test gets its own database, so the fresh-install and upgrade paths run
// for real instead of against whatever the other PG suites left behind.
describe.skipIf(!PG_URI)("PostgresServerStateDB activation_event", () => {
	let admin: postgres.Sql;
	let dbName = "";
	let testUri = "";
	let roleName = "";

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
		if (roleName) {
			await admin.unsafe(`DROP ROLE IF EXISTS "${roleName}"`);
			roleName = "";
		}
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

	async function indexExists(name: string): Promise<boolean> {
		return withDb(async (sql) => {
			const rows = await sql`
				SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ${name}
			`;
			return rows.length > 0;
		});
	}

	it("fresh init stamps schema v6 and creates activation_event", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
		} finally {
			await state.close();
		}
		expect(await schemaVersions()).toEqual([6]);
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
		expect(await schemaVersions()).toEqual([2, 3, 4, 5, 6]);
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
		expect(await schemaVersions()).toEqual([4, 5, 6]);
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
			});
			expect(await state.countActivationsByClient(1_500)).toEqual({
				"claude-code": 1,
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

	it("counts only activations that injected entries", async () => {
		const state = new PostgresServerStateDB(testUri);
		try {
			await state.initialize();
			await state.insertActivationEvents([
				row({ ts: 1_000 }),
				row({
					ts: 1_500,
					nReturned: 0,
					entryIds: [],
					rawSims: [],
					rerankScores: null,
					rerankModel: null,
				}),
				row({ ts: 2_000, clientDeadlineExceeded: true }),
			]);
			expect(await state.countActivationsByClient(0)).toEqual({
				"claude-code": 1,
			});
		} finally {
			await state.close();
		}
	});

	for (const fixture of LEGACY_FIXTURES) {
		it(`upgrades a ${fixture.label} state DB through v6 and keeps its rows`, async () => {
			await withDb((sql) => sql.unsafe(fixture.sql));

			const state = new PostgresServerStateDB(testUri);
			try {
				expect((await state.getPendingEpisodes(0)).map((e) => e.id)).toEqual([
					"ep-v3",
				]);
				await state.insertActivationEvents([
					row({ ts: 2_000, rerankModel: "remote/jev-1.14.0" }),
				]);
				expect(await state.countActivationsByClient(0)).toEqual({
					opencode: 1,
					"claude-code": 1,
				});
			} finally {
				await state.close();
			}

			expect(await schemaVersions()).toEqual([1, 2, 3, 4, 5, 6]);
			const stored = await withDb(
				(sql) => sql`
					SELECT ts, gate_dropped, injected_tokens, rerank_model
					FROM activation_event ORDER BY ts
				`,
			);
			expect(
				stored.map((r) => ({
					ts: Number(r.ts),
					gate_dropped: Number(r.gate_dropped),
					injected_tokens: Number(r.injected_tokens),
					rerank_model: r.rerank_model,
				})),
			).toEqual([
				{ ts: 1_000, gate_dropped: 0, injected_tokens: 0, rerank_model: null },
				{
					ts: 2_000,
					gate_dropped: 0,
					injected_tokens: 0,
					rerank_model: "remote/jev-1.14.0",
				},
			]);
			const index = await withDb(
				(sql) => sql`
					SELECT indexname FROM pg_indexes
					WHERE schemaname = current_schema() AND indexname = 'idx_pending_session'
				`,
			);
			expect(index.length).toBe(1);
		});
	}

	it("a role that does not own the state DB upgrades from v2 without the session index", async () => {
		// Owner-side setup: a v2 store with no session index. The role must not be able to create it.
		const seed = new PostgresServerStateDB(testUri);
		try {
			await seed.initialize();
		} finally {
			await seed.close();
		}
		roleName = `ks_state_nonowner_${randomUUID().replaceAll("-", "")}`;
		const password = randomUUID();
		await withDb(async (sql) => {
			await sql.unsafe("DROP INDEX idx_pending_session");
			await sql.unsafe("DROP TABLE activation_event");
			await sql`DELETE FROM state_schema_version`;
			await sql`INSERT INTO state_schema_version (version, applied_at) VALUES (2, 0)`;
			await sql.unsafe(
				`CREATE ROLE "${roleName}" LOGIN PASSWORD '${password}'`,
			);
			await sql.unsafe(`GRANT USAGE, CREATE ON SCHEMA public TO "${roleName}"`);
			await sql.unsafe(
				`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "${roleName}"`,
			);
		});

		const url = new URL(testUri);
		url.username = roleName;
		url.password = password;
		const warn = spyOn(logger, "warn");
		let skipped: string[] = [];
		const state = new PostgresServerStateDB(url.toString());
		try {
			await state.initialize();
			skipped = warn.mock.calls
				.map((call) => call.join(" "))
				.filter((line) => line.includes("idx_pending_session"));
			const episode: PendingEpisode = {
				id: "np-1",
				userId: "alice",
				source: "opencode",
				sessionId: "s1",
				startMessageId: "a",
				endMessageId: "b",
				sessionTitle: "",
				projectName: "",
				directory: "",
				content: "c",
				contentType: "messages",
				timeCreated: 1,
				maxMessageTime: 1,
				approxTokens: 1,
				uploadedAt: 1,
			};
			await state.insertPendingEpisode(episode);
			await state.insertActivationEvents([row({ ts: 2_000 })]);
			expect((await state.getPendingEpisodes(0)).map((e) => e.id)).toEqual([
				"np-1",
			]);
			expect(await state.countActivationsByClient(0)).toEqual({
				"claude-code": 1,
			});
		} finally {
			await state.close();
			warn.mockRestore();
		}

		// The upgrade completed: the step was skipped with a warning, not rolled back.
		expect(skipped).toHaveLength(1);
		expect(await schemaVersions()).toEqual([2, 3, 4, 5, 6]);
		expect(await indexExists("idx_pending_session")).toBe(false);
	});
});
