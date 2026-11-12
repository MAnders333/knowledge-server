import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import postgres from "postgres";
import { PostgresKnowledgeDB } from "../src/db/postgres/index";

/** Needs PG_TEST_URI (see tests/pg-integration.test.ts); skipped otherwise. */
const PG_URI = process.env.PG_TEST_URI;

describe.skipIf(!PG_URI)("PostgresKnowledgeDB injectionCount", () => {
	const uri = PG_URI as string;
	let db: PostgresKnowledgeDB;
	let sql: postgres.Sql;

	beforeAll(async () => {
		sql = postgres(uri, { max: 1 });
		db = new PostgresKnowledgeDB(uri);
		await db.initialize();
		await sql`TRUNCATE knowledge_entry, knowledge_cluster, embedding_metadata, schema_version CASCADE`;
	});

	afterAll(async () => {
		await db.close();
		await sql.end();
	});

	it("reads injection_count into injectionCount", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "pg-inj",
			type: "fact",
			content: "Injected fact",
			topics: [],
			confidence: 0.7,
			source: "test",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});
		expect((await db.getEntry("pg-inj"))?.injectionCount).toBe(0);
		await db.recordInjections(["pg-inj"], now + 1);
		expect((await db.getEntry("pg-inj"))?.injectionCount).toBe(1);
	});
});
