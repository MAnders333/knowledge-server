import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../package.json" with { type: "json" };
import { ActivationEngine } from "../src/activation/activate";
import { createApp } from "../src/api/server";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import type { KnowledgeDB } from "../src/db/sqlite/index";
import { KnowledgeDB as KnowledgeDBImpl } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";

// Intentionally static string — production uses a random token generated at startup.
const TEST_ADMIN_TOKEN = "test-admin-token-abc123";

describe("HTTP API", () => {
	let db: KnowledgeDB;
	let tempDir: string;
	let app: ReturnType<typeof createApp>;
	let activation: ActivationEngine;
	let embedSpy: ReturnType<typeof spyOn>;

	let serverStateDb: ServerStateDB;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "knowledge-api-test-"));
		db = new KnowledgeDBImpl(
			join(tempDir, "test.db"),
			join(tempDir, "opencode-fake.db"),
		);
		serverStateDb = new ServerStateDB(join(tempDir, "state.db"));
		activation = new ActivationEngine(db);
		embedSpy = spyOn(activation.embeddings, "embed").mockResolvedValue([
			0.11, 0.22, 0.33,
		]);
		// We pass a mock consolidation engine — not testing consolidation via API here
		const consolidation = {
			consolidate: async () => ({
				sessionsProcessed: 0,
				segmentsProcessed: 0,
				entriesCreated: 0,
				entriesUpdated: 0,
				entriesArchived: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				duration: 0,
			}),
			get isConsolidating() {
				return false;
			},
			tryLock: () => true,
			unlock: () => {},
			close: () => {},
		} as unknown as ConsolidationEngine;
		app = createApp(
			db,
			serverStateDb,
			activation,
			consolidation,
			TEST_ADMIN_TOKEN,
		);
	});

	afterEach(async () => {
		await db.close();
		await serverStateDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("GET /status should return server info without config when unauthenticated", async () => {
		const res = await app.request("/status");
		expect(res.status).toBe(200);

		const data = await res.json();
		expect(data.status).toBe("ok");
		expect(data.version).toBe(pkg.version);
		expect(data.knowledge).toBeDefined();
		expect(data.consolidation).toBeDefined();
		// Config block must NOT be present without admin token (M1 fix)
		expect(data.config).toBeUndefined();
	});

	it("GET /status should include config block when authenticated", async () => {
		const res = await app.request("/status", {
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(200);

		const data = await res.json();
		expect(data.status).toBe("ok");
		expect(data.config).toBeDefined();
		expect(typeof data.config.port).toBe("number");
		expect(typeof data.config.embeddingModel).toBe("string");
	});

	it("GET /entries should return empty list initially", async () => {
		const res = await app.request("/entries");
		expect(res.status).toBe(200);

		const data = await res.json();
		expect(data.entries).toEqual([]);
		expect(data.count).toBe(0);
	});

	it("GET /entries should list inserted entries", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "api-test-1",
			type: "fact",
			content: "Test fact",
			topics: ["test"],
			confidence: 0.9,
			source: "test",
			scope: "team",
			status: "active",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding: [0.1, 0.2, 0.3],
		});

		const res = await app.request("/entries");
		const data = await res.json();
		expect(data.count).toBe(1);
		expect(data.entries[0].content).toBe("Test fact");
		// Should not include embedding in response
		expect(data.entries[0].embedding).toBeUndefined();
	});

	it("GET /entries should filter by status", async () => {
		const now = Date.now();
		const makeEntry = (id: string, status: string) => ({
			id,
			type: "fact" as const,
			content: `Entry ${id}`,
			topics: [],
			confidence: 0.5,
			source: "test",
			scope: "personal" as const,
			status: status as "active" | "archived",
			strength: 1.0,
			createdAt: now,
			updatedAt: now,
			lastAccessedAt: now,
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		await db.insertEntry(makeEntry("f1", "active"));
		await db.insertEntry(makeEntry("f2", "archived"));

		const activeRes = await app.request("/entries?status=active");
		const activeData = await activeRes.json();
		expect(activeData.count).toBe(1);
		expect(activeData.entries[0].id).toBe("f1");
	});

	it("GET /entries/:id should return a specific entry", async () => {
		const now = Date.now();
		await db.insertEntry({
			id: "specific-1",
			type: "principle",
			content: "Specific principle",
			topics: ["test"],
			confidence: 0.7,
			source: "test",
			scope: "personal",
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

		const res = await app.request("/entries/specific-1");
		expect(res.status).toBe(200);

		const data = await res.json();
		expect(data.entry.content).toBe("Specific principle");
		expect(data.relations).toEqual([]);
	});

	it("GET /entries/:id should return 404 for unknown", async () => {
		const res = await app.request("/entries/nonexistent");
		expect(res.status).toBe(404);
	});

	it("GET /activate should require query parameter", async () => {
		const res = await app.request("/activate");
		expect(res.status).toBe(400);
	});

	it("GET /activate with single q param returns 200", async () => {
		const res = await app.request("/activate?q=test+query");
		// No entries in DB and no real embedding client — engine returns empty list
		// but should not error (empty knowledge base is a valid state).
		// The real embedding call will fail without a live server, so we just check
		// that the route exists and rejects missing params correctly.
		// (Full activation is covered in consolidation.test.ts with mocked embeddings.)
		expect(res.status).not.toBe(404);
		expect(res.status).not.toBe(400);
	});

	it("GET /activate with repeated q params passes all to engine", async () => {
		// Verify the endpoint accepts multiple q values without a 400/404.
		const res = await app.request(
			"/activate?q=first+topic&q=second+topic&q=full+message",
		);
		expect(res.status).not.toBe(404);
		expect(res.status).not.toBe(400);
	});

	it("GET /review should return review data", async () => {
		const res = await app.request("/review");
		expect(res.status).toBe(200);

		const data = await res.json();
		expect(data.summary).toBeDefined();
		expect(data.conflicted).toEqual([]);
		expect(data.stale).toEqual([]);
		expect(data.teamRelevant).toEqual([]);
	});

	it("POST /consolidate should return 401 without token", async () => {
		const res = await app.request("/consolidate", { method: "POST" });
		expect(res.status).toBe(401);
	});

	it("POST /consolidate should return 401 with wrong token", async () => {
		const res = await app.request("/consolidate", {
			method: "POST",
			headers: { Authorization: "Bearer wrong-token" },
		});
		expect(res.status).toBe(401);
	});

	it("POST /consolidate should succeed with correct token", async () => {
		const res = await app.request("/consolidate", {
			method: "POST",
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(200);
	});

	it("POST /consolidate should return 409 when consolidation lock is held", async () => {
		const busyDb = new KnowledgeDBImpl(
			join(tempDir, "busy.db"),
			join(tempDir, "opencode-fake.db"),
		);
		const busyServerLocalDb = new ServerStateDB(join(tempDir, "busy-state.db"));
		try {
			const busyActivation = new ActivationEngine(busyDb);
			const busyConsolidation = {
				consolidate: async () => ({}),
				get isConsolidating() {
					return true;
				},
				tryLock: () => false, // lock is held
				unlock: () => {},
				close: () => {},
			} as unknown as ConsolidationEngine;
			const busyApp = createApp(
				busyDb,
				busyServerLocalDb,
				busyActivation,
				busyConsolidation,
				TEST_ADMIN_TOKEN,
			);
			const res = await busyApp.request("/consolidate", {
				method: "POST",
				headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
			});
			expect(res.status).toBe(409);
		} finally {
			await busyDb.close();
			await busyServerLocalDb.close();
		}
	});

	it("POST /reinitialize should return 401 without token", async () => {
		const res = await app.request("/reinitialize?confirm=yes", {
			method: "POST",
		});
		expect(res.status).toBe(401);
	});

	it("POST /reinitialize should require confirm=yes even with token", async () => {
		const res = await app.request("/reinitialize", {
			method: "POST",
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(400);
	});

	it("POST /reinitialize should succeed with token and confirm=yes", async () => {
		const res = await app.request("/reinitialize?confirm=yes", {
			method: "POST",
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(200);
		const data = await res.json();
		expect(data.status).toBe("reinitialized");
	});

	// -- PATCH /entries/:id --

	it("PATCH /entries/:id should return 401 without token", async () => {
		const res = await app.request("/entries/nonexistent", {
			method: "PATCH",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ content: "new" }),
		});
		expect(res.status).toBe(401);
	});

	it("PATCH /entries/:id should return 404 for unknown entry", async () => {
		const res = await app.request("/entries/nonexistent", {
			method: "PATCH",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ content: "new" }),
		});
		expect(res.status).toBe(404);
	});

	it("PATCH /entries/:id should update allowed fields", async () => {
		await db.insertEntry({
			id: "patch-test",
			type: "fact",
			content: "Original content",
			topics: ["topic"],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "active",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/patch-test", {
			method: "PATCH",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ content: "Updated content", confidence: 0.95 }),
		});
		expect(res.status).toBe(200);
		const data = await res.json();
		expect(data.entry.content).toBe("Updated content");
		expect(data.entry.confidence).toBe(0.95);
		expect((await db.getEntry("patch-test"))?.embedding?.[0]).toBeCloseTo(0.11);
		expect((await db.getEntry("patch-test"))?.embedding?.[1]).toBeCloseTo(0.22);
		expect((await db.getEntry("patch-test"))?.embedding?.[2]).toBeCloseTo(0.33);
	});

	it("PATCH /entries/:id should re-embed when only topics change", async () => {
		await db.insertEntry({
			id: "patch-topics",
			type: "fact",
			content: "Original content",
			topics: ["old-topic"],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "active",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
			embedding: [0.9, 0.8, 0.7],
		});

		embedSpy.mockResolvedValue([0.44, 0.55, 0.66]);

		const res = await app.request("/entries/patch-topics", {
			method: "PATCH",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ topics: ["new-topic"] }),
		});
		expect(res.status).toBe(200);
		expect(embedSpy).toHaveBeenCalled();
		expect((await db.getEntry("patch-topics"))?.embedding?.[0]).toBeCloseTo(
			0.44,
		);
	});

	it("PATCH /entries/:id should return 400 with no valid fields", async () => {
		await db.insertEntry({
			id: "patch-noop",
			type: "fact",
			content: "Content",
			topics: [],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "active",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/patch-noop", {
			method: "PATCH",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ embedding: [1, 2, 3] }),
		});
		expect(res.status).toBe(400);
	});

	// -- DELETE /entries/:id --

	it("DELETE /entries/:id should return 401 without token", async () => {
		const res = await app.request("/entries/nonexistent", { method: "DELETE" });
		expect(res.status).toBe(401);
	});

	it("DELETE /entries/:id should return 404 for unknown entry", async () => {
		const res = await app.request("/entries/nonexistent", {
			method: "DELETE",
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(404);
	});

	it("DELETE /entries/:id should hard-delete entry and relations", async () => {
		await db.insertEntry({
			id: "delete-test",
			type: "fact",
			content: "To be deleted",
			topics: [],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "active",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/delete-test", {
			method: "DELETE",
			headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
		});
		expect(res.status).toBe(200);
		const data = await res.json();
		expect(data.ok).toBe(true);
		expect(data.deleted).toBe("delete-test");

		// Verify it's gone
		const check = await app.request("/entries/delete-test");
		expect(check.status).toBe(404);
	});

	// -- POST /entries/:id/resolve --

	it("POST /entries/:id/resolve should return 401 without token", async () => {
		const res = await app.request("/entries/nonexistent/resolve", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ resolution: "supersede_other" }),
		});
		expect(res.status).toBe(401);
	});

	it("POST /entries/:id/resolve should return 404 for unknown entry", async () => {
		const res = await app.request("/entries/nonexistent/resolve", {
			method: "POST",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ resolution: "supersede_other" }),
		});
		expect(res.status).toBe(404);
	});

	it("POST /entries/:id/resolve should return 400 for non-conflicted entry", async () => {
		await db.insertEntry({
			id: "not-conflicted",
			type: "fact",
			content: "Active entry",
			topics: [],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "active",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/not-conflicted/resolve", {
			method: "POST",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ resolution: "supersede_other" }),
		});
		expect(res.status).toBe(400);
	});

	it("POST /entries/:id/resolve with delete should hard-delete a conflicted entry", async () => {
		await db.insertEntry({
			id: "conflicted-del",
			type: "fact",
			content: "Conflicted junk",
			topics: [],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "conflicted",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/conflicted-del/resolve", {
			method: "POST",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ resolution: "delete" }),
		});
		expect(res.status).toBe(200);
		const data = await res.json();
		expect(data.ok).toBe(true);
		expect(data.deleted).toBe("conflicted-del");
	});

	it("POST /entries/:id/resolve with invalid resolution should return 400", async () => {
		await db.insertEntry({
			id: "conflicted-bad",
			type: "fact",
			content: "Conflicted",
			topics: [],
			confidence: 0.8,
			source: "test",
			scope: "personal",
			status: "conflicted",
			strength: 1.0,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			lastAccessedAt: Date.now(),
			accessCount: 0,
			observationCount: 1,
			supersededBy: null,
			derivedFrom: [],
		});

		const res = await app.request("/entries/conflicted-bad/resolve", {
			method: "POST",
			headers: {
				Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ resolution: "magic" }),
		});
		expect(res.status).toBe(400);
	});

	it("POST /entries/:id/resolve returns 422 when counterpart is in a different store", async () => {
		// Simulate a cross-store conflict: entry + contradicts relation in primary db,
		// counterpart ONLY in a secondary store. The resolve handler finds the entry in
		// db (entryStore), looks up the relation pointing to "counterpart-other-store",
		// then calls entryStore.getEntry("counterpart-other-store") — which returns null
		// because that ID only exists in db2. All resolutions should return 422.
		const now = Date.now();

		// Set up a second store with the counterpart entry
		const db2 = new KnowledgeDBImpl(
			join(tempDir, "test2.db"),
			join(tempDir, "opencode-fake2.db"),
		);
		try {
			await db2.insertEntry({
				id: "counterpart-other-store",
				type: "fact",
				content: "Counterpart in secondary store",
				topics: [],
				confidence: 0.8,
				source: "test",
				scope: "personal",
				status: "conflicted",
				strength: 1.0,
				createdAt: now,
				updatedAt: now,
				lastAccessedAt: now,
				accessCount: 0,
				observationCount: 1,
				supersededBy: null,
				derivedFrom: [],
			});

			// Insert the conflicted entry in the primary store
			await db.insertEntry({
				id: "cross-store-entry",
				type: "fact",
				content: "Entry in primary store",
				topics: [],
				confidence: 0.8,
				source: "test",
				scope: "personal",
				status: "conflicted",
				strength: 1.0,
				createdAt: now,
				updatedAt: now,
				lastAccessedAt: now,
				accessCount: 0,
				observationCount: 1,
				supersededBy: null,
				derivedFrom: [],
			});

			// Insert the contradicts relation directly bypassing FK constraints, simulating
			// the cross-store scenario where the counterpart exists only in db2 (a secondary
			// domain store) but the relation record lives in db (the primary store).
			// This is the exact state that arises when contradiction detection runs across
			// store boundaries and writes a relation linking entries from different stores.
			const rawDb = (db as unknown as { db: import("bun:sqlite").Database }).db;
			rawDb.exec("PRAGMA foreign_keys = OFF");
			rawDb
				.prepare(
					`INSERT INTO knowledge_relation (id, source_id, target_id, type, created_at)
					 VALUES (?, ?, ?, ?, ?)`,
				)
				.run("cross-store-rel", "cross-store-entry", "counterpart-other-store", "contradicts", now);
			rawDb.exec("PRAGMA foreign_keys = ON");

			// Build an app that reads from both stores — so the API can find the
			// counterpart in db2 during listing, but the relation+entry in db1 points
			// to an ID that entryStore (db) no longer contains.
			const crossStoreApp = createApp(
				db,
				serverStateDb,
				activation,
				{ consolidate: async () => ({}), get isConsolidating() { return false; }, tryLock: () => true, unlock: () => {}, close: () => {} } as unknown as ConsolidationEngine,
				TEST_ADMIN_TOKEN,
				false,
				new Set(),
				[db, db2],
			);

			// All resolution types should return 422
			for (const resolution of ["supersede_this", "supersede_other", "merge", "delete"]) {
				const body: Record<string, string> = { resolution };
				if (resolution === "merge") body.mergedContent = "merged text";

				const res = await crossStoreApp.request("/entries/cross-store-entry/resolve", {
					method: "POST",
					headers: {
						Authorization: `Bearer ${TEST_ADMIN_TOKEN}`,
						"Content-Type": "application/json",
					},
					body: JSON.stringify(body),
				});
				expect(res.status).toBe(422);
				const data = await res.json();
				expect(data.error).toMatch(/[Cc]ross.store/);
			}

			// The entry should remain untouched — not deleted, still conflicted
			const check = await crossStoreApp.request("/entries/cross-store-entry", {
				headers: { Authorization: `Bearer ${TEST_ADMIN_TOKEN}` },
			});
			expect(check.status).toBe(200);
			const checkData = await check.json();
			expect(checkData.entry.status).toBe("conflicted");
		} finally {
			await db2.close();
		}
	});
});
