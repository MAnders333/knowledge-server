import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import type { KnowledgeDB } from "../src/db/database";
import { KnowledgeDB as KnowledgeDBImpl } from "../src/db/database";
import { ActivationEngine } from "../src/activation/activate";
import type { ConsolidationEngine } from "../src/consolidation/consolidate";
import { createApp } from "../src/api/server";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const TEST_ADMIN_TOKEN = "test-admin-token-abc123";

describe("HTTP API", () => {
  let db: KnowledgeDB;
  let tempDir: string;
  let app: ReturnType<typeof createApp>;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "knowledge-api-test-"));
    db = new KnowledgeDBImpl(join(tempDir, "test.db"));
    const activation = new ActivationEngine(db);
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
      close: () => {},
    } as unknown as ConsolidationEngine;
    app = createApp(db, activation, consolidation, TEST_ADMIN_TOKEN);
  });

  afterEach(() => {
    db.close();
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("GET /status should return server info", async () => {
    const res = await app.request("/status");
    expect(res.status).toBe(200);

    const data = await res.json();
    expect(data.status).toBe("ok");
    expect(data.version).toBe("0.2.0");
    expect(data.knowledge).toBeDefined();
    expect(data.consolidation).toBeDefined();
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
    db.insertEntry({
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
      supersededBy: null,
      derivedFrom: [],
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
      supersededBy: null,
      derivedFrom: [],
    });

    db.insertEntry(makeEntry("f1", "active"));
    db.insertEntry(makeEntry("f2", "archived"));

    const activeRes = await app.request("/entries?status=active");
    const activeData = await activeRes.json();
    expect(activeData.count).toBe(1);
    expect(activeData.entries[0].id).toBe("f1");
  });

  it("GET /entries/:id should return a specific entry", async () => {
    const now = Date.now();
    db.insertEntry({
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

  it("POST /reinitialize should return 401 without token", async () => {
    const res = await app.request("/reinitialize?confirm=yes", { method: "POST" });
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
});
