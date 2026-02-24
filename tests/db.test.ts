import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { KnowledgeDB } from "../src/db/database";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

describe("KnowledgeDB", () => {
  let db: KnowledgeDB;
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "knowledge-test-"));
    db = new KnowledgeDB(join(tempDir, "test.db"));
  });

  afterEach(() => {
    db.close();
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("should create tables on initialization", () => {
    const stats = db.getStats();
    expect(stats.total).toBe(0);
  });

  it("should insert and retrieve an entry", () => {
    const now = Date.now();
    db.insertEntry({
      id: "test-1",
      type: "fact",
      content: "Churn rate is 4.2%",
      topics: ["churn", "metrics"],
      confidence: 0.9,
      source: "session: Churn Analysis",
      scope: "team",
      status: "active",
      strength: 1.0,
      createdAt: now,
      updatedAt: now,
      lastAccessedAt: now,
      accessCount: 0,
      supersededBy: null,
      derivedFrom: ["session-123"],
    });

    const entry = db.getEntry("test-1");
    expect(entry).not.toBeNull();
    expect(entry!.content).toBe("Churn rate is 4.2%");
    expect(entry!.topics).toEqual(["churn", "metrics"]);
    expect(entry!.scope).toBe("team");
    expect(entry!.derivedFrom).toEqual(["session-123"]);
  });

  it("should update entry fields", () => {
    const now = Date.now();
    db.insertEntry({
      id: "test-2",
      type: "fact",
      content: "Old content",
      topics: ["test"],
      confidence: 0.5,
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

    db.updateEntry("test-2", {
      content: "New content",
      confidence: 0.8,
      status: "superseded",
      supersededBy: "test-3",
    });

    const entry = db.getEntry("test-2");
    expect(entry!.content).toBe("New content");
    expect(entry!.confidence).toBe(0.8);
    expect(entry!.status).toBe("superseded");
    expect(entry!.supersededBy).toBe("test-3");
  });

  it("should record access and increment count", () => {
    const now = Date.now();
    db.insertEntry({
      id: "test-3",
      type: "principle",
      content: "Test principle",
      topics: [],
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

    db.recordAccess("test-3");
    db.recordAccess("test-3");
    db.recordAccess("test-3");

    const entry = db.getEntry("test-3");
    expect(entry!.accessCount).toBe(3);
    expect(entry!.lastAccessedAt).toBeGreaterThanOrEqual(now);
  });

  it("should filter entries by status", () => {
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

    db.insertEntry(makeEntry("a1", "active"));
    db.insertEntry(makeEntry("a2", "active"));
    db.insertEntry(makeEntry("a3", "archived"));

    const active = db.getActiveEntries();
    expect(active.length).toBe(2);

    const archived = db.getEntriesByStatus("archived");
    expect(archived.length).toBe(1);
  });

  it("should store and retrieve embeddings", () => {
    const now = Date.now();
    const embedding = [0.1, 0.2, 0.3, 0.4, 0.5];

    db.insertEntry({
      id: "test-emb",
      type: "fact",
      content: "Entry with embedding",
      topics: [],
      confidence: 0.5,
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
      embedding,
    });

    const entry = db.getEntry("test-emb");
    expect(entry!.embedding).toBeDefined();
    expect(entry!.embedding!.length).toBe(5);
    // Float32 precision — check approximate equality
    for (let i = 0; i < embedding.length; i++) {
      expect(Math.abs(entry!.embedding![i] - embedding[i])).toBeLessThan(
        0.0001
      );
    }
  });

  it("should manage consolidation state", () => {
    const state = db.getConsolidationState();
    expect(state.lastConsolidatedAt).toBe(0);
    expect(state.totalSessionsProcessed).toBe(0);

    db.updateConsolidationState({
      lastConsolidatedAt: 1000000,
      lastSessionTimeCreated: 999999,
      totalSessionsProcessed: 50,
      totalEntriesCreated: 25,
    });

    const updated = db.getConsolidationState();
    expect(updated.lastConsolidatedAt).toBe(1000000);
    expect(updated.totalSessionsProcessed).toBe(50);
    expect(updated.totalEntriesCreated).toBe(25);
  });

  it("should handle relations between entries", () => {
    const now = Date.now();
    const makeEntry = (id: string) => ({
      id,
      type: "fact" as const,
      content: `Entry ${id}`,
      topics: [],
      confidence: 0.5,
      source: "test",
      scope: "personal" as const,
      status: "active" as const,
      strength: 1.0,
      createdAt: now,
      updatedAt: now,
      lastAccessedAt: now,
      accessCount: 0,
      supersededBy: null,
      derivedFrom: [],
    });

    db.insertEntry(makeEntry("e1"));
    db.insertEntry(makeEntry("e2"));

    db.insertRelation({
      id: "rel-1",
      sourceId: "e1",
      targetId: "e2",
      type: "supports",
      createdAt: now,
    });

    const relations = db.getRelationsFor("e1");
    expect(relations.length).toBe(1);
    expect(relations[0].type).toBe("supports");
    expect(relations[0].targetId).toBe("e2");
  });

  it("should return correct stats", () => {
    const now = Date.now();
    const makeEntry = (id: string, status: string) => ({
      id,
      type: "fact" as const,
      content: `Entry ${id}`,
      topics: [],
      confidence: 0.5,
      source: "test",
      scope: "personal" as const,
      status: status as "active" | "archived" | "superseded",
      strength: 1.0,
      createdAt: now,
      updatedAt: now,
      lastAccessedAt: now,
      accessCount: 0,
      supersededBy: null,
      derivedFrom: [],
    });

    db.insertEntry(makeEntry("s1", "active"));
    db.insertEntry(makeEntry("s2", "active"));
    db.insertEntry(makeEntry("s3", "active"));
    db.insertEntry(makeEntry("s4", "archived"));
    db.insertEntry(makeEntry("s5", "superseded"));

    const stats = db.getStats();
    expect(stats.total).toBe(5);
    expect(stats.active).toBe(3);
    expect(stats.archived).toBe(1);
    expect(stats.superseded).toBe(1);
  });
});
