/**
 * Integration tests for the consolidation engine.
 *
 * These tests use a real in-memory KnowledgeDB but mock the LLM and embedding
 * clients so they run fast and offline. They verify the core reconsolidation
 * logic, contradiction scan wiring, and decay behaviour.
 */
import { describe, it, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { KnowledgeDB } from "../src/db/database";
import { ActivationEngine } from "../src/activation/activate";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { EpisodeReader } from "../src/consolidation/episodes";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

// ── helpers ──────────────────────────────────────────────────────────────────

function makeEntry(overrides: Partial<Parameters<KnowledgeDB["insertEntry"]>[0]> & { id: string }) {
  const now = Date.now();
  return {
    id: overrides.id,
    type: "fact" as const,
    content: `Content for ${overrides.id}`,
    topics: ["test"],
    confidence: 0.8,
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
    ...overrides,
  };
}

// Deterministic fake embedding: encodes the first 3 chars of content as a unit vector.
// Two entries with the same first 3 chars will have similarity 1.0 (near-duplicate).
// Entries with different first 3 chars will have much lower similarity.
function fakeEmbedding(content: string): number[] {
  const vec = new Array(8).fill(0);
  for (let i = 0; i < Math.min(3, content.length); i++) {
    vec[i % 8] += content.charCodeAt(i);
  }
  const norm = Math.sqrt(vec.reduce((s, v) => s + v * v, 0)) || 1;
  return vec.map((v) => v / norm);
}

// ── fixtures ─────────────────────────────────────────────────────────────────

let db: KnowledgeDB;
let activation: ActivationEngine;
let engine: ConsolidationEngine;
let tempDir: string;

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), "ks-consolidation-test-"));
  db = new KnowledgeDB(join(tempDir, "test.db"), join(tempDir, "opencode-fake.db"));
  activation = new ActivationEngine(db);
  engine = new ConsolidationEngine(db, activation);
});

afterEach(() => {
  db.close();
  rmSync(tempDir, { recursive: true, force: true });
});

// ── tests ─────────────────────────────────────────────────────────────────────

describe("ConsolidationEngine.applyDecay (via consolidate early-return path)", () => {
  it("archives entries below threshold even when no new sessions exist", async () => {
    // Insert a very weak entry that should be archived
    const weakEntry = makeEntry({
      id: "weak-1",
      content: "Weak entry",
      strength: 0.05, // below default archiveThreshold of 0.15
      lastAccessedAt: Date.now() - 365 * 24 * 60 * 60 * 1000, // 1 year ago
      createdAt: Date.now() - 365 * 24 * 60 * 60 * 1000,
      updatedAt: Date.now() - 365 * 24 * 60 * 60 * 1000,
    });
    db.insertEntry(weakEntry);

    // Mock getCandidateSessions so consolidate() hits the early-return path
    // without opening the real OpenCode DB (which may have sessions in CI).
    spyOn(EpisodeReader.prototype, "getCandidateSessions").mockReturnValue([]);
    // Mock ensureEmbeddings to avoid real embedding network calls
    spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

    const result = await engine.consolidate();

    expect(result.sessionsProcessed).toBe(0);
    // The key invariant: early-return still calls applyDecay
    expect(result.entriesArchived).toBe(1);
  });
});

describe("ConsolidationEngine — concurrency guard", () => {
  it("isConsolidating flag starts false", () => {
    expect(engine.isConsolidating).toBe(false);
  });

  it("isConsolidating flag is cleared after consolidate() completes", async () => {
    spyOn(EpisodeReader.prototype, "getCandidateSessions").mockReturnValue([]);
    spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

    expect(engine.isConsolidating).toBe(false);
    const promise = engine.consolidate();
    // The flag could be set or unset here depending on microtask scheduling —
    // what matters is it's false after the call resolves
    await promise;
    expect(engine.isConsolidating).toBe(false);
  });
});

describe("KnowledgeDB — getEntriesWithOverlappingTopics", () => {
  it("returns active entries sharing a topic, excluding specified IDs", () => {
    const emb = fakeEmbedding("test");
    db.insertEntry(makeEntry({ id: "a1", topics: ["typescript", "bun"], embedding: emb }));
    db.insertEntry(makeEntry({ id: "a2", topics: ["typescript", "sqlite"], embedding: emb }));
    db.insertEntry(makeEntry({ id: "a3", topics: ["python"], embedding: emb }));

    const results = db.getEntriesWithOverlappingTopics(
      ["typescript", "bun"],
      ["a1"] // exclude a1
    );

    const ids = results.map((r) => r.id);
    expect(ids).toContain("a2");
    expect(ids).not.toContain("a1"); // excluded
    expect(ids).not.toContain("a3"); // no topic overlap
  });

  it("returns empty array when topics list is empty", () => {
    const emb = fakeEmbedding("test");
    db.insertEntry(makeEntry({ id: "b1", topics: ["test"], embedding: emb }));
    const results = db.getEntriesWithOverlappingTopics([], []);
    expect(results).toEqual([]);
  });

  it("does not return archived or superseded entries", () => {
    const emb = fakeEmbedding("test");
    db.insertEntry(makeEntry({ id: "c1", topics: ["shared"], status: "archived", embedding: emb }));
    db.insertEntry(makeEntry({ id: "c2", topics: ["shared"], status: "superseded", embedding: emb }));
    db.insertEntry(makeEntry({ id: "c3", topics: ["shared"], status: "active", embedding: emb }));

    const results = db.getEntriesWithOverlappingTopics(["shared"], []);
    const ids = results.map((r) => r.id);
    expect(ids).toContain("c3");
    expect(ids).not.toContain("c1");
    expect(ids).not.toContain("c2");
  });
});

describe("KnowledgeDB — applyContradictionResolution", () => {
  it("supersede_old: marks existing entry as superseded, inserts supersedes relation", () => {
    db.insertEntry(makeEntry({ id: "new-1", topics: ["test"] }));
    db.insertEntry(makeEntry({ id: "old-1", topics: ["test"] }));

    db.applyContradictionResolution("supersede_old", "new-1", "old-1");

    const old = db.getEntry("old-1");
    expect(old?.status).toBe("superseded");
    expect(old?.supersededBy).toBe("new-1");

    const relations = db.getRelationsFor("new-1");
    expect(relations.some((r) => r.type === "supersedes" && r.targetId === "old-1")).toBe(true);
  });

  it("supersede_new: marks new entry as superseded, inserts supersedes relation", () => {
    db.insertEntry(makeEntry({ id: "new-2", topics: ["test"] }));
    db.insertEntry(makeEntry({ id: "old-2", topics: ["test"] }));

    db.applyContradictionResolution("supersede_new", "new-2", "old-2");

    const newEntry = db.getEntry("new-2");
    expect(newEntry?.status).toBe("superseded");
    expect(newEntry?.supersededBy).toBe("old-2");
  });

  it("irresolvable: marks BOTH entries as conflicted, inserts contradicts relation", () => {
    db.insertEntry(makeEntry({ id: "new-3", topics: ["test"] }));
    db.insertEntry(makeEntry({ id: "old-3", topics: ["test"] }));

    db.applyContradictionResolution("irresolvable", "new-3", "old-3");

    // Both halves of the conflict must be visible in the /review queue
    const newEntry = db.getEntry("new-3");
    expect(newEntry?.status).toBe("conflicted");

    const old = db.getEntry("old-3");
    expect(old?.status).toBe("conflicted");

    const relations = db.getRelationsFor("new-3");
    expect(relations.some((r) => r.type === "contradicts")).toBe(true);
  });

  it("merge: updates new entry content, marks existing as superseded", () => {
    db.insertEntry(makeEntry({ id: "new-4", content: "Original content", topics: ["test"] }));
    db.insertEntry(makeEntry({ id: "old-4", topics: ["test"] }));

    db.applyContradictionResolution("merge", "new-4", "old-4", {
      content: "Merged unified content",
      type: "principle",
      topics: ["test", "merged"],
      confidence: 0.9,
    });

    const newEntry = db.getEntry("new-4");
    expect(newEntry?.content).toBe("Merged unified content");
    expect(newEntry?.type).toBe("principle");
    expect(newEntry?.status).toBe("active"); // new entry stays active

    const old = db.getEntry("old-4");
    expect(old?.status).toBe("superseded");
  });

  it("merge: clamps invalid LLM type to 'fact' rather than crashing", () => {
    db.insertEntry(makeEntry({ id: "new-5", content: "Original", topics: ["test"] }));
    db.insertEntry(makeEntry({ id: "old-5", topics: ["test"] }));

    // LLM occasionally returns values like "fact/principle" that violate the CHECK constraint
    expect(() => {
      db.applyContradictionResolution("merge", "new-5", "old-5", {
        content: "Merged content",
        type: "fact/principle", // invalid — would previously throw SQLITE_CONSTRAINT_CHECK
        topics: ["test"],
        confidence: 0.8,
      });
    }).not.toThrow();

    const newEntry = db.getEntry("new-5");
    expect(newEntry?.type).toBe("fact"); // clamped to valid fallback
    expect(newEntry?.content).toBe("Merged content");

    const oldEntry = db.getEntry("old-5");
    expect(oldEntry?.status).toBe("superseded");
  });
});

describe("KnowledgeDB — getEntries with filters", () => {
  it("filters by status", () => {
    db.insertEntry(makeEntry({ id: "s1", status: "active" }));
    db.insertEntry(makeEntry({ id: "s2", status: "archived" }));

    const active = db.getEntries({ status: "active" });
    expect(active.map((e) => e.id)).toContain("s1");
    expect(active.map((e) => e.id)).not.toContain("s2");
  });

  it("filters by type", () => {
    db.insertEntry(makeEntry({ id: "t1", type: "fact" }));
    db.insertEntry(makeEntry({ id: "t2", type: "principle" }));

    const facts = db.getEntries({ type: "fact" });
    expect(facts.map((e) => e.id)).toContain("t1");
    expect(facts.map((e) => e.id)).not.toContain("t2");
  });

  it("filters by scope", () => {
    db.insertEntry(makeEntry({ id: "sc1", scope: "personal" }));
    db.insertEntry(makeEntry({ id: "sc2", scope: "team" }));

    const team = db.getEntries({ scope: "team" });
    expect(team.map((e) => e.id)).toContain("sc2");
    expect(team.map((e) => e.id)).not.toContain("sc1");
  });

  it("returns all entries when no filters given", () => {
    db.insertEntry(makeEntry({ id: "all1" }));
    db.insertEntry(makeEntry({ id: "all2", status: "archived" }));

    const all = db.getEntries({});
    expect(all.length).toBe(2);
  });

  it("combines multiple filters", () => {
    db.insertEntry(makeEntry({ id: "m1", type: "fact", scope: "team", status: "active" }));
    db.insertEntry(makeEntry({ id: "m2", type: "fact", scope: "personal", status: "active" }));
    db.insertEntry(makeEntry({ id: "m3", type: "principle", scope: "team", status: "active" }));

    const results = db.getEntries({ type: "fact", scope: "team" });
    expect(results.map((e) => e.id)).toEqual(["m1"]);
  });
});

describe("KnowledgeDB — conflicted entries included in similarity queries", () => {
  it("getEntriesWithOverlappingTopics returns conflicted entries alongside active ones", () => {
    const emb = fakeEmbedding("abc");
    db.insertEntry(makeEntry({ id: "ot-active", topics: ["shared"], status: "active", embedding: emb }));
    db.insertEntry(makeEntry({ id: "ot-conflicted", topics: ["shared"], status: "conflicted", embedding: emb }));
    db.insertEntry(makeEntry({ id: "ot-archived", topics: ["shared"], status: "archived", embedding: emb }));
    db.insertEntry(makeEntry({ id: "ot-superseded", topics: ["shared"], status: "superseded", embedding: emb }));

    const results = db.getEntriesWithOverlappingTopics(["shared"], []);
    const ids = results.map((r) => r.id);
    expect(ids).toContain("ot-active");
    expect(ids).toContain("ot-conflicted");
    expect(ids).not.toContain("ot-archived");
    expect(ids).not.toContain("ot-superseded");
  });

  it("getActiveEntriesWithEmbeddings returns conflicted entries alongside active ones", () => {
    const emb = fakeEmbedding("abc");
    db.insertEntry(makeEntry({ id: "ae-active", status: "active", embedding: emb }));
    db.insertEntry(makeEntry({ id: "ae-conflicted", status: "conflicted", embedding: emb }));
    db.insertEntry(makeEntry({ id: "ae-archived", status: "archived", embedding: emb }));

    const results = db.getActiveEntriesWithEmbeddings();
    const ids = results.map((r) => r.id);
    expect(ids).toContain("ae-active");
    expect(ids).toContain("ae-conflicted");
    expect(ids).not.toContain("ae-archived");
  });
});

describe("KnowledgeDB — applyContradictionResolution clears conflicted status on winner", () => {
  it("supersede_old: orphaned conflict counterpart of the loser is restored to active", () => {
    // "loser" and "winner" are an irresolvable pair
    db.insertEntry(makeEntry({ id: "winner", topics: ["x"] }));
    db.insertEntry(makeEntry({ id: "loser", topics: ["x"] }));
    db.applyContradictionResolution("irresolvable", "loser", "winner");

    expect(db.getEntry("winner")?.status).toBe("conflicted");
    expect(db.getEntry("loser")?.status).toBe("conflicted");

    // New entry decisively supersedes the loser
    db.insertEntry(makeEntry({ id: "new-decisive", topics: ["x"] }));
    db.applyContradictionResolution("supersede_old", "new-decisive", "loser");

    // loser is superseded
    expect(db.getEntry("loser")?.status).toBe("superseded");
    // winner (the orphaned counterpart) must be restored to active
    expect(db.getEntry("winner")?.status).toBe("active");
    // winner's contradicts relation should be gone
    expect(db.getRelationsFor("winner").some((r) => r.type === "contradicts")).toBe(false);
  });

  it("supersede_old: winner that was conflicted is restored to active, its counterpart too", () => {
    // conf-a and conf-b are an irresolvable pair; new-entry arrives and wins over conf-b
    db.insertEntry(makeEntry({ id: "conf-a", status: "active", topics: ["topic"] }));
    db.insertEntry(makeEntry({ id: "conf-b", status: "active", topics: ["topic"] }));
    db.applyContradictionResolution("irresolvable", "conf-a", "conf-b");

    expect(db.getEntry("conf-a")?.status).toBe("conflicted");
    expect(db.getEntry("conf-b")?.status).toBe("conflicted");

    // conf-a (new entry in this call) was conflicted with conf-b but now decisively wins
    // over a third entry "old-z". Winning should settle conf-a's prior conflict:
    // conf-a restored to active, conf-b (its orphaned counterpart) also restored.
    db.insertEntry(makeEntry({ id: "old-z", topics: ["topic"] }));
    db.applyContradictionResolution("supersede_old", "conf-a", "old-z");

    // old-z is superseded (the loser)
    expect(db.getEntry("old-z")?.status).toBe("superseded");
    // conf-a (the winner) was conflicted — should now be active
    expect(db.getEntry("conf-a")?.status).toBe("active");
    // conf-b (conf-a's orphaned counterpart) should also be restored to active
    expect(db.getEntry("conf-b")?.status).toBe("active");
    // conf-a's contradicts relation should be gone
    expect(db.getRelationsFor("conf-a").some((r) => r.type === "contradicts")).toBe(false);
  });

  it("supersede_new: winner that was conflicted is restored to active, its counterpart too", () => {
    // conf-a and conf-b are an irresolvable pair; new-entry arrives and loses to conf-b
    db.insertEntry(makeEntry({ id: "conf-a", status: "active", topics: ["topic"] }));
    db.insertEntry(makeEntry({ id: "conf-b", status: "active", topics: ["topic"] }));
    db.applyContradictionResolution("irresolvable", "conf-a", "conf-b");

    expect(db.getEntry("conf-a")?.status).toBe("conflicted");
    expect(db.getEntry("conf-b")?.status).toBe("conflicted");

    // new-entry (loser) has no prior conflict; conf-b (winner) was conflicted with conf-a.
    // Winning this battle decisively settles conf-b's conflict — both conf-b and conf-a
    // should be restored to active.
    db.insertEntry(makeEntry({ id: "new-entry", topics: ["topic"] }));
    db.applyContradictionResolution("supersede_new", "new-entry", "conf-b");

    expect(db.getEntry("new-entry")?.status).toBe("superseded");
    // conf-b won — should be restored to active
    expect(db.getEntry("conf-b")?.status).toBe("active");
    // conf-a (conf-b's orphaned counterpart) should also be restored
    expect(db.getEntry("conf-a")?.status).toBe("active");
    expect(db.getRelationsFor("conf-b").some((r) => r.type === "contradicts")).toBe(false);
  });

  it("supersede_new: restores the loser's conflict counterpart when loser was conflicted", () => {
    // conf-p and conf-q are an irresolvable pair
    db.insertEntry(makeEntry({ id: "conf-p", topics: ["topic"] }));
    db.insertEntry(makeEntry({ id: "conf-q", topics: ["topic"] }));
    db.applyContradictionResolution("irresolvable", "conf-p", "conf-q");

    // decisive-new arrives and supersede_new means conf-q wins (existing) — conf-p is superseded
    // conf-p was conf-q's conflict partner — conf-q should be restored to active
    db.insertEntry(makeEntry({ id: "decisive-new", topics: ["topic"] }));
    db.applyContradictionResolution("supersede_new", "conf-p", "decisive-new");

    // conf-p (the loser/new entry in this call) is superseded
    expect(db.getEntry("conf-p")?.status).toBe("superseded");
    // decisive-new (the winner) was never conflicted, stays active
    expect(db.getEntry("decisive-new")?.status).toBe("active");
    // conf-q (conf-p's orphaned counterpart) must be restored to active
    expect(db.getEntry("conf-q")?.status).toBe("active");
    expect(db.getRelationsFor("conf-q").some((r) => r.type === "contradicts")).toBe(false);
  });

  it("merge: when the new (winning) entry was conflicted, it is restored to active after merge", () => {
    // conf-x and conf-y are in irresolvable conflict
    db.insertEntry(makeEntry({ id: "conf-x", status: "active", topics: ["topic"] }));
    db.insertEntry(makeEntry({ id: "conf-y", status: "active", topics: ["topic"] }));
    db.applyContradictionResolution("irresolvable", "conf-x", "conf-y");

    expect(db.getEntry("conf-x")?.status).toBe("conflicted");

    // conf-x (the "new" entry in this call) wins via merge over a third entry "old-z"
    db.insertEntry(makeEntry({ id: "old-z", status: "active", topics: ["topic"] }));
    db.applyContradictionResolution("merge", "conf-x", "old-z", {
      content: "Merged decisive content",
      type: "fact",
      topics: ["topic"],
      confidence: 0.9,
    });

    // conf-x was conflicted — it's the winning entry in this merge, should now be active
    expect(db.getEntry("conf-x")?.status).toBe("active");
    // Its contradicts relations should be gone
    const relX = db.getRelationsFor("conf-x");
    expect(relX.some((r) => r.type === "contradicts")).toBe(false);
    // old-z is superseded
    expect(db.getEntry("old-z")?.status).toBe("superseded");
  });
});

describe("ActivationEngine — contradiction annotation on activated conflicted entries", () => {
  it("annotates conflicted entry when both sides of the conflict activate", async () => {
    // Both entries share the same embedding prefix — they will both score above
    // the similarity threshold for any query with that prefix.
    const emb = fakeEmbedding("abc");
    db.insertEntry(makeEntry({ id: "side-a", content: "abc approach A", status: "active", embedding: emb }));
    db.insertEntry(makeEntry({ id: "side-b", content: "abc approach B", status: "active", embedding: emb }));
    db.applyContradictionResolution("irresolvable", "side-a", "side-b");

    // Both are now conflicted — mock embedBatch so activate() uses deterministic vectors.
    // activate() always calls embedBatch (even for a single string query).
    const embedSpy = spyOn(activation["embeddings"], "embedBatch").mockResolvedValue([emb]);

    const result = await activation.activate("abc query");

    const sideA = result.entries.find((e) => e.entry.id === "side-a");
    const sideB = result.entries.find((e) => e.entry.id === "side-b");

    // Both should be present
    expect(sideA).toBeDefined();
    expect(sideB).toBeDefined();

    // Both should have contradiction annotations pointing at each other
    expect(sideA?.contradiction).toBeDefined();
    expect(sideA?.contradiction?.conflictingEntryId).toBe("side-b");
    expect(sideA?.contradiction?.conflictingContent).toBe("abc approach B");
    expect(sideA?.contradiction?.caveat).toContain("conflicts");

    expect(sideB?.contradiction).toBeDefined();
    expect(sideB?.contradiction?.conflictingEntryId).toBe("side-a");

    embedSpy.mockRestore();
  });

  it("does NOT annotate when the counterpart did not activate (below similarity threshold)", async () => {
    // side-c and side-d use orthogonal embeddings — cosine similarity = 0.
    // [1,0,0,...] and [0,1,0,...] are perpendicular, so their dot product is 0.
    const embC = [1, 0, 0, 0, 0, 0, 0, 0];
    const embD = [0, 1, 0, 0, 0, 0, 0, 0];
    db.insertEntry(makeEntry({ id: "side-c", content: "abc thing", status: "active", embedding: embC }));
    db.insertEntry(makeEntry({ id: "side-d", content: "xyz thing", status: "active", embedding: embD }));
    db.applyContradictionResolution("irresolvable", "side-c", "side-d");

    // Query embedding == side-c's embedding (similarity 1.0 with side-c, 0.0 with side-d)
    const embedSpy = spyOn(activation["embeddings"], "embedBatch").mockResolvedValue([embC]);

    const result = await activation.activate("abc query");

    const sideC = result.entries.find((e) => e.entry.id === "side-c");
    const sideD = result.entries.find((e) => e.entry.id === "side-d");

    // side-c activates (similarity = 1.0 * strength); side-d does not (similarity = 0)
    expect(sideC).toBeDefined();
    expect(sideD).toBeUndefined();

    // side-c should NOT be annotated since its counterpart didn't activate
    expect(sideC?.contradiction).toBeUndefined();

    embedSpy.mockRestore();
  });

  it("active entries are never annotated even if they have no contradicting partner", async () => {
    const emb = fakeEmbedding("abc");
    db.insertEntry(makeEntry({ id: "plain", content: "abc plain entry", status: "active", embedding: emb }));

    const embedSpy = spyOn(activation["embeddings"], "embedBatch").mockResolvedValue([emb]);

    const result = await activation.activate("abc query");

    const plain = result.entries.find((e) => e.entry.id === "plain");
    expect(plain).toBeDefined();
    expect(plain?.contradiction).toBeUndefined();

    embedSpy.mockRestore();
  });
});
