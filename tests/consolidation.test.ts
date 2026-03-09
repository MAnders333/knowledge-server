/**
 * Integration tests for the consolidation engine.
 *
 * These tests use a real in-memory KnowledgeDB but mock the LLM and embedding
 * clients so they run fast and offline. They verify the core reconsolidation
 * logic, contradiction scan wiring, and decay behaviour.
 */
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { config } from "../src/config";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { ConsolidationLLM } from "../src/consolidation/llm";
import { OpenCodeEpisodeReader } from "../src/consolidation/readers/opencode";
import { KnowledgeDB } from "../src/db/database";
import { RECONSOLIDATION_THRESHOLD } from "../src/types";

// ── helpers ──────────────────────────────────────────────────────────────────

import { fakeEmbedding, makeEntry } from "./fixtures";

// ── fixtures ─────────────────────────────────────────────────────────────────

let db: KnowledgeDB;
let activation: ActivationEngine;
let engine: ConsolidationEngine;
let tempDir: string;

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), "ks-consolidation-test-"));
	const fakeOpenCodeDbPath = join(tempDir, "opencode-fake.db");
	// Create an empty file so OpenCodeEpisodeReader can open it in readonly mode.
	// All tests spy on the prototype methods so no real DB queries ever run.
	writeFileSync(fakeOpenCodeDbPath, "");
	db = new KnowledgeDB(join(tempDir, "test.db"));
	activation = new ActivationEngine(db);
	// Pass a real OpenCodeEpisodeReader instance so prototype spies in each test
	// intercept the methods and no real OpenCode DB queries are executed.
	const reader = new OpenCodeEpisodeReader(fakeOpenCodeDbPath);
	engine = new ConsolidationEngine(db, activation, [reader]);
});

afterEach(() => {
	// Restore all spies so prototype mutations don't leak into other test files
	// (e.g. ConsolidationLLM.prototype.extractKnowledge mocks must not affect llm.test.ts).
	mock.restore();
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
		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([]);
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
		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([]);
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
		db.insertEntry(
			makeEntry({ id: "a1", topics: ["typescript", "bun"], embedding: emb }),
		);
		db.insertEntry(
			makeEntry({ id: "a2", topics: ["typescript", "sqlite"], embedding: emb }),
		);
		db.insertEntry(makeEntry({ id: "a3", topics: ["python"], embedding: emb }));

		const results = db.getEntriesWithOverlappingTopics(
			["typescript", "bun"],
			["a1"], // exclude a1
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
		db.insertEntry(
			makeEntry({
				id: "c1",
				topics: ["shared"],
				status: "archived",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "c2",
				topics: ["shared"],
				status: "superseded",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "c3",
				topics: ["shared"],
				status: "active",
				embedding: emb,
			}),
		);

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
		expect(
			relations.some((r) => r.type === "supersedes" && r.targetId === "old-1"),
		).toBe(true);
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
		db.insertEntry(
			makeEntry({ id: "new-4", content: "Original content", topics: ["test"] }),
		);
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
		db.insertEntry(
			makeEntry({ id: "new-5", content: "Original", topics: ["test"] }),
		);
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
		db.insertEntry(
			makeEntry({ id: "m1", type: "fact", scope: "team", status: "active" }),
		);
		db.insertEntry(
			makeEntry({
				id: "m2",
				type: "fact",
				scope: "personal",
				status: "active",
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "m3",
				type: "principle",
				scope: "team",
				status: "active",
			}),
		);

		const results = db.getEntries({ type: "fact", scope: "team" });
		expect(results.map((e) => e.id)).toEqual(["m1"]);
	});
});

describe("KnowledgeDB — conflicted entries included in similarity queries", () => {
	it("getEntriesWithOverlappingTopics returns conflicted entries alongside active ones", () => {
		const emb = fakeEmbedding("abc");
		db.insertEntry(
			makeEntry({
				id: "ot-active",
				topics: ["shared"],
				status: "active",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "ot-conflicted",
				topics: ["shared"],
				status: "conflicted",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "ot-archived",
				topics: ["shared"],
				status: "archived",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "ot-superseded",
				topics: ["shared"],
				status: "superseded",
				embedding: emb,
			}),
		);

		const results = db.getEntriesWithOverlappingTopics(["shared"], []);
		const ids = results.map((r) => r.id);
		expect(ids).toContain("ot-active");
		expect(ids).toContain("ot-conflicted");
		expect(ids).not.toContain("ot-archived");
		expect(ids).not.toContain("ot-superseded");
	});

	it("getActiveEntriesWithEmbeddings returns conflicted entries alongside active ones", () => {
		const emb = fakeEmbedding("abc");
		db.insertEntry(
			makeEntry({ id: "ae-active", status: "active", embedding: emb }),
		);
		db.insertEntry(
			makeEntry({ id: "ae-conflicted", status: "conflicted", embedding: emb }),
		);
		db.insertEntry(
			makeEntry({ id: "ae-archived", status: "archived", embedding: emb }),
		);

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
		expect(
			db.getRelationsFor("winner").some((r) => r.type === "contradicts"),
		).toBe(false);
	});

	it("supersede_old: winner that was conflicted is restored to active, its counterpart too", () => {
		// conf-a and conf-b are an irresolvable pair; new-entry arrives and wins over conf-b
		db.insertEntry(
			makeEntry({ id: "conf-a", status: "active", topics: ["topic"] }),
		);
		db.insertEntry(
			makeEntry({ id: "conf-b", status: "active", topics: ["topic"] }),
		);
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
		expect(
			db.getRelationsFor("conf-a").some((r) => r.type === "contradicts"),
		).toBe(false);
	});

	it("supersede_new: winner that was conflicted is restored to active, its counterpart too", () => {
		// conf-a and conf-b are an irresolvable pair; new-entry arrives and loses to conf-b
		db.insertEntry(
			makeEntry({ id: "conf-a", status: "active", topics: ["topic"] }),
		);
		db.insertEntry(
			makeEntry({ id: "conf-b", status: "active", topics: ["topic"] }),
		);
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
		expect(
			db.getRelationsFor("conf-b").some((r) => r.type === "contradicts"),
		).toBe(false);
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
		expect(
			db.getRelationsFor("conf-q").some((r) => r.type === "contradicts"),
		).toBe(false);
	});

	it("merge: when the new (winning) entry was conflicted, it is restored to active after merge", () => {
		// conf-x and conf-y are in irresolvable conflict
		db.insertEntry(
			makeEntry({ id: "conf-x", status: "active", topics: ["topic"] }),
		);
		db.insertEntry(
			makeEntry({ id: "conf-y", status: "active", topics: ["topic"] }),
		);
		db.applyContradictionResolution("irresolvable", "conf-x", "conf-y");

		expect(db.getEntry("conf-x")?.status).toBe("conflicted");

		// conf-x (the "new" entry in this call) wins via merge over a third entry "old-z"
		db.insertEntry(
			makeEntry({ id: "old-z", status: "active", topics: ["topic"] }),
		);
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

// ── Helper: build a synthetic Episode ────────────────────────────────────────

function makeEpisode(
	overrides: Partial<{
		sessionId: string;
		startMessageId: string;
		endMessageId: string;
		sessionTitle: string;
		projectName: string;
		directory: string;
		timeCreated: number;
		maxMessageTime: number;
		content: string;
		contentType: "messages" | "compaction_summary";
		approxTokens: number;
	}> = {},
) {
	const now = Date.now();
	return {
		sessionId: "session-1",
		startMessageId: "msg-start",
		endMessageId: "msg-end",
		sessionTitle: "Test Session",
		projectName: "test-project",
		directory: "/tmp/test",
		timeCreated: now,
		maxMessageTime: now,
		content: "User learned that TypeScript is statically typed.",
		contentType: "messages" as const,
		approxTokens: 20,
		...overrides,
	};
}

// ── ConsolidationEngine.reconsolidate() paths ─────────────────────────────────

describe("ConsolidationEngine.reconsolidate() — novel entry (below threshold)", () => {
	it("inserts a new entry when the knowledge base is empty", async () => {
		// No existing entries → similarity is 0 → insert unconditionally
		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// Give embed a deterministic vector
		const vec = fakeEmbedding("TypeScript");
		spyOn(activation.embeddings, "embed").mockResolvedValue(vec);

		// extractKnowledge returns one novel entry
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				confidence: 0.9,
				scope: "personal",
				source: "test",
			},
		]);

		const result = await engine.consolidate();

		expect(result.entriesCreated).toBe(1);
		expect(result.entriesUpdated).toBe(0);

		const entries = db.getEntries({ type: "fact" });
		expect(entries.length).toBe(1);
		expect(entries[0].content).toBe("TypeScript is statically typed.");
		expect(entries[0].status).toBe("active");
	});

	it("inserts a new entry when nearest neighbour is below RECONSOLIDATION_THRESHOLD", async () => {
		// Pre-populate one entry with an orthogonal embedding (similarity = 0)
		const existingEmb = [1, 0, 0, 0, 0, 0, 0, 0];
		db.insertEntry(
			makeEntry({
				id: "existing",
				content: "Unrelated fact.",
				topics: ["sql"],
				embedding: existingEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// New entry embedding is orthogonal to existing — sim = 0
		const newVec = [0, 1, 0, 0, 0, 0, 0, 0];
		spyOn(activation.embeddings, "embed").mockResolvedValue(newVec);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "Bun is a fast JS runtime.",
				topics: ["bun"],
				confidence: 0.85,
				scope: "personal",
				source: "test",
			},
		]);

		// decideMerge should NOT be called — we're below the threshold
		const decideMergeSpy = spyOn(ConsolidationLLM.prototype, "decideMerge");

		const result = await engine.consolidate();

		expect(result.entriesCreated).toBe(1);
		expect(decideMergeSpy).not.toHaveBeenCalled();

		const allEntries = db.getEntries({});
		expect(allEntries.length).toBe(2);
	});
});

describe("ConsolidationEngine.reconsolidate() — 'keep' decision (above threshold)", () => {
	it("reinforces the existing entry and does not create a new one", async () => {
		// Existing entry with a known embedding
		const existingEmb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "existing-ts",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: existingEmb,
				observationCount: 1,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// Near-identical embedding → similarity above threshold
		spyOn(activation.embeddings, "embed").mockResolvedValue(existingEmb);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "TypeScript uses static types.",
				topics: ["typescript"],
				confidence: 0.88,
				scope: "personal",
				source: "test",
			},
		]);

		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({
			action: "keep",
		});

		const result = await engine.consolidate();

		expect(result.entriesCreated).toBe(0);
		expect(result.entriesUpdated).toBe(0);

		// observationCount should have been incremented by reinforceObservation
		const entry = db.getEntry("existing-ts");
		expect(entry?.observationCount).toBe(2);

		const allEntries = db.getEntries({});
		expect(allEntries.length).toBe(1);
	});
});

describe("ConsolidationEngine.reconsolidate() — synthesis trigger on 'keep'", () => {
	// Override synthesis threshold to 3 for all tests in this block so they run
	// quickly without needing obs=9 → 10 setups. Restored in afterEach.
	const SYNTH_THRESHOLD = 3;
	let originalThreshold: number;
	beforeEach(() => {
		originalThreshold = config.consolidation.synthesisObservationThreshold;
		config.consolidation.synthesisObservationThreshold = SYNTH_THRESHOLD;
	});
	afterEach(() => {
		config.consolidation.synthesisObservationThreshold = originalThreshold;
	});

	function makeEpisode() {
		const now = Date.now();
		return {
			sessionId: "s1",
			startMessageId: "m1",
			endMessageId: "m2",
			sessionTitle: "Test",
			projectName: "test",
			directory: "/tmp",
			timeCreated: now,
			maxMessageTime: now,
			content: "user: hello\nassistant: world",
			contentType: "messages" as const,
			approxTokens: 10,
		};
	}

	it("does not call synthesizePrinciple when observation_count is below threshold", async () => {
		// Entry starts at observationCount=1; after keep it becomes 2 — below threshold of 3
		const emb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 1,
				lastSynthesizedObservationCount: null,
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({ action: "keep" });
		const synthSpy = spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue(null);

		await engine.consolidate();

		// obs=2 < threshold=3 → synthesis should not fire
		expect(synthSpy).not.toHaveBeenCalled();
	});

	it("calls synthesizePrinciple when observation_count reaches the threshold", async () => {
		// Entry starts at observationCount=2; after keep it becomes 3 = threshold
		const emb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 2,
				lastSynthesizedObservationCount: null,
			}),
		);
		// A neighbor entry is required so attemptSynthesis has candidates to look at
		db.insertEntry(
			makeEntry({
				id: "neighbor-entry",
				content: "neighbor content about static analysis",
				topics: ["analysis"],
				embedding: fakeEmbedding("neighbor content"),
				observationCount: 1,
				lastSynthesizedObservationCount: null,
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({ action: "keep" });
		const synthSpy = spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue(null);

		await engine.consolidate();
		// Give the fire-and-forget synthesis promise a tick to complete
		await new Promise((r) => setTimeout(r, 50));

		// obs=2 → reinforced to 3 = threshold → synthesis fires
		expect(synthSpy).toHaveBeenCalledTimes(1);
	});

	it("does not re-synthesize when already synthesized at same observation level", async () => {
		// Entry starts at obs=3, lastSynth=3. After keep it becomes obs=4.
		// Next threshold = 3+3=6. obs(4) < 6 → synthesis must NOT fire.
		// This tests the guard: "already synthesized at threshold=3, next level is 6".
		const emb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 3, // already AT the threshold=3 level
				lastSynthesizedObservationCount: 3, // synthesis already fired here
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({ action: "keep" });
		const synthSpy = spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue(null);

		await engine.consolidate();
		await new Promise((r) => setTimeout(r, 10));

		// Next threshold would be 3+3=6, obs=4 < 6 → no re-synthesis
		expect(synthSpy).not.toHaveBeenCalled();
	});

	it("inserts a synthesized entry and supports relations when synthesis returns a result", async () => {
		const emb = fakeEmbedding("TypeScript static");
		const neighborEmb = fakeEmbedding("neighbor content");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 2,
				lastSynthesizedObservationCount: null,
			}),
		);
		// Insert a neighbor so attemptSynthesis has entries to look at
		db.insertEntry(
			makeEntry({
				id: "neighbor-1",
				content: "neighbor content about types",
				topics: ["types"],
				embedding: neighborEmb,
				observationCount: 1,
				lastSynthesizedObservationCount: null,
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({ action: "keep" });
		spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue({
			type: "principle",
			content: "Static typing produces more reliable code.",
			topics: ["typescript", "types"],
			confidence: 0.75,
			sourceIds: ["neighbor-1"],
		});

		await engine.consolidate();
		await new Promise((r) => setTimeout(r, 50));

		// A synthesized entry should now exist
		const allEntries = db.getEntries({});
		const synthesized = allEntries.find((e) => e.content === "Static typing produces more reliable code.");
		expect(synthesized).toBeDefined();
		expect(synthesized?.type).toBe("principle");
		// anchor should be marked as synthesized at obs=3
		const anchor = db.getEntry("ts-entry");
		expect(anchor?.lastSynthesizedObservationCount).toBe(3);
	});
});

describe("ConsolidationEngine.reconsolidate() — 'update' decision (above threshold)", () => {
	it("merges the new observation into the existing entry in place", async () => {
		const existingEmb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "existing-ts",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				confidence: 0.8,
				embedding: existingEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		spyOn(activation.embeddings, "embed").mockResolvedValue(existingEmb);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "TypeScript has structural typing and type inference.",
				topics: ["typescript"],
				confidence: 0.9,
				scope: "personal",
				source: "test",
			},
		]);

		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({
			action: "update",
			content: "TypeScript is statically typed and supports type inference.",
			type: "fact",
			topics: ["typescript"],
			confidence: 0.92,
		});

		const result = await engine.consolidate();

		expect(result.entriesCreated).toBe(0);
		expect(result.entriesUpdated).toBe(1);

		const entry = db.getEntry("existing-ts");
		expect(entry?.content).toBe(
			"TypeScript is statically typed and supports type inference.",
		);
		expect(entry?.confidence).toBeCloseTo(0.92);
		expect(entry?.status).toBe("active");
	});
});

describe("ConsolidationEngine.reconsolidate() — synthesis trigger on 'update'", () => {
	// Override synthesis threshold to 3 for all tests in this block.
	const SYNTH_THRESHOLD = 3;
	let originalThreshold: number;
	beforeEach(() => {
		originalThreshold = config.consolidation.synthesisObservationThreshold;
		config.consolidation.synthesisObservationThreshold = SYNTH_THRESHOLD;
	});
	afterEach(() => {
		config.consolidation.synthesisObservationThreshold = originalThreshold;
	});

	function makeEpisode() {
		const now = Date.now();
		return {
			sessionId: "s1",
			startMessageId: "m1",
			endMessageId: "m2",
			sessionTitle: "Test",
			projectName: "test",
			directory: "/tmp",
			timeCreated: now,
			maxMessageTime: now,
			content: "user: hello\nassistant: world",
			contentType: "messages" as const,
			approxTokens: 10,
		};
	}

	it("calls synthesizePrinciple when update pushes observation_count to threshold", async () => {
		// Entry at obs=2; mergeEntry increments to 3 = threshold → synthesis fires
		const emb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 2,
				lastSynthesizedObservationCount: null,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "neighbor-entry",
				content: "neighbor content about static analysis",
				topics: ["analysis"],
				embedding: fakeEmbedding("neighbor content"),
				observationCount: 1,
				lastSynthesizedObservationCount: null,
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({
			action: "update",
			content: "TypeScript is statically typed (confirmed).",
			type: "fact",
			topics: ["typescript"],
			confidence: 0.9,
		});
		const synthSpy = spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue(null);

		await engine.consolidate();
		await new Promise((r) => setTimeout(r, 50));

		// obs=2 → update increments to 3 = threshold → synthesis fires
		expect(synthSpy).toHaveBeenCalledTimes(1);
		// anchor should be stamped as synthesized at obs=3
		const anchor = db.getEntry("ts-entry");
		expect(anchor?.lastSynthesizedObservationCount).toBe(3);
	});

	it("does not call synthesizePrinciple when update does not reach threshold", async () => {
		// Entry at obs=1; after update it becomes 2 < threshold(3) → no synthesis
		const emb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "ts-entry",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: emb,
				observationCount: 1,
				lastSynthesizedObservationCount: null,
			}),
		);

		spyOn(OpenCodeEpisodeReader.prototype, "getCandidateSessions").mockReturnValue([
			{ id: "session-1", maxMessageTime: Date.now() },
		]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([makeEpisode()]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(emb);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{ type: "fact", content: "TypeScript uses static types.", topics: ["typescript"], confidence: 0.88, scope: "personal", source: "test" },
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({
			action: "update",
			content: "TypeScript is statically typed (confirmed).",
			type: "fact",
			topics: ["typescript"],
			confidence: 0.9,
		});
		const synthSpy = spyOn(ConsolidationLLM.prototype, "synthesizePrinciple").mockResolvedValue(null);

		await engine.consolidate();
		await new Promise((r) => setTimeout(r, 10));

		expect(synthSpy).not.toHaveBeenCalled();
	});
});

describe("ConsolidationEngine.reconsolidate() — 'insert' decision (above threshold but distinct)", () => {
	it("inserts a new entry even when similarity exceeds the threshold", async () => {
		const existingEmb = fakeEmbedding("TypeScript static");
		db.insertEntry(
			makeEntry({
				id: "existing-ts",
				content: "TypeScript is statically typed.",
				topics: ["typescript"],
				embedding: existingEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		spyOn(activation.embeddings, "embed").mockResolvedValue(existingEmb);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content:
					"TypeScript compiles to JavaScript, unlike statically typed native languages.",
				topics: ["typescript", "compilation"],
				confidence: 0.87,
				scope: "personal",
				source: "test",
			},
		]);

		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue({
			action: "insert",
		});

		const result = await engine.consolidate();

		expect(result.entriesCreated).toBe(1);
		expect(result.entriesUpdated).toBe(0);

		const allEntries = db.getEntries({});
		expect(allEntries.length).toBe(2);
	});
});

// ── ConsolidationEngine.runContradictionScan() edge cases ─────────────────────

describe("ConsolidationEngine.runContradictionScan() — hallucinated candidateId guard", () => {
	it("ignores a candidateId the LLM invented that was not in the candidate list", async () => {
		const existingEmb = fakeEmbedding("server port");
		db.insertEntry(
			makeEntry({
				id: "candidate-real",
				content: "The server runs on port 8080.",
				topics: ["server", "config"],
				embedding: existingEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// The new entry's embedding must be in the mid-band [contradictionMinSimilarity, RECONSOLIDATION_THRESHOLD)
		// against existingEmb so the contradiction scan fires, but below RECONSOLIDATION_THRESHOLD so
		// reconsolidate inserts it (novel path, no decideMerge).
		//
		// existingEmb = fakeEmbedding("ser") ≈ [0.603, 0.529, 0.598, 0, ...]
		// novelEmb = [0, 0, 1, 0, ...] → cos = 0.598 ∈ [0.4, 0.82) ✓ (mid-band, below threshold)
		const novelEmb = [0, 0, 1, 0, 0, 0, 0, 0];

		const embedSpy = spyOn(activation.embeddings, "embed").mockResolvedValue(
			novelEmb,
		);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "The server port was changed to 9090.",
				topics: ["server", "config"],
				confidence: 0.9,
				scope: "personal",
				source: "test",
			},
		]);

		// LLM returns a hallucinated candidateId ("HALLUCINATED") not in the candidate list
		spyOn(
			ConsolidationLLM.prototype,
			"detectAndResolveContradiction",
		).mockResolvedValue([
			{
				candidateId: "HALLUCINATED-ID",
				resolution: "supersede_old",
				reason: "new port supersedes old",
			},
		]);

		const result = await engine.consolidate();

		// The hallucinated ID should be ignored — both entries remain active
		expect(result.conflictsResolved).toBe(0);
		const candidate = db.getEntry("candidate-real");
		expect(candidate?.status).toBe("active"); // not superseded
	});
});

describe("ConsolidationEngine.runContradictionScan() — supersede_new stops further candidates", () => {
	it("stops checking candidates after the new entry is itself superseded", async () => {
		// Craft embeddings so the new entry lands in the mid-band similarity window
		// against both candidates:
		//   candidateEmb = [1, 0, 0, 0, 0, 0, 0, 0]  (unit vector along dim-0)
		//   newEmb        = [0.6, 0.8, 0, 0, 0, 0, 0, 0]  unit; cos(new, candidate) = 0.6
		// 0.6 is in [contradictionMinSimilarity=0.4, RECONSOLIDATION_THRESHOLD=0.82) → mid-band.
		const candidateEmb = [1, 0, 0, 0, 0, 0, 0, 0];
		const newEmb = [0.6, 0.8, 0, 0, 0, 0, 0, 0]; // already unit (0.36+0.64=1)

		db.insertEntry(
			makeEntry({
				id: "candidate-a",
				content: "Port is 8080.",
				topics: ["server"],
				embedding: candidateEmb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "candidate-b",
				content: "Port is 3000.",
				topics: ["server"],
				embedding: candidateEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// embed returns newEmb → cos(newEmb, candidateEmb) = 0.6 → below RECONSOLIDATION_THRESHOLD
		// → reconsolidate inserts; scan then finds candidates in mid-band
		spyOn(activation.embeddings, "embed").mockResolvedValue(newEmb);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "Port was changed to 9090.",
				topics: ["server"],
				confidence: 0.9,
				scope: "personal",
				source: "test",
			},
		]);

		// LLM returns supersede_new for candidate-a → new entry loses; candidate-b result
		// must never be applied because the scan breaks on supersede_new.
		spyOn(
			ConsolidationLLM.prototype,
			"detectAndResolveContradiction",
		).mockResolvedValue([
			{
				candidateId: "candidate-a",
				resolution: "supersede_new",
				reason: "candidate-a is authoritative",
			},
			{
				candidateId: "candidate-b",
				resolution: "supersede_old",
				reason: "should not be reached",
			},
		]);

		const result = await engine.consolidate();

		// The new entry was superseded (supersede_new counts as resolved)
		expect(result.conflictsResolved).toBe(1);
		const newEntries = db.getEntries({ type: "fact" });
		const insertedNew = newEntries.find((e) => e.content.includes("9090"));
		expect(insertedNew?.status).toBe("superseded");

		// candidate-b must not have been superseded — the scan stopped after supersede_new
		expect(db.getEntry("candidate-b")?.status).toBe("active");
	});
});

describe("ConsolidationEngine.runContradictionScan() — superseded_in_scan tracking", () => {
	it("skips a candidate that was already superseded by an earlier new entry in the same scan", async () => {
		// candidate-x is a pre-existing entry in the mid-band range for the new entries.
		// new-entry-1 supersedes it in the first scan pass; new-entry-2 should not re-process it.
		//
		// Embedding layout:
		//   candidateEmb = [1, 0, 0, 0, 0, 0, 0, 0]  (unit on dim-0)
		//   newEmb1      = [0.6, 0.8, 0, 0, 0, 0, 0, 0]  cos=0.6 with candidate → mid-band
		//   newEmb2      = [0.6, 0, 0.8, 0, 0, 0, 0, 0]  cos=0.6 with candidate → mid-band
		//   cos(newEmb1, newEmb2) = 0.36 < threshold → both insert as novel independently
		const candidateEmb = [1, 0, 0, 0, 0, 0, 0, 0];
		const newEmb1 = [0.6, 0.8, 0, 0, 0, 0, 0, 0]; // unit: 0.36+0.64=1
		const newEmb2 = [0.6, 0, 0.8, 0, 0, 0, 0, 0]; // unit: 0.36+0.64=1
		// cos(newEmb1, newEmb2) = 0.6*0.6 + 0.8*0 + 0*0.8 = 0.36 < RECONSOLIDATION_THRESHOLD (0.82)

		db.insertEntry(
			makeEntry({
				id: "candidate-x",
				content: "Port is 8080.",
				topics: ["server"],
				embedding: candidateEmb,
			}),
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: Date.now() }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// embed is called once per extracted entry (during reconsolidate).
		// Alternate between the two mid-band embeddings.
		let embedCallCount = 0;
		spyOn(activation.embeddings, "embed").mockImplementation(async () => {
			embedCallCount++;
			return embedCallCount % 2 === 1 ? newEmb1 : newEmb2;
		});

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "Port changed to 9090.",
				topics: ["server"],
				confidence: 0.9,
				scope: "personal",
				source: "test",
			},
			{
				type: "fact",
				content: "Port also changed to 9091.",
				topics: ["server"],
				confidence: 0.85,
				scope: "personal",
				source: "test",
			},
		]);

		// First scan (new-entry-1): supersede candidate-x.
		// Second scan (new-entry-2): detectAndResolveContradiction must NOT receive candidate-x
		// because supersededInThisScan should filter it out.
		let scanCallCount = 0;
		let candidateXAppearedInScan2 = false;
		spyOn(
			ConsolidationLLM.prototype,
			"detectAndResolveContradiction",
		).mockImplementation(async (_newEntry, candidates) => {
			scanCallCount++;
			if (scanCallCount === 1) {
				// First scan: new-entry-1 supersedes candidate-x
				const cx = candidates.find((c) => c.id === "candidate-x");
				if (!cx) return [];
				return [
					{
						candidateId: "candidate-x",
						resolution: "supersede_old" as const,
						reason: "newer info",
					},
				];
			}
			// Second scan: record whether candidate-x incorrectly reappears
			if (candidates.some((c) => c.id === "candidate-x")) {
				candidateXAppearedInScan2 = true;
			}
			return [];
		});

		await engine.consolidate();

		// candidate-x must be superseded exactly once (by new-entry-1)
		expect(db.getEntry("candidate-x")?.status).toBe("superseded");
		// candidate-x must not have been passed to the second scan iteration
		expect(candidateXAppearedInScan2).toBe(false);
		// at most one scan call per new entry
		expect(scanCallCount).toBeLessThanOrEqual(2);
	});
});

// ── ConsolidationEngine.consolidate() cursor advancement ─────────────────────

describe("ConsolidationEngine.consolidate() — cursor advancement", () => {
	it("advances cursor past all sessions when batch is not full (no boundary risk)", async () => {
		const t1 = Date.now() - 2000;
		const t2 = Date.now() - 1000;

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([
			{ id: "session-1", maxMessageTime: t1 },
			{ id: "session-2", maxMessageTime: t2 },
		]);
		// No episodes → sessionsProcessed reflects candidates, not episodes
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue(
			[],
		);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		const initialCursor = db.getSourceCursor("opencode");

		await engine.consolidate();

		const cursor = db.getSourceCursor("opencode");
		// Batch was NOT full (returned 2, limit is much higher) → cursor must advance to t2
		expect(cursor.lastMessageTimeCreated).toBe(t2);
		expect(cursor.lastMessageTimeCreated).toBeGreaterThan(
			initialCursor.lastMessageTimeCreated,
		);
	});

	it("caps cursor below last session's maxMessageTime when batch is full", async () => {
		// Simulate a full batch: return exactly maxSessionsPerRun sessions.
		// To trigger the cap guard we also need episodes — otherwise maxEpisodeMessageTime=0
		// and Math.min(0, cap) = 0. Include one episode in the last session so that
		// maxEpisodeMessageTime = lastSession.maxMessageTime, then the cap fires.
		const limit = config.consolidation.maxSessionsPerRun;

		const baseTime = Date.now();
		const sessions = Array.from({ length: limit }, (_, i) => ({
			id: `session-${i}`,
			maxMessageTime: baseTime + i * 1000,
		}));

		const lastSession = sessions[sessions.length - 1];

		// One episode whose maxMessageTime equals the last session's timestamp
		const ep = makeEpisode({
			sessionId: lastSession.id,
			maxMessageTime: lastSession.maxMessageTime,
		});

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue(sessions);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			ep,
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(
			fakeEmbedding("test"),
		);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([]);

		await engine.consolidate();

		const cursor = db.getSourceCursor("opencode");
		// Batch was full → cursor must be capped to lastSession.maxMessageTime - 1
		expect(cursor.lastMessageTimeCreated).toBe(lastSession.maxMessageTime - 1);
	});

	it("never moves cursor backwards", async () => {
		// Set the opencode source cursor to a future timestamp
		const futureCursor = Date.now() + 100_000;
		db.updateSourceCursor("opencode", { lastMessageTimeCreated: futureCursor });

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		await engine.consolidate();

		const cursor = db.getSourceCursor("opencode");
		// Cursor must not go backwards
		expect(cursor.lastMessageTimeCreated).toBeGreaterThanOrEqual(futureCursor);
	});
});

// ── Full pipeline: extract → reconsolidate → contradiction scan ───────────────

describe("ConsolidationEngine.consolidate() — full pipeline with mocked LLM + embedding", () => {
	it("creates, updates, and resolves conflicts end-to-end", async () => {
		// Start with one pre-existing entry.
		// Its embedding is [1,0,0,...] (unit on dim-0).
		// The new entry will use embedding [0.6,0.8,0,...] → cos = 0.6 with existing-port.
		// 0.6 is in [contradictionMinSimilarity=0.4, RECONSOLIDATION_THRESHOLD=0.82) → mid-band.
		// cos = 0.6 < 0.82 → reconsolidate inserts (below RECONSOLIDATION_THRESHOLD).
		const existingEmb = [1, 0, 0, 0, 0, 0, 0, 0];
		db.insertEntry(
			makeEntry({
				id: "existing-port",
				content: "The server runs on port 8080.",
				topics: ["server", "config"],
				embedding: existingEmb,
			}),
		);

		const now = Date.now();
		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: now }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			makeEpisode(),
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);

		// mid-band embedding: cos(newEmb, existingEmb) = 0.6 → below threshold → insert
		// and also lands in the contradiction scan band [0.4, 0.82)
		const midBandEmb = [0.6, 0.8, 0, 0, 0, 0, 0, 0]; // unit: 0.36+0.64=1
		spyOn(activation.embeddings, "embed").mockResolvedValue(midBandEmb);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact",
				content: "The server port was changed to 9090.",
				topics: ["server", "config"],
				confidence: 0.9,
				scope: "team",
				source: "test",
			},
		]);

		// Contradiction scan: new entry vs existing-port — irresolvable
		spyOn(
			ConsolidationLLM.prototype,
			"detectAndResolveContradiction",
		).mockResolvedValue([
			{
				candidateId: "existing-port",
				resolution: "irresolvable",
				reason: "both ports are actively used for different services",
			},
		]);

		const result = await engine.consolidate();

		expect(result.sessionsProcessed).toBe(1);
		expect(result.entriesCreated).toBe(1);
		expect(result.conflictsDetected).toBe(1);
		expect(result.conflictsResolved).toBe(0); // irresolvable doesn't count as resolved

		// The new entry must be in the DB as conflicted
		const newEntry = db
			.getEntries({ type: "fact" })
			.find((e) => e.content.includes("9090"));
		expect(newEntry).toBeDefined();
		expect(newEntry?.status).toBe("conflicted"); // irresolvable marks both as conflicted

		// existing-port also marked conflicted
		expect(db.getEntry("existing-port")?.status).toBe("conflicted");
	});

	it("records episodes so they are not reprocessed on the next run", async () => {
		const now = Date.now();
		const ep = makeEpisode({
			startMessageId: "stable-start",
			endMessageId: "stable-end",
			maxMessageTime: now,
		});

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-1", maxMessageTime: now }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			ep,
		]);
		spyOn(activation, "ensureEmbeddings").mockResolvedValue(0);
		spyOn(activation.embeddings, "embed").mockResolvedValue(
			fakeEmbedding("anything"),
		);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([]);

		await engine.consolidate();

		// The episode range must now be recorded in the DB under the "opencode" source
		const ranges = db.getProcessedEpisodeRanges("opencode", ["session-1"]);
		const sessionRanges = ranges.get("session-1") ?? [];
		const recorded = sessionRanges.find(
			(r) =>
				r.startMessageId === "stable-start" && r.endMessageId === "stable-end",
		);
		expect(recorded).toBeDefined();
	});
});

describe("ActivationEngine — contradiction annotation on activated conflicted entries", () => {
	it("annotates conflicted entry when both sides of the conflict activate", async () => {
		// Both entries share the same embedding prefix — they will both score above
		// the similarity threshold for any query with that prefix.
		const emb = fakeEmbedding("abc");
		db.insertEntry(
			makeEntry({
				id: "side-a",
				content: "abc approach A",
				status: "active",
				embedding: emb,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "side-b",
				content: "abc approach B",
				status: "active",
				embedding: emb,
			}),
		);
		db.applyContradictionResolution("irresolvable", "side-a", "side-b");

		// Both are now conflicted — mock embedBatch so activate() uses deterministic vectors.
		// activate() always calls embedBatch (even for a single string query).
		const embedSpy = spyOn(
			activation.embeddings,
			"embedBatch",
		).mockResolvedValue([emb]);

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
		db.insertEntry(
			makeEntry({
				id: "side-c",
				content: "abc thing",
				status: "active",
				embedding: embC,
			}),
		);
		db.insertEntry(
			makeEntry({
				id: "side-d",
				content: "xyz thing",
				status: "active",
				embedding: embD,
			}),
		);
		db.applyContradictionResolution("irresolvable", "side-c", "side-d");

		// Query embedding == side-c's embedding (similarity 1.0 with side-c, 0.0 with side-d)
		const embedSpy = spyOn(
			activation.embeddings,
			"embedBatch",
		).mockResolvedValue([embC]);

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
		db.insertEntry(
			makeEntry({
				id: "plain",
				content: "abc plain entry",
				status: "active",
				embedding: emb,
			}),
		);

		const embedSpy = spyOn(
			activation.embeddings,
			"embedBatch",
		).mockResolvedValue([emb]);

		const result = await activation.activate("abc query");

		const plain = result.entries.find((e) => e.entry.id === "plain");
		expect(plain).toBeDefined();
		expect(plain?.contradiction).toBeUndefined();

		embedSpy.mockRestore();
	});
});
