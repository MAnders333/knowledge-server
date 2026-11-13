/**
 * Consolidation outcomes for pending_episodes rows.
 *
 * A pending row is deleted only when its episode was handled (recorded as
 * consolidated, or already processed). Rows whose store was locked or unavailable
 * are deferred; rows whose extraction or write failed keep their row and count an
 * attempt, and are dead-lettered after PENDING_EPISODE_MAX_ATTEMPTS. The drain
 * loops stop when a batch settles nothing.
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
import { mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import { drainConsolidation } from "../src/commands/consolidation-drain";
import { config } from "../src/config";
import type { KnowledgeServerConfig } from "../src/config-file";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { DomainRouter } from "../src/consolidation/domain-router";
import { ConsolidationLLM } from "../src/consolidation/llm";
import type { ExtractedKnowledge } from "../src/consolidation/llm";
import { MalformedLlmOutputError } from "../src/consolidation/llm-output";
import { PendingEpisodesReader } from "../src/consolidation/readers/pending";
import { PENDING_EPISODE_MAX_ATTEMPTS } from "../src/db/interface";
import type { IKnowledgeStore } from "../src/db/interface";
import { KnowledgeDB } from "../src/db/sqlite/index";
import { ServerStateDB } from "../src/db/state/index";
import { LLM_DEGRADED_FAILING_RUNS, componentHealth } from "../src/health";
import { logger } from "../src/logger";
import type { PendingEpisode as PendingRow } from "../src/types";
import { makeEntry } from "./fixtures";

// Each distinct text gets its own unit vector, so no two entries are similar and
// the reconsolidation LLM is never called.
function uniqueEmbedding(seen: Map<string, number[]>, text: string): number[] {
	let vec = seen.get(text);
	if (!vec) {
		vec = new Array(384).fill(0);
		vec[seen.size % 384] = 1;
		seen.set(text, vec);
	}
	return vec;
}

function entry(content: string, domain?: string): ExtractedKnowledge {
	return {
		type: "fact",
		content,
		topics: ["test"],
		confidence: 0.9,
		source: "test",
		...(domain ? { domain } : {}),
	};
}

/** A distinct entry per episode ("fact N" in the content), so no two are similar. */
function factFromSummary(summary: string): ExtractedKnowledge {
	const n = summary.match(/fact (\d+)/)?.[1] ?? "0";
	return entry(`The fact number ${n} is recorded.`);
}

function pendingRow(overrides: Partial<PendingRow>): PendingRow {
	const now = Date.now();
	return {
		id: "row-1",
		userId: "alice",
		source: "opencode",
		sessionId: "session-1",
		startMessageId: "msg-0",
		endMessageId: "msg-9",
		sessionTitle: "Session",
		projectName: "project",
		directory: "/tmp/outcome-test",
		content: "User: the sky is blue.",
		contentType: "messages",
		timeCreated: now - 2000,
		maxMessageTime: now - 1000,
		approxTokens: 10,
		uploadedAt: now,
		...overrides,
	};
}

let tempDir: string;
let db: KnowledgeDB;
let state: ServerStateDB;
let engine: ConsolidationEngine;
let seenEmbeddings: Map<string, number[]>;
let savedChunkSize: number;

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), "ks-outcome-test-"));
	db = new KnowledgeDB(join(tempDir, "knowledge.db"));
	state = new ServerStateDB(join(tempDir, "state.db"));
	const activation = new ActivationEngine(db);
	seenEmbeddings = new Map();
	spyOn(activation.embeddings, "embed").mockImplementation(async (text) =>
		uniqueEmbedding(seenEmbeddings, text),
	);
	engine = new ConsolidationEngine(
		db,
		state,
		activation,
		[new PendingEpisodesReader(state)],
		null,
	);
	// One episode per chunk, so a failing episode cannot take its neighbours down.
	savedChunkSize = config.consolidation.chunkSize;
	config.consolidation.chunkSize = 1;
});

afterEach(async () => {
	config.consolidation.chunkSize = savedChunkSize;
	mock.restore();
	await db.close();
	await state.close();
	rmSync(tempDir, { recursive: true, force: true });
});

describe("store locked by another process", () => {
	it("keeps pending rows for retry and the drain loop terminates", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-locked", sessionId: "session-locked" }),
		);
		spyOn(db, "tryAcquireConsolidationLock").mockResolvedValue(false);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockResolvedValue([entry("The sky is blue.")]);

		const drained = await drainConsolidation(engine);

		// The row survives, nothing is recorded, and nothing is reported as processed.
		const rows = await state.getPendingEpisodes(0);
		expect(rows.map((r) => r.id)).toEqual(["row-locked"]);
		expect(rows[0].failedAttempts).toBe(0);
		expect(await state.getProcessedEpisodeRanges(["session-locked"])).toEqual(
			new Map(),
		);
		expect(drained.totalSessions).toBe(0);
		// A batch that settles nothing stops the drain: one extraction, no retry loop.
		// The lock is checked before extraction, so a held lock costs no LLM call.
		expect(extract).toHaveBeenCalledTimes(0);
	});

	it("processes the deferred rows once the lock is free", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-later", sessionId: "session-later" }),
		);
		const lock = spyOn(db, "tryAcquireConsolidationLock").mockResolvedValue(
			false,
		);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			entry("The sky is blue."),
		]);
		await engine.consolidate();
		expect(await state.countPendingSessions()).toBe(1);

		lock.mockResolvedValue(true);
		const result = await engine.consolidate();

		expect(result.sessionsProcessed).toBe(1);
		expect(await state.getPendingEpisodes(0)).toEqual([]);
	});
});

describe("store lock contention across two engines", () => {
	it("a deferred episode is retried later in the same drain, not left for the next poll", async () => {
		const now = Date.now();
		for (const i of [1, 2, 3]) {
			await state.insertPendingEpisode(
				pendingRow({
					id: `row-contended-${i}`,
					sessionId: `session-contended-${i}`,
					content: `User: fact ${i}.`,
					maxMessageTime: now - 4000 + i,
				}),
			);
		}
		// Another process holds the store's consolidation lock for the first chunk only.
		const holder = new KnowledgeDB(join(tempDir, "knowledge.db"));
		expect(await holder.tryAcquireConsolidationLock()).toBe(true);
		const realTryAcquire = db.tryAcquireConsolidationLock.bind(db);
		let calls = 0;
		spyOn(db, "tryAcquireConsolidationLock").mockImplementation(async () => {
			calls++;
			if (calls === 2) await holder.releaseConsolidationLock();
			return realTryAcquire();
		});
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockImplementation(
			async (summary: string) => [factFromSummary(summary)],
		);
		try {
			const drained = await drainConsolidation(engine);

			expect(drained.totalSessions).toBe(3);
			expect(await state.getPendingEpisodes(0)).toEqual([]);
		} finally {
			await holder.close();
		}
	});
});

describe("chunk with one bad episode", () => {
	it("only the failing episode is counted; healthy episodes in the same chunk complete", async () => {
		const savedChunkSize = config.consolidation.chunkSize;
		config.consolidation.chunkSize = 10;
		try {
			const now = Date.now();
			for (const i of [1, 2, 3, 4, 5]) {
				await state.insertPendingEpisode(
					pendingRow({
						id: `row-mixed-${i}`,
						sessionId: `session-mixed-${i}`,
						content: i === 3 ? "POISON: oversized" : `User: fact ${i}.`,
						maxMessageTime: now - 6000 + i,
					}),
				);
			}
			const extract = spyOn(
				ConsolidationLLM.prototype,
				"extractKnowledge",
			).mockImplementation(async (summary: string) => {
				if (summary.includes("POISON")) {
					throw Object.assign(new Error("prompt is too long"), {
						statusCode: 400,
					});
				}
				return [factFromSummary(summary)];
			});

			await engine.consolidate();

			const rows = await state.getPendingEpisodes(0);
			expect(rows.map((r) => r.id)).toEqual(["row-mixed-3"]);
			expect(rows[0].failedAttempts).toBe(1);
			expect(await state.countDeadLetteredEpisodes()).toBe(0);
			// One chunk attempt plus one single-episode attempt per episode.
			expect(extract).toHaveBeenCalledTimes(1 + 5);
		} finally {
			config.consolidation.chunkSize = savedChunkSize;
		}
	});
});

describe("LLM outage tracked as health", () => {
	beforeEach(() => componentHealth.clear());
	afterEach(() => componentHealth.clear());

	it("counts runs without progress on transient provider errors, and resets on success", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-outage", sessionId: "session-outage" }),
		);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockRejectedValue(
			Object.assign(new Error("Too Many Requests: key sk-test-secret"), {
				statusCode: 429,
			}),
		);

		for (let i = 0; i < LLM_DEGRADED_FAILING_RUNS; i++) {
			await engine.consolidate();
		}
		const failing = componentHealth.snapshot("llm");
		expect(failing.consecutiveFailures).toBe(LLM_DEGRADED_FAILING_RUNS);
		expect(failing.failing).toBe(true);
		expect(failing.failingSince).not.toBeNull();
		// The status shows the failure class only, never the provider message.
		expect(failing.lastError).toBe("rate limited (HTTP 429)");
		expect(extract).toHaveBeenCalledTimes(LLM_DEGRADED_FAILING_RUNS);

		extract.mockResolvedValue([entry("The sky is blue.")]);
		await engine.consolidate();

		const recovered = componentHealth.snapshot("llm");
		expect(recovered.consecutiveFailures).toBe(0);
		expect(recovered.failingSince).toBeNull();
		expect(await state.getPendingEpisodes(0)).toEqual([]);
	});
});

describe("merge target deleted during a run", () => {
	it("logs and skips the merge instead of dropping it silently", async () => {
		const id = "merge-target";
		await db.insertEntry(makeEntry({ id, content: "Old fact." }));
		expect(await db.deleteEntry(id)).toBe(true);
		const warn = spyOn(logger, "warn");

		await db.mergeEntry(id, {
			content: "New fact.",
			type: "fact",
			topics: ["test"],
			confidence: 0.9,
			additionalSources: ["session-x"],
		});

		expect(await db.getEntry(id)).toBeNull();
		expect(
			warn.mock.calls.some((c) =>
				String(c[0]).includes("was deleted during this run"),
			),
		).toBe(true);
	});
});

describe("blocked rows at the head of the queue", () => {
	it("finds a healthy episode behind more than 2000 blocked rows", async () => {
		const now = Date.now();
		for (let i = 0; i < 2100; i++) {
			await state.insertPendingEpisode(
				pendingRow({
					id: `blocked-${i}`,
					sessionId: `session-blocked-${i}`,
					directory: "/blocked/store",
					maxMessageTime: now - 100_000 + i,
				}),
			);
		}
		await state.insertPendingEpisode(
			pendingRow({
				id: "healthy",
				sessionId: "session-healthy",
				directory: "/healthy/store",
				maxMessageTime: now,
			}),
		);
		const reader = new PendingEpisodesReader(state);

		await reader.prepare(0, (ep) => ep.directory.startsWith("/blocked"));

		expect(reader.getCandidateSessions(0, 10).map((c) => c.id)).toEqual([
			"session-healthy",
		]);
		// 2,100 sequential inserts take ~8 s on a loaded CI runner: well over bun's
		// 5 s default.
	}, 60_000);
});

describe("store write failure", () => {
	it("keeps the row for a later run and succeeds on retry", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-write", sessionId: "session-write" }),
		);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			entry("The sky is blue."),
		]);
		const failing = spyOn(
			db,
			"getActiveEntriesWithEmbeddings",
		).mockRejectedValueOnce(new Error("disk I/O error"));

		await engine.consolidate();

		const rows = await state.getPendingEpisodes(0);
		expect(rows.map((r) => r.id)).toEqual(["row-write"]);
		expect(rows[0].failedAttempts).toBe(0);
		expect(failing).toHaveBeenCalledTimes(1);

		await engine.consolidate();

		expect(await state.getPendingEpisodes(0)).toEqual([]);
		expect(await state.countPendingSessions()).toBe(0);
	});
});

describe("chunk whose extraction always fails", () => {
	it("does not stall newer sessions and is dead-lettered after the attempt limit", async () => {
		await state.insertPendingEpisode(
			pendingRow({
				id: "row-poison",
				sessionId: "session-poison",
				content: "POISON: exceeds the model context window",
				maxMessageTime: Date.now() - 3000,
			}),
		);
		await state.insertPendingEpisode(
			pendingRow({
				id: "row-good",
				sessionId: "session-good",
				maxMessageTime: Date.now() - 1000,
			}),
		);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockImplementation(async (summary: string) => {
			if (summary.includes("POISON")) {
				throw Object.assign(new Error("context length exceeded"), {
					statusCode: 400,
				});
			}
			return [entry("The sky is blue.")];
		});

		// First run: the newer session is still consolidated and recorded.
		const first = await engine.consolidate();
		expect(first.sessionsProcessed).toBe(1);
		expect(
			(await state.getProcessedEpisodeRanges(["session-good"])).get(
				"session-good",
			),
		).toHaveLength(1);
		const [poison] = (await state.getPendingEpisodes(0)).filter(
			(r) => r.id === "row-poison",
		);
		expect(poison.failedAttempts).toBe(1);

		// Repeated runs until the poison row reaches the attempt limit.
		for (let i = 1; i < PENDING_EPISODE_MAX_ATTEMPTS; i++) {
			await engine.consolidate();
		}

		// Dead-lettered: excluded from reads and counts, so it cannot block anything.
		expect(await state.getPendingEpisodes(0)).toEqual([]);
		expect(await state.countPendingSessions()).toBe(0);

		const callsBefore = extract.mock.calls.length;
		const after = await engine.consolidate();
		expect(after.sessionsProcessed).toBe(0);
		expect(extract.mock.calls.length).toBe(callsBefore);
	});
});

describe("entries routed to another store", () => {
	it("completes the episode instead of re-extracting it on every run", async () => {
		const workDb = new KnowledgeDB(join(tempDir, "work.db"));
		try {
			const homeDir = homedir();
			const routedConfig = {
				userId: "alice",
				stores: [
					{ id: "personal", kind: "sqlite", writable: true },
					{ id: "work", kind: "sqlite", writable: true },
				],
				domains: [
					{ id: "personal", description: "Personal", store: "personal" },
					{ id: "work", description: "Work", store: "work" },
				],
				projects: [{ path: `${homeDir}/personal`, default_domain: "personal" }],
			} as unknown as KnowledgeServerConfig;
			const stores = new Map<string, IKnowledgeStore>([
				["personal", db],
				["work", workDb],
			]);
			const router = new DomainRouter(routedConfig, stores, db);
			const activation = new ActivationEngine(db);
			spyOn(activation.embeddings, "embed").mockImplementation(async (text) =>
				uniqueEmbedding(seenEmbeddings, text),
			);
			const routedEngine = new ConsolidationEngine(
				db,
				state,
				activation,
				[new PendingEpisodesReader(state)],
				router,
			);
			await state.insertPendingEpisode(
				pendingRow({
					id: "row-routed",
					sessionId: "session-routed",
					directory: `${homeDir}/personal/notes`,
				}),
			);
			// The episode belongs to the personal domain, but the LLM assigns the
			// entry to the work domain. The personal store receives nothing.
			const extract = spyOn(
				ConsolidationLLM.prototype,
				"extractKnowledge",
			).mockResolvedValue([entry("Ship the release on Fridays.", "work")]);

			await routedEngine.consolidate();
			await routedEngine.consolidate();

			expect(extract).toHaveBeenCalledTimes(1);
			expect(await state.getPendingEpisodes(0)).toEqual([]);
			expect((await workDb.getStats()).total).toBe(1);
		} finally {
			await workDb.close();
		}
	});
});

describe("contradiction reply that stays malformed", () => {
	it("keeps the stored entry and completes the episode without re-extraction", async () => {
		// Two unit vectors with cosine 0.6: inside the contradiction band
		// [contradictionMinSimilarity, reconsolidationThreshold), so the second
		// episode's entry is a candidate-scanned insert rather than a merge.
		const vec = (x: number, y: number) => {
			const v = new Array(384).fill(0);
			v[0] = x;
			v[1] = y;
			return v;
		};
		const activation = new ActivationEngine(db);
		spyOn(activation.embeddings, "embed").mockImplementation(async (text) => {
			if (text.includes("port 8080")) return vec(1, 0);
			if (text.includes("9090")) return vec(0.6, 0.8);
			return uniqueEmbedding(seenEmbeddings, text);
		});
		const malformedEngine = new ConsolidationEngine(
			db,
			state,
			activation,
			[new PendingEpisodesReader(state)],
			null,
		);
		const now = Date.now();
		await state.insertPendingEpisode(
			pendingRow({
				id: "row-a",
				sessionId: "session-a",
				content: "EPISODE A",
				maxMessageTime: now - 3000,
			}),
		);
		await state.insertPendingEpisode(
			pendingRow({
				id: "row-b",
				sessionId: "session-b",
				content: "EPISODE B",
				maxMessageTime: now - 1000,
			}),
		);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockImplementation(async (summary: string) =>
			summary.includes("EPISODE A")
				? [entry("The server runs on port 8080.")]
				: [entry("The server port was changed to 9090.")],
		);
		const contradiction = spyOn(
			ConsolidationLLM.prototype,
			"detectAndResolveContradiction",
		).mockRejectedValue(
			new MalformedLlmOutputError("contradiction", "checked 1 of 1 candidates"),
		);

		await malformedEngine.consolidate();

		// The scan retried once and then skipped the batch: no throw, so the chunk
		// completed and both episodes are recorded.
		expect(contradiction).toHaveBeenCalledTimes(2);
		expect(await state.getPendingEpisodes(0)).toEqual([]);
		expect(
			(await state.getProcessedEpisodeRanges(["session-b"])).get("session-b"),
		).toHaveLength(1);
		expect(await state.countDeadLetteredEpisodes()).toBe(0);

		// The entry was written once, and the next run does not re-extract it.
		const written = (await db.getEntries({ type: "fact" })).filter((e) =>
			e.content.includes("9090"),
		);
		expect(written).toHaveLength(1);
		expect((await db.getStats()).total).toBe(2);

		await malformedEngine.consolidate();
		expect(extract).toHaveBeenCalledTimes(2);
	});
});

describe("state.db upgrade from schema v1", () => {
	it("creates the attempts table for an existing v1 state.db", async () => {
		const path = join(tempDir, "v1-state.db");
		const { Database } = await import("bun:sqlite");
		const legacy = new Database(path);
		// The v1 shape: no failed_attempts / last_error columns.
		legacy.exec(`
			CREATE TABLE schema_version (version INTEGER NOT NULL, applied_at INTEGER NOT NULL);
			INSERT INTO schema_version (version, applied_at) VALUES (1, 0);
			CREATE TABLE pending_episodes (
				id TEXT PRIMARY KEY, user_id TEXT NOT NULL DEFAULT 'default', source TEXT NOT NULL,
				session_id TEXT NOT NULL, start_message_id TEXT NOT NULL, end_message_id TEXT NOT NULL,
				session_title TEXT NOT NULL DEFAULT '', project_name TEXT NOT NULL DEFAULT '',
				directory TEXT NOT NULL DEFAULT '', content TEXT NOT NULL,
				content_type TEXT NOT NULL, session_timestamp INTEGER NOT NULL DEFAULT 0,
				max_message_time INTEGER NOT NULL DEFAULT 0, approx_tokens INTEGER NOT NULL DEFAULT 0,
				uploaded_at INTEGER NOT NULL
			);
			INSERT INTO pending_episodes (id, source, session_id, start_message_id, end_message_id,
				content, content_type, max_message_time, uploaded_at)
			VALUES ('row-legacy', 'opencode', 's', 'a', 'b', 'x', 'messages', 1, 1);
		`);
		legacy.close();

		const upgraded = new ServerStateDB(path);
		try {
			const [row] = await upgraded.getPendingEpisodes(0);
			expect(row.id).toBe("row-legacy");
			expect(row.failedAttempts).toBe(0);

			await upgraded.recordPendingEpisodeFailures(["row-legacy"], "boom");
			const [after] = await upgraded.getPendingEpisodes(0);
			expect(after.failedAttempts).toBe(1);
		} finally {
			await upgraded.close();
		}
	});
});

describe("failure classification and dead-letter retry", () => {
	it("transient provider errors never count toward dead-lettering", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-429", sessionId: "session-429" }),
		);
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockRejectedValue(
			Object.assign(new Error("Too Many Requests"), { statusCode: 429 }),
		);

		for (let i = 0; i < PENDING_EPISODE_MAX_ATTEMPTS + 2; i++) {
			await drainConsolidation(engine);
		}

		const [row] = await state.getPendingEpisodes(0);
		expect(row.failedAttempts).toBe(0);
		expect(await state.countDeadLetteredEpisodes()).toBe(0);
		expect(await state.countPendingSessions()).toBe(1);
	});

	it("a deterministic failure counts once per drain while healthy episodes still finish", async () => {
		const savedMax = config.consolidation.maxSessionsPerRun;
		config.consolidation.maxSessionsPerRun = 2;
		try {
			const now = Date.now();
			await state.insertPendingEpisode(
				pendingRow({
					id: "row-poison",
					sessionId: "session-poison",
					content: "POISON: malformed",
					maxMessageTime: now - 3000,
				}),
			);
			await state.insertPendingEpisode(
				pendingRow({
					id: "row-good-1",
					sessionId: "session-good-1",
					maxMessageTime: now - 2000,
				}),
			);
			await state.insertPendingEpisode(
				pendingRow({
					id: "row-good-2",
					sessionId: "session-good-2",
					maxMessageTime: now - 1000,
				}),
			);
			spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockImplementation(
				async (summary: string) => {
					if (summary.includes("POISON")) {
						throw new SyntaxError("Unexpected token in model output");
					}
					return [entry(`Fact ${Math.random()}`)];
				},
			);

			// The poison chunk is attempted in the first batch only; the second batch
			// must not retry it, so it is counted once for this drain.
			await drainConsolidation(engine);

			const rows = await state.getPendingEpisodes(0);
			expect(rows.map((r) => r.id)).toEqual(["row-poison"]);
			expect(rows[0].failedAttempts).toBe(1);
		} finally {
			config.consolidation.maxSessionsPerRun = savedMax;
		}
	});

	it("retryDeadLetteredEpisodes requeues dead-lettered rows", async () => {
		await state.insertPendingEpisode(
			pendingRow({ id: "row-dead", sessionId: "session-dead" }),
		);
		for (let i = 0; i < PENDING_EPISODE_MAX_ATTEMPTS; i++) {
			await state.recordPendingEpisodeFailures(["row-dead"], "boom");
		}
		expect(await state.countDeadLetteredEpisodes()).toBe(1);
		expect(await state.countPendingSessions()).toBe(0);

		expect(await state.retryDeadLetteredEpisodes()).toBe(1);

		expect(await state.countPendingSessions()).toBe(1);
		expect(await state.countDeadLetteredEpisodes()).toBe(0);
	});
});

describe("unavailable store and the batch window", () => {
	it("a healthy session is processed although the oldest sessions wait for an unavailable store", async () => {
		const savedMax = config.consolidation.maxSessionsPerRun;
		config.consolidation.maxSessionsPerRun = 2;
		const workDb = new KnowledgeDB(join(tempDir, "work-window.db"));
		try {
			const homeDir = homedir();
			const routedConfig = {
				userId: "alice",
				stores: [
					{ id: "personal", kind: "sqlite", writable: true },
					{ id: "work", kind: "sqlite", writable: true },
				],
				domains: [
					{ id: "personal", description: "Personal", store: "personal" },
					{ id: "work", description: "Work", store: "work" },
				],
				projects: [
					{ path: `${homeDir}/work`, default_domain: "work" },
					{ path: `${homeDir}/personal`, default_domain: "personal" },
				],
			} as unknown as KnowledgeServerConfig;
			const stores = new Map<string, IKnowledgeStore>([
				["personal", db],
				["work", workDb],
			]);
			// The work store is unavailable (it failed at startup).
			const router = new DomainRouter(
				routedConfig,
				stores,
				db,
				new Set(["work"]),
			);
			const activation = new ActivationEngine(db);
			spyOn(activation.embeddings, "embed").mockImplementation(async (text) =>
				uniqueEmbedding(seenEmbeddings, text),
			);
			const routed = new ConsolidationEngine(
				db,
				state,
				activation,
				[new PendingEpisodesReader(state)],
				router,
			);
			const now = Date.now();
			for (const i of [1, 2]) {
				await state.insertPendingEpisode(
					pendingRow({
						id: `work-${i}`,
						sessionId: `session-work-${i}`,
						directory: `${homeDir}/work/app`,
						maxMessageTime: now - 5000 + i,
					}),
				);
			}
			await state.insertPendingEpisode(
				pendingRow({
					id: "personal-1",
					sessionId: "session-personal",
					directory: `${homeDir}/personal/notes`,
					maxMessageTime: now - 1000,
				}),
			);
			spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
				entry("Ship the release on Fridays."),
			]);

			await routed.consolidate();

			expect(
				(await state.getProcessedEpisodeRanges(["session-personal"])).get(
					"session-personal",
				),
			).toHaveLength(1);
			const waiting = await state.getPendingEpisodes(0);
			expect(waiting.map((r) => r.id).sort()).toEqual(["work-1", "work-2"]);
			expect(waiting.every((r) => r.failedAttempts === 0)).toBe(true);
		} finally {
			config.consolidation.maxSessionsPerRun = savedMax;
			await workDb.close();
		}
	});
});

describe("episodes blocked after extraction", () => {
	/** Routing config: "work" is a configured domain whose store is unavailable. */
	function workUnavailableRouter(workDb: KnowledgeDB): DomainRouter {
		const homeDir = homedir();
		const routedConfig = {
			userId: "alice",
			stores: [
				{ id: "personal", kind: "sqlite", writable: true },
				{ id: "work", kind: "sqlite", writable: true },
			],
			domains: [
				{ id: "personal", description: "Personal", store: "personal" },
				{ id: "work", description: "Work", store: "work" },
			],
			projects: [
				{ path: `${homeDir}/work`, default_domain: "work" },
				{ path: `${homeDir}/personal`, default_domain: "personal" },
			],
		} as unknown as KnowledgeServerConfig;
		const stores = new Map<string, IKnowledgeStore>([
			["personal", db],
			["work", workDb],
		]);
		return new DomainRouter(routedConfig, stores, db, new Set(["work"]));
	}

	function routedEngine(router: DomainRouter): ConsolidationEngine {
		const activation = new ActivationEngine(db);
		spyOn(activation.embeddings, "embed").mockImplementation(async (text) =>
			uniqueEmbedding(seenEmbeddings, text),
		);
		return new ConsolidationEngine(
			db,
			state,
			activation,
			[new PendingEpisodesReader(state)],
			router,
		);
	}

	it("an entry routed to the unavailable store is not extracted again in later batches of the drain", async () => {
		const workDb = new KnowledgeDB(join(tempDir, "work-blocked.db"));
		try {
			const routed = routedEngine(workUnavailableRouter(workDb));
			const home = homedir();
			const now = Date.now();
			// The blocked episode is the oldest, so it is settled first in the drain.
			await state.insertPendingEpisode(
				pendingRow({
					id: "blocked",
					sessionId: "session-blocked",
					directory: `${home}/personal/notes`,
					content: "ROUTE-TO-WORK the release notes.",
					maxMessageTime: now - 5000,
				}),
			);
			await state.insertPendingEpisode(
				pendingRow({
					id: "healthy",
					sessionId: "session-healthy",
					directory: `${home}/personal/notes`,
					content: "Healthy session content.",
					maxMessageTime: now - 1000,
				}),
			);
			const extract = spyOn(
				ConsolidationLLM.prototype,
				"extractKnowledge",
			).mockImplementation(async (summary: string) =>
				summary.includes("ROUTE-TO-WORK")
					? [entry("Ship the release on Fridays.", "work")]
					: [entry("Use tabs for indentation.")],
			);

			// The drain loop: batches share one `attempted` set until a batch settles nothing.
			const attempted = new Set<string>();
			for (let batch = 0; batch < 5; batch++) {
				const result = await routed.consolidate({ attempted });
				if (result.sessionsProcessed === 0) break;
			}

			// One extraction for each episode. The blocked one is not paid for again.
			expect(extract).toHaveBeenCalledTimes(2);
			const waiting = await state.getPendingEpisodes(0);
			expect(waiting.map((r) => r.id)).toEqual(["blocked"]);
			expect(waiting[0].failedAttempts ?? 0).toBe(0);

			// A later drain (a new `attempted` set) tries it again: it was deferred, not failed.
			await routed.consolidate();
			expect(extract).toHaveBeenCalledTimes(3);
		} finally {
			await workDb.close();
		}
	});

	it("a store lock taken between the check and the write blocks the episode for the rest of the drain", async () => {
		const now = Date.now();
		await state.insertPendingEpisode(
			pendingRow({
				id: "locked-late",
				sessionId: "session-locked-late",
				content: "Late lock content.",
				maxMessageTime: now - 5000,
			}),
		);
		await state.insertPendingEpisode(
			pendingRow({
				id: "healthy",
				sessionId: "session-healthy",
				content: "Healthy session content.",
				maxMessageTime: now - 1000,
			}),
		);
		// Per chunk: one check before extraction (free, then released), one lock
		// attempt at write time. The write attempt of the first chunk finds it held.
		spyOn(db, "tryAcquireConsolidationLock")
			.mockResolvedValueOnce(true)
			.mockResolvedValueOnce(false)
			.mockResolvedValueOnce(true)
			.mockResolvedValueOnce(true)
			.mockResolvedValue(true);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockResolvedValue([entry("Use tabs for indentation.")]);

		const attempted = new Set<string>();
		for (let batch = 0; batch < 5; batch++) {
			const result = await engine.consolidate({ attempted });
			if (result.sessionsProcessed === 0) break;
		}

		expect(extract).toHaveBeenCalledTimes(2);
		const waiting = await state.getPendingEpisodes(0);
		expect(waiting.map((r) => r.id)).toEqual(["locked-late"]);
		expect(waiting[0].failedAttempts ?? 0).toBe(0);
	});
});

describe("secrets are scrubbed before extraction", () => {
	it("the extraction prompt carries no credential from the episode text or its metadata", async () => {
		const token = ["ghp_", "e".repeat(36)].join("");
		await state.insertPendingEpisode(
			pendingRow({
				id: "scrub-1",
				sessionId: "session-scrub",
				sessionTitle: `Debug ${token}`,
				projectName: `proj ${token}`,
				content: `User: deploy with ${token} please.`,
			}),
		);
		const extract = spyOn(
			ConsolidationLLM.prototype,
			"extractKnowledge",
		).mockResolvedValue([entry("Deploys use a vault key.")]);

		await engine.consolidate();

		expect(extract).toHaveBeenCalledTimes(1);
		const prompt = String(extract.mock.calls[0]?.[0]);
		expect(prompt).not.toContain(token);
		expect(prompt).toContain("[REDACTED:github_token]");
	});
});
