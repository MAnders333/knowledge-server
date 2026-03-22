/**
 * Integration tests for domain routing through the consolidation pipeline.
 *
 * Verifies that:
 * - Entries extracted from a session in a work project land in the work store
 * - Entries extracted from a personal project land in the personal store
 * - When the LLM assigns a domain that differs from the project default,
 *   the entry lands in the LLM-assigned domain's store
 * - Single-store mode (no DomainRouter) still works correctly
 *
 * Uses real KnowledgeDB instances but mocks the LLM and embedding clients.
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
import { homedir } from "node:os";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import type { KnowledgeServerConfig } from "../src/config-file";
import { ConsolidationEngine } from "../src/consolidation/consolidate";
import { ConsolidationLLM } from "../src/consolidation/llm";
import { OpenCodeEpisodeReader } from "../src/consolidation/readers/opencode";
import { KnowledgeDB } from "../src/db/database";
import { DomainRouter } from "../src/domain-router";
import { fakeEmbedding } from "./fixtures";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeMultiStoreConfig(
	personalId: string,
	workId: string,
): KnowledgeServerConfig {
	return {
		stores: [
			{ id: personalId, kind: "sqlite", writable: true },
			{ id: workId, kind: "sqlite", writable: true },
		],
		domains: [
			{
				id: "personal",
				description: "Personal preferences and individual workflows",
				store: personalId,
			},
			{
				id: "work",
				description: "Team knowledge and project decisions",
				store: workId,
			},
		],
		projects: [
			{
				path: `${homedir()}/work/my-project`,
				default_domain: "work",
			},
			{
				path: `${homedir()}/personal`,
				default_domain: "personal",
			},
		],
		userId: "default",
	};
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("Domain routing via DomainRouter + ConsolidationEngine", () => {
	let tempDir: string;
	let personalDb: KnowledgeDB;
	let workDb: KnowledgeDB;
	let fakeOpenCodeDbPath: string;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-domain-integration-test-"));
		fakeOpenCodeDbPath = join(tempDir, "opencode-fake.db");
		writeFileSync(fakeOpenCodeDbPath, "");
		personalDb = new KnowledgeDB(join(tempDir, "personal.db"));
		workDb = new KnowledgeDB(join(tempDir, "work.db"));
	});

	afterEach(async () => {
		mock.restore();
		await personalDb.close();
		await workDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});
	it("routes entries to the work store when session directory matches work project", async () => {
		const now = Date.now();
		const config = makeMultiStoreConfig("personal", "work");
		const storeMap = new Map([
			["personal", personalDb],
			["work", workDb],
		]);
		const router = new DomainRouter(config, storeMap, personalDb);
		const activation = new ActivationEngine(personalDb, [personalDb, workDb]);
		const reader = new OpenCodeEpisodeReader(fakeOpenCodeDbPath);
		const engine = new ConsolidationEngine(
			personalDb,
			activation,
			[reader],
			router,
		);

		// Mock embeddings — embedBatch for activation queries, embed for reconsolidate
		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("work entry"),
		]);
		spyOn(activation.embeddings, "embed").mockResolvedValue(
			fakeEmbedding("work entry"),
		);

		// Mock LLM: extractKnowledge returns one entry assigned to "work" domain
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact" as const,
				content: "The team uses Conventional Commits",
				topics: ["git", "conventions"],
				confidence: 0.9,
				scope: "team" as const,
				source: "test",
				domain: "work",
			},
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue(
			"insert",
		);

		// Mock candidate sessions from the work project directory
		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-work-1", maxMessageTime: now }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			{
				sessionId: "session-work-1",
				startMessageId: "msg-start",
				endMessageId: "msg-end",
				sessionTitle: "Work Session",
				projectName: "my-project",
				directory: `${homedir()}/work/my-project`,
				timeCreated: now,
				maxMessageTime: now,
				content: "session content about git conventions",
				contentType: "messages" as const,
				approxTokens: 20,
			},
		]);

		await engine.consolidate();

		// Entry should be in work store, not personal store
		const workEntries = await workDb.getActiveEntries();
		const personalEntries = await personalDb.getActiveEntries();

		expect(workEntries.length).toBe(1);
		expect(workEntries[0].content).toBe("The team uses Conventional Commits");
		expect(personalEntries.length).toBe(0);
	});

	it("overrides project default domain when LLM assigns a different domain", async () => {
		const now = Date.now();
		const config = makeMultiStoreConfig("personal", "work");
		const storeMap = new Map([
			["personal", personalDb],
			["work", workDb],
		]);
		const router = new DomainRouter(config, storeMap, personalDb);
		const activation = new ActivationEngine(personalDb, [personalDb, workDb]);
		const reader = new OpenCodeEpisodeReader(fakeOpenCodeDbPath);
		const engine = new ConsolidationEngine(
			personalDb,
			activation,
			[reader],
			router,
		);

		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("personal pref"),
		]);
		spyOn(activation.embeddings, "embed").mockResolvedValue(
			fakeEmbedding("personal pref"),
		);

		// LLM assigns "personal" domain even though session is from a work project
		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "decision" as const,
				content: "I prefer using uv over pip for Python",
				topics: ["python", "tooling"],
				confidence: 0.85,
				scope: "personal" as const,
				source: "test",
				domain: "personal", // LLM overrides the work project default
			},
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue(
			"insert",
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-work-2", maxMessageTime: now }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			{
				sessionId: "session-work-2",
				startMessageId: "msg-a",
				endMessageId: "msg-b",
				sessionTitle: "Work Session",
				projectName: "my-project",
				directory: `${homedir()}/work/my-project`, // work project
				timeCreated: now,
				maxMessageTime: now,
				content: "discussed python tooling preferences",
				contentType: "messages" as const,
				approxTokens: 15,
			},
		]);

		await engine.consolidate();

		// Despite work project, LLM said "personal" — should land in personal store
		const personalEntries = await personalDb.getActiveEntries();
		const workEntries = await workDb.getActiveEntries();

		expect(personalEntries.length).toBe(1);
		expect(personalEntries[0].content).toBe(
			"I prefer using uv over pip for Python",
		);
		expect(workEntries.length).toBe(0);
	});

	it("falls back to writable store when no domain router is configured", async () => {
		const now = Date.now();
		// Single-store mode — no DomainRouter
		const activation = new ActivationEngine(personalDb);
		const reader = new OpenCodeEpisodeReader(fakeOpenCodeDbPath);
		const engine = new ConsolidationEngine(personalDb, activation, [reader]); // no router

		spyOn(activation.embeddings, "embedBatch").mockResolvedValue([
			fakeEmbedding("solo entry"),
		]);
		spyOn(activation.embeddings, "embed").mockResolvedValue(
			fakeEmbedding("solo entry"),
		);

		spyOn(ConsolidationLLM.prototype, "extractKnowledge").mockResolvedValue([
			{
				type: "fact" as const,
				content: "Single store entry",
				topics: ["test"],
				confidence: 0.9,
				scope: "personal" as const,
				source: "test",
			},
		]);
		spyOn(ConsolidationLLM.prototype, "decideMerge").mockResolvedValue(
			"insert",
		);

		spyOn(
			OpenCodeEpisodeReader.prototype,
			"getCandidateSessions",
		).mockReturnValue([{ id: "session-solo-1", maxMessageTime: now }]);
		spyOn(OpenCodeEpisodeReader.prototype, "getNewEpisodes").mockReturnValue([
			{
				sessionId: "session-solo-1",
				startMessageId: "msg-s",
				endMessageId: "msg-e",
				sessionTitle: "Solo Session",
				projectName: "my-project",
				directory: `${homedir()}/work/my-project`,
				timeCreated: now,
				maxMessageTime: now,
				content: "some session content",
				contentType: "messages" as const,
				approxTokens: 10,
			},
		]);

		await engine.consolidate();

		const entries = await personalDb.getActiveEntries();
		expect(entries.length).toBe(1);
		expect(entries[0].content).toBe("Single store entry");
	});
});
