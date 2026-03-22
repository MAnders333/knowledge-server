import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import type { KnowledgeServerConfig } from "../src/config-file";
import { KnowledgeDB } from "../src/db/database";
import { DomainRouter } from "../src/consolidation/domain-router";

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeConfig(
	overrides: Partial<KnowledgeServerConfig> = {},
): KnowledgeServerConfig {
	return {
		// Both writable — domains are configured so multiple writable stores are valid
		stores: [
			{ id: "personal", kind: "sqlite", writable: true },
			{ id: "work", kind: "sqlite", writable: true },
		],
		domains: [
			{
				id: "personal",
				description: "Personal preferences and workflows",
				store: "personal",
			},
			{
				id: "work",
				description: "Team knowledge and project decisions",
				store: "work",
			},
		],
		projects: [
			{ path: `${homedir()}/work/project-a`, default_domain: "work" },
			{ path: `${homedir()}/personal`, default_domain: "personal" },
		],
		userId: "default",
		...overrides,
	};
}

describe("DomainRouter.resolve", () => {
	let tempDir: string;
	let personalDb: KnowledgeDB;
	let workDb: KnowledgeDB;
	let stores: Map<string, KnowledgeDB>;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-domain-test-"));
		personalDb = new KnowledgeDB(join(tempDir, "personal.db"));
		workDb = new KnowledgeDB(join(tempDir, "work.db"));
		stores = new Map([
			["personal", personalDb],
			["work", workDb],
		]);
	});

	afterEach(async () => {
		await personalDb.close();
		await workDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	// ── No domains configured ─────────────────────────────────────────────────

	it("returns fallback store with no domain context when no domains configured", () => {
		const config = makeConfig({ domains: [], projects: [] });
		const router = new DomainRouter(
			config,
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a`);

		expect(result.domainId).toBeUndefined();
		expect(result.store).toBe(personalDb);
		expect(result.domainContext).toBeNull();
	});

	// ── Project path matching ─────────────────────────────────────────────────

	it("matches project path and returns correct domain store", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a`);

		expect(result.domainId).toBe("work");
		expect(result.store).toBe(workDb);
		expect(result.domainContext?.defaultDomain).toBe("work");
	});

	it("matches subdirectory within project path", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a/src/components`);

		expect(result.domainId).toBe("work");
	});

	it("matches personal project path", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/personal/knowledge-server`);

		expect(result.domainId).toBe("personal");
		expect(result.store).toBe(personalDb);
	});

	it("falls back to first domain when no project matches", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/unknown/project`);

		// First domain is "personal"
		expect(result.domainId).toBe("personal");
		expect(result.store).toBe(personalDb);
	});

	it("returns first domain when directory is empty", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve("");

		expect(result.domainId).toBe("personal");
	});

	// ── Longest match wins ────────────────────────────────────────────────────

	it("longest path prefix wins when multiple projects could match", () => {
		const config = makeConfig({
			projects: [
				{ path: `${homedir()}/work`, default_domain: "personal" },
				{ path: `${homedir()}/work/project-a`, default_domain: "work" },
			],
		});
		const router = new DomainRouter(
			config,
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a/src`);

		// More specific path should win
		expect(result.domainId).toBe("work");
	});

	// ── Tilde expansion ───────────────────────────────────────────────────────

	it("expands tilde in project paths (handled by config-file.ts validateProject)", () => {
		// Projects with already-expanded paths (validateProject expands ~ at load time)
		const config = makeConfig({
			projects: [
				{ path: `${homedir()}/work/project-a`, default_domain: "work" },
			],
		});
		const router = new DomainRouter(
			config,
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a`);
		expect(result.domainId).toBe("work");
	});

	// ── Domain context ────────────────────────────────────────────────────────

	it("includes all domains in domainContext", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const result = router.resolve(`${homedir()}/work/project-a`);

		expect(result.domainContext?.domains).toHaveLength(2);
		expect(result.domainContext?.domains.map((d) => d.id)).toContain(
			"personal",
		);
		expect(result.domainContext?.domains.map((d) => d.id)).toContain("work");
	});

	it("domainContext.defaultDomain reflects the resolved domain", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		const work = router.resolve(`${homedir()}/work/project-a`);
		expect(work.domainContext?.defaultDomain).toBe("work");

		const personal = router.resolve(`${homedir()}/personal/notes`);
		expect(personal.domainContext?.defaultDomain).toBe("personal");
	});
});

describe("DomainRouter.resolveStore", () => {
	let tempDir: string;
	let personalDb: KnowledgeDB;
	let workDb: KnowledgeDB;
	let stores: Map<string, KnowledgeDB>;

	beforeEach(() => {
		tempDir = mkdtempSync(join(tmpdir(), "ks-domain-store-test-"));
		personalDb = new KnowledgeDB(join(tempDir, "personal.db"));
		workDb = new KnowledgeDB(join(tempDir, "work.db"));
		stores = new Map([
			["personal", personalDb],
			["work", workDb],
		]);
	});

	afterEach(async () => {
		await personalDb.close();
		await workDb.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("returns the correct store for a known domain id", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		expect(router.resolveStore("personal")).toBe(personalDb);
		expect(router.resolveStore("work")).toBe(workDb);
	});

	it("returns undefined for unknown domain id", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		expect(router.resolveStore("nonexistent")).toBeUndefined();
	});

	it("returns undefined for undefined input", () => {
		const router = new DomainRouter(
			makeConfig(),
			stores as Map<string, import("../src/db/interface").IKnowledgeDB>,
			personalDb,
		);

		expect(router.resolveStore(undefined)).toBeUndefined();
	});
});
