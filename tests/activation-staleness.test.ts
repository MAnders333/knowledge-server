import { describe, expect, it, mock } from "bun:test";
import { ActivationEngine } from "../src/activation/activate";
import { config } from "../src/config";
import type { IKnowledgeStore } from "../src/db/interface";
import type { KnowledgeEntry } from "../src/types";
import { fakeEmbedding, makeEntry } from "./fixtures";

const QUERY = "staleness predicate test cue";
const DAY_MS = 24 * 60 * 60 * 1000;
const HALF_LIFE_DAYS = config.decay.typeHalfLife.fact;

/**
 * Entry older than its half-life. Access and injection counts are set per case;
 * access_count is frozen once injections stop incrementing it, so the predicate
 * must read injection_count and the time since the last access.
 */
function aged(
	id: string,
	overrides: Partial<KnowledgeEntry> & { lastAccessedDaysAgo: number },
): KnowledgeEntry {
	const now = Date.now();
	const { lastAccessedDaysAgo, ...rest } = overrides;
	return makeEntry({
		id,
		type: "fact",
		content: `${id} knowledge`,
		createdAt: now - (HALF_LIFE_DAYS + 10) * DAY_MS,
		lastAccessedAt: now - lastAccessedDaysAgo * DAY_MS,
		accessCount: 0,
		injectionCount: 0,
		embedding: fakeEmbedding(QUERY),
		...rest,
	});
}

async function mayBeStaleById(entries: KnowledgeEntry[]) {
	const store = {
		id: "test-store",
		getActiveEntriesWithEmbeddings: mock(() => Promise.resolve(entries)),
		scanEmbeddings: mock(() =>
			Promise.resolve(
				entries.map((e) => ({
					id: e.id,
					embedding: new Float32Array(e.embedding ?? []),
				})),
			),
		),
		getEntriesByIds: mock((want: string[]) =>
			Promise.resolve(entries.filter((e) => want.includes(e.id))),
		),
		getSupportSourcesForIds: mock(() => Promise.resolve(new Map())),
		getContradictPairsForIds: mock(() => Promise.resolve(new Map())),
		recordInjections: mock(() => Promise.resolve()),
	} as unknown as IKnowledgeStore;
	const engine = new ActivationEngine(store);
	engine.embeddings.embedBatch = mock(async (qs: string[]) =>
		qs.map((q) => fakeEmbedding(q)),
	) as typeof engine.embeddings.embedBatch;
	const result = await engine.activate(QUERY, { limit: 20 });
	return Object.fromEntries(
		result.entries.map((r) => [r.entry.id, r.staleness.mayBeStale]),
	);
}

describe("mayBeStale", () => {
	it("is not set for an old entry injected recently, even with accessCount 0", async () => {
		const flags = await mayBeStaleById([
			aged("recent-injection", { lastAccessedDaysAgo: 0.5, injectionCount: 1 }),
		]);
		expect(flags["recent-injection"]).toBe(false);
	});

	it("is set for an old entry not used for longer than its half-life with little use", async () => {
		const flags = await mayBeStaleById([
			aged("long-unused", { lastAccessedDaysAgo: HALF_LIFE_DAYS + 5 }),
		]);
		expect(flags["long-unused"]).toBe(true);
	});

	it("is not set when injections alone show repeated use", async () => {
		const flags = await mayBeStaleById([
			aged("injected-often", {
				lastAccessedDaysAgo: HALF_LIFE_DAYS + 5,
				injectionCount: 3,
			}),
		]);
		expect(flags["injected-often"]).toBe(false);
	});

	it("is not set when retrieval count alone shows repeated use", async () => {
		const flags = await mayBeStaleById([
			aged("accessed-often", {
				lastAccessedDaysAgo: HALF_LIFE_DAYS + 5,
				accessCount: 3,
			}),
		]);
		expect(flags["accessed-often"]).toBe(false);
	});

	it("is not set for an entry younger than its half-life", async () => {
		const now = Date.now();
		const young = makeEntry({
			id: "young",
			type: "fact",
			content: "young knowledge",
			createdAt: now - 2 * DAY_MS,
			lastAccessedAt: now - 2 * DAY_MS,
			accessCount: 0,
			injectionCount: 0,
			embedding: fakeEmbedding(QUERY),
		});
		const flags = await mayBeStaleById([young]);
		expect(flags.young).toBe(false);
	});
});
