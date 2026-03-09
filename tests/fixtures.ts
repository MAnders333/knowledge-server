/**
 * Shared test fixtures for the knowledge-server test suite.
 */
import type { KnowledgeEntry } from "../src/types";

/**
 * Build a complete KnowledgeEntry with sensible defaults.
 * Pass overrides to customise only the fields relevant to your test.
 */
export function makeEntry(
	overrides: Partial<KnowledgeEntry> = {},
): KnowledgeEntry {
	const now = Date.now();
	return {
		id: "test",
		type: "fact",
		content: "Test content",
		topics: ["test"],
		confidence: 0.8,
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
		...overrides,
	};
}

/**
 * Deterministic fake embedding: encodes the first 3 chars of content as a
 * unit vector. Two entries with the same first 3 chars will have similarity
 * ~1.0 (near-duplicate). Used to avoid real embedding API calls in tests.
 */
export function fakeEmbedding(content: string): number[] {
	const vec = new Array(8).fill(0);
	for (let i = 0; i < Math.min(3, content.length); i++) {
		vec[i % 8] += content.charCodeAt(i);
	}
	const norm = Math.sqrt(vec.reduce((s, v) => s + v * v, 0)) || 1;
	return vec.map((v) => v / norm);
}
