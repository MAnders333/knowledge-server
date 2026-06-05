import { afterAll, describe, expect, it } from "bun:test";
import { cosineSimilarity } from "../src/activation/embeddings";

describe("cosineSimilarity", () => {
	it("should return 1 for identical vectors", () => {
		const v = [1, 2, 3, 4, 5];
		expect(cosineSimilarity(v, v)).toBeCloseTo(1.0);
	});

	it("should return 0 for orthogonal vectors", () => {
		const a = [1, 0, 0];
		const b = [0, 1, 0];
		expect(cosineSimilarity(a, b)).toBeCloseTo(0.0);
	});

	it("should return -1 for opposite vectors", () => {
		const a = [1, 2, 3];
		const b = [-1, -2, -3];
		expect(cosineSimilarity(a, b)).toBeCloseTo(-1.0);
	});

	it("should handle zero vectors gracefully", () => {
		const a = [0, 0, 0];
		const b = [1, 2, 3];
		expect(cosineSimilarity(a, b)).toBe(0);
	});

	it("should throw on dimension mismatch", () => {
		expect(() => cosineSimilarity([1, 2], [1, 2, 3])).toThrow(
			"Vector dimension mismatch",
		);
	});

	it("should compute similarity for realistic embeddings", () => {
		// Simulate normalized embeddings (typical for embedding models)
		const a = [0.1, 0.3, 0.5, 0.7, 0.2];
		const b = [0.15, 0.28, 0.52, 0.68, 0.25]; // slightly different
		const c = [-0.5, -0.3, -0.1, 0.1, -0.7]; // very different

		const simAB = cosineSimilarity(a, b);
		const simAC = cosineSimilarity(a, c);

		// a and b should be much more similar than a and c
		expect(simAB).toBeGreaterThan(0.99);
		expect(simAC).toBeLessThan(0.0);
		expect(simAB).toBeGreaterThan(simAC);
	});
});

describe("EmbeddingClient.embedBatch timeout", () => {
	// Regression test: without an AbortSignal on the fetch, a slow or
	// rate-limited embedding proxy causes the request to hang indefinitely
	// and silently breaks the /activate route and MCP `activate` tool
	// (the MCP 15s timeout then surfaces the hang as a generic TimeoutError).

	const server = Bun.serve({
		port: 0, // OS-assigned
		fetch: () => new Promise(() => {}), // hang forever, no response
	});
	const baseURL = `http://127.0.0.1:${server.port}`;

	afterAll(() => {
		server.stop(true);
	});

	it("aborts the fetch when the embedding proxy hangs past the timeout", async () => {
		// `config.embedding.baseURL` is captured at module load. We must set
		// the env var BEFORE the config module is evaluated, then dynamically
		// import with a cache-busting suffix so the modules re-evaluate and
		// re-read the env. Mutating process.env post-import would not affect
		// the already-resolved config object.
		const prev = process.env.EMBEDDING_BASE_URL;
		process.env.EMBEDDING_BASE_URL = baseURL;
		try {
			const { EmbeddingClient: DynamicClient } = await import(
				`../src/activation/embeddings.ts?timeout=${Date.now()}-${Math.random()}`
			);
			const client = new DynamicClient({ timeoutMs: 100 });
			const start = Date.now();
			let thrown: unknown;
			try {
				await client.embed("test query");
			} catch (e) {
				thrown = e;
			}
			const elapsed = Date.now() - start;
			expect(thrown).toBeDefined();
			// Must abort close to the 100ms timeout, not hang forever.
			expect(elapsed).toBeLessThan(2_000);
		} finally {
			if (prev === undefined) {
				process.env.EMBEDDING_BASE_URL = undefined;
			} else {
				process.env.EMBEDDING_BASE_URL = prev;
			}
		}
	});
});
