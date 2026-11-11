import {
	afterAll,
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	mock,
	spyOn,
} from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ActivationEngine } from "../src/activation/activate";
import {
	EmbeddingClient,
	type EmbeddingError,
	cosineSimilarity,
	isRetryableEmbeddingError,
	retryDelayMs,
} from "../src/activation/embeddings";
import { config } from "../src/config";
import { KnowledgeDB } from "../src/db/sqlite/index";
import * as loggerModule from "../src/logger";

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

describe("EmbeddingClient.embedBatch response handling", () => {
	const realFetch = globalThis.fetch;
	let savedDims: number | undefined;

	beforeEach(() => {
		savedDims = config.embedding.dimensions;
		config.embedding.dimensions = undefined;
	});

	afterEach(() => {
		globalThis.fetch = realFetch;
		config.embedding.dimensions = savedDims;
		mock.restore();
	});

	// Text "tN" maps to vector vec(N), so ordering errors show up as wrong vectors.
	const vec = (i: number) => [i, i + 0.5, 1];
	const texts = (n: number) => Array.from({ length: n }, (_, i) => `t${i}`);
	const okResponse = (data: unknown[]) =>
		Response.json({ object: "list", data });
	const ok = (inputs: string[]) =>
		okResponse(
			inputs.map((t, i) => ({ index: i, embedding: vec(Number(t.slice(1))) })),
		);

	type Handler = (
		inputs: string[],
		init?: RequestInit,
	) => Response | Promise<Response>;

	function stubFetch(...handlers: Handler[]) {
		const state = { count: 0 };
		globalThis.fetch = (async (_url: unknown, init?: RequestInit) => {
			const inputs = JSON.parse(String(init?.body)).input as string[];
			const handler = handlers[Math.min(state.count, handlers.length - 1)];
			state.count++;
			return handler(inputs, init);
		}) as typeof fetch;
		return state;
	}

	async function errorOf(p: Promise<unknown>): Promise<EmbeddingError> {
		try {
			await p;
		} catch (e) {
			return e as EmbeddingError;
		}
		throw new Error("expected embedBatch to reject");
	}

	const hang: Handler = (_inputs, init) =>
		new Promise<Response>((_, reject) =>
			init?.signal?.addEventListener("abort", () =>
				reject(init.signal?.reason),
			),
		);

	it("uses the array position when the first item has no index", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ embedding: vec(0) },
				{ index: 1, embedding: vec(1) },
				{ index: 2, embedding: vec(2) },
			]),
		);
		expect(await client.embedBatch(texts(3))).toEqual([vec(0), vec(1), vec(2)]);
		expect(warnSpy).not.toHaveBeenCalled();
	});

	it("reorders a shuffled response by index", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: 2, embedding: vec(2) },
				{ index: 0, embedding: vec(0) },
				{ index: 1, embedding: vec(1) },
			]),
		);
		expect(await client.embedBatch(texts(3))).toEqual([vec(0), vec(1), vec(2)]);
	});

	it("handles a missing index on item 0 combined with a shuffle", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ embedding: vec(0) },
				{ index: 2, embedding: vec(2) },
				{ index: 1, embedding: vec(1) },
			]),
		);
		expect(await client.embedBatch(texts(3))).toEqual([vec(0), vec(1), vec(2)]);
		expect(warnSpy).not.toHaveBeenCalled();
	});

	it("keeps array order and warns once when indices are duplicated", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: 0, embedding: vec(0) },
				{ index: 0, embedding: vec(1) },
				{ index: 2, embedding: vec(2) },
			]),
		);
		expect(await client.embedBatch(texts(3), { maxRetries: 0 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(warnSpy).toHaveBeenCalledTimes(1);
		expect(String(warnSpy.mock.calls[0][0])).toContain("duplicate indices");
	});

	it("treats a missing index colliding with an explicit one as a duplicate", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([{ embedding: vec(0) }, { index: 0, embedding: vec(1) }]),
		);
		expect(await client.embedBatch(texts(2), { maxRetries: 0 })).toEqual([
			vec(0),
			vec(1),
		]);
		expect(warnSpy).toHaveBeenCalledTimes(1);
	});

	it("warns only once per client when every response repeats index 0", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const allZero = (inputs: string[]) =>
			okResponse(
				inputs.map((t) => ({ index: 0, embedding: vec(Number(t.slice(1))) })),
			);
		stubFetch(allZero, allZero);
		expect(await client.embedBatch(texts(2))).toEqual([vec(0), vec(1)]);
		expect(await client.embedBatch(texts(2))).toEqual([vec(0), vec(1)]);
		expect(warnSpy).toHaveBeenCalledTimes(1);
	});

	it("orders a 1-based shuffled response by relative index", async () => {
		const warnSpy = spyOn(loggerModule.logger, "warn").mockImplementation(
			() => {},
		);
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: 3, embedding: vec(2) },
				{ index: 1, embedding: vec(0) },
				{ index: 2, embedding: vec(1) },
			]),
		);
		expect(await client.embedBatch(texts(3))).toEqual([vec(0), vec(1), vec(2)]);
		expect(warnSpy).not.toHaveBeenCalled();
	});

	it("orders numeric-string indices by value", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: "1", embedding: vec(1) },
				{ index: "0", embedding: vec(0) },
			]),
		);
		expect(await client.embedBatch(texts(2))).toEqual([vec(0), vec(1)]);
	});

	it("retries a truncated response and succeeds on the next attempt", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(
			() => okResponse([{ index: 0, embedding: vec(0) }]),
			ok,
		);
		expect(await client.embedBatch(texts(3), { maxRetries: 2 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(2);
	});

	it("gives up on a persistently truncated response with a clear error", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(() =>
			okResponse([{ index: 0, embedding: vec(0) }]),
		);
		const err = await errorOf(client.embedBatch(texts(3), { maxRetries: 2 }));
		expect(err.message).toContain("Embedding chunk 1/1");
		expect(err.message).toContain("after 3 attempt(s)");
		expect(err.message).toContain("returned 1 embeddings for 3 inputs");
		expect(state.count).toBe(3);
	});

	it("retries a body that is not valid JSON (SyntaxError) and succeeds", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(
			() => new Response('{"data":[{"ind', { status: 200 }),
			ok,
		);
		expect(await client.embedBatch(texts(3), { maxRetries: 1 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(2);
	});

	it("retries a network failure (TypeError) and succeeds", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(() => {
			throw new TypeError("fetch failed");
		}, ok);
		expect(await client.embedBatch(texts(3), { maxRetries: 1 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(2);
	});

	it("retries HTTP 408", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(
			() => new Response("request timeout", { status: 408 }),
			ok,
		);
		expect(await client.embedBatch(texts(3), { maxRetries: 1 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(2);
	});

	it.each([401, 403])("does not retry HTTP %i", async (status) => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(() => new Response("denied", { status }), ok);
		const err = await errorOf(client.embedBatch(texts(3), { maxRetries: 2 }));
		expect(err.message).toContain(`Embedding API error ${status}`);
		expect(err.retryable).toBe(false);
		expect(state.count).toBe(1);
	});

	it("does not retry HTTP 400", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(() => new Response("bad request", { status: 400 }));
		const err = await errorOf(client.embedBatch(texts(3), { maxRetries: 2 }));
		expect(err.message).toContain("Embedding API error 400");
		expect(state.count).toBe(1);
		expect(err.status).toBe(400);
	});

	it("retries HTTP 429 and 503 until success", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(
			() => new Response("slow down", { status: 429 }),
			() => new Response("unavailable", { status: 503 }),
			ok,
		);
		expect(await client.embedBatch(texts(3), { maxRetries: 2 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(3);
	});

	it("retries a timed-out attempt", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(hang, ok);
		expect(await client.embedBatch(texts(3), { maxRetries: 1 })).toEqual([
			vec(0),
			vec(1),
			vec(2),
		]);
		expect(state.count).toBe(2);
	});

	it("does not retry by default (interactive path) and names the timeout", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(hang);
		const err = await errorOf(client.embedBatch(texts(3)));
		expect(err.message).toContain("TimeoutError");
		expect(err.message).toContain("chunk 1/1");
		expect(state.count).toBe(1);
	});

	it("does not retry a truncated response by default", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(
			() => okResponse([{ index: 0, embedding: vec(0) }]),
			ok,
		);
		await errorOf(client.embedBatch(texts(3)));
		expect(state.count).toBe(1);
	});

	it("rejects a response whose length differs from EMBEDDING_DIMENSIONS without retrying", async () => {
		config.embedding.dimensions = 4;
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(ok);
		const err = await errorOf(client.embedBatch(texts(3), { maxRetries: 2 }));
		expect(err.message).toContain("EMBEDDING_DIMENSIONS=4");
		expect(err.message).toContain("Unset EMBEDDING_DIMENSIONS");
		expect(err.retryable).toBe(false);
		expect(state.count).toBe(1);
	});

	it("rejects an empty embedding vector", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() => okResponse([{ index: 0, embedding: [] }]));
		const err = await errorOf(client.embedBatch(texts(1), { maxRetries: 0 }));
		expect(err.message).toContain("invalid embedding at position 0 (index 0)");
	});

	it("rejects a response without a data array", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() => Response.json({ object: "list" }));
		const err = await errorOf(client.embedBatch(texts(1), { maxRetries: 0 }));
		expect(err.message).toContain("no data array");
		expect(err.retryable).toBe(true);
	});

	it.each([
		["a string value", ["a"]],
		["a null value", [null]],
		["a null value next to numbers", [null, 1]],
	])("rejects an embedding containing %s", async (_label, embedding) => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() => okResponse([{ index: 0, embedding }]));
		const err = await errorOf(client.embedBatch(texts(1), { maxRetries: 0 }));
		expect(err.message).toContain("invalid embedding at position 0 (index 0)");
	});

	it("rejects a null item in the data array", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() => okResponse([null]));
		const err = await errorOf(client.embedBatch(texts(1), { maxRetries: 0 }));
		expect(err.message).toContain("invalid embedding at position 0 (index 0)");
	});

	it("rejects mixed vector lengths within one chunk", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: 0, embedding: [1, 2] },
				{ index: 1, embedding: [1] },
			]),
		);
		const err = await errorOf(client.embedBatch(texts(2), { maxRetries: 0 }));
		expect(err.message).toContain("inconsistent embedding lengths");
	});

	it("retries a chunk whose vector length differs from earlier chunks", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const state = stubFetch(ok, (inputs) =>
			okResponse(inputs.map((_, i) => ({ index: i, embedding: [1, 2] }))),
		);
		const err = await errorOf(client.embedBatch(texts(150), { maxRetries: 1 }));
		expect(err.message).toContain("earlier chunks had 3");
		expect(err.retryable).toBe(true);
		// Chunk 1 once, chunk 2 twice (initial attempt plus one retry).
		expect(state.count).toBe(3);
	});

	it("falls back to array position for non-integer index values", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(() =>
			okResponse([
				{ index: "0", embedding: vec(0) },
				{ index: 1.5, embedding: vec(1) },
			]),
		);
		expect(await client.embedBatch(texts(2))).toEqual([vec(0), vec(1)]);
	});

	it("names the failing chunk in a multi-chunk batch", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(ok, () => new Response("bad", { status: 400 }));
		const err = await errorOf(client.embedBatch(texts(150)));
		expect(err.message).toContain("chunk 2/2");
	});

	it("returns 150 embeddings in input order across two chunks", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		stubFetch(ok, ok);
		expect(await client.embedBatch(texts(150))).toEqual(
			texts(150).map((_, k) => vec(k)),
		);
	});

	it("logs the chunk number and attempt before retrying", async () => {
		const client = new EmbeddingClient({ timeoutMs: 50, retryBaseDelayMs: 0 });
		const write = spyOn(process.stdout, "write").mockImplementation(() => true);
		stubFetch(() => okResponse([{ index: 0, embedding: vec(0) }]), ok);
		await client.embedBatch(texts(3), { maxRetries: 1 });
		const output = write.mock.calls.map((c) => String(c[0])).join("");
		expect(output).toContain(
			"[embedding] Chunk 1/1 (3 texts) failed on attempt 1/2",
		);
	});
});

describe("activate with a mismatched EMBEDDING_DIMENSIONS", () => {
	const realFetch = globalThis.fetch;
	let savedDims: number | undefined;
	let tempDir: string;
	let db: KnowledgeDB;

	beforeEach(() => {
		savedDims = config.embedding.dimensions;
		tempDir = mkdtempSync(join(tmpdir(), "ks-embed-dims-test-"));
		db = new KnowledgeDB(join(tempDir, "test.db"));
	});

	afterEach(async () => {
		globalThis.fetch = realFetch;
		config.embedding.dimensions = savedDims;
		mock.restore();
		await db.close();
		rmSync(tempDir, { recursive: true, force: true });
	});

	it("rejects activation with an actionable message when the API returns another length", async () => {
		// The provider ignores `dimensions` and returns 3-dimensional vectors.
		config.embedding.dimensions = 4;
		globalThis.fetch = (async () =>
			Response.json({
				object: "list",
				data: [{ index: 0, embedding: [1, 2, 3] }],
			})) as typeof fetch;
		const activation = new ActivationEngine(db);
		await expect(activation.activate("x")).rejects.toThrow(
			"Embedding API returned 3-dimensional embeddings but EMBEDDING_DIMENSIONS=4. Unset EMBEDDING_DIMENSIONS or use a provider/model that honours the dimensions parameter.",
		);
	});
});

describe("isRetryableEmbeddingError", () => {
	it("retries AbortError and TimeoutError", () => {
		const abort = new Error("aborted");
		abort.name = "AbortError";
		const timeout = new Error("timed out");
		timeout.name = "TimeoutError";
		expect(isRetryableEmbeddingError(abort)).toBe(true);
		expect(isRetryableEmbeddingError(timeout)).toBe(true);
	});

	it("retries errors carrying a string code (e.g. ECONNRESET)", () => {
		expect(
			isRetryableEmbeddingError(
				Object.assign(new Error("socket reset"), { code: "ECONNRESET" }),
			),
		).toBe(true);
	});

	it("does not retry plain errors or non-error values", () => {
		expect(isRetryableEmbeddingError(new Error("boom"))).toBe(false);
		expect(isRetryableEmbeddingError("boom")).toBe(false);
	});
});

describe("retryDelayMs", () => {
	it("doubles from the base and caps at 30s", () => {
		expect(
			Array.from({ length: 7 }, (_, i) => retryDelayMs(i + 1, 1_000)),
		).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]);
	});

	it("stays capped for large attempt numbers", () => {
		expect(retryDelayMs(1_000, 1_000)).toBe(30_000);
	});
});
