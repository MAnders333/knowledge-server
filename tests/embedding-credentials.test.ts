/**
 * What the embedding client sends to a dedicated EMBEDDING_BASE_URL.
 *
 * The client reads the resolved key from config.embedding.apiKey (see
 * resolveScopedApiKey in config.ts). These tests point it at a local
 * capturing server and check the Authorization header on the wire.
 */
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
} from "bun:test";
import { EmbeddingClient } from "../src/activation/embeddings";
import { config } from "../src/config";

// `config` is typed readonly (as const); tests set the resolved key at runtime.
const mutableEmbedding = config.embedding as unknown as {
	baseURL: string;
	apiKey: string;
};

describe("EmbeddingClient credentials on a dedicated endpoint", () => {
	const seenAuth: Array<string | null> = [];
	let server: { port?: number; stop(force?: boolean): void };

	beforeAll(() => {
		server = Bun.serve({
			port: 0,
			hostname: "127.0.0.1",
			fetch(req) {
				seenAuth.push(req.headers.get("authorization"));
				return Response.json({ data: [{ index: 0, embedding: [1, 2, 3] }] });
			},
		});
	});

	afterAll(() => {
		server.stop(true);
	});

	function withEndpoint(
		apiKey: string,
		run: () => Promise<void>,
	): Promise<void> {
		const savedBase = mutableEmbedding.baseURL;
		const savedKey = mutableEmbedding.apiKey;
		mutableEmbedding.baseURL = `http://127.0.0.1:${server.port}/v1`;
		mutableEmbedding.apiKey = apiKey;
		return run().finally(() => {
			mutableEmbedding.baseURL = savedBase;
			mutableEmbedding.apiKey = savedKey;
		});
	}

	it("sends no Authorization header when no embedding key is resolved", async () => {
		seenAuth.length = 0;
		await withEndpoint("", async () => {
			const client = new EmbeddingClient({ retryBaseDelayMs: 1 });
			await client.embed("hello");
		});
		expect(seenAuth).toEqual([null]);
	});

	it("sends the resolved embedding key as a bearer token", async () => {
		seenAuth.length = 0;
		await withEndpoint("embed-only-key", async () => {
			const client = new EmbeddingClient({ retryBaseDelayMs: 1 });
			await client.embed("hello");
		});
		expect(seenAuth).toEqual(["Bearer embed-only-key"]);
	});
});

describe("EmbeddingClient key scope and transport", () => {
	const seenAuth: Array<string | null> = [];
	let server: { port: number; stop(force?: boolean): void };
	const embedding = config.embedding as unknown as {
		baseURL: string;
		apiKey: string;
	};
	const llm = config.llm as unknown as {
		baseEndpoint: string;
		apiKey: string;
		openai: { baseURL: string; apiKey: string };
	};
	let saved: {
		embedding: { baseURL: string; apiKey: string };
		llm: { baseEndpoint: string; apiKey: string };
		openai: { baseURL: string; apiKey: string };
	};

	beforeAll(() => {
		server = Bun.serve({
			port: 0,
			hostname: "127.0.0.1",
			fetch(req) {
				seenAuth.push(req.headers.get("authorization"));
				return Response.json({ data: [{ index: 0, embedding: [1, 2, 3] }] });
			},
		});
	});

	afterAll(() => {
		server.stop(true);
	});

	beforeEach(() => {
		seenAuth.length = 0;
		saved = {
			embedding: { baseURL: embedding.baseURL, apiKey: embedding.apiKey },
			llm: { baseEndpoint: llm.baseEndpoint, apiKey: llm.apiKey },
			openai: { ...llm.openai },
		};
		embedding.baseURL = "";
		embedding.apiKey = "";
	});

	afterEach(() => {
		Object.assign(embedding, saved.embedding);
		Object.assign(llm, saved.llm);
		Object.assign(llm.openai, saved.openai);
	});

	it("forwards LLM_API_KEY to an OPENAI_BASE_URL only when it is the LLM_BASE_ENDPOINT origin", async () => {
		llm.openai.baseURL = `http://127.0.0.1:${server.port}/v1`;
		llm.openai.apiKey = "";
		llm.apiKey = "llm-key";
		llm.baseEndpoint = "https://gateway.example.net";
		await new EmbeddingClient({ retryBaseDelayMs: 1 }).embed("hello");
		expect(seenAuth).toEqual([null]);

		llm.baseEndpoint = `http://127.0.0.1:${server.port}`;
		await new EmbeddingClient({ retryBaseDelayMs: 1 }).embed("hello");
		expect(seenAuth).toEqual([null, "Bearer llm-key"]);
	});

	it("refuses to send a key over plain http to a non-loopback host", async () => {
		embedding.baseURL = "http://embeddings.example.net/v1";
		embedding.apiKey = "embed-key";
		await expect(
			new EmbeddingClient({ retryBaseDelayMs: 1 }).embed("hello"),
		).rejects.toThrow("plain http");
		expect(seenAuth).toEqual([]);
	});
});
