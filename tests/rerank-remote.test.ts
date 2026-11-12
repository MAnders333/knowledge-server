/**
 * RemoteReranker against a stub gateway (tests/stub HTTP server, no network).
 *
 * Covered:
 *   - Happy path: one listwise call, scores returned in input order
 *   - Request shape: model, questions template, query tail, document heads
 *   - Malformed responses, partial scores, out-of-range probabilities
 *   - 503 error body is reported with its cause
 *   - Timeout aborts the request and rejects (caller falls back to dense)
 *   - Circuit breaker: opens after 3 failures, skips while open, one probe after cooldown
 *   - Candidate cap (40) and unconfigured gateway (no remote reranker)
 */
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
	spyOn,
} from "bun:test";
import {
	REMOTE_RERANK_MAX_DOCUMENTS,
	RemoteReranker,
	RerankSkippedError,
	createRemoteRerankerFromConfig,
} from "../src/activation/rerank-remote";
import { PLACEHOLDER_API_KEY, config } from "../src/config";
import { logger } from "../src/logger";

// Built at runtime so no secret-shaped literal lands in the source.
const API_KEY = ["stub", "gateway", String(process.pid)].join("-");

// Request bodies are inspected loosely: the test asserts on fields of the wire format.
// biome-ignore lint/suspicious/noExplicitAny: arbitrary JSON from the wire
type Json = Record<string, any>;

interface Recorded {
	path: string;
	auth: string | null;
	body: Json;
}

let server: { port: number; stop: (closeActiveConnections?: boolean) => void };
let requests: Recorded[];
let respond: (body: Json) => Response | Promise<Response>;

beforeAll(() => {
	server = Bun.serve({
		port: 0,
		hostname: "127.0.0.1",
		async fetch(req) {
			const body = (await req.json()) as Json;
			requests.push({
				path: new URL(req.url).pathname,
				auth: req.headers.get("authorization"),
				body,
			});
			return respond(body);
		},
	});
});

afterAll(() => {
	server.stop(true);
});

beforeEach(() => {
	requests = [];
	respond = () => completion({});
});

afterEach(() => {
	config.llm.baseEndpoint = "";
	config.llm.apiKey = "";
});

/** A gateway completion whose message content carries the per-question answers. */
function completion(
	answers: Record<string, number | string>,
	usage: { prompt_tokens: number; cost: number } = {
		prompt_tokens: 8200,
		cost: 0.00197,
	},
): Response {
	const content = Object.fromEntries(
		Object.entries(answers).map(([id, p]) => [id, { noul: p, type: "noul" }]),
	);
	return Response.json({
		model: "stub/clef",
		choices: [
			{ message: { role: "assistant", content: JSON.stringify(content) } },
		],
		usage,
	});
}

function makeReranker(
	opts: {
		timeoutMs?: number;
		maxDocTokens?: number;
		maxQueryTokens?: number;
		now?: () => number;
	} = {},
): RemoteReranker {
	return new RemoteReranker({
		baseEndpoint: `http://127.0.0.1:${server.port}`,
		apiKey: API_KEY,
		model: "clef",
		timeoutMs: 2_000,
		maxDocTokens: 256,
		maxQueryTokens: 128,
		...opts,
	});
}

describe("RemoteReranker request and response", () => {
	it("scores all candidates from one call, in input order", async () => {
		respond = () => completion({ D00: 0.12, D01: 0.91, D02: 0.5 });
		const scores = await makeReranker().scoreBatch("rotate the keys", [
			"alpha doc",
			"beta doc",
			"gamma doc",
		]);

		expect(scores).toEqual([0.12, 0.91, 0.5]);
		expect(requests).toHaveLength(1);
		expect(requests[0].path).toBe("/openai/v1/chat/completions");
		expect(requests[0].auth).toBe(`Bearer ${API_KEY}`);
	});

	it("sends the questions template with one Noul per document id", async () => {
		respond = () => completion({ D00: 0.5, D01: 0.5 });
		await makeReranker().scoreBatch("q", ["first", "second"]);

		const body = requests[0].body;
		expect(body.model).toBe("clef");
		expect(body.response_format.type).toBe("questions");
		const questions = body.response_format.questions;
		expect(Object.keys(questions)).toEqual(["D00", "D01"]);
		expect(questions.D01.type).toBe("noul");
		expect(questions.D01.instructions).toContain("`documents.D01`");
		expect(questions.D01.criteria.true).toContain("answers the query");
		expect(questions.D01.criteria.false).toContain("loosely related");

		const content = JSON.parse(body.messages[0].content);
		expect(content.query).toBe("q");
		expect(Object.keys(content.documents)).toEqual(["D00", "D01"]);
		expect(content.documents.D01).toEqual({ title: "", text: "second" });
	});

	it("keeps the query tail and the document head within the token caps", async () => {
		respond = () => completion({ D00: 0.5 });
		// 1 token = 4 characters in the remote backend.
		await makeReranker({ maxDocTokens: 2, maxQueryTokens: 1 }).scoreBatch(
			"0123456789",
			["abcdefghijklmnop"],
		);

		const content = JSON.parse(requests[0].body.messages[0].content);
		expect(content.query).toBe("6789");
		expect(content.documents.D00.text).toBe("abcdefgh");
	});

	it("accepts a code-fenced JSON message", async () => {
		respond = () =>
			Response.json({
				choices: [
					{
						message: {
							content: '```json\n{"D00": {"noul": 0.25, "type": "noul"}}\n```',
						},
					},
				],
			});
		expect(await makeReranker().scoreBatch("q", ["doc"])).toEqual([0.25]);
	});

	it("rejects a non-JSON message content", async () => {
		respond = () =>
			Response.json({
				choices: [{ message: { content: "I think doc one is best." } }],
			});
		await expect(makeReranker().scoreBatch("q", ["doc"])).rejects.toThrow(
			"not JSON",
		);
	});

	it("rejects a response without choices", async () => {
		respond = () => Response.json({ model: "stub/clef" });
		await expect(makeReranker().scoreBatch("q", ["doc"])).rejects.toThrow(
			"malformed gateway response",
		);
	});

	it("rejects a score outside [0, 1]", async () => {
		respond = () => completion({ D00: 1.7 });
		await expect(makeReranker().scoreBatch("q", ["doc"])).rejects.toThrow(
			"malformed gateway response",
		);
	});

	it("rejects partial scores instead of mixing them with dense order", async () => {
		respond = () => completion({ D00: 0.9, D01: 0.1 });
		await expect(
			makeReranker().scoreBatch("q", ["one", "two", "three"]),
		).rejects.toThrow("no score for D02");
	});

	it("reports a 503 with the cause from the error body", async () => {
		respond = () =>
			new Response(
				JSON.stringify({
					error: {
						message: "upstream StatusCode: TooManyRequests, retry later",
					},
				}),
				{ status: 503 },
			);
		await expect(makeReranker().scoreBatch("q", ["doc"])).rejects.toThrow(
			/HTTP 503.*TooManyRequests/,
		);
	});

	it("aborts at the timeout and rejects", async () => {
		// The gateway never answers. The client budget is a real timer
		// (AbortSignal.timeout), so this test necessarily runs on the platform clock.
		respond = () => new Promise<Response>(() => {});
		await expect(
			makeReranker({ timeoutMs: 100 }).scoreBatch("q", ["doc"]),
		).rejects.toThrow("timed out after 100ms");
	});

	it("refuses more candidates than one call carries, without sending", async () => {
		const docs = Array.from(
			{ length: REMOTE_RERANK_MAX_DOCUMENTS + 1 },
			(_, i) => `doc ${i}`,
		);
		await expect(makeReranker().scoreBatch("q", docs)).rejects.toThrow(
			"at most 40",
		);
		expect(requests).toHaveLength(0);
	});

	it("returns an empty result for no documents without a request", async () => {
		expect(await makeReranker().scoreBatch("q", [])).toEqual([]);
		expect(requests).toHaveLength(0);
	});

	it("exposes provider and model as its identity", () => {
		expect(makeReranker().identity).toEqual({
			provider: "remote",
			model: "clef",
		});
	});
});

describe("RemoteReranker circuit breaker", () => {
	it("opens after 3 failures, skips while open, probes once after the cooldown", async () => {
		let clock = 1_000_000;
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		const log = spyOn(logger, "log").mockImplementation(() => {});
		try {
			respond = () => new Response("unavailable", { status: 503 });
			const reranker = makeReranker({ now: () => clock });

			for (let i = 0; i < 3; i++) {
				await expect(reranker.scoreBatch("q", ["doc"])).rejects.toThrow(
					"HTTP 503",
				);
			}
			expect(requests).toHaveLength(3);
			expect(warn).toHaveBeenCalledTimes(1);

			// Open: no request leaves the process.
			await expect(reranker.scoreBatch("q", ["doc"])).rejects.toBeInstanceOf(
				RerankSkippedError,
			);
			expect(requests).toHaveLength(3);

			// Cooldown over: one probe goes out; it fails and the breaker stays open quietly.
			clock += 60_000;
			await expect(reranker.scoreBatch("q", ["doc"])).rejects.toThrow(
				"HTTP 503",
			);
			expect(requests).toHaveLength(4);
			expect(warn).toHaveBeenCalledTimes(1);
			await expect(reranker.scoreBatch("q", ["doc"])).rejects.toBeInstanceOf(
				RerankSkippedError,
			);
			expect(requests).toHaveLength(4);

			// Next probe succeeds: breaker closes, one recovery log, calls flow again.
			clock += 60_000;
			respond = () => completion({ D00: 0.3 });
			expect(await reranker.scoreBatch("q", ["doc"])).toEqual([0.3]);
			expect(await reranker.scoreBatch("q", ["doc"])).toEqual([0.3]);
			expect(requests).toHaveLength(6);
			const recovered = log.mock.calls.filter((args) =>
				String(args[0]).includes("recovered"),
			);
			expect(recovered).toHaveLength(1);
			expect(warn).toHaveBeenCalledTimes(1);
		} finally {
			warn.mockRestore();
			log.mockRestore();
		}
	});

	it("does not open on fewer than 3 consecutive failures", async () => {
		const warn = spyOn(logger, "warn").mockImplementation(() => {});
		try {
			const reranker = makeReranker();
			respond = () => new Response("unavailable", { status: 503 });
			await expect(reranker.scoreBatch("q", ["doc"])).rejects.toThrow();
			await expect(reranker.scoreBatch("q", ["doc"])).rejects.toThrow();
			respond = () => completion({ D00: 0.4 });
			expect(await reranker.scoreBatch("q", ["doc"])).toEqual([0.4]);
			expect(warn).not.toHaveBeenCalled();
		} finally {
			warn.mockRestore();
		}
	});
});

describe("createRemoteRerankerFromConfig", () => {
	it("returns null when the gateway endpoint or key is missing or a placeholder", () => {
		config.llm.baseEndpoint = "";
		config.llm.apiKey = "key";
		expect(createRemoteRerankerFromConfig()).toBeNull();

		config.llm.baseEndpoint = `http://127.0.0.1:${server.port}`;
		config.llm.apiKey = "";
		expect(createRemoteRerankerFromConfig()).toBeNull();

		config.llm.apiKey = PLACEHOLDER_API_KEY;
		expect(createRemoteRerankerFromConfig()).toBeNull();
	});

	it("builds a reranker from the unified gateway settings", () => {
		config.llm.baseEndpoint = `http://127.0.0.1:${server.port}/`;
		config.llm.apiKey = API_KEY;
		const reranker = createRemoteRerankerFromConfig();
		expect(reranker?.identity.provider).toBe("remote");
	});
});
