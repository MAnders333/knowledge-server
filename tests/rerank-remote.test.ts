/**
 * RemoteReranker against a stub System One endpoint (local Bun.serve, no network).
 *
 * Covered:
 *   - Request shape: state (query tail, document heads), one Noul per id, id order
 *   - Happy path, resolved model in the log, partial and malformed answers, out-of-range scores
 *   - Errors: 400 max_tokens_exceeded (string and object detail), 401, 422 (no echo of
 *     rejected input), 429 with and without Retry-After, 529, timeout
 *   - Circuit breaker: opens, skips, probe, recovery, failed probe restarts cooldown
 *   - Key scoping and configuration: createRemoteRerankerFromConfig, validateConfig gating
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
	RemoteReranker,
	RerankSkippedError,
	createRemoteRerankerFromConfig,
	parseRetryAfterMs,
} from "../src/activation/rerank-remote";
import {
	PLACEHOLDER_API_KEY,
	PLACEHOLDER_ENDPOINT,
	REMOTE_RERANK_MAX_CANDIDATES,
	REMOTE_RERANK_REQUEST_CHAR_BUDGET,
	config,
	validateConfig,
} from "../src/config";
import { logger } from "../src/logger";

// Built at runtime so no secret-shaped literal lands in the source.
const API_KEY = ["stub", "gateway", String(process.pid)].join("-");
const MODEL = "jev-1.14.0";
const ROUTE = "/decisions/v1/systemone";

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
	respond = () => systemOne({});
});

/** A System One response: one noul answer per id, plus the model and usage. */
function systemOne(
	answers: Record<string, unknown>,
	model: string | null = MODEL,
): Response {
	return Response.json({
		...(model === null ? {} : { model }),
		answers: Object.fromEntries(
			Object.entries(answers).map(([id, noul]) => [id, { type: "noul", noul }]),
		),
		usage: { input_tokens: 8200, output_tokens: 12 },
	});
}

function makeReranker(
	opts: {
		url?: string;
		timeoutMs?: number;
		maxDocTokens?: number;
		maxQueryTokens?: number;
		now?: () => number;
	} = {},
): RemoteReranker {
	return new RemoteReranker({
		url: `http://127.0.0.1:${server.port}${ROUTE}`,
		apiKey: API_KEY,
		model: MODEL,
		timeoutMs: 2_000,
		maxDocTokens: 256,
		maxQueryTokens: 128,
		...opts,
	});
}

describe("RemoteReranker request", () => {
	it("posts to the System One route with the bearer key, once per call", async () => {
		respond = () => systemOne({ D00: 0.12, D01: 0.91, D02: 0.5 });
		const scores = await makeReranker().scoreBatch("rotate the keys", [
			"alpha doc",
			"beta doc",
			"gamma doc",
		]);

		expect(scores).toEqual([0.12, 0.91, 0.5]);
		expect(requests).toHaveLength(1);
		expect(requests[0].path).toBe(ROUTE);
		expect(requests[0].auth).toBe(`Bearer ${API_KEY}`);
	});

	it("sends the model, a state with the query and documents, and one noul question per id", async () => {
		respond = () => systemOne({ D00: 0.5, D01: 0.5 });
		await makeReranker().scoreBatch("q", ["first", "second"]);

		const body = requests[0].body;
		expect(body.model).toBe(MODEL);
		expect(Object.keys(body.state.documents)).toEqual(["D00", "D01"]);
		expect(Object.keys(body.questions)).toEqual(["D00", "D01"]);
		expect(body.questions.D01).toEqual({
			type: "noul",
			instructions:
				"Document `documents.D01` is relevant to `query`: it contains information that answers or directly addresses it.",
			criteria: {
				true: "The document contains information that answers the query or directly addresses what it asks about.",
				false:
					"The document is only loosely related, on a similar topic, or does not address what the query asks.",
			},
		});
		expect(body.state.query).toBe("q");
	});

	it("keeps candidate ids in the order given, so D00 is the first document", async () => {
		respond = () => systemOne({ D00: 0.9, D01: 0.1 });
		await makeReranker().scoreBatch("q", ["best dense", "second dense"]);

		expect(requests[0].body.state.documents.D00.text).toBe("best dense");
		expect(requests[0].body.state.documents.D01.text).toBe("second dense");
	});

	it("sends the last maxQueryTokens x 4 characters of the query and the first maxDocTokens x 4 of each document", async () => {
		respond = () => systemOne({ D00: 0.5 });
		await makeReranker({ maxDocTokens: 2, maxQueryTokens: 2 }).scoreBatch(
			"0123456789abcdef",
			["abcdefghijklmnop"],
		);

		expect(requests[0].body.state.query).toBe("89abcdef");
		expect(requests[0].body.state.documents.D00.text).toBe("abcdefgh");
	});

	it("never splits a surrogate pair at the query or document cut", async () => {
		respond = () => systemOne({ D00: 0.5 });
		// Head cut: 8 characters would end on the high surrogate of the emoji, so 7 are sent.
		// Tail cut: the last 8 characters start on the low surrogate, so the tail begins after it.
		await makeReranker({ maxDocTokens: 2, maxQueryTokens: 2 }).scoreBatch(
			"abc😀defghij",
			["aaaaaaa😀"],
		);

		expect(requests[0].body.state.query).toBe("defghij");
		expect(requests[0].body.state.documents.D00.text).toBe("aaaaaaa");
	});

	it("returns no scores and makes no call for an empty candidate list", async () => {
		expect(await makeReranker().scoreBatch("q", [])).toEqual([]);
		expect(requests).toHaveLength(0);
	});

	it("rejects more than the candidate cap without calling the endpoint", async () => {
		const docs = Array.from(
			{ length: REMOTE_RERANK_MAX_CANDIDATES + 1 },
			(_, i) => `doc ${i}`,
		);
		await expect(makeReranker().scoreBatch("q", docs)).rejects.toThrow(
			/at most 40 candidates per call/,
		);
		expect(requests).toHaveLength(0);
	});
});

describe("RemoteReranker response", () => {
	it("ignores extra noul answers for ids it did not ask about", async () => {
		respond = () => systemOne({ D00: 0.3, D09: 0.99 });
		expect(await makeReranker().scoreBatch("q", ["a"])).toEqual([0.3]);
	});

	it("logs the model the endpoint resolved next to the configured one", async () => {
		respond = () => systemOne({ D00: 0.2, D01: 0.8 }, "jev-1.14.1");
		const log = spyOn(logger, "log").mockImplementation(() => {});
		try {
			expect(await makeReranker().scoreBatch("q", ["a", "b"])).toEqual([
				0.2, 0.8,
			]);
			expect(log).toHaveBeenCalledWith(
				expect.stringContaining("model=jev-1.14.0 resolved_model=jev-1.14.1"),
			);
		} finally {
			log.mockRestore();
		}
	});

	it("fails the whole batch when an id has no answer", async () => {
		respond = () => systemOne({ D00: 0.3 });
		await expect(makeReranker().scoreBatch("q", ["a", "b"])).rejects.toThrow(
			"gateway response has no answer for D01",
		);
	});

	it("fails the whole batch when one answer is not a noul", async () => {
		respond = () =>
			Response.json({
				model: MODEL,
				answers: {
					D00: { type: "noul", noul: 0.3 },
					D01: { type: "choice", choice: "a", confidence: 0.5 },
				},
				usage: { input_tokens: 1, output_tokens: 1 },
			});
		await expect(makeReranker().scoreBatch("q", ["a", "b"])).rejects.toThrow(
			/malformed gateway response/,
		);
	});

	it("rejects a probability above 1", async () => {
		respond = () => systemOne({ D00: 1.5 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			/malformed gateway response/,
		);
	});

	it("rejects a negative probability", async () => {
		respond = () => systemOne({ D00: -0.1 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			/malformed gateway response/,
		);
	});

	it("rejects a null score (what a non-finite number serializes to)", async () => {
		respond = () => systemOne({ D00: null });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			/malformed gateway response/,
		);
	});

	it("rejects a body that is not JSON", async () => {
		respond = () => new Response("<html>gateway page</html>", { status: 200 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"body is not JSON",
		);
	});
});

describe("RemoteReranker errors", () => {
	it("reports a 400 max_tokens_exceeded with its string detail, without making a retry", async () => {
		respond = () =>
			Response.json(
				{ detail: "max_tokens_exceeded: request is longer than the context" },
				{ status: 400 },
			);
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 400: max_tokens_exceeded: request is longer than the context",
		);
		expect(requests).toHaveLength(1);
	});

	it("reduces an object detail to its error type or code, dropping free text", async () => {
		respond = () =>
			Response.json(
				{
					detail: {
						type: "max_tokens_exceeded",
						message: "SECRET-ECHO the request was 61234 tokens",
					},
				},
				{ status: 400 },
			);
		const err = await makeReranker()
			.scoreBatch("q", ["a"])
			.catch((e: unknown) => e);

		const message = (err as Error).message;
		expect(message).toBe(
			"gateway returned HTTP 400 (error: max_tokens_exceeded)",
		);
		expect(message).not.toContain("SECRET-ECHO");
		expect(message).not.toContain("{");
	});

	it("keeps validation locations and messages from a 422, but not the rejected input they echo", async () => {
		respond = () =>
			Response.json(
				{
					detail: [
						{
							loc: ["body", "questions", "D01", "type"],
							msg: "Input should be 'noul'",
							type: "literal_error",
							input: "SECRET-CANDIDATE-TEXT",
						},
					],
				},
				{ status: 422 },
			);
		const err = await makeReranker()
			.scoreBatch("q", ["a", "b"])
			.catch((e: unknown) => e);

		expect(err).toBeInstanceOf(Error);
		const message = (err as Error).message;
		expect(message).toContain("HTTP 422");
		expect(message).toContain(
			"body.questions.D01.type: Input should be 'noul'",
		);
		expect(message).not.toContain("SECRET-CANDIDATE-TEXT");
	});

	it("reports a 401 and does not retry", async () => {
		respond = () =>
			Response.json({ detail: "Invalid API key" }, { status: 401 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 401: Invalid API key",
		);
		expect(requests).toHaveLength(1);
	});

	it("429 without Retry-After fails fast", async () => {
		respond = () => new Response("slow down", { status: 429 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 429",
		);
		expect(requests).toHaveLength(1);
	});

	it("429 with a Retry-After that fits the budget retries once and succeeds", async () => {
		let calls = 0;
		respond = () => {
			calls++;
			return calls === 1
				? new Response("slow down", {
						status: 429,
						headers: { "retry-after": "0" },
					})
				: systemOne({ D00: 0.7 });
		};
		expect(await makeReranker().scoreBatch("q", ["a"])).toEqual([0.7]);
		expect(requests).toHaveLength(2);
	});

	it("429 whose Retry-After exceeds the remaining budget fails fast", async () => {
		respond = () =>
			new Response("slow down", {
				status: 429,
				headers: { "retry-after": "60" },
			});
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 429",
		);
		expect(requests).toHaveLength(1);
	});

	it("a second 429 after the retry fails the batch", async () => {
		respond = () =>
			new Response("slow down", {
				status: 429,
				headers: { "retry-after": "0" },
			});
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 429",
		);
		expect(requests).toHaveLength(2);
	});

	it("529 Overloaded fails the batch without a retry", async () => {
		respond = () => Response.json({ detail: "Overloaded" }, { status: 529 });
		await expect(makeReranker().scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 529: Overloaded",
		);
		expect(requests).toHaveLength(1);
	});

	it("aborts a call that outlives the budget", async () => {
		// Real clock on purpose: the abort comes from AbortSignal.timeout, which fake
		// timers cannot drive. The stub answers after the budget has run out.
		respond = async () => {
			await Bun.sleep(500);
			return systemOne({ D00: 0.5 });
		};
		await expect(
			makeReranker({ timeoutMs: 100 }).scoreBatch("q", ["a"]),
		).rejects.toThrow("timed out after 100ms");
	});
});

describe("RemoteReranker parseRetryAfterMs", () => {
	const now = 1_700_000_000_000;

	it("reads delay-seconds, including fractions", () => {
		expect(parseRetryAfterMs("2", now)).toBe(2_000);
		expect(parseRetryAfterMs("1.5", now)).toBe(1_500);
		expect(parseRetryAfterMs("0", now)).toBe(0);
	});

	it("reads an HTTP-date relative to now, never negative", () => {
		expect(parseRetryAfterMs(new Date(now + 5_000).toUTCString(), now)).toBe(
			5_000,
		);
		expect(parseRetryAfterMs(new Date(now - 5_000).toUTCString(), now)).toBe(0);
	});

	it("returns null when absent or unreadable", () => {
		expect(parseRetryAfterMs(null, now)).toBeNull();
		expect(parseRetryAfterMs("soon", now)).toBeNull();
	});
});

describe("RemoteReranker circuit breaker", () => {
	it("opens after 3 failures, skips while open, probes after the cooldown, and recovers", async () => {
		let clock = 0;
		const reranker = makeReranker({ now: () => clock });
		respond = () => new Response("boom", { status: 500 });

		for (let i = 0; i < 3; i++) {
			await expect(reranker.scoreBatch("q", ["a"])).rejects.toThrow(
				"gateway returned HTTP 500",
			);
		}
		expect(requests).toHaveLength(3);

		await expect(reranker.scoreBatch("q", ["a"])).rejects.toBeInstanceOf(
			RerankSkippedError,
		);
		expect(requests).toHaveLength(3);

		clock = 60_000;
		respond = () => systemOne({ D00: 0.6 });
		expect(await reranker.scoreBatch("q", ["a"])).toEqual([0.6]);
		expect(requests).toHaveLength(4);
		expect(await reranker.scoreBatch("q", ["a"])).toEqual([0.6]);
		expect(requests).toHaveLength(5);
	});

	it("a failed probe restarts the cooldown", async () => {
		let clock = 0;
		const reranker = makeReranker({ now: () => clock });
		respond = () => new Response("boom", { status: 500 });
		for (let i = 0; i < 3; i++) {
			await reranker.scoreBatch("q", ["a"]).catch(() => undefined);
		}

		clock = 60_000;
		await expect(reranker.scoreBatch("q", ["a"])).rejects.toThrow(
			"gateway returned HTTP 500",
		);
		expect(requests).toHaveLength(4);

		clock = 119_999;
		await expect(reranker.scoreBatch("q", ["a"])).rejects.toBeInstanceOf(
			RerankSkippedError,
		);
		expect(requests).toHaveLength(4);

		clock = 120_000;
		respond = () => systemOne({ D00: 0.2 });
		expect(await reranker.scoreBatch("q", ["a"])).toEqual([0.2]);
	});
});

describe("createRemoteRerankerFromConfig", () => {
	const mutable = config.activation.rerank.remote as unknown as {
		url: string;
		apiKey: string;
		model: string;
	};
	const rerank = config.activation.rerank as unknown as { provider: string };
	const saved = { ...mutable };
	const savedProvider = rerank.provider;

	beforeEach(() => {
		rerank.provider = "remote";
	});

	afterEach(() => {
		Object.assign(mutable, saved);
		rerank.provider = savedProvider;
	});

	it("is null without a URL, without a key, or with template placeholders", () => {
		mutable.url = "";
		mutable.apiKey = API_KEY;
		expect(createRemoteRerankerFromConfig()).toBeNull();

		mutable.url = `http://127.0.0.1:${server.port}${ROUTE}`;
		mutable.apiKey = "";
		expect(createRemoteRerankerFromConfig()).toBeNull();

		mutable.url = `${PLACEHOLDER_ENDPOINT}/decisions/v1/systemone`;
		mutable.apiKey = API_KEY;
		expect(createRemoteRerankerFromConfig()).toBeNull();

		mutable.url = `http://127.0.0.1:${server.port}${ROUTE}`;
		mutable.apiKey = PLACEHOLDER_API_KEY;
		expect(createRemoteRerankerFromConfig()).toBeNull();
	});

	it("builds a reranker on the configured URL and model", async () => {
		mutable.url = `http://127.0.0.1:${server.port}${ROUTE}`;
		mutable.apiKey = API_KEY;
		mutable.model = MODEL;
		const reranker = createRemoteRerankerFromConfig();
		expect(reranker?.identity).toEqual({ provider: "remote", model: MODEL });

		respond = () => systemOne({ D00: 0.33 });
		expect(await (reranker as RemoteReranker).scoreBatch("q", ["a"])).toEqual([
			0.33,
		]);
		expect(requests[0].path).toBe(ROUTE);
	});
});

describe("validateConfig: remote reranker", () => {
	const mutable = config.activation.rerank as unknown as {
		enabled: boolean;
		provider: "local" | "remote";
		candidates: number;
		maxDocTokens: number;
		maxQueryTokens: number;
		remote: { url: string; apiKey: string };
	};
	const saved = {
		enabled: mutable.enabled,
		provider: mutable.provider,
		candidates: mutable.candidates,
		maxDocTokens: mutable.maxDocTokens,
		maxQueryTokens: mutable.maxQueryTokens,
		remote: { ...mutable.remote },
	};
	const envNames = [
		"RERANK_ENABLED",
		"RERANK_CANDIDATES",
		"RERANK_REMOTE_URL",
		"RERANK_REMOTE_API_KEY",
		"LLM_BASE_ENDPOINT",
		"LLM_API_KEY",
	] as const;
	const savedEnv: Record<string, string | undefined> = {};

	beforeEach(() => {
		for (const name of envNames) savedEnv[name] = process.env[name];
		for (const name of envNames) delete process.env[name];
		mutable.enabled = true;
		mutable.provider = "remote";
		mutable.candidates = 16;
		mutable.maxDocTokens = 256;
		mutable.maxQueryTokens = 128;
		mutable.remote.url = "https://gateway.example.org/decisions/v1/systemone";
		mutable.remote.apiKey = API_KEY;
	});

	afterEach(() => {
		Object.assign(mutable, { ...saved, remote: { ...saved.remote } });
		for (const name of envNames) {
			if (savedEnv[name] === undefined) delete process.env[name];
			else process.env[name] = savedEnv[name];
		}
	});

	/** Remote-reranker errors only; other config checks are out of scope here. */
	function remoteErrors(): string[] {
		return validateConfig().filter((e) =>
			/remote|RERANK_CANDIDATES|RERANK_REMOTE/.test(e),
		);
	}

	it("passes the default layout: gateway route, same origin, no key needed", () => {
		process.env.LLM_BASE_ENDPOINT = "https://gateway.example.org";
		process.env.LLM_API_KEY = API_KEY;
		expect(remoteErrors()).toEqual([]);
	});

	it("accepts a direct TypeSafe URL with its own key and no gateway at all", () => {
		process.env.RERANK_REMOTE_URL = "https://api.typesafe.ai/v1/systemone";
		process.env.RERANK_REMOTE_API_KEY = API_KEY;
		mutable.remote.url = "https://api.typesafe.ai/v1/systemone";
		mutable.remote.apiKey = API_KEY;
		expect(remoteErrors()).toEqual([]);
	});

	it("does not check remote settings when the endpoint key is missing, so the reranker falls back to local", () => {
		// An explicit URL on another origin without RERANK_REMOTE_API_KEY resolves to
		// no key (LLM_API_KEY never leaves its origin): the remote reranker is inactive.
		process.env.LLM_BASE_ENDPOINT = "https://gateway.example.org";
		process.env.LLM_API_KEY = API_KEY;
		process.env.RERANK_REMOTE_URL = "https://api.typesafe.ai/v1/systemone";
		mutable.remote.url = "https://api.typesafe.ai/v1/systemone";
		mutable.remote.apiKey = "";
		mutable.candidates = REMOTE_RERANK_MAX_CANDIDATES + 10;
		expect(remoteErrors()).toEqual([]);
	});

	it("does not check remote settings when reranking is disabled", () => {
		process.env.RERANK_ENABLED = "false";
		process.env.RERANK_CANDIDATES = "50";
		mutable.enabled = false;
		mutable.candidates = 50;
		expect(remoteErrors()).toEqual([]);
	});

	it("warns on plain http to a non-loopback host, not on loopback", () => {
		const warn = spyOn(console, "warn").mockImplementation(() => {});
		try {
			mutable.remote.url = "http://models.example.org/decisions/v1/systemone";
			expect(remoteErrors()).toEqual([]);
			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining("plain http on a non-loopback host"),
			);
			warn.mockClear();
			mutable.remote.url = "http://127.0.0.1:8080/decisions/v1/systemone";
			expect(remoteErrors()).toEqual([]);
			expect(warn).not.toHaveBeenCalled();
		} finally {
			warn.mockRestore();
		}
	});

	it("rejects a URL that is not absolute http(s)", () => {
		mutable.remote.url = "gateway.example.org/systemone";
		expect(remoteErrors()).toEqual([
			expect.stringContaining("must be an absolute http or https URL"),
		]);
	});

	it("rejects more candidates than one call can carry", () => {
		mutable.candidates = REMOTE_RERANK_MAX_CANDIDATES + 1;
		expect(remoteErrors()).toEqual([
			expect.stringContaining(`at most ${REMOTE_RERANK_MAX_CANDIDATES}`),
		]);
	});

	it("rejects a request that could exceed the character budget, and accepts the defaults", () => {
		expect(remoteErrors()).toEqual([]);

		mutable.candidates = REMOTE_RERANK_MAX_CANDIDATES;
		mutable.maxDocTokens = 1024;
		expect(remoteErrors()).toEqual([
			expect.stringContaining(
				`above the ${REMOTE_RERANK_REQUEST_CHAR_BUDGET}-character budget`,
			),
		]);
	});

	it("checks nothing when the local provider is active", () => {
		mutable.provider = "local";
		mutable.candidates = REMOTE_RERANK_MAX_CANDIDATES + 1;
		mutable.remote.url = "not a url";
		process.env.RERANK_REMOTE_URL = "https://api.typesafe.ai/v1/systemone";
		expect(remoteErrors()).toEqual([]);
	});
});

describe("default candidate pool", () => {
	/** Candidate count read in a child process, so the config singleton loads fresh. */
	function candidatesWith(env: Record<string, string>): number {
		const childEnv = Object.fromEntries(
			Object.entries(process.env).filter(
				([name]) => name !== "RERANK_CANDIDATES" && name !== "RERANK_PROVIDER",
			),
		);
		const configPath = new URL("../src/config.ts", import.meta.url).pathname;
		const child = Bun.spawnSync({
			cmd: [
				process.execPath,
				"-e",
				`import { config } from ${JSON.stringify(configPath)}; console.log(config.activation.rerank.candidates);`,
			],
			env: { ...childEnv, ...env },
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(child.exitCode).toBe(0);
		return Number(child.stdout.toString().trim().split("\n").pop());
	}

	it("is 30 for the remote reranker when RERANK_CANDIDATES is unset", () => {
		expect(candidatesWith({ RERANK_PROVIDER: "remote" })).toBe(30);
	});

	it("keeps an explicit RERANK_CANDIDATES for the remote reranker", () => {
		expect(
			candidatesWith({ RERANK_PROVIDER: "remote", RERANK_CANDIDATES: "16" }),
		).toBe(16);
	});

	it("stays 16 for the local reranker", () => {
		expect(candidatesWith({})).toBe(16);
	});
});
