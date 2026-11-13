/**
 * LocalReranker scoring and model-fetch behaviour (src/activation/rerank.ts).
 *
 * Stub tests: a synthetic WordPiece tokenizer.json is written into a temp
 * models dir and InferenceSession.create is spied to return a fake session
 * whose run() busy-waits like a synchronous WASM inference. The real scoring
 * loop, deadline, yielding and download code run unchanged.
 *
 * Real-model tests run only when the default model is cached locally (they
 * also verify the pinned checksums load). The tick and deadline checks use
 * the platform clock on purpose: they measure the real event loop, which a
 * fake timer cannot model.
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
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as ort from "onnxruntime-web";
import { LocalReranker } from "../src/activation/rerank";
import { config } from "../src/config";

const SYNTH_MODEL = "test-org/synthetic-reranker"; // not pinned → no checksum
const DEFAULT_MODEL = "Xenova/ms-marco-MiniLM-L-6-v2"; // pinned
const REAL_MODELS_DIR = config.activation.rerank.modelsDir;
const HAS_REAL_MODEL = existsSync(
	join(REAL_MODELS_DIR, DEFAULT_MODEL, "onnx", "model_quantized.onnx"),
);

// ── Synthetic vocab: w0..w999 are single tokens (id = 10 + index) ────────────

const VOCAB_SIZE = 1000;

function syntheticTokenizerJson() {
	const vocab: Record<string, number> = {
		"[PAD]": 0,
		"[UNK]": 1,
		"[CLS]": 2,
		"[SEP]": 3,
	};
	for (let i = 0; i < VOCAB_SIZE; i++) vocab[`w${i}`] = 10 + i;
	return {
		normalizer: {
			type: "BertNormalizer",
			clean_text: true,
			handle_chinese_chars: true,
			strip_accents: null,
			lowercase: true,
		},
		model: { type: "WordPiece", unk_token: "[UNK]", vocab },
	};
}

/** `n` words w<start>…w<start+n-1>; one token each. */
function words(n: number, start = 0): string {
	return Array.from({ length: n }, (_, i) => `w${start + i}`).join(" ");
}

// ── Fake ORT session ─────────────────────────────────────────────────────────

type Feed = { data: BigInt64Array; dims: readonly number[] };
type Feeds = Record<string, Feed>;

function busyWait(ms: number): void {
	const end = performance.now() + ms;
	while (performance.now() < end) {
		// spin: a synchronous WASM run blocks the event loop the same way
	}
}

/** Logit = (document-side tokens − 1) / 100, so scores depend on the document. */
function fakeSession(busyMs: number, onRun: () => void = () => {}) {
	const runs: Feeds[] = [];
	return {
		runs,
		async run(feeds: Feeds) {
			runs.push(feeds);
			onRun();
			busyWait(busyMs);
			const { dims, data: types } = feeds.token_type_ids;
			let docSide = 0;
			for (let i = 0; i < dims[1]; i++) if (types[i] === 1n) docSide++;
			return { logits: { data: new Float32Array([(docSide - 1) / 100]) } };
		},
	};
}

async function useFakeSession(busyMs = 0, onRun?: () => void) {
	const session = fakeSession(busyMs, onRun);
	spyOn(ort.InferenceSession, "create").mockImplementation(
		async () => session as unknown as ort.InferenceSession,
	);
	return session;
}

// ── Stub-session suite ───────────────────────────────────────────────────────

describe("LocalReranker (stub session)", () => {
	let modelsDir: string;
	let savedModelsDir: string;

	beforeEach(() => {
		savedModelsDir = config.activation.rerank.modelsDir;
		modelsDir = mkdtempSync(join(tmpdir(), "ks-rerank-test-"));
		config.activation.rerank.modelsDir = modelsDir;
	});

	afterEach(() => {
		mock.restore();
		config.activation.rerank.modelsDir = savedModelsDir;
		rmSync(modelsDir, { recursive: true, force: true });
	});

	function installSyntheticModel() {
		const dir = join(modelsDir, SYNTH_MODEL);
		mkdirSync(join(dir, "onnx"), { recursive: true });
		writeFileSync(
			join(dir, "tokenizer.json"),
			JSON.stringify(syntheticTokenizerJson()),
		);
		writeFileSync(
			join(dir, "onnx", "model_quantized.onnx"),
			"fake model bytes",
		);
	}

	function synthReranker(timeoutMs = 10_000, downloadTimeoutMs?: number) {
		return new LocalReranker({
			model: SYNTH_MODEL,
			timeoutMs,
			downloadTimeoutMs,
		});
	}

	it("scores one pair per session run, in input order", async () => {
		installSyntheticModel();
		const session = await useFakeSession();
		const docs = [1, 2, 3, 4, 5].map((k) => words(k * 5, 100));
		const scores = await synthReranker().scoreBatch(words(4), docs);

		expect(session.runs).toHaveLength(5);
		for (const run of session.runs) expect(run.input_ids.dims[0]).toBe(1);
		expect(scores).toHaveLength(5);
		for (let i = 1; i < scores.length; i++) {
			expect(scores[i]).toBeGreaterThan(scores[i - 1]);
		}
	});

	it("yields to the event loop between runs (ticks observed between consecutive runs)", async () => {
		installSyntheticModel();
		let ticks = 0;
		const ticksAtRun: number[] = [];
		await useFakeSession(20, () => ticksAtRun.push(ticks));
		const docs = Array.from({ length: 8 }, (_, i) => words(10, i * 10));

		let maxGap = 0;
		let last = performance.now();
		const iv = setInterval(() => {
			const now = performance.now();
			maxGap = Math.max(maxGap, now - last);
			last = now;
			ticks++;
		}, 5);
		try {
			await synthReranker(5_000).scoreBatch(words(4), docs);
		} finally {
			clearInterval(iv);
		}
		expect(ticksAtRun).toHaveLength(8);
		expect(ticksAtRun[7] - ticksAtRun[0]).toBeGreaterThanOrEqual(4);
		expect(maxGap).toBeLessThan(100);
	});

	it("bounds the call by RERANK_TIMEOUT_MS and stops scoring at the deadline", async () => {
		installSyntheticModel();
		const session = await useFakeSession(30);
		const docs = Array.from({ length: 40 }, (_, i) => words(20, i * 20));

		const started = performance.now();
		await expect(synthReranker(200).scoreBatch(words(4), docs)).rejects.toThrow(
			/timeout after 200ms/,
		);
		expect(performance.now() - started).toBeLessThan(500);
		expect(session.runs.length).toBeLessThan(docs.length);
	});

	it("C3: a query longer than the model window still lets documents change the score", async () => {
		installSyntheticModel();
		await useFakeSession();
		const docs = [words(5, 0), words(20, 200), words(50, 400)];
		const scores = await synthReranker().scoreBatch(words(600, 0), docs);
		expect(new Set(scores).size).toBe(3);
	});

	it("keeps the last 64 query tokens and the first 128 document tokens", async () => {
		installSyntheticModel();
		const session = await useFakeSession();
		await synthReranker().scoreBatch(words(600, 0), [words(300, 500)]);

		const ids = Array.from(session.runs[0].input_ids.data, Number);
		const queryTail = Array.from({ length: 64 }, (_, i) => 10 + 536 + i);
		expect(ids.slice(1, 65)).toEqual(queryTail);
		expect(ids[65]).toBe(3); // [SEP]
		expect(ids[66]).toBe(10 + 500); // document starts at its first word
		expect(ids[66 + 128]).toBe(3); // [SEP] after 128 document tokens
		expect(ids).toHaveLength(66 + 128 + 1);
	});

	it("downloads artifacts through a temp file and renames them into place", async () => {
		await useFakeSession();
		const fetchSpy = spyOn(globalThis, "fetch").mockImplementation((async (
			input: RequestInfo | URL,
		) => {
			const url = String(input);
			return new Response(
				url.endsWith("tokenizer.json")
					? JSON.stringify(syntheticTokenizerJson())
					: "fake model bytes",
			);
		}) as typeof fetch);

		const scores = await synthReranker().scoreBatch(words(2), [words(2)]);

		expect(scores).toHaveLength(1);
		const urls = fetchSpy.mock.calls.map((c) => String(c[0]));
		expect(
			urls.some((u) =>
				u.endsWith(`/${SYNTH_MODEL}/resolve/main/tokenizer.json`),
			),
		).toBe(true);
		const dir = join(modelsDir, SYNTH_MODEL);
		expect(readdirSync(dir).sort()).toEqual(["onnx", "tokenizer.json"]);
		expect(readdirSync(join(dir, "onnx"))).toEqual(["model_quantized.onnx"]);
	});

	it("fails a stalled download after the download timeout instead of hanging", async () => {
		await useFakeSession();
		spyOn(globalThis, "fetch").mockImplementation((async (
			_input: RequestInfo | URL,
			init?: RequestInit,
		) => {
			const signal = init?.signal;
			const body = new ReadableStream({
				start(controller) {
					signal?.addEventListener("abort", () =>
						controller.error(signal.reason),
					);
				},
			});
			return new Response(body);
		}) as typeof fetch);

		const started = performance.now();
		await expect(
			synthReranker(10_000, 200).scoreBatch(words(2), [words(2)]),
		).rejects.toThrow();
		expect(performance.now() - started).toBeLessThan(2_000);
	});

	it("rejects a pinned artifact whose checksum does not match and caches nothing", async () => {
		await useFakeSession();
		spyOn(globalThis, "fetch").mockImplementation(
			(async () => new Response("not the pinned file")) as typeof fetch,
		);

		const reranker = new LocalReranker({
			model: DEFAULT_MODEL,
			timeoutMs: 10_000,
		});
		await expect(reranker.scoreBatch(words(2), [words(2)])).rejects.toThrow(
			/checksum mismatch/,
		);
		expect(existsSync(join(modelsDir, DEFAULT_MODEL, "tokenizer.json"))).toBe(
			false,
		);
	});

	it("re-downloads a cached pinned artifact that fails its checksum", async () => {
		mkdirSync(join(modelsDir, DEFAULT_MODEL), { recursive: true });
		writeFileSync(
			join(modelsDir, DEFAULT_MODEL, "tokenizer.json"),
			"{truncated",
		);
		await useFakeSession();
		const fetchSpy = spyOn(globalThis, "fetch").mockImplementation(
			(async () => new Response("still not the pinned file")) as typeof fetch,
		);

		const reranker = new LocalReranker({
			model: DEFAULT_MODEL,
			timeoutMs: 10_000,
		});
		await expect(reranker.scoreBatch(words(2), [words(2)])).rejects.toThrow(
			/checksum mismatch/,
		);
		expect(fetchSpy).toHaveBeenCalled();
		expect(existsSync(join(modelsDir, DEFAULT_MODEL, "tokenizer.json"))).toBe(
			false,
		);
	});
});

// ── Real cached model ────────────────────────────────────────────────────────

const SHORT_DOCS = Array.from(
	{ length: 40 },
	(_, i) =>
		`note ${i}: the consolidation daemon moves pending episodes into the knowledge store and records decisions about project ${i % 7}`,
);

describe.skipIf(!HAS_REAL_MODEL)("LocalReranker (real cached model)", () => {
	it("scores 40 short pairs with the pinned default model, yielding between runs", async () => {
		const reranker = new LocalReranker({
			model: DEFAULT_MODEL,
			timeoutMs: 60_000,
		});
		await reranker.scoreBatch("pending episodes", ["warm up"]);

		let ticks = 0;
		const iv = setInterval(() => ticks++, 10);
		let scores: number[];
		try {
			scores = await reranker.scoreBatch(
				"how does the consolidation daemon handle pending episodes?",
				SHORT_DOCS,
			);
		} finally {
			clearInterval(iv);
		}
		expect(scores).toHaveLength(40);
		expect(new Set(scores.map((s) => s.toFixed(6))).size).toBeGreaterThan(1);
		expect(ticks).toBeGreaterThan(0);
	}, 30_000);

	it("bounds a batch containing one ~6 kB document by the timeout", async () => {
		const reranker = new LocalReranker({
			model: DEFAULT_MODEL,
			timeoutMs: 60_000,
		});
		await reranker.scoreBatch("pending episodes", ["warm up"]);

		const long =
			"the knowledge base stores durable notes about projects. ".repeat(120);
		const timed = new LocalReranker({ model: DEFAULT_MODEL, timeoutMs: 400 });
		let ticks = 0;
		const iv = setInterval(() => ticks++, 10);
		const started = performance.now();
		try {
			await expect(
				timed.scoreBatch("pending episodes", [long, ...SHORT_DOCS]),
			).rejects.toThrow(/timeout after 400ms/);
		} finally {
			clearInterval(iv);
		}
		expect(performance.now() - started).toBeLessThan(1_000);
		expect(ticks).toBeGreaterThan(0);
	}, 30_000);

	it("C3: a 500+ token query still yields distinct scores on the real model", async () => {
		const reranker = new LocalReranker({
			model: DEFAULT_MODEL,
			timeoutMs: 60_000,
		});
		const query = "the knowledge base keeps notes about the project ".repeat(
			120,
		);
		const docs = [
			"short note about pending episodes",
			"a longer note about pending episodes and how the daemon drains them into the store every night",
			"unrelated text about gardening and tomatoes",
		];
		const scores = await reranker.scoreBatch(query, docs);
		expect(new Set(scores.map((s) => s.toFixed(6))).size).toBe(3);
	}, 30_000);
});
