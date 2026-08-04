/**
 * Tests for the vendored BERT WordPiece tokenizer
 * (src/activation/bert-tokenizer.ts).
 *
 * Two layers:
 *   1. Unit tests against a tiny synthetic tokenizer.json — deterministic,
 *      no network, exercises normalization / basic tokenization / WordPiece /
 *      pair encoding / batching / unsupported-model rejection.
 *   2. Exact-ID parity against @huggingface/transformers AutoTokenizer
 *      (devDependency) — SKIPPED when the real model files are not present
 *      locally (e.g. CI). This guards against silent drift between the
 *      vendored implementation and the reference.
 */
import { describe, expect, it } from "bun:test";
import { existsSync } from "node:fs";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { BertWordPieceTokenizer } from "../src/activation/bert-tokenizer";

// ── Synthetic tokenizer.json (mini BERT vocab) ───────────────────────────────

function syntheticTokenizerJson() {
	const vocab: Record<string, number> = {
		"[PAD]": 0,
		"[UNK]": 1,
		"[CLS]": 2,
		"[SEP]": 3,
		hello: 10,
		world: 11,
		"##s": 12,
		",": 13,
		un: 14,
		"##aff": 15,
		"##able": 16,
		cafe: 17,
		"\u4e2d": 18, // 中
	};
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

function makeTokenizer(): BertWordPieceTokenizer {
	return BertWordPieceTokenizer.fromTokenizerJson(syntheticTokenizerJson());
}

describe("BertWordPieceTokenizer (synthetic vocab)", () => {
	it("rejects non-WordPiece models", () => {
		expect(() =>
			BertWordPieceTokenizer.fromTokenizerJson({
				model: { type: "Unigram", vocab: {} },
			}),
		).toThrow(/WordPiece/);
	});

	it("encodes basic words and punctuation", () => {
		const tok = makeTokenizer();
		expect(tok.encode("hello world,")).toEqual([10, 11, 13]);
	});

	it("lowercases and strips accents", () => {
		const tok = makeTokenizer();
		// CAFÉ → cafe (lowercase + accent strip follows lowercase when null)
		expect(tok.encode("CAFÉ")).toEqual([17]);
	});

	it("splits CJK chars into standalone tokens", () => {
		const tok = makeTokenizer();
		// handle_chinese_chars pads CJK with spaces → 中 becomes its own token
		expect(tok.encode("a中b")).toContain(18);
	});

	it("applies greedy longest-match WordPiece with ## continuations", () => {
		const tok = makeTokenizer();
		// "unaffable" → un + ##aff + ##able (greedy from the left)
		expect(tok.encode("unaffable")).toEqual([14, 15, 16]);
	});

	it("falls back to [UNK] for out-of-vocab tokens", () => {
		const tok = makeTokenizer();
		expect(tok.encode("zzzznotinvocab")).toEqual([1]);
	});

	it("encodes pairs with special tokens and segment ids", () => {
		const tok = makeTokenizer();
		const { ids, tokenTypeIds } = tok.encodePair("hello", "world");
		expect(ids).toEqual([2, 10, 3, 11, 3]); // [CLS] hello [SEP] world [SEP]
		expect(tokenTypeIds).toEqual([0, 0, 0, 1, 1]);
	});

	it("batch-encodes with right padding and attention masks", () => {
		const tok = makeTokenizer();
		const enc = tok.batchEncodePairs(["hello", "hello world"], ["world", "hello"]);
		// Pair 1: [CLS] hello [SEP] world [SEP]  (5)
		// Pair 2: [CLS] hello world [SEP] hello [SEP] (6) → seqLen 6
		expect(enc.dims).toEqual([2, 6]);
		const row0Ids = Array.from(enc.inputIds.slice(0, 6), Number);
		const row0Mask = Array.from(enc.attentionMask.slice(0, 6), Number);
		expect(row0Ids).toEqual([2, 10, 3, 11, 3, 0]); // [PAD] at the end
		expect(row0Mask).toEqual([1, 1, 1, 1, 1, 0]);
	});

	it("truncates overlong pairs (document side first)", () => {
		const tok = makeTokenizer();
		const long = "hello ".repeat(50);
		const { ids } = tok.encodePair("hello", long, 12);
		expect(ids.length).toBeLessThanOrEqual(12);
		expect(ids[0]).toBe(2); // [CLS]
		expect(ids[ids.length - 1]).toBe(3); // [SEP]
	});
});

// ── Parity with the reference implementation (skipped without local model) ───

const MODEL_DIR = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"models",
	"Xenova/ms-marco-MiniLM-L-6-v2",
);
const PARITY_CASES = [
	"how does the consolidation daemon handle pending episodes?",
	"Hello, World! Punctuation: commas, periods... and (parens).",
	"Café Übermensch naïve résumé — accented chars",
	"中文字符 mixed with English 漢字",
	"supercalifragilisticexpialidociousantidisestablishmentarianism",
	"Don't stop believin' — contractions & symbols @#$%",
	"https://example.com/path?q=1&r=2",
];

describe("BertWordPieceTokenizer parity with AutoTokenizer", () => {
	const tokenizerJsonPath = join(MODEL_DIR, "tokenizer.json");
	const itIfModel = existsSync(tokenizerJsonPath) ? it : it.skip;

	itIfModel("produces identical token IDs on tricky inputs", async () => {
		const { AutoTokenizer, env } = await import("@huggingface/transformers");
		env.cacheDir = join(homedir(), ".local", "share", "knowledge-server", "models");
		const ref = await AutoTokenizer.from_pretrained(
			"Xenova/ms-marco-MiniLM-L-6-v2",
		);
		const mine = BertWordPieceTokenizer.fromString(
			readFileSync(tokenizerJsonPath, "utf8"),
		);

		for (const text of PARITY_CASES) {
			const refIds = Array.from(
				(await ref(text)).input_ids.data as BigInt64Array,
				Number,
			);
			const mineIds = [refIds[0], ...mine.encode(text), refIds[refIds.length - 1]];
			expect(mineIds).toEqual(refIds);
		}
	});
});
