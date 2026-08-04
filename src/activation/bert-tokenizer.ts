/**
 * Minimal BERT WordPiece tokenizer driven by a Hugging Face `tokenizer.json`.
 *
 * Exists because every packaged tokenizer runtime drags in native code:
 * @huggingface/transformers (node build) statically imports onnxruntime-node,
 * and @huggingface/tokenizers uses napi prebuilds — both crash single-file
 * `bun build --compile` binaries at module load. Tokenization for BERT-family
 * models is pure string processing, so we implement it directly against the
 * authoritative tokenizer.json (normalizer settings + vocab), keeping the
 * reranker 100% native-free.
 *
 * Scope: WordPiece models with a BertNormalizer (e.g. the default
 * Xenova/ms-marco-MiniLM-L-6-v2). SentencePiece/tokenizers-with-precompiled
 * FSTs (XLM-R, jina, bge-m3) are NOT supported — switching RERANK_MODEL to
 * one of those requires a different tokenizer (would surface as a load error,
 * not silent corruption).
 *
 * Parity: tests/bert-tokenizer.test.ts includes an exact-ID comparison
 * against @huggingface/transformers AutoTokenizer (devDependency), skipped
 * when the model files are not present locally.
 */

export interface BertTokenizerConfig {
	lowercase: boolean;
	stripAccents: boolean;
	cleanText: boolean;
	handleChineseChars: boolean;
	unkToken: string;
}

export interface BatchEncoding {
	inputIds: BigInt64Array;
	attentionMask: BigInt64Array;
	tokenTypeIds: BigInt64Array;
	/** [batch, seqLen] */
	dims: [number, number];
}

/** Special tokens expected in the vocab (standard BERT layout). */
const PAD = "[PAD]";
const UNK = "[UNK]";
const CLS = "[CLS]";
const SEP = "[SEP]";

/** Longest single token the WordPiece algorithm will attempt (HF constant). */
const MAX_WORD_CHARS = 100;

/** Default sequence budget (MiniLM max_position_embeddings = 512). */
const MAX_SEQUENCE_LENGTH = 512;

export class BertWordPieceTokenizer {
	private readonly vocab: Map<string, number>;
	private readonly unkId: number;
	private readonly clsId: number;
	private readonly sepId: number;
	private readonly padId: number;
	private readonly cfg: BertTokenizerConfig;

	private constructor(vocab: Map<string, number>, cfg: BertTokenizerConfig) {
		this.vocab = vocab;
		this.cfg = cfg;
		const id = (t: string) => {
			const v = vocab.get(t);
			if (v === undefined)
				throw new Error(`tokenizer vocab missing special token ${t}`);
			return v;
		};
		this.padId = id(PAD);
		this.unkId = id(UNK);
		this.clsId = id(CLS);
		this.sepId = id(SEP);
	}

	/**
	 * Build from a parsed tokenizer.json. Throws on unsupported layouts
	 * (non-WordPiece model, missing vocab) — callers should treat this as a
	 * "model not supported by the vendored tokenizer" signal.
	 */
	static fromTokenizerJson(raw: unknown): BertWordPieceTokenizer {
		const json = raw as {
			normalizer?: {
				type?: string;
				lowercase?: boolean;
				strip_accents?: boolean | null;
				clean_text?: boolean;
				handle_chinese_chars?: boolean;
			};
			model?: { type?: string; unk_token?: string; vocab?: Record<string, number> };
		};

		if (json.model?.type !== "WordPiece" || !json.model.vocab) {
			throw new Error(
				`unsupported tokenizer model type "${json.model?.type ?? "unknown"}" — the vendored tokenizer supports WordPiece (BERT-family) models only`,
			);
		}

		const normalizer = json.normalizer ?? {};
		if (normalizer.type && normalizer.type !== "BertNormalizer") {
			throw new Error(
				`unsupported normalizer "${normalizer.type}" — vendored tokenizer supports BertNormalizer only`,
			);
		}

		const lowercase = normalizer.lowercase ?? false;
		return new BertWordPieceTokenizer(
			new Map(Object.entries(json.model.vocab)),
			{
				lowercase,
				// HF tokenizers: strip_accents null falls back to the lowercase flag
				stripAccents: normalizer.strip_accents ?? lowercase,
				cleanText: normalizer.clean_text ?? true,
				handleChineseChars: normalizer.handle_chinese_chars ?? true,
				unkToken: json.model.unk_token ?? UNK,
			},
		);
	}

	/** Parse + build from a tokenizer.json file content string. */
	static fromString(content: string): BertWordPieceTokenizer {
		return BertWordPieceTokenizer.fromTokenizerJson(JSON.parse(content));
	}

	// ── Normalization (mirrors BertNormalizer) ─────────────────────────────

	private normalize(text: string): string {
		let out = text;
		if (this.cfg.cleanText) {
			// Remove control characters (keep tab/newline/carriage return) and
			// U+FFFD replacement chars; normalize whitespace to single spaces.
			out = out
				.replace(/[\u0000-\u0008\u000B\u000E-\u001F\u007F]/g, "")
				.replace(/\uFFFD/g, "")
				.replace(/\s+/g, " ");
		}
		if (this.cfg.handleChineseChars) {
			// Put spaces around CJK ideographs so they become standalone tokens.
			out = out.replace(
				/([一-鿿㐀-䶿豈-﫿])/g,
				" $1 ",
			);
		}
		if (this.cfg.lowercase) out = out.toLowerCase();
		if (this.cfg.stripAccents) {
			// NFD then drop combining marks (Mn) — the standard accent strip.
			out = out.normalize("NFD").replace(/\p{Mn}/gu, "");
		}
		return out.trim();
	}

	// ── Pre-tokenization (BertTokenizer basic tokenization) ────────────────

	/**
	 * Split into word tokens: whitespace split, then split off every
	 * punctuation character (each becomes its own token).
	 */
	private basicTokenize(text: string): string[] {
		const tokens: string[] = [];
		for (const chunk of text.split(/\s+/).filter(Boolean)) {
			let current = "";
			for (const ch of chunk) {
				if (isPunctuation(ch)) {
					if (current) tokens.push(current);
					tokens.push(ch);
					current = "";
				} else {
					current += ch;
				}
			}
			if (current) tokens.push(current);
		}
		return tokens;
	}

	// ── WordPiece (greedy longest-match-first) ─────────────────────────────

	private wordPiece(token: string): number[] {
		if (token.length > MAX_WORD_CHARS) return [this.unkId];

		const ids: number[] = [];
		let start = 0;
		while (start < token.length) {
			let end = token.length;
			let foundId: number | undefined;
			let foundEnd = -1;
			while (start < end) {
				const piece =
					start > 0 ? `##${token.slice(start, end)}` : token.slice(start, end);
				const id = this.vocab.get(piece);
				if (id !== undefined) {
					foundId = id;
					foundEnd = end;
					break;
				}
				end -= 1;
			}
			if (foundEnd < 0) return [this.unkId]; // whole token → [UNK]
			ids.push(foundId as number);
			start = foundEnd;
		}
		return ids;
	}

	/** Encode a single text into token IDs (no special tokens). */
	encode(text: string): number[] {
		const normalized = this.normalize(text);
		if (!normalized) return [];
		const ids: number[] = [];
		for (const token of this.basicTokenize(normalized)) {
			ids.push(...this.wordPiece(token));
		}
		return ids;
	}

	/**
	 * Encode a (query, document) pair: [CLS] q [SEP] d [SEP].
	 * token_type_ids mark the two segments (0 for query+first SEP, 1 after).
	 * Overlong pairs are truncated (document side first, matching the
	 * longest_first strategy used for pair truncation in HF tokenizers).
	 */
	encodePair(
		query: string,
		document: string,
		maxLength: number = MAX_SEQUENCE_LENGTH,
	): { ids: number[]; tokenTypeIds: number[] } {
		const qIds = this.encode(query);
		const dIds = this.encode(document);

		// Budget: 3 special tokens ([CLS], [SEP], [SEP]).
		const budget = maxLength - 3;
		if (qIds.length + dIds.length > budget) {
			// Truncate the document first; if the query alone overflows, truncate it.
			const qKeep = Math.min(qIds.length, budget);
			const dKeep = Math.max(0, budget - qKeep);
			qIds.length = qKeep;
			dIds.length = dKeep;
		}

		const ids = [this.clsId, ...qIds, this.sepId, ...dIds, this.sepId];
		const tokenTypeIds = [
			...new Array(qIds.length + 2).fill(0),
			...new Array(dIds.length + 1).fill(1),
		];
		return { ids, tokenTypeIds };
	}

	/**
	 * Batch-encode pairs with right padding to the longest sequence.
	 * Returns flat BigInt64Array buffers + dims, ready for ort.Tensor.
	 */
	batchEncodePairs(
		queries: string[],
		documents: string[],
		maxLength: number = MAX_SEQUENCE_LENGTH,
	): BatchEncoding {
		if (queries.length !== documents.length) {
			throw new Error("queries and documents must have the same length");
		}
		const encoded = queries.map((q, i) =>
			this.encodePair(q, documents[i], maxLength),
		);
		const batch = encoded.length;
		const seqLen = Math.max(1, ...encoded.map((e) => e.ids.length));

		const inputIds = new BigInt64Array(batch * seqLen);
		const attentionMask = new BigInt64Array(batch * seqLen);
		const tokenTypeIds = new BigInt64Array(batch * seqLen);

		encoded.forEach((e, row) => {
			const offset = row * seqLen;
			for (let i = 0; i < seqLen; i++) {
				if (i < e.ids.length) {
					inputIds[offset + i] = BigInt(e.ids[i]);
					tokenTypeIds[offset + i] = BigInt(e.tokenTypeIds[i]);
					attentionMask[offset + i] = 1n;
				} else {
					inputIds[offset + i] = BigInt(this.padId);
				}
			}
		});

		return {
			inputIds,
			attentionMask,
			tokenTypeIds,
			dims: [batch, seqLen],
		};
	}
}

/**
 * BERT basic-tokenizer punctuation test: ASCII punctuation, or any Unicode
 * punctuation/symbol category (covers CJK punctuation and symbols, matching
 * the reference implementation's category-based check).
 */
function isPunctuation(ch: string): boolean {
	const cp = ch.codePointAt(0) ?? 0;
	if (
		(cp >= 33 && cp <= 47) ||
		(cp >= 58 && cp <= 64) ||
		(cp >= 91 && cp <= 96) ||
		(cp >= 123 && cp <= 126)
	) {
		return true;
	}
	return /\p{P}|\p{S}/u.test(ch);
}
