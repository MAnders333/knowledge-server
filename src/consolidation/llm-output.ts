/**
 * Schemas for model output. Every LLM response is validated before any of its
 * values reach the store.
 *
 * Validation is lenient per item: an item that fails its schema is dropped with
 * a warning and its siblings are kept. Values are normalised rather than
 * rejected where a safe default exists (confidence is clamped to [0, 1], topics
 * are filtered to short strings). A response with the wrong top-level shape (not
 * a JSON array, or for contradiction not the {checked, conflicts} object, or a
 * `checked` count that differs from the batch size) is malformed and throws
 * MalformedLlmOutputError. That is a SyntaxError, so the
 * chunk-level error classifier treats it as a deterministic failure.
 */
import { z } from "zod";
import { logger } from "../logger.js";
import { clampKnowledgeType } from "../types.js";
import { scrubUntrusted } from "./untrusted.js";

/** Longest content string accepted from the model for storage. */
const MAX_STORED_CONTENT_CHARS = 8000;
/** Most topics kept per entry. */
const MAX_TOPICS = 10;
/** Longest single topic kept. Longer topics are dropped, not truncated. */
const MAX_TOPIC_CHARS = 100;

/** Thrown when a model response does not have the shape we asked for. */
export class MalformedLlmOutputError extends SyntaxError {
	constructor(label: string, detail = "response is not a JSON array") {
		super(`[llm] ${label}: ${detail}`);
		this.name = "MalformedLlmOutputError";
	}
}

/**
 * Validate each item with its schema. Invalid items are dropped and logged,
 * valid items are returned in their original order.
 */
export function parseItems<S extends z.ZodTypeAny>(
	items: readonly unknown[],
	schema: S,
	label: string,
): z.output<S>[] {
	const out: z.output<S>[] = [];
	items.forEach((raw, i) => {
		const result = schema.safeParse(raw);
		if (result.success) {
			out.push(result.data);
			return;
		}
		// Log paths and issue codes only. Zod messages can echo the received value
		// (an enum mismatch quotes it), and that value is model output.
		const reason = result.error.issues
			.map((issue) => `${issue.path.join(".") || "(item)"}: ${issue.code}`)
			.join("; ");
		logger.warn(`[llm] ${label}: dropped item ${i + 1} (${reason})`);
	});
	return out;
}

/** Plain text for storage: invisible characters removed, trimmed, non-empty, bounded. */
const storedText = z
	.string()
	.transform((s) => scrubUntrusted(s).trim())
	.pipe(z.string().min(1).max(MAX_STORED_CONTENT_CHARS));

/** Model output sometimes returns numbers as strings. Anything else is undefined. */
function toNumber(v: unknown): number | undefined {
	const n = typeof v === "string" && v.trim() !== "" ? Number(v) : v;
	return typeof n === "number" && Number.isFinite(n) ? n : undefined;
}

/** Confidence that must be present; clamped into [0, 1]. */
const requiredConfidence = z.preprocess(
	toNumber,
	z.number().transform((n) => Math.min(1, Math.max(0, n))),
);

/** Confidence with a default when missing or not numeric; clamped into [0, 1]. */
const confidenceOr = (fallback: number) =>
	z.preprocess(
		(v) => toNumber(v) ?? fallback,
		z.number().transform((n) => Math.min(1, Math.max(0, n))),
	);

/** Topics: non-strings and over-long values are dropped, the rest capped. */
const topicsField = z.preprocess(
	(v) => (Array.isArray(v) ? v : []),
	z.array(z.unknown()).transform((items) =>
		items
			.filter((t): t is string => typeof t === "string")
			.map((t) => scrubUntrusted(t).trim())
			.filter((t) => t.length > 0 && t.length <= MAX_TOPIC_CHARS)
			.slice(0, MAX_TOPICS),
	),
);

/** Knowledge type. Compound or unusual strings are clamped; non-strings are rejected. */
const knowledgeType = z
	.string()
	.min(1)
	.transform((s) => clampKnowledgeType(s));

// ── Extraction ───────────────────────────────────────────────────────────────

export const extractedItemSchema = z.object({
	type: knowledgeType,
	content: storedText,
	topics: topicsField,
	confidence: confidenceOr(0.5),
	source: z.preprocess(
		(v) => (typeof v === "string" ? scrubUntrusted(v).trim() : ""),
		z.string(),
	),
	domain: z.preprocess(
		(v) => (typeof v === "string" ? v : undefined),
		z.string().optional(),
	),
});

// ── Merge decisions (single pair and batch share this schema) ────────────────

const mergeWriteFields = {
	content: storedText,
	type: knowledgeType,
	topics: topicsField,
	confidence: requiredConfidence,
};

export const mergeDecisionSchema = z.discriminatedUnion("action", [
	z.object({ action: z.literal("keep") }),
	z.object({ action: z.literal("insert") }),
	z.object({ action: z.literal("update"), ...mergeWriteFields }),
	z.object({ action: z.literal("replace"), ...mergeWriteFields }),
]);

// ── Synthesis ────────────────────────────────────────────────────────────────

export const synthesisItemSchema = z.object({
	type: knowledgeType,
	content: storedText,
	topics: topicsField,
	confidence: confidenceOr(0.7),
	sourceIds: z.preprocess(
		(v) =>
			Array.isArray(v)
				? v.filter((s): s is string => typeof s === "string")
				: [],
		z.array(z.string()),
	),
});

// ── Contradiction (one batch per call, wrapper object) ──────────────────────

const CONTRADICTION_TYPES = [
	"supersede_old",
	"supersede_new",
	"merge",
	"irresolvable",
] as const;

type ContradictionType = (typeof CONTRADICTION_TYPES)[number];

/**
 * Reply shape. "verdicts" (default): one result per candidate, `no_conflict`
 * allowed, list key `results`. "conflicts": only the conflicting candidates, list
 * key `conflicts`. Both carry `checked`, which must equal the batch size.
 */
export type ContradictionReplyFormat = "verdicts" | "conflicts";

/** One validated conflict from a contradiction reply. `index` is 1-based. */
export interface ContradictionConflict {
	index: number;
	resolution: ContradictionType;
	reason: string;
	mergedContent?: string;
	mergedType?: string;
	mergedTopics: string[];
	mergedConfidence: number;
}

/**
 * Candidate reference: the 1-based number shown in the prompt. Cheap models
 * sometimes write "[3]" or "3" as a string, so both forms are accepted.
 */
const candidateNumber = z.preprocess((v) => {
	if (typeof v === "string") {
		const m = /^\s*\[?\s*(\d+)\s*\]?\s*$/.exec(v);
		return m ? Number(m[1]) : v;
	}
	return toNumber(v) ?? v;
}, z.number().int());

/** Items are checked against the batch size so an out-of-range index is dropped. */
const contradictionItemSchema = (batchSize: number) =>
	z.preprocess(
		// Accept `resolution` as an alias of `type`.
		(raw) =>
			raw && typeof raw === "object" && !("type" in raw) && "resolution" in raw
				? { ...raw, type: (raw as { resolution: unknown }).resolution }
				: raw,
		z.object({
			candidate: candidateNumber.pipe(z.number().min(1).max(batchSize)),
			type: z.enum([...CONTRADICTION_TYPES, "no_conflict"]),
			reason: z.preprocess(
				(v) => (typeof v === "string" ? scrubUntrusted(v).trim() : ""),
				z.string(),
			),
			// Merge fields are only needed for "merge". A malformed value there must not
			// invalidate a supersede or irresolvable verdict, so each one falls back to undefined.
			mergedContent: storedText.optional().catch(undefined),
			mergedType: knowledgeType.optional().catch(undefined),
			mergedTopics: topicsField,
			mergedConfidence: confidenceOr(0.7),
		}),
	);

/** The wrapper object. Only `checked` and the list for the requested format are read. */
const contradictionReplySchema = z.object({
	checked: z.preprocess(toNumber, z.number().int()),
	conflicts: z.array(z.unknown()).optional(),
	results: z.array(z.unknown()).optional(),
});

/**
 * Validate a contradiction reply for a batch of `batchSize` candidates.
 *
 * Throws MalformedLlmOutputError when the reply is not the wrapper object, when
 * the list for `format` is missing, or when `checked` is not exactly `batchSize`
 * (the model did not review the whole batch). Individual items are validated
 * leniently: an item with an invalid candidate number, an unknown type, or a
 * duplicate is dropped. Returns conflicts only; no_conflict verdicts are not
 * returned.
 */
export function parseContradictionReply(
	raw: unknown,
	batchSize: number,
	format: ContradictionReplyFormat,
): ContradictionConflict[] {
	const wrapper = contradictionReplySchema.safeParse(raw);
	const items =
		format === "verdicts"
			? wrapper.success
				? wrapper.data.results
				: undefined
			: wrapper.success
				? wrapper.data.conflicts
				: undefined;
	if (!wrapper.success || items === undefined) {
		throw new MalformedLlmOutputError(
			"contradiction",
			`reply is not {checked, ${format === "verdicts" ? "results" : "conflicts"}}`,
		);
	}
	if (wrapper.data.checked !== batchSize) {
		throw new MalformedLlmOutputError(
			"contradiction",
			`checked ${wrapper.data.checked} of ${batchSize} candidates`,
		);
	}

	const seen = new Set<number>();
	const out: ContradictionConflict[] = [];
	for (const item of parseItems(
		items,
		contradictionItemSchema(batchSize),
		"contradiction",
	)) {
		if (item.type === "no_conflict") continue;
		if (seen.has(item.candidate)) {
			logger.warn(
				`[llm] contradiction: duplicate verdict for candidate ${item.candidate} — kept the first`,
			);
			continue;
		}
		seen.add(item.candidate);
		out.push({
			index: item.candidate,
			resolution: item.type,
			reason: item.reason,
			mergedContent: item.mergedContent,
			mergedType: item.mergedType,
			mergedTopics: item.mergedTopics,
			mergedConfidence: item.mergedConfidence,
		});
	}
	return out;
}
