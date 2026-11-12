/**
 * Schemas for model output. Every LLM response is validated before any of its
 * values reach the store.
 *
 * Validation is lenient per item: an item that fails its schema is dropped with
 * a warning and its siblings are kept. Values are normalised rather than
 * rejected where a safe default exists (confidence is clamped to [0, 1], topics
 * are filtered to short strings). A response that is not a JSON array at all is
 * malformed and throws MalformedLlmOutputError. That is a SyntaxError, so the
 * chunk-level error classifier treats it as a deterministic failure.
 */
import { z } from "zod";
import { logger } from "../logger.js";
import { clampKnowledgeType } from "../types.js";
import { scrubUntrusted } from "./untrusted.js";

/** Longest content string accepted from the model for storage. */
export const MAX_STORED_CONTENT_CHARS = 8000;
/** Most topics kept per entry. */
export const MAX_TOPICS = 10;
/** Longest single topic kept. Longer topics are dropped, not truncated. */
export const MAX_TOPIC_CHARS = 100;

/** Thrown when a model response is not the JSON array we asked for. */
export class MalformedLlmOutputError extends SyntaxError {
	constructor(label: string) {
		super(`[llm] ${label}: response is not a JSON array`);
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
		// Log paths and messages only, never the values (they are model output).
		const reason = result.error.issues
			.map((issue) => `${issue.path.join(".") || "(item)"}: ${issue.message}`)
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

// ── Contradiction ────────────────────────────────────────────────────────────

export const contradictionItemSchema = z.object({
	candidateId: z.string().min(1),
	resolution: z.enum([
		"no_conflict",
		"supersede_old",
		"supersede_new",
		"merge",
		"irresolvable",
	]),
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
});
