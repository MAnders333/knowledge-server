import { EmbeddingClient, cosineSimilarity, formatEmbeddingText } from "../activation/embeddings.js";
import { config } from "../config.js";
import type { KnowledgeType } from "../types.js";

/**
 * Pre-defined calibration prompt pairs.
 *
 * Each pair is formatted through `formatEmbeddingText` so the embeddings match
 * the exact format used at runtime (type prefix + content + topics suffix).
 *
 * Three categories:
 *   1. NEAR_DUPLICATES  — semantically equivalent statements that should trigger
 *      a merge decision (reconsolidation threshold).
 *   2. TOPICALLY_RELATED — same topic but different claims; could plausibly
 *      contradict each other (contradiction band lower bound).
 *   3. UNRELATED — no meaningful semantic relationship; should be below the
 *      activation threshold (noise floor).
 */

interface CalibrationEntry {
	type: KnowledgeType;
	content: string;
	topics: string[];
}

interface CalibrationPair {
	a: CalibrationEntry;
	b: CalibrationEntry;
	label: string;
}

// ──────────────────────────────────────────────────────────────
// Category 1: Near-duplicates — same meaning, different wording.
// Expected: highest similarities. Reconsolidation threshold should
// sit at or below the lowest similarity in this group.
// ──────────────────────────────────────────────────────────────
const NEAR_DUPLICATES: CalibrationPair[] = [
	{
		label: "deploy process (reworded)",
		a: {
			type: "procedure",
			content: "To deploy to production, first run the test suite, then create a release tag, and finally trigger the CI/CD pipeline.",
			topics: ["deployment", "ci-cd", "production"],
		},
		b: {
			type: "procedure",
			content: "Production deployment procedure: execute all tests, tag a release, and kick off the CI/CD pipeline.",
			topics: ["deployment", "ci-cd", "production"],
		},
	},
	{
		label: "database timeout fact (reworded)",
		a: {
			type: "fact",
			content: "PostgreSQL queries on the orders table time out after 30 seconds when the table exceeds 50 million rows.",
			topics: ["postgresql", "performance", "orders"],
		},
		b: {
			type: "fact",
			content: "Queries against the orders table in PostgreSQL hit a 30-second timeout once row count goes above 50M.",
			topics: ["postgresql", "performance", "orders"],
		},
	},
	{
		label: "API rate limit pattern (reworded)",
		a: {
			type: "pattern",
			content: "The Stripe API returns 429 errors when batch processing exceeds 100 requests per second.",
			topics: ["stripe", "api", "rate-limiting"],
		},
		b: {
			type: "pattern",
			content: "Stripe's API rate-limits at 100 req/s — exceeding that triggers HTTP 429 responses during batch operations.",
			topics: ["stripe", "api", "rate-limiting"],
		},
	},
	{
		label: "tech stack decision (reworded)",
		a: {
			type: "decision",
			content: "We chose React over Vue for the frontend because the team has more React experience and the existing component library is React-based.",
			topics: ["frontend", "react", "tech-stack"],
		},
		b: {
			type: "decision",
			content: "The frontend framework decision was React instead of Vue, driven by team expertise and our existing React component library.",
			topics: ["frontend", "react", "tech-stack"],
		},
	},
	{
		label: "caching principle (reworded)",
		a: {
			type: "principle",
			content: "Always cache database query results that are accessed more than 10 times per minute and change less than once per hour.",
			topics: ["caching", "database", "performance"],
		},
		b: {
			type: "principle",
			content: "Database results with >10 reads/min and <1 write/hour should always be cached.",
			topics: ["caching", "database", "performance"],
		},
	},
];

// ──────────────────────────────────────────────────────────────
// Category 2: Topically related but semantically different.
// Expected: moderate similarities. These sit in the contradiction
// scan band — similar enough to be about the same thing, but
// making different (possibly contradictory) claims.
// ──────────────────────────────────────────────────────────────
const TOPICALLY_RELATED: CalibrationPair[] = [
	{
		label: "deploy: different steps",
		a: {
			type: "procedure",
			content: "To deploy to production, first run the test suite, then create a release tag, and finally trigger the CI/CD pipeline.",
			topics: ["deployment", "ci-cd", "production"],
		},
		b: {
			type: "procedure",
			content: "To roll back a production deployment, revert the release tag, run the rollback script, and notify the on-call team.",
			topics: ["deployment", "rollback", "production"],
		},
	},
	{
		label: "PostgreSQL: different performance issues",
		a: {
			type: "fact",
			content: "PostgreSQL queries on the orders table time out after 30 seconds when the table exceeds 50 million rows.",
			topics: ["postgresql", "performance", "orders"],
		},
		b: {
			type: "fact",
			content: "PostgreSQL VACUUM operations on the orders table take over 4 hours and cause elevated I/O during business hours.",
			topics: ["postgresql", "performance", "orders", "maintenance"],
		},
	},
	{
		label: "API: different services same domain",
		a: {
			type: "pattern",
			content: "The Stripe API returns 429 errors when batch processing exceeds 100 requests per second.",
			topics: ["stripe", "api", "rate-limiting"],
		},
		b: {
			type: "pattern",
			content: "The Stripe webhook delivery retries up to 3 times with exponential backoff before marking the event as failed.",
			topics: ["stripe", "api", "webhooks"],
		},
	},
	{
		label: "frontend: different aspects",
		a: {
			type: "decision",
			content: "We chose React over Vue for the frontend because the team has more React experience and the existing component library is React-based.",
			topics: ["frontend", "react", "tech-stack"],
		},
		b: {
			type: "principle",
			content: "Frontend components should be kept under 200 lines of code to maintain readability and testability.",
			topics: ["frontend", "react", "code-quality"],
		},
	},
	{
		label: "caching: different strategies",
		a: {
			type: "principle",
			content: "Always cache database query results that are accessed more than 10 times per minute and change less than once per hour.",
			topics: ["caching", "database", "performance"],
		},
		b: {
			type: "decision",
			content: "We chose Redis over Memcached for caching because we need support for sorted sets and pub/sub messaging.",
			topics: ["caching", "redis", "infrastructure"],
		},
	},
];

// ──────────────────────────────────────────────────────────────
// Category 3: Unrelated — completely different domains/topics.
// Expected: lowest similarities (noise floor). The activation
// threshold should sit above these values so unrelated entries
// don't activate.
// ──────────────────────────────────────────────────────────────
const UNRELATED: CalibrationPair[] = [
	{
		label: "deploy vs. cooking recipe",
		a: {
			type: "procedure",
			content: "To deploy to production, first run the test suite, then create a release tag, and finally trigger the CI/CD pipeline.",
			topics: ["deployment", "ci-cd", "production"],
		},
		b: {
			type: "procedure",
			content: "To make sourdough bread, feed the starter 12 hours before, mix the dough, bulk ferment for 4 hours, then bake at 450F.",
			topics: ["cooking", "bread", "sourdough"],
		},
	},
	{
		label: "PostgreSQL vs. gardening",
		a: {
			type: "fact",
			content: "PostgreSQL queries on the orders table time out after 30 seconds when the table exceeds 50 million rows.",
			topics: ["postgresql", "performance", "orders"],
		},
		b: {
			type: "fact",
			content: "Tomato plants require at least 6 hours of direct sunlight per day and should be watered deeply twice a week.",
			topics: ["gardening", "tomatoes", "plants"],
		},
	},
	{
		label: "Stripe API vs. music theory",
		a: {
			type: "pattern",
			content: "The Stripe API returns 429 errors when batch processing exceeds 100 requests per second.",
			topics: ["stripe", "api", "rate-limiting"],
		},
		b: {
			type: "principle",
			content: "In Western music theory, the circle of fifths maps all 12 chromatic pitches into a cycle of perfect fifth intervals.",
			topics: ["music", "theory", "harmony"],
		},
	},
	{
		label: "React decision vs. astronomy",
		a: {
			type: "decision",
			content: "We chose React over Vue for the frontend because the team has more React experience and the existing component library is React-based.",
			topics: ["frontend", "react", "tech-stack"],
		},
		b: {
			type: "fact",
			content: "The James Webb Space Telescope orbits the L2 Lagrange point approximately 1.5 million kilometers from Earth.",
			topics: ["astronomy", "space", "telescope"],
		},
	},
	{
		label: "caching vs. ancient history",
		a: {
			type: "principle",
			content: "Always cache database query results that are accessed more than 10 times per minute and change less than once per hour.",
			topics: ["caching", "database", "performance"],
		},
		b: {
			type: "fact",
			content: "The construction of the Great Pyramid of Giza took approximately 20 years and required an estimated 2.3 million limestone blocks.",
			topics: ["history", "egypt", "architecture"],
		},
	},
];

/** Compute percentile from a sorted array. */
function percentile(sorted: number[], p: number): number {
	if (sorted.length === 0) return 0;
	const idx = (p / 100) * (sorted.length - 1);
	const lo = Math.floor(idx);
	const hi = Math.ceil(idx);
	if (lo === hi) return sorted[lo];
	return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

/**
 * `knowledge-server calibrate`
 *
 * Embeds pre-defined prompt pairs using the configured embedding model and
 * computes cosine similarities to recommend threshold values.
 *
 * The three similarity thresholds (reconsolidation, contradiction band lower
 * bound, activation) are calibrated for text-embedding-3-large. When using a
 * different model, cosine similarity distributions shift and the defaults may
 * no longer be correct. This command measures the actual distribution and
 * recommends values tuned to the active model.
 */
export async function runCalibrate(): Promise<void> {
	const model = config.embedding.model;
	const dimensions = config.embedding.dimensions;

	console.log("Threshold Calibration");
	console.log("───────────────────────────────────────");
	console.log(`  Embedding model:  ${model}`);
	if (dimensions !== undefined) {
		console.log(`  Dimensions:       ${dimensions}`);
	}
	console.log("");
	console.log("  Embedding calibration prompts...");

	const client = new EmbeddingClient();

	// Build unique text list (some entries are reused across categories).
	const textMap = new Map<string, number>(); // text → index
	const texts: string[] = [];

	function addText(entry: CalibrationEntry): number {
		const text = formatEmbeddingText(entry.type, entry.content, entry.topics);
		const existing = textMap.get(text);
		if (existing !== undefined) return existing;
		const idx = texts.length;
		textMap.set(text, idx);
		texts.push(text);
		return idx;
	}

	// Pre-compute text indices for each pair set.
	const ndIndices = NEAR_DUPLICATES.map((p) => ({ aIdx: addText(p.a), bIdx: addText(p.b), label: p.label }));
	const trIndices = TOPICALLY_RELATED.map((p) => ({ aIdx: addText(p.a), bIdx: addText(p.b), label: p.label }));
	const urIndices = UNRELATED.map((p) => ({ aIdx: addText(p.a), bIdx: addText(p.b), label: p.label }));

	// Embed all texts in one batch call.
	let embeddings: number[][];
	try {
		embeddings = await client.embedBatch(texts);
	} catch (e) {
		console.error(`\n  Failed to generate embeddings: ${e instanceof Error ? e.message : String(e)}`);
		process.exit(1);
	}

	console.log(`  Embedded ${texts.length} unique texts.\n`);

	// Compute similarities per category.
	function computeSims(indices: { aIdx: number; bIdx: number; label: string }[]): { label: string; similarity: number }[] {
		return indices.map(({ aIdx, bIdx, label }) => ({
			label,
			similarity: cosineSimilarity(embeddings[aIdx], embeddings[bIdx]),
		}));
	}

	const ndSims = computeSims(ndIndices);
	const trSims = computeSims(trIndices);
	const urSims = computeSims(urIndices);

	// Compute stats for each category.
	function computeStats(sims: { label: string; similarity: number }[]) {
		const values = sims.map((s) => s.similarity).sort((a, b) => a - b);
		return {
			min: values[0],
			max: values[values.length - 1],
			mean: values.reduce((s, v) => s + v, 0) / values.length,
			sorted: values,
		};
	}

	const ndStats = computeStats(ndSims);
	const trStats = computeStats(trSims);
	const urStats = computeStats(urSims);

	// Print per-category results.
	const categoryOutputs = [
		{
			sims: ndSims,
			stats: ndStats,
			title: "Near-Duplicate Pairs (should trigger merge)",
			desc: "Informs RECONSOLIDATION_SIMILARITY_THRESHOLD — entries above this are routed to decideMerge.",
		},
		{
			sims: trSims,
			stats: trStats,
			title: "Topically Related Pairs (contradiction scan band)",
			desc: "Informs CONTRADICTION_MIN_SIMILARITY — entries in [contradiction_min, reconsolidation) get the contradiction LLM call.",
		},
		{
			sims: urSims,
			stats: urStats,
			title: "Unrelated Pairs (noise floor)",
			desc: "Informs ACTIVATION_SIMILARITY_THRESHOLD — entries below this are filtered out of activation results.",
		},
	];

	for (const cat of categoryOutputs) {
		console.log(`  ${cat.title}`);
		console.log(`  ${cat.desc}`);
		console.log("");
		for (const s of cat.sims) {
			console.log(`    ${s.similarity.toFixed(4)}  ${s.label}`);
		}
		console.log("");
		console.log(`    min: ${cat.stats.min.toFixed(4)}  max: ${cat.stats.max.toFixed(4)}  mean: ${cat.stats.mean.toFixed(4)}`);
		console.log("");
	}

	// ──────────────────────────────────────────────────────────────
	// Derive recommended thresholds.
	//
	// Reconsolidation threshold:
	//   Should be low enough to catch all near-duplicates but high enough
	//   to avoid merging merely-related entries. Use P10 of near-duplicate
	//   similarities (conservative floor), but cap at the max of topically-related
	//   similarities + a safety margin so the two bands don't overlap.
	//
	// Contradiction min similarity:
	//   The lower bound of the contradiction scan band. Should sit between the
	//   noise floor (unrelated) and the topically-related range. Use the midpoint
	//   between P90 of unrelated and P10 of topically-related.
	//
	// Activation similarity threshold:
	//   Should filter out noise while preserving recall. Use P90 of unrelated
	//   similarities (most noise is below this) with a small buffer.
	// ──────────────────────────────────────────────────────────────

	// Reconsolidation: P10 of near-duplicates, but at least max(topically-related) + 0.03
	const ndP10 = percentile(ndStats.sorted, 10);
	const trMaxPlusMargin = trStats.max + 0.03;
	const reconsolidationRaw = Math.max(ndP10, trMaxPlusMargin);
	// Round to 2 decimal places for clean config values.
	const reconsolidation = Math.round(reconsolidationRaw * 100) / 100;

	// Contradiction min: midpoint between P90(unrelated) and P10(topically-related)
	const urP90 = percentile(urStats.sorted, 90);
	const trP10 = percentile(trStats.sorted, 10);
	const contradictionMinRaw = (urP90 + trP10) / 2;
	const contradictionMin = Math.round(contradictionMinRaw * 100) / 100;

	// Activation: P90(unrelated) + small buffer so we're safely above noise
	const activationRaw = urP90 + 0.02;
	const activation = Math.round(activationRaw * 100) / 100;

	// Sanity: activation < contradictionMin < reconsolidation
	// If the model's distribution is very compressed, these may overlap.
	// Detect and warn but still output the best-effort values.
	const bandOk = activation < contradictionMin && contradictionMin < reconsolidation;

	console.log("───────────────────────────────────────");
	console.log("  Recommended Thresholds");
	console.log("───────────────────────────────────────");
	console.log("");
	console.log(`  RECONSOLIDATION_SIMILARITY_THRESHOLD          ${reconsolidation.toFixed(2)}`);
	console.log("    Near-duplicate merge cutoff. Entries above this \u2192 decideMerge LLM call.");
	console.log(`    Derived from: P10(near-duplicates)=${ndP10.toFixed(4)}, max(topical)+0.03=${trMaxPlusMargin.toFixed(4)}`);
	console.log("");
	console.log(`  CONTRADICTION_MIN_SIMILARITY       ${contradictionMin.toFixed(2)}`);
	console.log(`    Lower bound of contradiction scan band [${contradictionMin.toFixed(2)}, ${reconsolidation.toFixed(2)}).`);
	console.log(`    Derived from: midpoint(P90(unrelated)=${urP90.toFixed(4)}, P10(topical)=${trP10.toFixed(4)})`);
	console.log("");
	console.log(`  ACTIVATION_SIMILARITY_THRESHOLD    ${activation.toFixed(2)}`);
	console.log("    Minimum cosine similarity for activation. Entries below this are noise.");
	console.log(`    Derived from: P90(unrelated)=${urP90.toFixed(4)} + 0.02 buffer`);
	console.log("");

	if (!bandOk) {
		console.log("  WARNING: The derived thresholds overlap or are inverted. This suggests the");
		console.log("  embedding model does not produce well-separated similarity distributions.");
		console.log("  The values above are best-effort — manual tuning may be required.");
		console.log("");
	}

	// Show current vs recommended comparison.
	const currentReconsolidation = config.consolidation.reconsolidationThreshold;
	const currentContradictionMin = config.consolidation.contradictionMinSimilarity;
	const currentActivation = config.activation.similarityThreshold;

	const reconsolidationChanged = currentReconsolidation.toFixed(2) !== reconsolidation.toFixed(2);
	const contradictionMinChanged = currentContradictionMin.toFixed(2) !== contradictionMin.toFixed(2);
	const activationChanged = currentActivation.toFixed(2) !== activation.toFixed(2);

	console.log("  Current vs Recommended");
	console.log("  ─────────────────────────────────────────────────────────────");
	console.log(`  Reconsolidation:       ${currentReconsolidation.toFixed(2)} \u2192 ${reconsolidation.toFixed(2)}${reconsolidationChanged ? "" : "  (no change)"}`);
	console.log(`  Contradiction min:     ${currentContradictionMin.toFixed(2)} \u2192 ${contradictionMin.toFixed(2)}${contradictionMinChanged ? "" : "  (no change)"}`);
	console.log(`  Activation threshold:  ${currentActivation.toFixed(2)} \u2192 ${activation.toFixed(2)}${activationChanged ? "" : "  (no change)"}`);
	console.log("");

	// Output ready-to-paste env vars if any value changed.
	const anyChanged = reconsolidationChanged || contradictionMinChanged || activationChanged;

	if (anyChanged) {
		console.log("  To apply, add these to your .env file:");
		console.log("");
		if (reconsolidationChanged) {
			console.log(`    RECONSOLIDATION_SIMILARITY_THRESHOLD=${reconsolidation.toFixed(2)}`);
		}
		if (contradictionMinChanged) {
			console.log(`    CONTRADICTION_MIN_SIMILARITY=${contradictionMin.toFixed(2)}`);
		}
		if (activationChanged) {
			console.log(`    ACTIVATION_SIMILARITY_THRESHOLD=${activation.toFixed(2)}`);
		}
		console.log("");
	} else {
		console.log("  All thresholds match the current defaults \u2014 no changes needed.");
	}
	console.log("");
}
