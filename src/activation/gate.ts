/**
 * Precision gate: decides how many of the ranked candidates are injected.
 *
 * Retrieval (dense, then optional rerank) orders candidates but never says which
 * of them are relevant. The gate adds three rules, applied in this order by
 * ActivationEngine.activate():
 *
 *   1. Floor. Rerank mode: a candidate's rerank score must reach
 *      max(absolute floor for the scorer, relative fraction × top rerank score).
 *      Any other ordering (rerank disabled, failed, skipped or out of time): the raw
 *      cosine must reach the similarity threshold plus a margin.
 *   2. Support sources. An entry pulled in by relation expansion inherits no
 *      rerank score. When a reranker scored the candidates, or ACTIVATION_DENSE_MARGIN
 *      is set, it is gated at 0.9× its parent's gate score, the same discount it
 *      carries in ranking, so expansion cannot bypass the floor. Otherwise sources
 *      are not gated.
 *   3. Token budget. The block is capped at a token estimate. The top-ranked
 *      entry is always kept.
 *
 * Pure functions only; the engine owns the store and telemetry.
 */
import type { RerankIdentity } from "./rerank.js";

/**
 * Absolute rerank-score floor per scorer, keyed by `${provider}/${model}`. Empty:
 * no model has a calibrated floor yet, so the relative rule applies alone unless
 * RERANK_FLOOR sets one. Add an entry only after a replay on that model.
 */
export const RERANK_ABS_FLOOR_BY_MODEL: Readonly<Record<string, number>> = {};

/** Scorer identity as a table key. */
export function scorerKey(identity: RerankIdentity): string {
	return `${identity.provider}/${identity.model}`;
}

/**
 * Minimum rerank score a candidate needs to be injected. `floorOverride` is
 * RERANK_FLOOR; `topScore` is the best rerank score among the candidates.
 */
export function rerankFloor(input: {
	identity: RerankIdentity;
	floorOverride: number | null;
	relFrac: number;
	topScore: number;
}): number {
	const absFloor =
		input.floorOverride ??
		RERANK_ABS_FLOOR_BY_MODEL[scorerKey(input.identity)] ??
		0;
	return Math.max(absFloor, input.relFrac * input.topScore);
}

/** Minimum raw cosine for dense-only ordering: the similarity threshold plus a margin. */
export function denseFloor(
	similarityThreshold: number,
	margin: number,
): number {
	return similarityThreshold + margin;
}

/** Token estimate for injected text. A cheap, tokenizer-free approximation: ~4 characters per token. */
export function estimateTokens(chars: number): number {
	return Math.ceil(chars / 4);
}

/**
 * Keep the longest prefix of `items` that fits `budget` tokens. The first item
 * is kept whenever `keepFirst` allows it, even over budget, so the top-ranked
 * entry is never lost to the budget. Prefix semantics keep the ranking order:
 * a later, smaller entry is never injected ahead of a larger, higher-ranked one.
 */
export function applyTokenBudget<T>(
	items: readonly T[],
	budget: number,
	tokensOf: (item: T) => number,
	keepFirst: (item: T) => boolean,
): T[] {
	const kept: T[] = [];
	let used = 0;
	for (const item of items) {
		const tokens = tokensOf(item);
		if (kept.length === 0) {
			if (!keepFirst(item)) break;
		} else if (used + tokens > budget) {
			break;
		}
		kept.push(item);
		used += tokens;
	}
	return kept;
}
