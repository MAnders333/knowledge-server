import { config } from "../config.js";
import type { KnowledgeEntry } from "../types.js";

/**
 * Compute the current strength of a knowledge entry.
 *
 * Models the human forgetting curve (Ebbinghaus), with bonuses that extend
 * the effective half-life rather than multiply a compounding product.
 *
 * Formula:
 *   effectiveHalfLife = baseHalfLife × observationBonus × accessBonus
 *   decayFactor = e^(-ln2 × daysSinceLastAccess / effectiveHalfLife)
 *   strength = confidence × decayFactor   (clamped to [0, 1])
 *
 * Bonuses (both use the same log2 shape for diminishing returns):
 *   observationBonus = 1 + log2(1 + observationCount)
 *     — evidence signal: how many episodes produced this knowledge
 *     — observationCount=1 (new entry): bonus = 1 + log2(2) = 2×
 *     — observationCount=3: bonus = 1 + log2(4) = 3×
 *     — observationCount=7: bonus = 1 + log2(8) = 4×
 *   accessBonus = 1 + log2(1 + accessCount)
 *     — retrieval signal: how many times this was surfaced during activation
 *     — starts at 1 when never accessed (accessCount = 0)
 *
 * `confidence` is the LLM's extraction-time quality estimate (prior).
 * It is a ceiling, never mutated after insertion.
 *
 * Examples (fact type, baseHalfLife = 30 days):
 *   New entry, never accessed:       effectiveHalfLife = 30 × 1 × 1   = 30 days
 *   4 observations, never accessed:  effectiveHalfLife = 30 × 2.3 × 1 ≈ 69 days
 *   4 obs + 4 accesses:              effectiveHalfLife = 30 × 2.3 × 2.3 ≈ 159 days
 */
export function computeStrength(entry: KnowledgeEntry): number {
  const now = Date.now();
  const daysSinceAccess =
    (now - entry.lastAccessedAt) / (1000 * 60 * 60 * 24);

  // Base half-life in days (type-specific)
  const baseHalfLife =
    config.decay.typeHalfLife[entry.type] ??
    config.decay.typeHalfLife.fact;

  // Observation bonus: more evidence → longer effective half-life
  // observationCount=1 (new): 1 + log2(2) = 2×
  // observationCount=3:       1 + log2(4) = 3×
  // observationCount=7:       1 + log2(8) = 4×
  const observationBonus = 1 + Math.log2(1 + entry.observationCount);

  // Access bonus: more retrievals → longer effective half-life
  // log2(1+0) = 0 → bonus = 1× for accessCount=0 (no retrieval boost yet)
  const accessBonus = 1 + Math.log2(1 + entry.accessCount);

  // Combined effective half-life
  const effectiveHalfLife = baseHalfLife * observationBonus * accessBonus;

  // Exponential decay from last access point
  const decayFactor = Math.exp(
    (-Math.LN2 * daysSinceAccess) / effectiveHalfLife
  );

  // Confidence is a ceiling — decay only brings strength down from it
  const strength = entry.confidence * decayFactor;

  return Math.max(0, Math.min(1, strength));
}
