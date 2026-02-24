import { config } from "../config.js";
import type { KnowledgeEntry } from "../types.js";

/**
 * Compute the current strength of a knowledge entry.
 *
 * Models the human forgetting curve (Ebbinghaus):
 * - Strength decays exponentially over time since last access
 * - Each access reinforces the entry (slows decay)
 * - Different knowledge types decay at different rates
 *   (procedures are very stable, facts go stale quickly)
 *
 * Formula:
 *   strength = confidence × recency_factor × access_bonus × type_modifier
 *
 * Where:
 *   recency_factor = e^(-t / halfLife)           -- exponential decay
 *   access_bonus = 1 + log2(1 + accessCount)     -- diminishing returns on access
 *   type_modifier = per-type adjustment           -- procedures decay slower than facts
 */
export function computeStrength(entry: KnowledgeEntry): number {
  const now = Date.now();
  const daysSinceAccess =
    (now - entry.lastAccessedAt) / (1000 * 60 * 60 * 24);

  // Half-life in days (type-specific)
  const halfLife =
    config.decay.typeHalfLife[entry.type] ||
    config.decay.typeHalfLife.fact;

  // Exponential decay based on time since last access
  const recencyFactor = Math.exp(
    (-Math.LN2 * daysSinceAccess) / halfLife
  );

  // Access bonus: more accesses = slower effective decay
  // Using log2 for diminishing returns (10 accesses ≈ 4.3x, 100 ≈ 7.6x)
  const accessBonus = 1 + Math.log2(1 + entry.accessCount);

  // Base confidence
  const confidence = entry.confidence;

  // Combined strength
  const strength = confidence * recencyFactor * Math.min(accessBonus, 10);

  // Clamp to [0, 1] (though it can theoretically exceed 1 with high access counts)
  return Math.max(0, Math.min(1, strength));
}
