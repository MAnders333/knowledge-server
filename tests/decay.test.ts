import { describe, it, expect } from "bun:test";
import { computeStrength } from "../src/consolidation/decay";
import type { KnowledgeEntry } from "../src/types";

function makeEntry(overrides: Partial<KnowledgeEntry> = {}): KnowledgeEntry {
  const now = Date.now();
  return {
    id: "test",
    type: "fact",
    content: "Test",
    topics: [],
    confidence: 0.8,
    source: "test",
    scope: "personal",
    status: "active",
    strength: 1.0,
    createdAt: now,
    updatedAt: now,
    lastAccessedAt: now,
    accessCount: 0,
    observationCount: 1,
    supersededBy: null,
    derivedFrom: [],
    ...overrides,
  };
}

const ONE_DAY = 24 * 60 * 60 * 1000;

describe("computeStrength", () => {
  it("returns ~confidence for a brand-new entry (just accessed, obs=1, access=0)", () => {
    // effectiveHalfLife = 30 × (1+log2(2)) × (1+log2(1)) = 30 × 2 × 1 = 60 days
    // daysSinceAccess ≈ 0 → decayFactor ≈ 1
    // strength ≈ confidence × 1 = 0.9
    const entry = makeEntry({ confidence: 0.9, lastAccessedAt: Date.now(), observationCount: 1, accessCount: 0 });
    const strength = computeStrength(entry);
    expect(strength).toBeGreaterThan(0.85);
    expect(strength).toBeLessThanOrEqual(1.0);
  });

  it("decays to ~half confidence after one effective half-life (fact, obs=1, access=0)", () => {
    // baseHalfLife = 30 days (fact)
    // observationBonus = 1 + log2(2) = 2, accessBonus = 1 + log2(1) = 1
    // effectiveHalfLife = 30 × 2 × 1 = 60 days
    // After 60 days: decayFactor = e^(-ln2) = 0.5
    // strength = 0.8 × 0.5 = 0.4
    const now = Date.now();
    const entry = makeEntry({
      type: "fact",
      confidence: 0.8,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 1,
      accessCount: 0,
    });
    const strength = computeStrength(entry);
    expect(strength).toBeGreaterThan(0.35);
    expect(strength).toBeLessThan(0.45);
  });

  it("decays slower for procedures (365-day base half-life) than facts (30-day)", () => {
    const now = Date.now();
    const entryFact = makeEntry({
      type: "fact",
      confidence: 0.8,
      lastAccessedAt: now - 30 * ONE_DAY,
      observationCount: 1,
      accessCount: 0,
    });
    const entryProcedure = makeEntry({
      type: "procedure",
      confidence: 0.8,
      lastAccessedAt: now - 30 * ONE_DAY,
      observationCount: 1,
      accessCount: 0,
    });

    const factStrength = computeStrength(entryFact);
    const procStrength = computeStrength(entryProcedure);
    expect(procStrength).toBeGreaterThan(factStrength);
  });

  it("more observations → longer effective half-life → higher strength after same time", () => {
    const now = Date.now();
    const fewObs = makeEntry({
      confidence: 0.8,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 1,
      accessCount: 0,
    });
    const manyObs = makeEntry({
      confidence: 0.8,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 8,
      accessCount: 0,
    });

    expect(computeStrength(manyObs)).toBeGreaterThan(computeStrength(fewObs));
  });

  it("more accesses → longer effective half-life → higher strength after same time", () => {
    const now = Date.now();
    const fewAccess = makeEntry({
      confidence: 0.8,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 1,
      accessCount: 1,
    });
    const manyAccess = makeEntry({
      confidence: 0.8,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 1,
      accessCount: 20,
    });

    expect(computeStrength(manyAccess)).toBeGreaterThan(computeStrength(fewAccess));
  });

  it("archives entries that fall below threshold (low confidence, long ago, no reinforcement)", () => {
    // obs=1, access=0 → effectiveHalfLife = 30 × 2 × 1 = 60 days
    // After 180 days: decayFactor = e^(-ln2 × 180/60) = e^(-3*ln2) = 0.125
    // strength = 0.3 × 0.125 = 0.0375 → below archiveThreshold of 0.15
    const now = Date.now();
    const entry = makeEntry({
      type: "fact",
      confidence: 0.3,
      lastAccessedAt: now - 180 * ONE_DAY,
      observationCount: 1,
      accessCount: 0,
    });
    expect(computeStrength(entry)).toBeLessThan(0.15);
  });

  it("highly-observed entry stays alive well past the base half-life", () => {
    // obs=4, access=0 → observationBonus = 1 + log2(5) ≈ 3.32
    // principle base half-life = 180 days
    // effectiveHalfLife = 180 × 3.32 × 1 ≈ 598 days
    // After 60 days: decayFactor = e^(-ln2 × 60/598) ≈ 0.932
    // strength = 0.9 × 0.932 ≈ 0.84 → well above archive threshold
    const now = Date.now();
    const entry = makeEntry({
      type: "principle",
      confidence: 0.9,
      lastAccessedAt: now - 60 * ONE_DAY,
      observationCount: 4,
      accessCount: 0,
    });
    expect(computeStrength(entry)).toBeGreaterThan(0.5);
  });

  it("result is always clamped to [0, 1]", () => {
    // Even with extreme observation + access counts, can't exceed confidence ceiling
    const entry = makeEntry({
      confidence: 0.7,
      lastAccessedAt: Date.now(),
      observationCount: 1000,
      accessCount: 1000,
    });
    const strength = computeStrength(entry);
    expect(strength).toBeLessThanOrEqual(1.0);
    expect(strength).toBeGreaterThanOrEqual(0.0);
  });
});
