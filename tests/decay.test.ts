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
    supersededBy: null,
    derivedFrom: [],
    ...overrides,
  };
}

const ONE_DAY = 24 * 60 * 60 * 1000;

describe("computeStrength", () => {
  it("should return ~confidence for just-accessed entries", () => {
    const entry = makeEntry({ confidence: 0.9, lastAccessedAt: Date.now() });
    const strength = computeStrength(entry);
    // With 0 access count, accessBonus = 1 + log2(1) = 1
    // recencyFactor ≈ 1 (just accessed)
    // strength ≈ 0.9 * 1 * 1 = 0.9
    expect(strength).toBeGreaterThan(0.85);
    expect(strength).toBeLessThanOrEqual(1.0);
  });

  it("should decay over time for facts (30-day half-life)", () => {
    const now = Date.now();
    const entry = makeEntry({
      type: "fact",
      confidence: 0.8,
      lastAccessedAt: now - 30 * ONE_DAY, // 30 days ago
      accessCount: 0,
    });

    const strength = computeStrength(entry);
    // After one half-life, strength should be roughly halved
    // 0.8 * 0.5 * 1 = 0.4
    expect(strength).toBeGreaterThan(0.3);
    expect(strength).toBeLessThan(0.5);
  });

  it("should decay slower for procedures (365-day half-life)", () => {
    const now = Date.now();
    const entryFact = makeEntry({
      type: "fact",
      confidence: 0.8,
      lastAccessedAt: now - 30 * ONE_DAY,
    });
    const entryProcedure = makeEntry({
      type: "procedure",
      confidence: 0.8,
      lastAccessedAt: now - 30 * ONE_DAY,
    });

    const factStrength = computeStrength(entryFact);
    const procStrength = computeStrength(entryProcedure);

    // Procedures should retain more strength after same time period
    expect(procStrength).toBeGreaterThan(factStrength);
  });

  it("should boost strength with more accesses", () => {
    const now = Date.now();
    // Use lower confidence + longer time so neither gets clamped to 1.0
    const fewAccesses = makeEntry({
      confidence: 0.3,
      lastAccessedAt: now - 30 * ONE_DAY,
      accessCount: 1,
    });
    const manyAccesses = makeEntry({
      confidence: 0.3,
      lastAccessedAt: now - 30 * ONE_DAY,
      accessCount: 20,
    });

    const fewStrength = computeStrength(fewAccesses);
    const manyStrength = computeStrength(manyAccesses);

    expect(manyStrength).toBeGreaterThan(fewStrength);
  });

  it("should archive entries that fall below threshold", () => {
    const now = Date.now();
    // A low-confidence fact accessed 90 days ago with no accesses
    const entry = makeEntry({
      type: "fact",
      confidence: 0.3,
      lastAccessedAt: now - 90 * ONE_DAY,
      accessCount: 0,
    });

    const strength = computeStrength(entry);
    // Should be very low: 0.3 * exp(-ln2 * 90/30) * 1 ≈ 0.3 * 0.125 = 0.0375
    expect(strength).toBeLessThan(0.15); // Below archive threshold
  });

  it("should keep high-access entries alive even after long time", () => {
    const now = Date.now();
    const entry = makeEntry({
      type: "principle",
      confidence: 0.9,
      lastAccessedAt: now - 60 * ONE_DAY,
      accessCount: 50,
    });

    const strength = computeStrength(entry);
    // High access count provides significant bonus
    // accessBonus = 1 + log2(51) ≈ 6.67 (capped at 10)
    // recencyFactor = exp(-ln2 * 60/180) ≈ 0.79
    // 0.9 * 0.79 * 6.67 ≈ 4.74 → capped at 1.0
    expect(strength).toBeGreaterThan(0.5);
  });
});
