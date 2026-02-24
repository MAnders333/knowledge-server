import { describe, it, expect } from "bun:test";
import { cosineSimilarity } from "../src/activation/embeddings";

describe("cosineSimilarity", () => {
  it("should return 1 for identical vectors", () => {
    const v = [1, 2, 3, 4, 5];
    expect(cosineSimilarity(v, v)).toBeCloseTo(1.0);
  });

  it("should return 0 for orthogonal vectors", () => {
    const a = [1, 0, 0];
    const b = [0, 1, 0];
    expect(cosineSimilarity(a, b)).toBeCloseTo(0.0);
  });

  it("should return -1 for opposite vectors", () => {
    const a = [1, 2, 3];
    const b = [-1, -2, -3];
    expect(cosineSimilarity(a, b)).toBeCloseTo(-1.0);
  });

  it("should handle zero vectors gracefully", () => {
    const a = [0, 0, 0];
    const b = [1, 2, 3];
    expect(cosineSimilarity(a, b)).toBe(0);
  });

  it("should throw on dimension mismatch", () => {
    expect(() => cosineSimilarity([1, 2], [1, 2, 3])).toThrow(
      "Vector dimension mismatch"
    );
  });

  it("should compute similarity for realistic embeddings", () => {
    // Simulate normalized embeddings (typical for embedding models)
    const a = [0.1, 0.3, 0.5, 0.7, 0.2];
    const b = [0.15, 0.28, 0.52, 0.68, 0.25]; // slightly different
    const c = [-0.5, -0.3, -0.1, 0.1, -0.7]; // very different

    const simAB = cosineSimilarity(a, b);
    const simAC = cosineSimilarity(a, c);

    // a and b should be much more similar than a and c
    expect(simAB).toBeGreaterThan(0.99);
    expect(simAC).toBeLessThan(0.0);
    expect(simAB).toBeGreaterThan(simAC);
  });
});
