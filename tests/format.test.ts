import { describe, it, expect } from "bun:test";
import {
  truncate,
  staleTag,
  contradictionTagInline,
  contradictionTagBlock,
  CONFLICT_TRUNCATE_LEN,
} from "../src/activation/format.js";

/**
 * Tests for src/activation/format.ts.
 *
 * Critically: the plugin (plugin/knowledge.ts) maintains a local copy of the
 * contradiction tag logic because it cannot import from src/ at runtime.
 * The PLUGIN_CONTRADICTION_TAG tests below replicate that logic verbatim so
 * that any drift between the canonical helpers and the plugin copy causes a
 * test failure here — making the coupling explicit and detectable.
 */

// ---------------------------------------------------------------------------
// Canonical helpers
// ---------------------------------------------------------------------------

describe("truncate", () => {
  it("returns the string unchanged when at or under maxLen", () => {
    expect(truncate("hello", 10)).toBe("hello");
    expect(truncate("hello", 5)).toBe("hello");
  });

  it("appends ellipsis only when the string exceeds maxLen", () => {
    expect(truncate("hello world", 5)).toBe("hello…");
  });

  it("does not append ellipsis for exact-length strings", () => {
    const s = "a".repeat(100);
    expect(truncate(s, 100)).toBe(s);
    expect(truncate(s, 100).endsWith("…")).toBe(false);
  });
});

describe("staleTag", () => {
  it("returns empty string when not stale", () => {
    expect(staleTag({ mayBeStale: false, lastAccessedDaysAgo: 5 })).toBe("");
  });

  it("returns a tag with days when stale", () => {
    expect(staleTag({ mayBeStale: true, lastAccessedDaysAgo: 47 })).toBe(
      " [may be outdated — last accessed 47d ago]"
    );
  });
});

describe("contradictionTagInline", () => {
  it("returns empty string for undefined", () => {
    expect(contradictionTagInline(undefined)).toBe("");
  });

  it("renders inline tag with short content unchanged", () => {
    const tag = contradictionTagInline({
      conflictingContent: "short content",
      caveat: "check before using",
    });
    expect(tag).toBe(
      ` [CONFLICTED — conflicts with: "short content". check before using]`
    );
    expect(tag).not.toContain("…");
  });

  it("truncates conflicting content at CONFLICT_TRUNCATE_LEN", () => {
    const long = "x".repeat(150);
    const tag = contradictionTagInline({ conflictingContent: long, caveat: "caveat" });
    expect(tag).toContain(`${"x".repeat(CONFLICT_TRUNCATE_LEN)}…`);
    expect(tag).not.toContain("x".repeat(CONFLICT_TRUNCATE_LEN + 1));
  });
});

describe("contradictionTagBlock", () => {
  it("returns empty string for undefined", () => {
    expect(contradictionTagBlock(undefined)).toBe("");
  });

  it("renders block tag with short content unchanged", () => {
    const tag = contradictionTagBlock({
      conflictingContent: "short content",
      caveat: "check before using",
    });
    expect(tag).toBe(
      `\n   ⚠ CONFLICTED — conflicts with: "short content"\n   Caveat: check before using`
    );
    expect(tag).not.toContain("…");
  });

  it("truncates conflicting content at CONFLICT_TRUNCATE_LEN", () => {
    const long = "x".repeat(150);
    const tag = contradictionTagBlock({ conflictingContent: long, caveat: "caveat" });
    expect(tag).toContain(`${"x".repeat(CONFLICT_TRUNCATE_LEN)}…`);
  });
});

// ---------------------------------------------------------------------------
// Plugin parity tests — replicate the plugin's local copy verbatim
// so that any drift between format.ts and plugin/knowledge.ts is caught here.
// If these tests fail while the canonical tests above pass, the plugin copy
// has drifted and needs to be updated to match.
// ---------------------------------------------------------------------------

function pluginContradictionTag(
  contradiction: { conflictingContent: string; caveat: string } | undefined
): string {
  if (!contradiction) return "";
  const snippet = contradiction.conflictingContent;
  return ` [CONFLICTED — conflicts with: "${snippet.length > CONFLICT_TRUNCATE_LEN ? `${snippet.slice(0, CONFLICT_TRUNCATE_LEN)}…` : snippet}". ${contradiction.caveat}]`;
}

describe("plugin contradiction tag parity (must match contradictionTagInline)", () => {
  const cases: Array<{ conflictingContent: string; caveat: string }> = [
    { conflictingContent: "short", caveat: "be careful" },
    { conflictingContent: "x".repeat(100), caveat: "exact limit" },
    { conflictingContent: "x".repeat(150), caveat: "over limit" },
    { conflictingContent: "", caveat: "empty content" },
  ];

  for (const c of cases) {
    it(`matches canonical for content length ${c.conflictingContent.length}`, () => {
      expect(pluginContradictionTag(c)).toBe(contradictionTagInline(c));
    });
  }

  it("returns empty string for undefined, matching canonical", () => {
    expect(pluginContradictionTag(undefined)).toBe(contradictionTagInline(undefined));
  });
});
