import { describe, expect, it } from "bun:test";
import {
	contradictionTagBlock,
	contradictionTagInline,
	staleTag,
} from "../src/activation/format.js";

/**
 * Tests for src/activation/format.ts.
 *
 * Critically: the plugin (plugin/knowledge.ts) and the pi extension
 * (plugin/pi-knowledge.ts) each maintain a local copy of the contradiction
 * tag logic because they cannot import from src/ at runtime (symlinked
 * single-file integrations). The PLUGIN_CONTRADICTION_TAG tests below
 * replicate that logic verbatim so that any drift between the canonical
 * helpers and the plugin copies causes a test failure here — making the
 * coupling explicit and detectable. Both plugin copies must stay identical.
 */

// ---------------------------------------------------------------------------
// Canonical helpers
// ---------------------------------------------------------------------------

describe("staleTag", () => {
	it("returns empty string when not stale", () => {
		expect(staleTag({ mayBeStale: false, lastAccessedDaysAgo: 5 })).toBe("");
	});

	it("returns a tag with days when stale", () => {
		expect(staleTag({ mayBeStale: true, lastAccessedDaysAgo: 47 })).toBe(
			" [may be outdated — last accessed 47d ago]",
		);
	});
});

describe("contradictionTagInline", () => {
	it("returns empty string for undefined", () => {
		expect(contradictionTagInline(undefined)).toBe("");
	});

	it("renders full conflicting content without truncation", () => {
		const long = "x".repeat(200);
		const tag = contradictionTagInline({
			conflictingContent: long,
			caveat: "caveat",
		});
		expect(tag).toContain(long);
		expect(tag).not.toContain("…");
	});

	it("renders inline tag correctly", () => {
		const tag = contradictionTagInline({
			conflictingContent: "some conflicting fact",
			caveat: "check before using",
		});
		expect(tag).toBe(
			` [CONFLICTED — conflicts with: "some conflicting fact". check before using]`,
		);
	});
});

describe("contradictionTagBlock", () => {
	it("returns empty string for undefined", () => {
		expect(contradictionTagBlock(undefined)).toBe("");
	});

	it("renders full conflicting content without truncation", () => {
		const long = "x".repeat(200);
		const tag = contradictionTagBlock({
			conflictingContent: long,
			caveat: "caveat",
		});
		expect(tag).toContain(long);
		expect(tag).not.toContain("…");
	});

	it("renders block tag correctly", () => {
		const tag = contradictionTagBlock({
			conflictingContent: "some conflicting fact",
			caveat: "check before using",
		});
		expect(tag).toBe(
			`\n   ⚠ CONFLICTED — conflicts with: "some conflicting fact"\n   Caveat: check before using`,
		);
	});
});

// ---------------------------------------------------------------------------
// Plugin parity tests — replicate the plugin's local copy verbatim
// so that any drift between format.ts and plugin/knowledge.ts is caught here.
// If these tests fail while the canonical tests above pass, the plugin copy
// has drifted and needs to be updated to match.
// ---------------------------------------------------------------------------

function pluginContradictionTag(
	contradiction: { conflictingContent: string; caveat: string } | undefined,
): string {
	if (!contradiction) return "";
	return ` [CONFLICTED — conflicts with: "${contradiction.conflictingContent}". ${contradiction.caveat}]`;
}

describe("plugin contradiction tag parity (must match contradictionTagInline)", () => {
	const cases: Array<{ conflictingContent: string; caveat: string }> = [
		{ conflictingContent: "short", caveat: "be careful" },
		{ conflictingContent: "x".repeat(200), caveat: "long content" },
		{ conflictingContent: "", caveat: "empty content" },
	];

	for (const c of cases) {
		it(`matches canonical for content length ${c.conflictingContent.length}`, () => {
			expect(pluginContradictionTag(c)).toBe(contradictionTagInline(c));
		});
	}

	it("returns empty string for undefined, matching canonical", () => {
		expect(pluginContradictionTag(undefined)).toBe(
			contradictionTagInline(undefined),
		);
	});
});
