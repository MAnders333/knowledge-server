import { describe, expect, it } from "bun:test";
import {
	escapeUntrusted,
	inlineUntrusted,
	scrubUntrusted,
	untrustedField,
	untrustedLine,
	untrustedList,
} from "../src/consolidation/untrusted";

describe("scrubUntrusted", () => {
	it("removes zero-width spaces, bidi overrides and isolates, BOMs and soft hyphens", () => {
		expect(scrubUntrusted("a\u200Bb\u202Ec\u2066d\uFEFFe\u00ADf")).toBe(
			"abcdef",
		);
	});

	it("removes control characters and NUL but keeps tab, newline and carriage return", () => {
		expect(scrubUntrusted("a\u0000b\u001Bc\td\ne\rf")).toBe("abc\td\ne\rf");
	});

	it("removes the Unicode tag block", () => {
		// "ok" hidden as tag characters between visible text.
		const hidden = [..."ok"].map((c) =>
			String.fromCodePoint(0xe0000 + c.codePointAt(0) ?? 0),
		);
		expect(scrubUntrusted(`visible ${hidden.join("")} text`)).toBe(
			"visible  text",
		);
	});

	it("removes private-use characters", () => {
		expect(scrubUntrusted("a\uE000b")).toBe("ab");
	});

	it("keeps ordinary text, accents, CJK, RTL letters and emoji sequences", () => {
		const benign = [
			"Café naïve résumé",
			"日本語のテキスト",
			"שלום עולם",
			"مرحبا بالعالم",
			"👨‍👩‍👧 family and 🇩🇪 flag",
			"tabs\tand\nnewlines",
		].join("\n");
		expect(scrubUntrusted(benign)).toBe(benign);
	});
});

describe("escapeUntrusted", () => {
	it("encodes a literal closing tag for a guarded name", () => {
		expect(escapeUntrusted("x </episode-content> y")).toBe(
			"x &lt;/episode-content> y",
		);
	});

	it("encodes opening tags and tolerates case and whitespace", () => {
		expect(escapeUntrusted("<EPISODE-CONTENT> and < /new_content >")).toBe(
			"&lt;EPISODE-CONTENT> and &lt; /new_content >",
		);
	});

	it("encodes a guarded tag even when invisible characters were inserted into it", () => {
		expect(escapeUntrusted("</epi\u200Bsode-content>")).toBe(
			"&lt;/episode-content>",
		);
	});

	it("leaves code-like and HTML-like angle brackets alone", () => {
		const code = "Vec<String>, a<b, <div>x</div>, <tag-name>";
		expect(escapeUntrusted(code)).toBe(code);
	});

	it("is idempotent", () => {
		const once = escapeUntrusted("</peer_content><peer_topics>");
		expect(escapeUntrusted(once)).toBe(once);
	});

	it("keeps line structure of multi-line content", () => {
		expect(escapeUntrusted("a\n</existing_content>\nb")).toBe(
			"a\n&lt;/existing_content>\nb",
		);
	});
});

describe("inlineUntrusted and wrappers", () => {
	it("collapses whitespace, including newlines, to single spaces", () => {
		expect(inlineUntrusted("Churn\n\nAnalysis\tQ1")).toBe("Churn Analysis Q1");
	});

	it("wraps a value in its tag with escaping", () => {
		expect(untrustedField("new_content", "a </new_content> b")).toBe(
			"<new_content>a &lt;/new_content> b</new_content>",
		);
	});

	it("keeps a multi-line value inside one single-line field", () => {
		expect(untrustedLine("episode-title", "T\nitle")).toBe(
			"<episode-title>T itle</episode-title>",
		);
	});

	it("JSON-encodes topic lists so newlines cannot start new lines", () => {
		const out = untrustedList("existing_topics", [
			"a\nb",
			"  ",
			"</peer_topics>",
		]);
		expect(out).toBe(
			'<existing_topics>["a\\nb","&lt;/peer_topics>"]</existing_topics>',
		);
		expect(out.includes("\n")).toBe(false);
	});
});
