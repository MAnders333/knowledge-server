import { describe, expect, it } from "bun:test";
import {
	containsInjectedKnowledge,
	escapeUntrusted,
	inlineUntrusted,
	scrubUntrusted,
	stripInjectedKnowledge,
	untrustedField,
	untrustedLine,
	untrustedList,
} from "../src/consolidation/untrusted";

const INJECTED =
	"<addrl-activated-knowledge>\n## Recalled\n- [fact] Deploys on Thursdays\n</addrl-activated-knowledge>";

/** Runs `fn` and returns the elapsed milliseconds. */
function timed(fn: () => unknown): number {
	const start = performance.now();
	fn();
	return performance.now() - start;
}

/** Hostile inputs at 1 MB. Each one is a shape a regex with nested or adjacent runs handles badly. */
const MB = 1_000_000;
const ADVERSARIAL: Record<string, string> = {
	"< then whitespace run": `<${" ".repeat(MB)}x`,
	"tabs and slashes after <": `<${"\t/".repeat(MB / 2)}x`,
	"injected tag then whitespace run": `<addrl-activated-knowledge${" ".repeat(MB)}`,
	"many < each followed by a short run": "< ".repeat(MB / 2),
	"ZWJ run between ASCII": `a${"\u200D".repeat(MB / 2)}b`,
};

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

describe("escapeUntrusted: joiners and long whitespace", () => {
	it("encodes a guarded tag whose name has ZWJ or ZWNJ inserted between ASCII letters", () => {
		expect(escapeUntrusted("</epi\u200Dsode-content>")).toBe(
			"&lt;/episode-content>",
		);
		expect(escapeUntrusted("</epi\u200Csode-content>")).toBe(
			"&lt;/episode-content>",
		);
		expect(escapeUntrusted("<addrl\u200D-activated-knowledge>")).toBe(
			"&lt;addrl-activated-knowledge>",
		);
	});

	it("keeps ZWJ between non-ASCII text (emoji sequences, Persian)", () => {
		const emoji = "\u{1F468}\u200D\u{1F4BB}";
		expect(scrubUntrusted(emoji)).toBe(emoji);
		const persian = "\u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645";
		expect(scrubUntrusted(persian)).toBe(persian);
	});

	it("encodes a guarded tag even when its gap is longer than 32 characters", () => {
		expect(escapeUntrusted(`<${" ".repeat(40)}/episode-content>`)).toBe(
			`&lt;${" ".repeat(40)}/episode-content>`,
		);
	});

	it("runs in linear time on 1 MB adversarial input", () => {
		for (const [name, text] of Object.entries(ADVERSARIAL)) {
			expect(
				timed(() => escapeUntrusted(text)),
				name,
			).toBeLessThan(500);
		}
	});
});

describe("stripInjectedKnowledge", () => {
	it("removes the block and keeps the text around it", () => {
		expect(stripInjectedKnowledge(`Before.\n${INJECTED}\nAfter.`)).toBe(
			"Before.\n\nAfter.",
		);
	});

	it("returns the text unchanged when there is no tag", () => {
		const text = "plain text with <div> and Vec<String>";
		expect(stripInjectedKnowledge(text)).toBe(text);
	});

	it("leaves an empty result when the message is only the block", () => {
		expect(stripInjectedKnowledge(INJECTED).trim()).toBe("");
	});

	it("removes a tag whose name is split by a ZWJ", () => {
		const split = INJECTED.replace(
			"<addrl-activated-knowledge>",
			"<addrl\u200D-activated-knowledge>",
		);
		expect(stripInjectedKnowledge(`Hi ${split} there`)).toBe("Hi  there");
	});

	it("removes a stray closing tag and keeps the rest", () => {
		expect(
			stripInjectedKnowledge("note </addrl-activated-knowledge> done"),
		).toBe("note  done");
	});

	it("removes everything after an opening tag that is never closed", () => {
		expect(
			stripInjectedKnowledge("keep <addrl-activated-knowledge> drop"),
		).toBe("keep ");
	});

	it("keeps two separate blocks' surroundings and removes both", () => {
		expect(stripInjectedKnowledge(`a ${INJECTED} b ${INJECTED} c`)).toBe(
			"a  b  c",
		);
	});

	it("does not swallow text after a stray '<' followed by the tag name", () => {
		const text = "x <addrl-activated-knowledge without close and <b>bold</b>";
		expect(stripInjectedKnowledge(text).startsWith("x ")).toBe(true);
	});

	it("runs in linear time on 1 MB adversarial input", () => {
		for (const [name, text] of Object.entries(ADVERSARIAL)) {
			expect(
				timed(() => stripInjectedKnowledge(text)),
				name,
			).toBeLessThan(500);
		}
	});
});

describe("containsInjectedKnowledge", () => {
	it("detects the opening and closing tag", () => {
		expect(containsInjectedKnowledge("<addrl-activated-knowledge>\nfoo")).toBe(
			true,
		);
		expect(containsInjectedKnowledge("bar\n</addrl-activated-knowledge>")).toBe(
			true,
		);
	});

	it("detects the tag when invisible characters are inserted", () => {
		expect(containsInjectedKnowledge("<addrl-activated-\u200Bknowledge>")).toBe(
			true,
		);
	});

	it("ignores unrelated text", () => {
		expect(containsInjectedKnowledge("knowledge base and <div> tags")).toBe(
			false,
		);
	});

	it("still detects a tag with spaces and slashes inside the brackets", () => {
		expect(containsInjectedKnowledge("< / addrl-activated-knowledge>")).toBe(
			true,
		);
	});

	it("answers quickly on a long whitespace run after '<'", () => {
		const hostile = `<${" ".repeat(200_000)}x`;
		const started = performance.now();
		expect(containsInjectedKnowledge(hostile)).toBe(false);
		expect(performance.now() - started).toBeLessThan(250);
	});
});
