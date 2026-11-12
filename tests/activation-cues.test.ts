import { describe, expect, it, mock, spyOn } from "bun:test";
import {
	ActivationEngine,
	MAX_CUES,
	MIN_CUE_CHARS,
	normalizeCues,
	splitIntoCues,
} from "../src/activation/activate";
import type { IKnowledgeStore } from "../src/db/interface";

const key = (s: string) => s.replace(/\s+/g, " ").trim().toLowerCase();

/** 300 lines; every line in positions 3 and 4 of each 5 is a duplicate (40%). */
function syntheticPrompt(): string {
	const lines: string[] = [];
	const uniques: string[] = [];
	for (let i = 0; i < 300; i++) {
		if (i % 5 >= 3) {
			const src = uniques[(i * 7) % uniques.length];
			lines.push(
				i % 2 === 0 ? `  ${src.toUpperCase()}  ` : src.replace(/ /g, "   "),
			);
		} else {
			const u = `Topic ${i}: notes about widget ${(i * 13) % 97} and step ${i}`;
			uniques.push(u);
			lines.push(u);
		}
	}
	return lines.join("\n");
}

describe("splitIntoCues: dedupe and cap", () => {
	it("caps 300 lines with 40% duplicates to MAX_CUES, with no duplicate keys", () => {
		const prompt = syntheticPrompt();
		const cues = splitIntoCues(prompt);
		expect(MAX_CUES).toBe(12);
		expect(cues).toHaveLength(MAX_CUES);
		expect(new Set(cues.map(key)).size).toBe(cues.length);
	});

	it("is deterministic: same input, same output", () => {
		const prompt = syntheticPrompt();
		expect(splitIntoCues(prompt)).toEqual(splitIntoCues(prompt));
	});

	it("keeps the first ceil(MAX/2) and last floor(MAX/2) of the deduped list, full prompt last", () => {
		const prompt = syntheticPrompt();
		const segments = prompt
			.split("\n")
			.map((s) => s.trim())
			.filter((s) => s.length >= MIN_CUE_CHARS);
		const seen = new Set<string>();
		const firstOccurrences = segments.filter((s) => {
			if (seen.has(key(s))) return false;
			seen.add(key(s));
			return true;
		});
		const deduped = [...firstOccurrences, prompt.trim()];
		expect(splitIntoCues(prompt)).toEqual([
			...deduped.slice(0, 6),
			...deduped.slice(-6),
		]);
		expect(splitIntoCues(prompt).at(-1)).toBe(prompt.trim());
	});

	it("leaves short prompts unchanged apart from dedupe", () => {
		expect(splitIntoCues("Just one question about deployment")).toEqual([
			"Just one question about deployment",
		]);
		expect(
			splitIntoCues(
				"Hello there world\nhello there world\nshort\nAnother line of text here",
			),
		).toEqual([
			"Hello there world",
			"Another line of text here",
			"Hello there world\nhello there world\nshort\nAnother line of text here",
		]);
	});
});

describe("normalizeCues", () => {
	it("dedupes by normalised key, keeping the first occurrence's text", () => {
		expect(
			normalizeCues(["Foo  Bar", "foo bar", " FOO\tbar ", "Baz qux"]),
		).toEqual(["Foo  Bar", "Baz qux"]);
	});

	it("drops blank cues", () => {
		expect(normalizeCues(["  ", "", "x"])).toEqual(["x"]);
	});

	it("leaves exactly MAX_CUES unique cues unchanged", () => {
		const cues = Array.from({ length: MAX_CUES }, (_, i) => `cue number ${i}`);
		expect(normalizeCues(cues)).toEqual(cues);
	});

	it("caps 13 unique cues to head 6 + tail 6", () => {
		const cues = Array.from({ length: 13 }, (_, i) => `cue number ${i}`);
		expect(normalizeCues(cues)).toEqual([
			...cues.slice(0, 6),
			...cues.slice(7),
		]);
	});
});

describe("ActivationEngine.activate: q list goes through the same cap", () => {
	it("embeds at most MAX_CUES deduped cues and keeps the full message last", async () => {
		const store = {
			id: "scan",
			scanEmbeddings: mock(async () => []),
		} as unknown as IKnowledgeStore;
		const engine = new ActivationEngine(store);
		const embed = spyOn(engine.embeddings, "embedBatch").mockImplementation(
			async (texts: string[]) => texts.map(() => [1, 0, 0, 0, 0, 0, 0, 0]),
		);
		const q = [
			...Array.from({ length: 30 }, (_, i) => `segment number ${i % 15}  here`),
			"full message text",
		];

		await engine.activate(q);

		expect(embed).toHaveBeenCalledTimes(1);
		const sent = embed.mock.calls[0][0] as string[];
		expect(sent).toHaveLength(MAX_CUES);
		expect(sent.at(-1)).toBe("full message text");
		expect(new Set(sent.map(key)).size).toBe(sent.length);
	});
});
