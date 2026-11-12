import { describe, expect, it } from "bun:test";
import { CLIENT_TAGS, parseClientTag } from "../src/activation/client";

describe("parseClientTag", () => {
	it("returns known tags as given", () => {
		for (const tag of CLIENT_TAGS) {
			expect(parseClientTag(tag)).toBe(tag);
		}
	});

	it("is case- and whitespace-insensitive", () => {
		expect(parseClientTag("  OpenCode ")).toBe("opencode");
		expect(parseClientTag("Claude-Code")).toBe("claude-code");
	});

	it("maps missing or empty values to unknown", () => {
		expect(parseClientTag(undefined)).toBe("unknown");
		expect(parseClientTag(null)).toBe("unknown");
		expect(parseClientTag("")).toBe("unknown");
		expect(parseClientTag("   ")).toBe("unknown");
	});

	it("maps unrecognised values to unknown rather than rejecting", () => {
		expect(parseClientTag("some-random-client")).toBe("unknown");
		expect(parseClientTag("opencode;drop table")).toBe("unknown");
	});
});
