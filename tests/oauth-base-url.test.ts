/**
 * Claude subscription OAuth tokens are Anthropic bearer credentials: they may be
 * sent only to the official API. The anthropic provider calls this before it
 * attaches a token.
 */
import { describe, expect, it } from "bun:test";
import { assertOAuthBaseUrl } from "../src/auth/claude-oauth";

function refusalFor(url: string): string {
	try {
		assertOAuthBaseUrl(url);
	} catch (e) {
		return e instanceof Error ? e.message : String(e);
	}
	return "";
}

describe("assertOAuthBaseUrl", () => {
	it("allows the SDK default and the official API over https", () => {
		expect(() => assertOAuthBaseUrl(undefined)).not.toThrow();
		expect(() =>
			assertOAuthBaseUrl("https://api.anthropic.com/v1"),
		).not.toThrow();
	});

	it.each([
		"https://gateway.example.org/anthropic/v1",
		"http://api.anthropic.com/v1",
		"https://api.anthropic.com.attacker.example/v1",
		"https://user:hunter2@proxy.example.org/anthropic/v1",
	])("refuses %s and names only the scheme and host", (url) => {
		const message = refusalFor(url);
		expect(message).toContain("only sent to api.anthropic.com");
		expect(message).not.toContain("hunter2");
		expect(message).not.toContain("/anthropic/v1");
	});

	it("refuses an unparseable base URL without echoing it", () => {
		const message = refusalFor("not a url at all");
		expect(message).toContain("not a valid URL");
		expect(message).not.toContain("not a url at all");
	});
});
