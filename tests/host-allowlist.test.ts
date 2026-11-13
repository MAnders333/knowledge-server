import { describe, expect, it } from "bun:test";
import { buildHostAllowlist } from "../src/api/host-allowlist";
import { parseAllowedHosts } from "../src/config";

describe("buildHostAllowlist: Host header", () => {
	const allow = buildHostAllowlist("127.0.0.1", []);

	it("accepts loopback names with and without a port", () => {
		for (const host of [
			"localhost",
			"localhost:3179",
			"127.0.0.1",
			"127.0.0.1:3179",
			"[::1]",
			"[::1]:3179",
			"LOCALHOST:3179",
		]) {
			expect({ host, ok: allow.matchesHost(host) }).toEqual({ host, ok: true });
		}
	});

	it("refuses rebinding names and lookalikes", () => {
		for (const host of [
			"evil.example",
			"evil.example:3179",
			"localhost.evil.example",
			"127.0.0.1.evil.example",
			"127.0.0.2",
			"::2",
			"",
		]) {
			expect({ host, ok: allow.matchesHost(host) }).toEqual({
				host,
				ok: false,
			});
		}
	});

	it("refuses values that could be re-read as a different authority", () => {
		for (const host of [
			"evil.example@127.0.0.1",
			"127.0.0.1/evil",
			"127.0.0.1?x",
			"127.0.0.1#x",
			"127.0.0.1 evil.example",
			"127.0.0.1:99999",
			"127.0.0.1:0",
		]) {
			expect({ host, ok: allow.matchesHost(host) }).toEqual({
				host,
				ok: false,
			});
		}
	});

	it("uses the configured bind host", () => {
		const custom = buildHostAllowlist("[::1]", []);
		expect(custom.matchesHost("[::1]:3179")).toBe(true);
		expect(custom.matchesHost("localhost")).toBe(true);
	});

	it("matches extra hosts by name, with an optional port restriction", () => {
		const extra = buildHostAllowlist("127.0.0.1", [
			"knowledge.example.com",
			"proxy.example.net:8443",
		]);
		expect(extra.matchesHost("knowledge.example.com")).toBe(true);
		expect(extra.matchesHost("knowledge.example.com:443")).toBe(true);
		expect(extra.matchesHost("proxy.example.net:8443")).toBe(true);
		expect(extra.matchesHost("proxy.example.net:9000")).toBe(false);
		expect(extra.matchesHost("proxy.example.net")).toBe(false);
	});

	it("throws on a malformed extra entry", () => {
		expect(() => buildHostAllowlist("127.0.0.1", ["bad host"])).toThrow(
			/KNOWLEDGE_ALLOWED_HOSTS/,
		);
		expect(() => buildHostAllowlist("127.0.0.1", ["a/b"])).toThrow();
	});
});

describe("buildHostAllowlist: Origin header", () => {
	const allow = buildHostAllowlist("127.0.0.1", ["knowledge.example.com"]);

	it("accepts http(s) origins on allowed hosts", () => {
		for (const origin of [
			"http://127.0.0.1:3179",
			"http://localhost",
			"https://knowledge.example.com",
			"http://[::1]:3179",
		]) {
			expect({ origin, ok: allow.matchesOrigin(origin) }).toEqual({
				origin,
				ok: true,
			});
		}
	});

	it("refuses foreign, opaque, and non-http origins", () => {
		for (const origin of [
			"http://evil.example",
			"https://evil.example:3179",
			"http://localhost.evil.example",
			"null",
			"",
			"file:///etc/passwd",
			"ws://127.0.0.1:3179",
		]) {
			expect({ origin, ok: allow.matchesOrigin(origin) }).toEqual({
				origin,
				ok: false,
			});
		}
	});
});

describe("buildHostAllowlist: MCP SDK header list", () => {
	it("lists each allowed name bare and with the server port", () => {
		const values = buildHostAllowlist("127.0.0.1", [
			"proxy.example.net:8443",
		]).sdkHostHeaders(3179);
		expect(values).toContain("localhost:3179");
		expect(values).toContain("localhost");
		expect(values).toContain("127.0.0.1:3179");
		expect(values).toContain("[::1]:3179");
		expect(values).toContain("proxy.example.net:8443");
		expect(values).not.toContain("proxy.example.net:3179");
	});
});

describe("parseAllowedHosts", () => {
	it("splits, trims, lower-cases and drops empty entries", () => {
		expect(parseAllowedHosts(" Foo.example , bar.example:8080,, ")).toEqual([
			"foo.example",
			"bar.example:8080",
		]);
		expect(parseAllowedHosts(undefined)).toEqual([]);
		expect(parseAllowedHosts("")).toEqual([]);
	});
});
