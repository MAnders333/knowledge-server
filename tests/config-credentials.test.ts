/**
 * Credential routing and admin-token validation.
 *
 * resolveScopedApiKey is pure. validateConfig reads the frozen config
 * singleton, so the admin-token cases mutate it and restore it afterwards.
 */
import { afterEach, describe, expect, it } from "bun:test";
import { config, resolveScopedApiKey, validateConfig } from "../src/config";

// `config` is typed readonly (as const); tests swap the admin token at runtime.
const mutableConfig = config as unknown as { adminToken: string | null };

describe("resolveScopedApiKey", () => {
	const llmEndpoint = "https://llm-proxy.example.com";

	it("uses the dedicated key for a dedicated endpoint", () => {
		expect(
			resolveScopedApiKey({
				endpointUrl: "https://embed.example.org/v1",
				dedicatedApiKey: "embed-key",
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("embed-key");
	});

	it("never forwards LLM_API_KEY to a dedicated endpoint on another origin", () => {
		expect(
			resolveScopedApiKey({
				endpointUrl: "https://embed.example.org/v1",
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("");
		expect(
			resolveScopedApiKey({
				endpointUrl: "http://localhost:11434/v1",
				llmApiKey: "llm-key",
			}),
		).toBe("");
	});

	it("forwards LLM_API_KEY when the endpoint shares LLM_BASE_ENDPOINT's origin", () => {
		expect(
			resolveScopedApiKey({
				endpointUrl: `${llmEndpoint}/openai/v1`,
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("llm-key");
	});

	it("gives the gateway's systemone route the LLM key, and direct TypeSafe only its own key", () => {
		expect(
			resolveScopedApiKey({
				endpointUrl: `${llmEndpoint}/decisions/v1/systemone`,
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("llm-key");
		expect(
			resolveScopedApiKey({
				endpointUrl: "https://api.typesafe.ai/v1/systemone",
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("");
		expect(
			resolveScopedApiKey({
				endpointUrl: "https://api.typesafe.ai/v1/systemone",
				dedicatedApiKey: "typesafe-key",
				llmBaseEndpoint: llmEndpoint,
				llmApiKey: "llm-key",
			}),
		).toBe("typesafe-key");
	});

	it("does not treat a lookalike host or a different port as the same origin", () => {
		for (const endpointUrl of [
			"https://llm-proxy.example.com.evil.net/v1",
			"https://llm-proxy.example.com:8443/v1",
			"http://llm-proxy.example.com/v1",
		]) {
			expect(
				resolveScopedApiKey({
					endpointUrl,
					llmBaseEndpoint: llmEndpoint,
					llmApiKey: "llm-key",
				}),
			).toBe("");
		}
	});

	it("returns an empty key when no endpoint is configured", () => {
		expect(resolveScopedApiKey({ llmApiKey: "llm-key" })).toBe("");
	});
});

describe("validateConfig: admin token", () => {
	const saved = config.adminToken;

	afterEach(() => {
		mutableConfig.adminToken = saved;
	});

	function adminErrors(): string[] {
		return validateConfig().filter((e) => e.includes("KNOWLEDGE_ADMIN_TOKEN"));
	}

	it("rejects the documented placeholder", () => {
		mutableConfig.adminToken = "your-stable-token-here";
		const errors = adminErrors();
		expect(errors.length).toBe(1);
		expect(errors[0]).toMatch(/placeholder/);
	});

	it("rejects tokens shorter than 16 characters", () => {
		mutableConfig.adminToken = "short-token-1";
		expect(adminErrors()[0]).toMatch(/at least 16 characters/);
	});

	it("accepts a 16-character or longer non-placeholder token", () => {
		mutableConfig.adminToken = "fixture-token-value-ok";
		expect(adminErrors()).toEqual([]);
	});
});
