import { describe, expect, it } from "bun:test";
import { formatLlmError } from "../src/consolidation/llm";
import { providerErrorLabel, providerErrorSuffix } from "../src/provider-error";

// Bodies that echo the request are assembled as data, the way a gateway returns them.
const ECHO = "PROMPT-TEXT-THAT-MUST-NOT-LEAK";

describe("providerErrorLabel", () => {
	it("reads the error code in the OpenAI shape", () => {
		expect(
			providerErrorLabel(
				JSON.stringify({
					error: { message: ECHO, code: "rate_limit_exceeded" },
				}),
			),
		).toBe("rate_limit_exceeded");
	});

	it("reads the error type in the Anthropic shape", () => {
		expect(
			providerErrorLabel(
				JSON.stringify({
					type: "error",
					error: { type: "invalid_request_error", message: ECHO },
				}),
			),
		).toBe("invalid_request_error");
	});

	it("reads the type of the first item in a FastAPI detail array, not its message", () => {
		const body = JSON.stringify({
			detail: [{ loc: ["body", "input"], msg: ECHO, type: "string_type" }],
		});
		expect(providerErrorLabel(body)).toBe("string_type");
	});

	it("returns null for free text, HTML and a plain detail string", () => {
		expect(providerErrorLabel(ECHO)).toBeNull();
		expect(providerErrorLabel("<html>bad gateway</html>")).toBeNull();
		expect(providerErrorLabel(JSON.stringify({ detail: ECHO }))).toBeNull();
	});

	it("drops a label that is not a plain identifier", () => {
		expect(
			providerErrorLabel(JSON.stringify({ error: { code: `${ECHO} here` } })),
		).toBeNull();
	});

	it("suffix is empty when there is no label", () => {
		expect(providerErrorSuffix(ECHO)).toBe("");
		expect(
			providerErrorSuffix(JSON.stringify({ error: { code: "x_1" } })),
		).toBe(" (error: x_1)");
	});
});

describe("formatLlmError", () => {
	it("keeps the class, status and provider code of an SDK retry error, not the body", () => {
		const apiError = Object.assign(new Error("Unprocessable entity"), {
			name: "AI_APICallError",
			statusCode: 422,
			responseBody: JSON.stringify({
				detail: [{ loc: ["body", "input"], msg: ECHO, type: "string_type" }],
			}),
		});
		const retryError = Object.assign(new Error("Failed after retries"), {
			name: "AI_RetryError",
			lastError: apiError,
		});
		const text = formatLlmError(retryError);
		expect(text).toBe("AI_APICallError HTTP 422 (error: string_type)");
		expect(text).not.toContain(ECHO);
	});

	it("does not include the message of an error that has no status", () => {
		const err = new Error(`model output echoed: ${ECHO}`);
		expect(formatLlmError(err)).toBe("Error");
	});
});
