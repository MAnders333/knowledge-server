import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { logger } from "../src/logger";

describe("logger error serialization", () => {
	afterEach(() => {
		mock.restore();
	});

	// Regression for #100: fetch-thrown DOMExceptions have stack === "" in Bun,
	// which previously rendered as an empty string.
	function captureError(err: unknown): string {
		const write = spyOn(process.stderr, "write").mockImplementation(() => true);
		logger.error("[x] failed.", err);
		return write.mock.calls.map((c) => String(c[0])).join("");
	}

	it("renders name and message when the error stack is empty", () => {
		const err = new DOMException("The operation timed out.", "TimeoutError");
		Object.defineProperty(err, "stack", { value: "", configurable: true });
		expect(captureError(err)).toContain(
			"[ERROR] [x] failed. TimeoutError: The operation timed out.",
		);
	});

	it("renders name and message for a DOMException without a stack", () => {
		const err = new DOMException("The operation timed out.", "TimeoutError");
		expect(captureError(err)).toContain(
			"TimeoutError: The operation timed out.",
		);
	});

	it("still renders the stack for ordinary errors", () => {
		expect(captureError(new Error("boom"))).toContain("Error: boom");
	});
});
