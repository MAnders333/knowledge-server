/**
 * Tests for formatMessages in src/daemon/readers/shared.ts.
 *
 * Covers the role labels the extraction prompt sees: user and assistant turns
 * keep their labels, and tool output is marked as third-party text so it is
 * never read as a user turn.
 */
import { describe, expect, it } from "bun:test";
import { formatMessages } from "../src/daemon/readers/shared";

describe("formatMessages", () => {
	it("labels user and assistant turns", () => {
		const out = formatMessages([
			{ messageId: "m1", role: "user", content: "hi", timestamp: 0 },
			{ messageId: "m2", role: "assistant", content: "hello", timestamp: 1 },
		]);
		expect(out).toBe("  user: hi\n  assistant: hello");
	});

	it("renders a tool-role message as tool output, never as user", () => {
		const out = formatMessages([
			{
				messageId: "m1",
				role: "tool",
				content: "[tool: read]\nfile body",
				timestamp: 0,
			},
		]);
		expect(out).toBe(
			"  tool output (third-party text): [tool: read]\nfile body",
		);
		expect(out).not.toContain("user:");
	});
});
