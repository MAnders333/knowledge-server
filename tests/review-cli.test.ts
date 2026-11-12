import { describe, expect, it } from "bun:test";
import { isBlockedWrite } from "../src/commands/review";

describe("knowledge-server review: writable:false", () => {
	it("blocks delete, archive and edit on an entry from a read-only store", () => {
		for (const answer of ["d", "delete", "a", "archive", "e", "edit"]) {
			expect({ answer, blocked: isBlockedWrite(true, answer) }).toEqual({
				answer,
				blocked: true,
			});
		}
	});

	it("allows keep and quit on a read-only entry, and every action on a writable one", () => {
		for (const answer of ["k", "keep", "", "q", "quit"]) {
			expect(isBlockedWrite(true, answer)).toBe(false);
		}
		for (const answer of ["d", "a", "e"]) {
			expect(isBlockedWrite(false, answer)).toBe(false);
		}
	});
});
