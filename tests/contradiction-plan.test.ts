/**
 * Unit tests for the backend-neutral contradiction plan. The store conformance
 * suite checks the same branches end to end on both backends; these pin the op
 * order and the partner handling of the plan itself.
 */
import { describe, expect, it } from "bun:test";
import {
	type ConflictPartners,
	planContradictionResolution,
} from "../src/db/shared/contradiction-plan";

const NONE: ConflictPartners = { newPartner: null, existingPartner: null };
const MERGED = {
	content: "merged",
	type: "Fact/Principle",
	topics: ["m"],
	confidence: 0.9,
};

describe("planContradictionResolution", () => {
	it("supersede_old: the existing entry is superseded by the new one, with a supersedes link", () => {
		expect(
			planContradictionResolution(
				"supersede_old",
				"n",
				"e",
				NONE,
				undefined,
				7,
			),
		).toEqual({
			missingMergedData: false,
			ops: [
				{ kind: "supersede", id: "e", supersededBy: "n", at: 7 },
				{
					kind: "link",
					relation: "supersedes",
					sourceId: "n",
					targetId: "e",
					at: 7,
				},
			],
		});
	});

	it("supersede_new: the new entry is superseded by the existing one", () => {
		const plan = planContradictionResolution(
			"supersede_new",
			"n",
			"e",
			NONE,
			undefined,
			7,
		);
		expect(plan.ops[0]).toEqual({
			kind: "supersede",
			id: "n",
			supersededBy: "e",
			at: 7,
		});
		expect(plan.ops[1]).toEqual({
			kind: "link",
			relation: "supersedes",
			sourceId: "e",
			targetId: "n",
			at: 7,
		});
	});

	it("supersede_new with both partners: each partner is restored against the entry it was conflicted with", () => {
		// n (new) is the loser, so its partner "l" is restored against n; e (existing) wins,
		// so its partner "w" is restored against e and e is settled first.
		expect(
			planContradictionResolution(
				"supersede_new",
				"n",
				"e",
				{ newPartner: "l", existingPartner: "w" },
				undefined,
				7,
			).ops,
		).toEqual([
			{ kind: "supersede", id: "n", supersededBy: "e", at: 7 },
			{
				kind: "link",
				relation: "supersedes",
				sourceId: "e",
				targetId: "n",
				at: 7,
			},
			{
				kind: "restoreCounterpart",
				counterpartId: "l",
				resolvedId: "n",
				at: 7,
			},
			{ kind: "activateIfConflicted", id: "e", at: 7 },
			{
				kind: "restoreCounterpart",
				counterpartId: "w",
				resolvedId: "e",
				at: 7,
			},
		]);
	});

	it("restores the loser's partner before the winner's, and settles a conflicted winner", () => {
		const plan = planContradictionResolution(
			"supersede_old",
			"n",
			"e",
			{ newPartner: "w", existingPartner: "l" },
			undefined,
			7,
		);
		expect(plan.ops.slice(2)).toEqual([
			{
				kind: "restoreCounterpart",
				counterpartId: "l",
				resolvedId: "e",
				at: 7,
			},
			{ kind: "activateIfConflicted", id: "n", at: 7 },
			{
				kind: "restoreCounterpart",
				counterpartId: "w",
				resolvedId: "n",
				at: 7,
			},
		]);
	});

	it("merge with merged data: rewrites the new entry first, normalising its type", () => {
		const plan = planContradictionResolution(
			"merge",
			"n",
			"e",
			{ newPartner: "w", existingPartner: null },
			MERGED,
			7,
		);
		expect(plan.missingMergedData).toBe(false);
		expect(plan.ops[0]).toEqual({
			kind: "mergeContent",
			id: "n",
			content: "merged",
			type: "fact",
			topics: ["m"],
			confidence: 0.9,
			at: 7,
		});
		expect(plan.ops.slice(1).map((op) => op.kind)).toEqual([
			"supersede",
			"link",
			"activateIfConflicted",
			"restoreCounterpart",
		]);
	});

	it("merge without merged data: no content write, the new entry is not settled, and the caller is told", () => {
		const plan = planContradictionResolution(
			"merge",
			"n",
			"e",
			{ newPartner: "w", existingPartner: "l" },
			undefined,
			7,
		);
		expect(plan.missingMergedData).toBe(true);
		expect(plan.ops.map((op) => op.kind)).toEqual([
			"supersede",
			"link",
			"restoreCounterpart",
		]);
	});

	it("irresolvable: links the pair as contradicting and marks both conflicted", () => {
		expect(
			planContradictionResolution(
				"irresolvable",
				"n",
				"e",
				{ newPartner: "x", existingPartner: "y" },
				undefined,
				7,
			),
		).toEqual({
			missingMergedData: false,
			ops: [
				{
					kind: "link",
					relation: "contradicts",
					sourceId: "n",
					targetId: "e",
					at: 7,
				},
				{ kind: "markConflicted", ids: ["n", "e"], at: 7 },
			],
		});
	});
});
