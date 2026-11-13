/**
 * Backend-neutral decision for a contradiction resolution: which entry gets which
 * status, which links are written, and which vectors are cleared. It is pure.
 *
 * A backend reads the two conflict partners, calls planContradictionResolution,
 * and executes the ops with its own SQL inside its own transaction. The SQL
 * (and the vector columns) stay in the backends.
 */
import { type KnowledgeType, clampKnowledgeType } from "../../types.js";

export type ContradictionResolution =
	| "supersede_old"
	| "supersede_new"
	| "merge"
	| "irresolvable";

export interface ContradictionMergedData {
	content: string;
	type: string;
	topics: string[];
	confidence: number;
}

/**
 * The entry on the other end of a `contradicts` relation, per entry (null when
 * none). Read before any status changes: the plan needs them to undo a conflict.
 */
export interface ConflictPartners {
	newPartner: string | null;
	existingPartner: string | null;
}

export type ContradictionOp =
	| {
			kind: "mergeContent";
			id: string;
			content: string;
			type: KnowledgeType;
			topics: string[];
			confidence: number;
			at: number;
	  }
	| { kind: "supersede"; id: string; supersededBy: string; at: number }
	| {
			kind: "link";
			relation: "supersedes" | "contradicts";
			sourceId: string;
			targetId: string;
			at: number;
	  }
	/** Counterpart back to active (if still conflicted) and its contradicts link to `resolvedId` removed. */
	| {
			kind: "restoreCounterpart";
			counterpartId: string;
			resolvedId: string;
			at: number;
	  }
	/** `id` back to active, only when it is still conflicted. */
	| { kind: "activateIfConflicted"; id: string; at: number }
	| { kind: "markConflicted"; ids: [string, string]; at: number };

export interface ContradictionPlan {
	ops: ContradictionOp[];
	/**
	 * merge was requested without merged data. The new entry keeps its content and
	 * is not settled; the caller logs this.
	 */
	missingMergedData: boolean;
}

/**
 * Decide the writes for one resolution. Ops run in order.
 *
 * - supersede_old: the new entry wins; the existing one is superseded.
 * - supersede_new: the existing entry wins; the new one is superseded.
 * - merge: the new entry takes the merged content and the existing one is superseded.
 * - irresolvable: both entries are conflicted and linked as contradicting.
 *
 * A winner that was itself conflicted is settled by a supersede or a merge with
 * merged data, and its partner is restored. A loser's partner is restored too.
 */
export function planContradictionResolution(
	resolution: ContradictionResolution,
	newId: string,
	existingId: string,
	partners: ConflictPartners,
	mergedData: ContradictionMergedData | undefined,
	at: number,
): ContradictionPlan {
	switch (resolution) {
		case "supersede_old":
			return {
				missingMergedData: false,
				ops: supersession({
					winner: newId,
					loser: existingId,
					loserPartner: partners.existingPartner,
					winnerPartner: partners.newPartner,
					at,
				}),
			};
		case "supersede_new":
			return {
				missingMergedData: false,
				ops: supersession({
					winner: existingId,
					loser: newId,
					loserPartner: partners.newPartner,
					winnerPartner: partners.existingPartner,
					at,
				}),
			};
		case "merge": {
			const ops: ContradictionOp[] = [];
			if (mergedData) {
				ops.push({
					kind: "mergeContent",
					id: newId,
					content: mergedData.content,
					type: clampKnowledgeType(mergedData.type),
					topics: mergedData.topics,
					confidence: mergedData.confidence,
					at,
				});
			}
			ops.push(
				...supersession({
					winner: newId,
					loser: existingId,
					loserPartner: partners.existingPartner,
					// Without merged data the new entry is not settled: it stays conflicted.
					winnerPartner: mergedData ? partners.newPartner : null,
					at,
				}),
			);
			return { ops, missingMergedData: mergedData === undefined };
		}
		case "irresolvable":
			return {
				missingMergedData: false,
				ops: [
					{
						kind: "link",
						relation: "contradicts",
						sourceId: newId,
						targetId: existingId,
						at,
					},
					{ kind: "markConflicted", ids: [newId, existingId], at },
				],
			};
	}
}

function supersession(args: {
	winner: string;
	loser: string;
	loserPartner: string | null;
	winnerPartner: string | null;
	at: number;
}): ContradictionOp[] {
	const { winner, loser, loserPartner, winnerPartner, at } = args;
	const ops: ContradictionOp[] = [
		{ kind: "supersede", id: loser, supersededBy: winner, at },
		{
			kind: "link",
			relation: "supersedes",
			sourceId: winner,
			targetId: loser,
			at,
		},
	];
	if (loserPartner !== null) {
		ops.push({
			kind: "restoreCounterpart",
			counterpartId: loserPartner,
			resolvedId: loser,
			at,
		});
	}
	if (winnerPartner !== null) {
		ops.push({ kind: "activateIfConflicted", id: winner, at });
		ops.push({
			kind: "restoreCounterpart",
			counterpartId: winnerPartner,
			resolvedId: winner,
			at,
		});
	}
	return ops;
}
