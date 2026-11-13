/**
 * The activation_event columns, defined once for both state DB backends.
 *
 * CREATE TABLE, ALTER TABLE ... ADD COLUMN and the INSERT column lists are all
 * built from ACTIVATION_EVENT_COLUMNS. A new field is one entry here, with
 * `since` set to the schema version that adds it, plus a migration in each
 * backend that calls addActivationEventColumns(..., version).
 */

import type { ActivationEventRow } from "../../types.js";

export type ActivationEventValue = string | number | boolean | null;

export interface ActivationEventColumn {
	readonly name: string;
	/** Schema version that introduced the column. */
	readonly since: number;
	/** Type and constraints after the name, SQLite dialect. */
	readonly sqlite: string;
	/** Type and constraints after the name, Postgres dialect. */
	readonly pg: string;
	/**
	 * Stored value. Arrays are JSON text. Booleans are passed through; the SQLite
	 * backend binds them as 1 or 0.
	 */
	readonly value: (row: ActivationEventRow) => ActivationEventValue;
}

export const ACTIVATION_EVENT_COLUMNS: readonly ActivationEventColumn[] = [
	{
		name: "ts",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "BIGINT NOT NULL",
		value: (r) => r.ts,
	},
	{
		name: "client",
		since: 3,
		sqlite: "TEXT NOT NULL",
		pg: "TEXT NOT NULL",
		value: (r) => r.client,
	},
	{
		name: "session_hash",
		since: 3,
		sqlite: "TEXT",
		pg: "TEXT",
		value: (r) => r.sessionHash,
	},
	{
		name: "prompt_chars",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "INTEGER NOT NULL",
		value: (r) => r.promptChars,
	},
	{
		name: "cue_count",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "INTEGER NOT NULL",
		value: (r) => r.cueCount,
	},
	{
		name: "n_candidates",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "INTEGER NOT NULL",
		value: (r) => r.nCandidates,
	},
	{
		name: "n_returned",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "INTEGER NOT NULL",
		value: (r) => r.nReturned,
	},
	{
		name: "entry_ids",
		since: 3,
		sqlite: "TEXT NOT NULL",
		pg: "TEXT NOT NULL",
		value: (r) => JSON.stringify(r.entryIds),
	},
	{
		name: "raw_sims",
		since: 3,
		sqlite: "TEXT NOT NULL",
		pg: "TEXT NOT NULL",
		value: (r) => JSON.stringify(r.rawSims),
	},
	{
		name: "rerank_scores",
		since: 3,
		sqlite: "TEXT",
		pg: "TEXT",
		value: (r) =>
			r.rerankScores === null ? null : JSON.stringify(r.rerankScores),
	},
	{
		name: "gate_mode",
		since: 3,
		sqlite: "TEXT NOT NULL CHECK(gate_mode IN ('rerank', 'dense', 'off'))",
		pg: "TEXT NOT NULL CHECK (gate_mode IN ('rerank', 'dense', 'off'))",
		value: (r) => r.gateMode,
	},
	{
		name: "latency_ms",
		since: 3,
		sqlite: "INTEGER NOT NULL",
		pg: "INTEGER NOT NULL",
		value: (r) => r.latencyMs,
	},
	{
		name: "client_deadline_exceeded",
		since: 3,
		sqlite: "INTEGER NOT NULL DEFAULT 0",
		pg: "BOOLEAN NOT NULL DEFAULT FALSE",
		value: (r) => r.clientDeadlineExceeded,
	},
	{
		name: "gate_dropped",
		since: 4,
		sqlite: "INTEGER NOT NULL DEFAULT 0",
		pg: "INTEGER NOT NULL DEFAULT 0",
		value: (r) => r.gateDropped,
	},
	{
		name: "budget_dropped",
		since: 4,
		sqlite: "INTEGER NOT NULL DEFAULT 0",
		pg: "INTEGER NOT NULL DEFAULT 0",
		value: (r) => r.budgetDropped,
	},
	{
		name: "deduped",
		since: 4,
		sqlite: "INTEGER NOT NULL DEFAULT 0",
		pg: "INTEGER NOT NULL DEFAULT 0",
		value: (r) => r.deduped,
	},
	{
		name: "injected_tokens",
		since: 4,
		sqlite: "INTEGER NOT NULL DEFAULT 0",
		pg: "INTEGER NOT NULL DEFAULT 0",
		value: (r) => r.injectedTokens,
	},
	{
		name: "rerank_model",
		since: 5,
		sqlite: "TEXT",
		pg: "TEXT",
		value: (r) => r.rerankModel,
	},
];

/**
 * Column definitions for CREATE TABLE activation_event, excluding the id primary
 * key (each backend writes its own). Includes only the columns introduced at or
 * before `version`; all of them when omitted.
 */
export function activationEventColumnDefs(
	dialect: "sqlite" | "pg",
	version = Number.POSITIVE_INFINITY,
): string {
	return ACTIVATION_EVENT_COLUMNS.filter((c) => c.since <= version)
		.map((c) => `${c.name} ${dialect === "sqlite" ? c.sqlite : c.pg}`)
		.join(",\n    ");
}
