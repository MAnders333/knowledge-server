/**
 * The stages of one activate() call that read no store: scan scoring, candidate
 * merge, rerank, precision gate, session dedupe, support-source expansion,
 * contradiction annotation and the render budget. ActivationEngine (activate.ts)
 * does the store reads and telemetry, and runs these in order.
 */
import { config } from "../config.js";
import { computeStrength } from "../consolidation/decay.js";
import type { IKnowledgeStore } from "../db/index.js";
import { componentHealth } from "../health.js";
import { logger } from "../logger.js";
import type {
	ActivationResult,
	ContradictionAnnotation,
	GateMode,
	KnowledgeEntry,
} from "../types.js";
import { DEFAULT_ACTIVATION_BUDGET_MS } from "./client.js";
import {
	type PreparedQuery,
	cosineToPrepared,
	formatEmbeddingText,
	storedNorm,
} from "./embeddings.js";
import { MAX_CONTEXT_CHARS, formatKnowledgeLine } from "./format.js";
import {
	applyTokenBudget,
	denseFloor,
	estimateTokens,
	rerankFloor,
	scorerKey,
} from "./gate.js";
import { RerankSkippedError } from "./rerank-remote.js";
import type { RerankScorer } from "./rerank.js";
import type { SessionDedupe } from "./session-dedupe.js";

/** Relation-expanded sources are gated and ranked at this fraction of their parent. */
const SOURCE_GATE_DISCOUNT = 0.9;

/**
 * Rerank gets what is left of the request budget after the embedding and store
 * reads, less RERANK_RESERVE_MS for the work after it. The budget itself is per
 * client (see client.ts).
 */
const RERANK_RESERVE_MS = 500;
/** Below this much time left, rerank is not started and dense order is used. */
const RERANK_MIN_BUDGET_MS = 300;

/**
 * Token cap of the injected block: the formatter's character cap in the same
 * estimate the token budget uses. The budget never exceeds it, so every entry the
 * engine keeps is rendered, and nothing is recorded, remembered or logged that the
 * formatter would cut.
 */
const RENDER_TOKEN_CAP = estimateTokens(MAX_CONTEXT_CHARS);

const DAY_MS = 1000 * 60 * 60 * 24;

export type Staleness = ActivationResult["entries"][number]["staleness"];

/** A scored entry on its way from retrieval to the response. */
export interface Candidate {
	entry: KnowledgeEntry;
	/** Raw cosine similarity: the threshold, the session rescore and the budget keep use it. */
	rawSimilarity: number;
	/** Ranking score: raw or rerank score weighted by live strength. */
	similarity: number;
	/** Cross-encoder score, present only when the reranker scored this candidate. */
	rerankScore?: number;
	/** Score the precision gate compares with its floor (rerank score, or cosine when dense). */
	gateScore: number;
	/** Store the entry was read from. Injection writes go to this store. */
	owner: IKnowledgeStore;
	staleness: Staleness;
}

/** A dense hit from a DB-side ANN search (Path A). */
export interface AnnHit {
	db: IKnowledgeStore;
	entry: KnowledgeEntry;
	similarity: number;
}

/** A scan-path row that cleared the threshold. Hydrated into an entry later. */
export interface ScanHit {
	db: IKnowledgeStore;
	id: string;
	rawSimilarity: number;
}

/** A relation-pulled support source and the store that owns it. */
export interface SupportSource {
	source: KnowledgeEntry;
	owner: IKnowledgeStore;
}

/** Whether an entry is a synthesized abstraction whose support sources are pulled in. */
export function isSynthesized(entry: Pick<KnowledgeEntry, "type">): boolean {
	return entry.type === "principle" || entry.type === "pattern";
}

/** A conflicted entry with no contradicts partner: nothing can be shown beside it, so it is not injected. */
export function hasFlagWithoutPartner(
	entry: Pick<KnowledgeEntry, "id" | "status">,
	contradictPairs: ReadonlyMap<string, string>,
): boolean {
	return entry.status === "conflicted" && !contradictPairs.has(entry.id);
}

/**
 * Whether an entry older than its type's half-life may be outdated. Usage is the
 * retrieval count plus the injection count: access_count no longer moves on
 * injection, so injection_count is the live signal. An entry used within its
 * half-life is not flagged, so an entry injected daily stays clean.
 */
function isMayBeStale(
	entry: Pick<KnowledgeEntry, "accessCount" | "injectionCount">,
	ageDays: number,
	lastAccessedDaysAgo: number,
	halfLife: number,
): boolean {
	const uses = entry.accessCount + entry.injectionCount;
	return ageDays > halfLife && lastAccessedDaysAgo > halfLife && uses < 3;
}

/** Live strength and the staleness signals of one entry at `now`. */
export function stalenessOf(
	entry: KnowledgeEntry,
	now: number,
): { strength: number; staleness: Staleness } {
	const ageDays = (now - entry.createdAt) / DAY_MS;
	const lastAccessedDaysAgo = (now - entry.lastAccessedAt) / DAY_MS;
	const halfLife =
		config.decay.typeHalfLife[entry.type] || config.decay.typeHalfLife.fact;
	const strength = computeStrength(entry, now);
	return {
		strength,
		staleness: {
			ageDays: Math.round(ageDays),
			strength,
			lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
			mayBeStale: isMayBeStale(entry, ageDays, lastAccessedDaysAgo, halfLife),
		},
	};
}

export type ScanRow = { id: string; embedding: Float32Array };

export interface ScanScore {
	hits: ScanHit[];
	/** Every entry ID scored, whether or not it cleared the threshold. */
	seenIds: Set<string>;
	/** Per store: the dimension of the first row that could not be scored, or undefined. */
	mismatches: Array<{ storeId: string; mismatchedDims: number | undefined }>;
}

/**
 * Score scan-path rows (id + embedding, per store) against every query vector;
 * the best cosine wins. Entries synced to several scan stores share an ID, so the
 * first occurrence is scored and the rest are skipped. A row whose dimension does
 * not match a query cannot be scored: it is skipped without claiming its ID, so a
 * matching copy of the same entry in another store still counts.
 */
export function scoreScanRows(
	storeRows: ReadonlyArray<{ db: IKnowledgeStore; rows: readonly ScanRow[] }>,
	queries: readonly PreparedQuery[],
	threshold: number,
): ScanScore {
	const seenIds = new Set<string>();
	const hits: ScanHit[] = [];
	const mismatches: ScanScore["mismatches"] = [];
	for (const { db, rows } of storeRows) {
		let mismatchedDims: number | undefined;
		for (const row of rows) {
			if (queries.some((q) => q.unit.length !== row.embedding.length)) {
				mismatchedDims ??= row.embedding.length;
				continue;
			}
			if (seenIds.has(row.id)) continue;
			seenIds.add(row.id);
			const bNorm = storedNorm(row.embedding);
			let rawSimilarity = Number.NEGATIVE_INFINITY;
			for (const q of queries) {
				rawSimilarity = Math.max(
					rawSimilarity,
					cosineToPrepared(q, row.embedding, bNorm),
				);
			}
			// ANN stores filter by threshold in the DB; apply the same cut here.
			// A non-finite score cannot be ranked, so it is a miss.
			if (!Number.isFinite(rawSimilarity) || rawSimilarity < threshold) {
				continue;
			}
			hits.push({ db, id: row.id, rawSimilarity });
		}
		mismatches.push({ storeId: db.id, mismatchedDims });
	}
	return { hits, seenIds, mismatches };
}

/**
 * One candidate per entry ID with its best raw similarity across query vectors and
 * stores. ANN hits are merged first, so on a tie the ANN copy is kept. A scan hit
 * contributes only when its entry was hydrated.
 */
export function mergeCandidates(
	annResults: ReadonlyArray<readonly AnnHit[]>,
	scanHits: readonly ScanHit[],
	hydrated: readonly KnowledgeEntry[],
	now: number,
): Candidate[] {
	const bestById = new Map<
		string,
		{ entry: KnowledgeEntry; rawSimilarity: number; owner: IKnowledgeStore }
	>();
	const keepBest = (
		entry: KnowledgeEntry,
		rawSimilarity: number,
		owner: IKnowledgeStore,
	) => {
		const existing = bestById.get(entry.id);
		if (!existing || rawSimilarity > existing.rawSimilarity) {
			bestById.set(entry.id, { entry, rawSimilarity, owner });
		}
	};
	for (const queryResults of annResults) {
		for (const { db, entry, similarity } of queryResults) {
			keepBest(entry, similarity, db);
		}
	}
	const hydratedById = new Map(hydrated.map((e) => [e.id, e]));
	for (const hit of scanHits) {
		const entry = hydratedById.get(hit.id);
		if (entry) keepBest(entry, hit.rawSimilarity, hit.db);
	}
	return [...bestById.values()].map(({ entry, rawSimilarity, owner }) => {
		const { strength, staleness } = stalenessOf(entry, now);
		return {
			entry,
			// Threshold filtering uses raw cosine similarity so entry age never
			// prevents a semantically relevant entry from activating.
			// Ranking uses raw similarity weighted by live strength - well-consolidated
			// entries sort higher, but stale entries still appear if they're on-topic.
			// The staleness signals in the response let the LLM reason about reliability.
			rawSimilarity,
			similarity: rawSimilarity * strength,
			gateScore: rawSimilarity,
			owner,
			staleness,
		};
	});
}

export interface RerankTiming {
	/** performance.now() when the request entered the server. Omitted: no deadline. */
	requestStartedAt?: number;
	/** Request time budget in ms, counted from requestStartedAt. */
	budgetMs?: number;
	/** performance.now() at the decision. */
	now: number;
}

export interface RerankOutcome {
	candidates: Candidate[];
	gateMode: GateMode;
	/** Scorer `provider/model` when the reranker scored the candidates. */
	rerankModel: string | null;
}

/**
 * Milliseconds the rerank call may use at `now`: the budget less the reserve and the
 * time already spent. Undefined when the request has no start time (no deadline).
 */
function rerankBudgetMs(
	requestStartedAt: number | undefined,
	budgetMs: number,
	now: number,
): number | undefined {
	return requestStartedAt === undefined
		? undefined
		: requestStartedAt + budgetMs - RERANK_RESERVE_MS - now;
}

/**
 * Second stage: the cross-encoder rerank. Dense retrieval is the recall stage; the
 * reranker jointly encodes each (query, candidate) pair for a sharper relevance
 * signal. The final ranking score becomes rerankScore × liveStrength (was:
 * rawSimilarity × liveStrength), so well-consolidated entries still sort higher.
 *
 * Fail-open: a refusal, a timeout or a scorer error logs and returns the dense
 * ordering. Rerank runs only when the request has time left for it. The scorer
 * sees at most `rerank.candidates` candidates, best dense first, so listwise
 * scorers get a fixed, reproducible order.
 */
export async function scoreCandidates(
	reranker: RerankScorer | null,
	candidates: Candidate[],
	query: string,
	timing: RerankTiming,
): Promise<RerankOutcome> {
	if (!reranker || candidates.length === 0) {
		return {
			candidates,
			gateMode: reranker ? "dense" : "off",
			rerankModel: null,
		};
	}
	const denseOnly: RerankOutcome = {
		candidates,
		gateMode: "dense",
		rerankModel: null,
	};
	const { provider, model } = reranker.identity;
	const rerankBudget = rerankBudgetMs(
		timing.requestStartedAt,
		timing.budgetMs ?? DEFAULT_ACTIVATION_BUDGET_MS,
		timing.now,
	);
	const rerankInTime =
		rerankBudget === undefined || rerankBudget >= RERANK_MIN_BUDGET_MS;
	if (!rerankInTime) {
		logger.log(
			`[activation] rerank skipped: request budget. Dense-only ordering: provider=${provider} model=${model} gate_mode=dense.`,
		);
		return denseOnly;
	}

	// The order is always dense similarity, best first: listwise scorers show
	// position bias, and a fixed order (D00 is the dense best) keeps their output
	// reproducible.
	const pool = [...candidates]
		.sort((a, b) => b.rawSimilarity - a.rawSimilarity)
		.slice(0, config.activation.rerank.candidates);

	try {
		const documents = pool.map((c) =>
			formatEmbeddingText(c.entry.type, c.entry.content, c.entry.topics),
		);
		const scores = await reranker.scoreBatch(query, documents, {
			budgetMs: rerankBudget,
		});
		const scored = pool.map((c, i) => ({
			...c,
			rerankScore: scores[i],
			gateScore: scores[i],
			similarity:
				scores[i] *
				(config.activation.gate.strengthDampening
					? 0.5 + 0.5 * c.staleness.strength
					: c.staleness.strength),
		}));
		const rerankModel = scorerKey(reranker.identity);
		componentHealth.recordSuccess("rerank");
		return { candidates: scored, gateMode: "rerank", rerankModel };
	} catch (err) {
		// A refusal (full local queue, open remote breaker, or a call the request
		// budget cut short) is back-pressure, not a provider failure, so it does
		// not count toward health.
		if (!(err instanceof RerankSkippedError)) {
			componentHealth.recordFailure("rerank", err);
		}
		const reason = err instanceof Error ? err.message : String(err);
		if (err instanceof RerankSkippedError) {
			// Skipped calls are logged at info; the remote breaker warns once when
			// it opens.
			logger.log(
				`[activation] rerank skipped: ${reason}. Dense-only ordering: provider=${provider} model=${model} gate_mode=dense.`,
			);
		} else {
			logger.warn(
				`[activation] Rerank failed — using dense-only ordering. provider=${provider} model=${model} gate_mode=dense. Reason: ${reason}`,
			);
		}
		// The dense-best pool is kept: the truncation to `rerank.candidates` stands.
		return { candidates: pool, gateMode: "dense", rerankModel: null };
	}
}

/**
 * Floor of the precision gate. A rerank score is gated by the rerank floor. Any
 * other ordering (rerank disabled, failed, skipped or out of time) is gated by the
 * dense floor, so ACTIVATION_DENSE_MARGIN applies whenever the gate is on.
 */
export function precisionFloor(
	reranker: RerankScorer | null,
	gateMode: GateMode,
	candidates: ReadonlyArray<{ gateScore: number }>,
	similarityThreshold: number,
): number {
	const gate = config.activation.gate;
	if (gateMode === "rerank" && reranker) {
		let top = 0;
		for (const s of candidates) top = Math.max(top, s.gateScore);
		return rerankFloor({
			identity: reranker.identity,
			floorOverride: gate.rerankFloor,
			relFrac: gate.rerankRelFrac,
			topScore: top,
		});
	}
	return denseFloor(similarityThreshold, gate.denseMargin);
}

export interface GateOutcome {
	candidates: Candidate[];
	gateDropped: number;
	/**
	 * The floor support sources are gated against, or null when they are not gated.
	 * A source inherits no rerank score. It is gated only when a reranker scored the
	 * candidates or the dense margin is set; otherwise the floor is the bare
	 * threshold, which the candidates already meet, and gating a source at 0.9× its
	 * parent would drop sources that were always injected.
	 */
	sourceFloor: number | null;
}

/**
 * Precision gate, first rule: keep candidates whose gate score reaches the floor
 * (null floor: gate off, keep all). It runs before the rank cap, so the cap is
 * spent on candidates that pass.
 */
export function applyPrecisionGate(
	candidates: Candidate[],
	floor: number | null,
	gateMode: GateMode,
): GateOutcome {
	const passing =
		floor === null
			? candidates
			: candidates.filter((s) => s.gateScore >= floor);
	const sourceFloor =
		floor !== null &&
		(gateMode === "rerank" || config.activation.gate.denseMargin > 0)
			? floor
			: null;
	return {
		candidates: passing,
		gateDropped: candidates.length - passing.length,
		sourceFloor,
	};
}

/** Ranking order: similarity, best first. Returns a new array. */
export function sortByRank(candidates: readonly Candidate[]): Candidate[] {
	return [...candidates].sort((a, b) => b.similarity - a.similarity);
}

/**
 * Drop flagged conflicted entries that have no contradicts partner. Nothing can be
 * shown beside them, so they are not injected.
 */
export function dropUnflaggedConflicts(
	candidates: readonly Candidate[],
	contradictPairs: ReadonlyMap<string, string>,
): { kept: Candidate[]; dropped: number } {
	const kept = candidates.filter(
		({ entry }) => !hasFlagWithoutPartner(entry, contradictPairs),
	);
	return { kept, dropped: candidates.length - kept.length };
}

/**
 * Drop candidates this session already received (session-dedupe.ts). Without a
 * session key nothing is deduplicated. Returns a new array in every case.
 */
export function applySessionDedupe(
	candidates: readonly Candidate[],
	sessions: SessionDedupe,
	sessionKey: string | undefined,
	now: number,
): { kept: Candidate[]; deduped: number } {
	if (sessionKey === undefined) return { kept: [...candidates], deduped: 0 };
	let deduped = 0;
	const kept = candidates.filter(({ entry, rawSimilarity }) => {
		const suppressed = sessions.isSuppressed(
			sessionKey,
			{ id: entry.id, rawSimilarity },
			now,
		);
		if (suppressed) deduped++;
		return !suppressed;
	});
	return { kept, deduped };
}

export interface SourceExpansion {
	/** The input candidates followed by the support sources that were added. */
	candidates: Candidate[];
	gateDropped: number;
	deduped: number;
}

/**
 * Relation-aware activation: a synthesized principle or pattern in `candidates`
 * pulls in its support sources, so the agent sees the abstraction and the evidence
 * it was distilled from. A source inherits its parent's rank (0.9× its similarity)
 * and its parent's raw similarity. It is not added when it is already present, is
 * gated below `sourceFloor`, is a flagged conflict without a partner, or was
 * already shown in this session. No source is added past `maxResults`.
 */
export function appendSupportSources(input: {
	candidates: readonly Candidate[];
	sourcesByParent: ReadonlyMap<string, readonly SupportSource[]>;
	contradictPairs: ReadonlyMap<string, string>;
	sourceFloor: number | null;
	maxResults: number;
	sessions: SessionDedupe;
	sessionKey: string | undefined;
	now: number;
}): SourceExpansion {
	const out = [...input.candidates];
	const present = new Set(out.map(({ entry }) => entry.id));
	let gateDropped = 0;
	let deduped = 0;
	for (const parent of input.candidates) {
		const { entry } = parent;
		if (!isSynthesized(entry)) continue;
		const sources = input.sourcesByParent.get(entry.id);
		if (!sources) continue;

		for (const { source, owner } of sources) {
			if (present.has(source.id)) continue;
			if (out.length >= input.maxResults) break;

			const sourceGate = parent.gateScore * SOURCE_GATE_DISCOUNT;
			if (input.sourceFloor !== null && sourceGate < input.sourceFloor) {
				gateDropped++;
				continue;
			}
			if (hasFlagWithoutPartner(source, input.contradictPairs)) {
				gateDropped++;
				continue;
			}
			if (
				input.sessionKey !== undefined &&
				input.sessions.isSuppressed(
					input.sessionKey,
					{ id: source.id, rawSimilarity: parent.rawSimilarity },
					input.now,
				)
			) {
				deduped++;
				continue;
			}

			const { strength, staleness } = stalenessOf(source, input.now);
			out.push({
				entry: source,
				rawSimilarity: parent.rawSimilarity,
				similarity: parent.similarity * SOURCE_GATE_DISCOUNT * strength,
				gateScore: sourceGate,
				owner,
				staleness,
			});
			present.add(source.id);
		}

		if (out.length >= input.maxResults) break;
	}
	return { candidates: out, gateDropped, deduped };
}

/**
 * Contradiction annotations for conflicted candidates whose counterpart also
 * activated in this query: both sides relevant is the only case the caveat helps.
 * The counterpart's content comes from the activated set, so no store read is needed.
 */
export function buildContradictionAnnotations(
	candidates: readonly Candidate[],
	contradictPairs: ReadonlyMap<string, string>,
): Map<string, ContradictionAnnotation> {
	const activeById = new Map(candidates.map(({ entry }) => [entry.id, entry]));
	const annotations = new Map<string, ContradictionAnnotation>();
	for (const { entry } of candidates) {
		if (entry.status !== "conflicted") continue;

		const counterpartId = contradictPairs.get(entry.id);
		if (!counterpartId) continue;

		const counterpart = activeById.get(counterpartId);
		if (!counterpart) continue;

		annotations.set(entry.id, {
			conflictingEntryId: counterpartId,
			conflictingContent: counterpart.content,
			caveat:
				"This knowledge conflicts with another activated entry and has not been resolved. " +
				"Verify before relying on it; the conflicting view is also shown in this response.",
		});
	}
	return annotations;
}

export interface RenderOutcome {
	/** The entries the response renders, in rank order. */
	final: Candidate[];
	/** The annotations whose counterpart is still rendered. */
	contradictions: Map<string, ContradictionAnnotation>;
	budgetDropped: number;
	injectedTokens: number;
}

/**
 * Token budget and render cap. One cap for everything that is counted, recorded
 * and remembered: the block cannot exceed RENDER_TOKEN_CAP, and the token budget is
 * clamped to it. Each line is costed with the formatter's newline, so the greedy
 * prefix kept here is exactly what formatActivatedKnowledge renders. The budget
 * applies with the gate off too.
 *
 * Lines are costed with their annotation, before annotations whose counterpart the
 * budget dropped are removed. The estimate is therefore a little high for those
 * entries, never low.
 */
export function applyRenderBudget(
	candidates: readonly Candidate[],
	contradictions: ReadonlyMap<string, ContradictionAnnotation>,
	opts: { gateOn: boolean; similarityThreshold: number },
): RenderOutcome {
	const budget = opts.gateOn
		? Math.min(config.activation.gate.tokenBudget, RENDER_TOKEN_CAP)
		: RENDER_TOKEN_CAP;
	const lineTokens = new Map(
		candidates.map((s) => [
			s,
			estimateTokens(
				formatKnowledgeLine({
					entry: s.entry,
					staleness: s.staleness,
					contradiction: contradictions.get(s.entry.id),
				}).length + 1,
			),
		]),
	);
	const final = applyTokenBudget(
		candidates,
		budget,
		(s) => lineTokens.get(s) ?? 0,
		(s) => s.rawSimilarity >= opts.similarityThreshold,
	);
	// A counterpart the budget dropped is no longer in the response, so its
	// annotation would point at nothing.
	const finalIds = new Set(final.map(({ entry }) => entry.id));
	return {
		final,
		contradictions: new Map(
			[...contradictions].filter(([, annotation]) =>
				finalIds.has(annotation.conflictingEntryId),
			),
		),
		budgetDropped: candidates.length - final.length,
		injectedTokens: final.reduce((n, s) => n + (lineTokens.get(s) ?? 0), 0),
	};
}

/** The response entries: the embedding is stripped, and each annotation is attached by entry ID. */
export function toActivationEntries(
	final: readonly Candidate[],
	contradictions: ReadonlyMap<string, ContradictionAnnotation>,
): ActivationResult["entries"] {
	return final.map(
		({ entry, rawSimilarity, similarity, rerankScore, staleness }) => ({
			entry: { ...entry, embedding: undefined } as KnowledgeEntry,
			similarity,
			rawSimilarity,
			rerankScore,
			staleness,
			contradiction: contradictions.get(entry.id),
		}),
	);
}
