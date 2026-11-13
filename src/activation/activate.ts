import { config } from "../config.js";
import { computeStrength } from "../consolidation/decay.js";
import type { IKnowledgeStore } from "../db/index.js";
import { componentHealth } from "../health.js";
import { logger } from "../logger.js";
import type {
	ActivationResult,
	ContradictionAnnotation,
	GateMode,
	InjectionPlan,
	KnowledgeEntry,
} from "../types.js";
import {
	EmbeddingClient,
	bulkEmbedOptions,
	cosineToPrepared,
	describeError,
	formatEmbeddingText,
	prepareQuery,
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
import { SessionDedupe } from "./session-dedupe.js";

/** Session dedupe: retained sessions (LRU). */
const MAX_SESSIONS = 1000;
/** Raw-cosine gain that lets a suppressed session entry be injected again. */
const SESSION_RESCORE_MARGIN = 0.1;
/** Relation-expanded sources are gated and ranked at this fraction of their parent. */
const SOURCE_GATE_DISCOUNT = 0.9;
/**
 * Token cap of the injected block: the formatter's character cap in the same
 * estimate the token budget uses. The budget never exceeds it, so every entry the
 * engine keeps is rendered, and nothing is recorded, remembered or logged that the
 * formatter would cut.
 */
const RENDER_TOKEN_CAP = estimateTokens(MAX_CONTEXT_CHARS);

/** Server-side cap on activation cues per call (see normalizeCues). */
export const MAX_CUES = 12;

/**
 * Dedupe and cap activation cues. Applied wherever cues enter the engine:
 * splitIntoCues and every activate() call (the /activate `q` list, the MCP tool
 * and the plugin all go through it).
 *
 *   - Cues are compared by a normalised key (trimmed, whitespace collapsed,
 *     lowercased). The first occurrence wins and keeps its original text.
 *     Blank cues are dropped.
 *   - More than MAX_CUES unique cues keep the first ceil(MAX_CUES/2) and the last
 *     floor(MAX_CUES/2). The last cue is the holistic full message, so it is kept.
 *
 * Deterministic: the same input always yields the same output.
 */
export function normalizeCues(cues: readonly string[]): string[] {
	const seen = new Set<string>();
	const unique: string[] = [];
	for (const cue of cues) {
		const key = cue.replace(/\s+/g, " ").trim().toLowerCase();
		if (key.length === 0 || seen.has(key)) continue;
		seen.add(key);
		unique.push(cue);
	}
	if (unique.length <= MAX_CUES) return unique;
	const head = Math.ceil(MAX_CUES / 2);
	const tail = Math.floor(MAX_CUES / 2);
	return [...unique.slice(0, head), ...unique.slice(unique.length - tail)];
}

/**
 * Split a prompt into activation cues for multi-topic retrieval.
 *
 * Strategy (mirrors the OpenCode plugin):
 *   1. Per-line segments — each newline is a topic boundary (Shift+Enter in most UIs).
 *      Segments shorter than MIN_CUE_CHARS are skipped (greetings, "yes", "ok", etc.)
 *   2. The full trimmed prompt as a holistic cue — captures overall intent that no
 *      individual line may fully express. It is always the last cue.
 *   3. normalizeCues — duplicates (by normalised key) are dropped and the list is
 *      capped at MAX_CUES.
 *
 * All resulting cues are embedded in a single batched API call downstream.
 *
 * Exported so server.ts (claude-code hook) can reuse the same logic without
 * duplicating it. The plugin (plugin/knowledge.ts) runs as a standalone file
 * and must keep its own copy — kept in sync via tests/format.test.ts parity tests.
 */
export const MIN_CUE_CHARS = 15;

export function splitIntoCues(prompt: string): string[] {
	const trimmed = prompt.trim();
	const segments = trimmed
		.split("\n")
		.map((s) => s.trim())
		.filter((s) => s.length >= MIN_CUE_CHARS);
	return normalizeCues([...segments, trimmed]);
}

/** Per store-and-operation admission state within one activate() call (see callStore). */
interface StoreSlot {
	inFlight: number;
	/** The operation has returned a result in this activation; the in-flight cap lifts. */
	answered: boolean;
	/** Callers parked until one of this store's in-flight calls settles. */
	waiting: Array<() => void>;
}

interface BreakerState {
	status: "closed" | "open" | "half-open";
	/** Consecutive failures while closed. */
	failures: number;
	openedAt: number;
}

/**
 * Breaker key for one operation kind against one store. A store's ANN search and
 * its count are separate operations: a fast success must not reset the failures
 * of a timed-out search.
 */
function opKey(store: string, op: string): string {
	return `${store}\u0000${op}`;
}

/**
 * Per-operation circuit breaker for the activation fan-out. State is keyed by
 * store id and operation label (see opKey) and lives as long as the
 * ActivationEngine (one per process), so it is shared across activations.
 *
 *   closed    — calls go through; `threshold` consecutive failures open it.
 *   open      — calls are refused (no timeout wait, no pool slot) for `windowMs`.
 *   half-open — after the window exactly one probe call is admitted. Success
 *               closes the circuit; failure re-opens it for another window.
 *
 * Warns once per state change (open, probe succeeded, probe failed), never per call.
 */
export class StoreBreaker {
	private readonly states = new Map<string, BreakerState>();
	private readonly threshold: number;
	private readonly windowMs: number;
	private readonly now: () => number;

	constructor(opts: {
		threshold: number;
		windowMs: number;
		now?: () => number;
	}) {
		this.threshold = opts.threshold;
		this.windowMs = opts.windowMs;
		this.now = opts.now ?? Date.now;
	}

	private stateOf(store: string, op: string): BreakerState {
		const key = opKey(store, op);
		let s = this.states.get(key);
		if (!s) {
			s = { status: "closed", failures: 0, openedAt: 0 };
			this.states.set(key, s);
		}
		return s;
	}

	/**
	 * Max calls in flight for this operation before it has answered in an
	 * activation. Only a closed circuit lets callers queue; otherwise admit()
	 * decides at once.
	 */
	budget(store: string, op: string): number {
		const s = this.stateOf(store, op);
		return s.status === "closed"
			? Math.max(1, this.threshold - s.failures)
			: Number.POSITIVE_INFINITY;
	}

	/** Admit one call. After the window, the first caller becomes the probe. */
	admit(store: string, op: string): boolean {
		const s = this.stateOf(store, op);
		if (s.status === "closed") return true;
		if (s.status === "open" && this.now() - s.openedAt >= this.windowMs) {
			s.status = "half-open";
			return true;
		}
		return false;
	}

	/** An admitted call succeeded. */
	recordSuccess(store: string, op: string): void {
		const s = this.stateOf(store, op);
		if (s.status === "half-open") {
			s.status = "closed";
			s.failures = 0;
			logger.warn(
				`[activation] Store "${store}" ${op} recovered — circuit closed.`,
			);
		} else if (s.status === "closed") {
			s.failures = 0;
		}
	}

	/** An admitted call failed; `reason` goes into the warning. */
	recordFailure(store: string, op: string, reason: string): void {
		const s = this.stateOf(store, op);
		const windowS = this.windowMs / 1000;
		if (s.status === "closed") {
			s.failures++;
			if (s.failures < this.threshold) return;
			this.open(s);
			logger.warn(
				`[activation] Store "${store}" ${op} failed ${this.threshold} times in a row ` +
					`(${reason}) — circuit open, skipping this call for ${windowS}s.`,
			);
		} else if (s.status === "half-open") {
			this.open(s);
			logger.warn(
				`[activation] Store "${store}" ${op} probe failed (${reason}) — ` +
					`circuit re-opened for ${windowS}s.`,
			);
		}
	}

	private open(s: BreakerState): void {
		s.status = "open";
		s.openedAt = this.now();
	}
}

/**
 * Activation engine — the core retrieval mechanism.
 *
 * Models associative activation from cognitive science:
 * - Input cues are embedded into the same vector space as knowledge entries
 * - Entries activate based on semantic similarity (not keyword match)
 * - Activation strength is modulated by the entry's decay-adjusted strength
 * - The same mechanism serves both passive (plugin-triggered) and active (agent-triggered) retrieval
 */
export class ActivationEngine {
	/**
	 * The primary writable store — used for access records and single-store compat.
	 */
	private db: IKnowledgeStore;
	/**
	 * All writable stores — used for ensureEmbeddings, checkAndReEmbed, and
	 * embedding metadata writes. In multi-store setups this includes every store
	 * that domain routing can write to, not just the primary.
	 */
	private writableDbs: IKnowledgeStore[];
	/**
	 * All stores to read from during activation (includes all writable stores).
	 * Fan-out: each activation query runs against all read stores and results
	 * are merged and re-ranked by similarity score.
	 */
	private readDbs: IKnowledgeStore[];
	readonly embeddings: EmbeddingClient;

	/**
	 * Optional second-stage reranker (local cross-encoder). When present,
	 * dense retrieval overfetches candidates and the reranker re-scores them
	 * for precision. Null = dense-only ordering (zero behavior change).
	 * Injected by the composition root; tests pass fakes or omit.
	 */
	private readonly reranker: RerankScorer | null;

	/**
	 * Per-store circuit breaker (see StoreBreaker). A store that keeps failing is
	 * skipped for STORE_RETRY_INTERVAL_MS instead of costing a timeout per cue.
	 */
	private readonly breaker: StoreBreaker;

	/** Per-call store timeout; a store that does not answer in time counts as a failure. */
	private readonly storeTimeoutMs: number;

	/** Per-session dedupe of injected entries (see session-dedupe.ts). */
	private readonly sessions: SessionDedupe;

	/** Stores whose injection writes are currently failing (logged once per streak). */
	private readonly injectionFailing = new Set<string>();

	/**
	 * Scan stores whose rows have an embedding dimension other than the query's,
	 * keyed by store id. Logged once per streak (see the scan loop).
	 */
	private readonly dimMismatchStores = new Set<string>();

	/**
	 * Last logged ANN vs full-scan partition of read stores. Used to log the
	 * retrieval path per store only when it changes, not on every activation.
	 */
	private lastPathPartition: string | undefined;

	/** Per-store call timeout — a dead store is skipped after this duration. */
	private static readonly STORE_TIMEOUT_MS = 5_000;

	/** Consecutive store failures that open the circuit. */
	private static readonly STORE_FAILURE_THRESHOLD = 3;

	/** How long an open circuit skips a store before one half-open probe. */
	private static readonly STORE_RETRY_INTERVAL_MS = 60_000;

	/**
	 * @param writableDb   The primary writable store — receives access records.
	 * @param readDbs      All stores to fan out activation reads across.
	 *                     If omitted, defaults to [writableDb] (single-store mode).
	 * @param writableDbs  All writable stores for embedding/re-embed operations.
	 *                     If omitted, defaults to [writableDb] (single-store mode).
	 * @param reranker     Optional cross-encoder scorer for the second rerank
	 *                     stage. Null/omitted = dense-only ordering.
	 * @param options      Test seam: `storeTimeoutMs` overrides STORE_TIMEOUT_MS.
	 */
	constructor(
		writableDb: IKnowledgeStore,
		readDbs?: IKnowledgeStore[],
		writableDbs?: IKnowledgeStore[],
		reranker?: RerankScorer | null,
		options?: { storeTimeoutMs?: number },
	) {
		this.db = writableDb;
		this.readDbs = readDbs ?? [writableDb];
		this.writableDbs = writableDbs ?? [writableDb];
		this.embeddings = new EmbeddingClient();
		this.reranker = reranker ?? null;
		this.storeTimeoutMs =
			options?.storeTimeoutMs ?? ActivationEngine.STORE_TIMEOUT_MS;
		this.breaker = new StoreBreaker({
			threshold: ActivationEngine.STORE_FAILURE_THRESHOLD,
			windowMs: ActivationEngine.STORE_RETRY_INTERVAL_MS,
		});
		this.sessions = new SessionDedupe({
			ttlMs: config.activation.gate.sessionTtlMs,
			maxSessions: MAX_SESSIONS,
			rescoreMargin: SESSION_RESCORE_MARGIN,
		});
	}

	/**
	 * Fan out a function across stores, tolerating per-store failures.
	 *
	 * Each store call is bounded by STORE_TIMEOUT_MS; the timer is cleared when the
	 * call settles. One dead store cannot reject the operation: its result is
	 * dropped and the remaining stores' results are returned.
	 */
	private async fanOutSafe<T>(
		stores: IKnowledgeStore[],
		fn: (db: IKnowledgeStore) => Promise<T>,
		label: string,
		slots: Map<string, StoreSlot>,
	): Promise<T[]> {
		if (stores.length === 0) return [];
		const results = await Promise.all(
			stores.map((db) => this.callStore(db, fn, label, slots)),
		);
		return results.flatMap((r) => (r.ok ? [r.value] : []));
	}

	/**
	 * One store call under the circuit breaker, within one activation.
	 *
	 * Until the store answers in this activation, at most breaker.budget() of its
	 * calls are in flight, so a dead store receives at most STORE_FAILURE_THRESHOLD
	 * calls however many cues fan out. Calls the breaker refuses return at once:
	 * no timeout wait, no warning.
	 */
	private async callStore<T>(
		db: IKnowledgeStore,
		fn: (db: IKnowledgeStore) => Promise<T>,
		label: string,
		slots: Map<string, StoreSlot>,
	): Promise<{ ok: true; value: T } | { ok: false }> {
		const key = opKey(db.id, label);
		const slot: StoreSlot = slots.get(key) ?? {
			inFlight: 0,
			answered: false,
			waiting: [],
		};
		slots.set(key, slot);
		while (
			!slot.answered &&
			slot.inFlight >= this.breaker.budget(db.id, label)
		) {
			await new Promise<void>((resolve) => slot.waiting.push(resolve));
		}
		if (!this.breaker.admit(db.id, label)) return { ok: false };

		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(
				() => reject(new Error(`timeout after ${this.storeTimeoutMs / 1000}s`)),
				this.storeTimeoutMs,
			);
		});
		slot.inFlight++;
		try {
			const value = await Promise.race([fn(db), timeout]);
			slot.answered = true;
			this.breaker.recordSuccess(db.id, label);
			return { ok: true, value };
		} catch (err) {
			this.breaker.recordFailure(
				db.id,
				label,
				err instanceof Error ? err.message : String(err),
			);
			return { ok: false };
		} finally {
			clearTimeout(timer);
			slot.inFlight--;
			for (const wake of slot.waiting.splice(0)) wake();
		}
	}

	/**
	 * Activate knowledge entries based on one or more queries.
	 *
	 * This is the single retrieval mechanism — used by both:
	 * - The plugin (passive: message segments + full message -> activate -> inject)
	 * - The MCP tool (active: agent sends cues -> activate -> return)
	 *
	 * When multiple queries are provided (e.g. per-line segments of a multi-topic
	 * message plus the full message as a holistic cue), they are embedded in a
	 * single batched API call. Each entry is scored against ALL query vectors and
	 * its best (highest) similarity across queries is kept. Results are then
	 * deduplicated, filtered by threshold, and ranked — so a multi-topic message
	 * retrieves relevant knowledge for every topic, not just the dominant one.
	 *
	 * @param queries - One query string, or an array of query strings (segments + full message)
	 * @returns Ranked knowledge entries above the similarity threshold, with staleness signals
	 */
	async activate(
		queries: string | string[],
		options?: {
			/** Max entries to return. Overrides config.activation.maxResults. */
			limit?: number;
			/** Min similarity threshold. Overrides config.activation.similarityThreshold. */
			threshold?: number;
			/**
			 * Read-only: no store writes (the plan's writes are empty). Session dedupe
			 * state is recorded only when the plan is committed. Used by the replay harness and the CLI.
			 */
			dryRun?: boolean;
			/**
			 * Client-scoped session key (for example `${client}\0${sessionId}`). When set,
			 * entries already shown in this session are suppressed (session-dedupe.ts),
			 * once the plan is committed. Requests without a key are not deduplicated.
			 */
			session?: string;
		},
	): Promise<ActivationResult> {
		const queryList = normalizeCues(
			Array.isArray(queries) ? queries : [queries],
		);
		if (queryList.length === 0) {
			return { entries: [], query: "", totalActive: 0 };
		}

		// Use the last query as the display query — the plugin sends [segments..., fullMessage]
		// so the last element is the most holistic representation of the user's intent.
		// For a single string, queryList[0] === queryList[last], so this is always correct.
		const primaryQuery = queryList[queryList.length - 1];

		const dryRun = options?.dryRun ?? false;
		const maxResults = options?.limit ?? config.activation.maxResults;
		const similarityThreshold =
			options?.threshold ?? config.activation.similarityThreshold;

		// Candidate pool size for dense retrieval. With a reranker active we
		// overfetch (dense = recall stage) and let the cross-encoder pick the
		// final maxResults; without one the pool IS the final result set.
		const fetchLimit = this.reranker
			? Math.max(config.activation.rerank.candidates, maxResults)
			: maxResults;

		const slots = new Map<string, StoreSlot>();

		// Embed all queries in a single batched API call
		const queryEmbeddings = await this.embeddings.embedBatch(queryList);

		const now = Date.now();
		const DAY_MS = 1000 * 60 * 60 * 24;

		// ── Candidate retrieval ────────────────────────────────────────────────────
		// The retrieval path is chosen per store (see storeSupportsAnn):
		//
		// Path A - pgvector (PostgresKnowledgeDB with findSimilarEntries, ready):
		//   For each query vector, run one ANN query against the store. The HNSW
		//   index does the heavy lifting in the DB; we receive only the top
		//   candidates above the threshold. Similarity values are pre-computed by
		//   the DB (1 - cosine distance).
		//
		// Path B - in-process full scan (SQLite, or pgvector not ready):
		//   Load all active+conflicted entries of the store into memory and
		//   compute cosine similarity in JS. Correct at any scale; linear cost.
		//
		// Both paths run concurrently, so one scan-only store (e.g. a local SQLite)
		// no longer forces every Postgres store into a full table load (#93).
		// Candidates from both paths are merged by entry ID, keeping the highest
		// rawSimilarity across query vectors and stores.

		// Typed as KnowledgeEntry (not the narrower & { embedding: number[] }) so that
		// source entries fetched via getSupportSourcesForIds() can be pushed later without
		// a type error (those entries may have embedding undefined).
		const scored: Array<{
			entry: KnowledgeEntry;
			rawSimilarity: number;
			similarity: number;
			rerankScore?: number;
			/** Score the precision gate compares with its floor (rerank score, or cosine when dense). */
			gateScore: number;
			/** Store the entry was read from. Injection writes go to this store. */
			owner: IKnowledgeStore;
			staleness: {
				ageDays: number;
				strength: number;
				lastAccessedDaysAgo: number;
				mayBeStale: boolean;
			};
		}> = [];

		// Split read stores by retrieval path. Breaker state is per operation and
		// is checked per call in callStore, so every store takes part here.
		const annDbs: IKnowledgeStore[] = [];
		const scanDbs: IKnowledgeStore[] = [];
		for (const db of this.readDbs) {
			(storeSupportsAnn(db) ? annDbs : scanDbs).push(db);
		}

		// Log the retrieval path per store, but only when the partition changes
		// (first activation or a vector search readiness flip).
		const pathPartition = `ann=${annDbs.map((d) => d.id).join(",")};scan=${scanDbs.map((d) => d.id).join(",")}`;
		if (pathPartition !== this.lastPathPartition) {
			this.lastPathPartition = pathPartition;
			const fmt = (dbs: IKnowledgeStore[]) =>
				dbs.length > 0 ? `[${dbs.map((d) => d.id).join(", ")}]` : "none";
			logger.log(
				`[activation] Retrieval path per store: ANN=${fmt(annDbs)}, full scan=${fmt(scanDbs)}`,
			);
		}

		// Run all retrieval work concurrently. fanOutSafe tolerates per-store
		// failures (a failing store trips its circuit breaker and is skipped) and
		// returns [] for an empty store list, so a topology
		// without ANN or without scan stores makes no calls for that path.
		// - ANN: one fan-out per query vector, fetchLimit per query vector per
		//   store, flattened across stores so annResultsPerQuery is
		//   [query][result], not [query][store][result].
		// - COUNT: per ANN store, for totalActive without loading entries.
		// - Full scan: every entry of every scan-only store.
		const [annResultsPerQuery, perStoreCounts, scanEntriesPerStore] =
			await Promise.all([
				Promise.all(
					queryEmbeddings.map(async (qEmb) =>
						(
							await this.fanOutSafe(
								annDbs,
								async (db) => {
									// Path A only runs for stores that pass storeSupportsAnn.
									// biome-ignore lint/style/noNonNullAssertion: guarded by storeSupportsAnn
									const hits = await db.findSimilarEntries!(
										qEmb,
										fetchLimit,
										similarityThreshold,
									);
									return hits.map((hit) => ({ db, ...hit }));
								},
								"ANN search",
								slots,
							)
						).flat(),
					),
				),
				this.fanOutSafe(
					annDbs,
					(db) =>
						typeof db.getActiveEntryCount === "function"
							? db.getActiveEntryCount()
							: Promise.resolve(0),
					"active entry count",
					slots,
				),
				this.fanOutSafe(
					scanDbs,
					async (db) => ({ db, rows: await db.scanEmbeddings() }),
					"full-scan load",
					slots,
				),
			]);

		// Scan stores: score id + embedding rows only. Entries synced to several scan
		// stores share an ID, so the first occurrence is scored and the rest skipped.
		const preparedQueries = queryEmbeddings.map((q) => prepareQuery(q));
		const scanSeenIds = new Set<string>();
		const scanHits: Array<{
			db: IKnowledgeStore;
			id: string;
			rawSimilarity: number;
		}> = [];
		for (const { db, rows } of scanEntriesPerStore) {
			let mismatchedDims: number | undefined;
			for (const row of rows) {
				// Checked before the ID: a row that cannot be scored must not claim its
				// ID, or a matching copy of the same entry in another store is skipped.
				if (
					preparedQueries.some((q) => q.unit.length !== row.embedding.length)
				) {
					mismatchedDims ??= row.embedding.length;
					continue;
				}
				if (scanSeenIds.has(row.id)) continue;
				scanSeenIds.add(row.id);
				const bNorm = storedNorm(row.embedding);
				let rawSimilarity = Number.NEGATIVE_INFINITY;
				for (const q of preparedQueries) {
					rawSimilarity = Math.max(
						rawSimilarity,
						cosineToPrepared(q, row.embedding, bNorm),
					);
				}
				// ANN stores filter by threshold in the DB; apply the same cut here.
				// A non-finite score cannot be ranked, so it is a miss.
				if (
					!Number.isFinite(rawSimilarity) ||
					rawSimilarity < similarityThreshold
				) {
					continue;
				}
				scanHits.push({ db, id: row.id, rawSimilarity });
			}
			// Warn once per streak; a scan with no mismatched rows ends the streak.
			if (mismatchedDims === undefined) {
				this.dimMismatchStores.delete(db.id);
			} else if (!this.dimMismatchStores.has(db.id)) {
				this.dimMismatchStores.add(db.id);
				logger.warn(
					`[activation] Store "${db.id}" has ${mismatchedDims}-dim embeddings but the query is ${preparedQueries[0].unit.length}-dim — skipping those rows until they are re-embedded.`,
				);
			}
		}

		// Merge candidates from both paths: entry ID -> best rawSimilarity across
		// all query vectors and stores. ANN candidates are inserted first, so on a
		// tie the ANN copy is kept; otherwise the higher similarity wins and no
		// entry is lost when the same ID lives in an ANN and a scan store.
		const bestById = new Map<
			string,
			{
				entry: KnowledgeEntry;
				rawSimilarity: number;
				owner: IKnowledgeStore;
			}
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
		for (const queryResults of annResultsPerQuery) {
			for (const { db, entry, similarity } of queryResults) {
				keepBest(entry, similarity, db);
			}
		}
		// Hydrate only the scan hits that cleared the threshold.
		const hitIdsByStore = new Map<IKnowledgeStore, string[]>();
		for (const hit of scanHits) {
			const ids = hitIdsByStore.get(hit.db);
			if (ids) ids.push(hit.id);
			else hitIdsByStore.set(hit.db, [hit.id]);
		}
		const hydrated = (
			await this.fanOutSafe(
				[...hitIdsByStore.keys()],
				(db) => db.getEntriesByIds(hitIdsByStore.get(db) ?? []),
				"entry hydrate",
				slots,
			)
		).flat();
		const hydratedById = new Map(hydrated.map((e) => [e.id, e]));
		for (const hit of scanHits) {
			const entry = hydratedById.get(hit.id);
			if (entry) keepBest(entry, hit.rawSimilarity, hit.db);
		}

		for (const { entry, rawSimilarity, owner } of bestById.values()) {
			const ageDays = (now - entry.createdAt) / DAY_MS;
			const lastAccessedDaysAgo = (now - entry.lastAccessedAt) / DAY_MS;
			const halfLife =
				config.decay.typeHalfLife[entry.type] || config.decay.typeHalfLife.fact;
			const liveStrength = computeStrength(entry, now);

			scored.push({
				entry,
				// Threshold filtering uses raw cosine similarity so entry age never
				// prevents a semantically relevant entry from activating.
				// Ranking uses raw similarity weighted by live strength - well-consolidated
				// entries sort higher, but stale entries still appear if they're on-topic.
				// The staleness signals in the response let the LLM reason about reliability.
				rawSimilarity,
				similarity: rawSimilarity * liveStrength,
				gateScore: rawSimilarity,
				owner,
				staleness: {
					ageDays: Math.round(ageDays),
					strength: liveStrength,
					lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
					mayBeStale: isMayBeStale(
						entry,
						ageDays,
						lastAccessedDaysAgo,
						halfLife,
					),
				},
			});
		}

		// totalActive: per-store COUNT for ANN stores plus unique entries loaded
		// from scan stores. An ID present in both an ANN and a scan store is
		// counted twice (COUNT returns no IDs to dedupe against), the same as
		// Path A already does across several ANN stores. In single-path
		// topologies one term is 0, so the value matches the old per-path result.
		const totalActive =
			perStoreCounts.reduce((sum, n) => sum + n, 0) + scanSeenIds.size;

		if (scored.length === 0) {
			return {
				entries: [],
				query: primaryQuery,
				totalActive,
				telemetry: {
					cueCount: queryList.length,
					nCandidates: 0,
					gateMode: this.reranker ? "dense" : "off",
					gateDropped: 0,
					budgetDropped: 0,
					deduped: 0,
					injectedTokens: 0,
				},
			};
		}

		// ── Second stage: cross-encoder rerank ─────────────────────────────────
		// Dense retrieval (above) is the recall stage — it overfetched
		// `fetchLimit` candidates per cue. The reranker is the precision stage:
		// it jointly encodes each (query, candidate) pair and produces a much
		// sharper relevance signal than cosine similarity over lossy single
		// vectors. Decay-strength modulation is preserved exactly: the final
		// ranking score becomes rerankScore × liveStrength (was: rawSimilarity ×
		// liveStrength), so well-consolidated entries still sort higher.
		//
		// Fail-open: any reranker error (model load, inference, timeout) logs a
		// warning and leaves the dense-only ordering intact.
		const nCandidates = scored.length;
		let gateDropped = 0;
		let gateMode: GateMode = this.reranker ? "dense" : "off";
		let rerankModel: string | null = null;
		if (this.reranker && scored.length > 0) {
			// Bound the rerank input: across multiple cues and stores the pool can exceed
			// `candidates`, so keep the dense-best slice. The order is always dense
			// similarity, best first: listwise scorers show position bias, and a fixed
			// order (D00 is the dense best) keeps their output reproducible.
			const maxCandidates = config.activation.rerank.candidates;
			scored.sort((a, b) => b.rawSimilarity - a.rawSimilarity);
			if (scored.length > maxCandidates) scored.length = maxCandidates;

			try {
				const documents = scored.map((s) =>
					formatEmbeddingText(s.entry.type, s.entry.content, s.entry.topics),
				);
				const scores = await this.reranker.scoreBatch(primaryQuery, documents);
				for (let i = 0; i < scored.length; i++) {
					scored[i].rerankScore = scores[i];
					scored[i].gateScore = scores[i];
					scored[i].similarity =
						scores[i] *
						(config.activation.gate.strengthDampening
							? 0.5 + 0.5 * scored[i].staleness.strength
							: scored[i].staleness.strength);
				}
				gateMode = "rerank";
				rerankModel = scorerKey(this.reranker.identity);
				componentHealth.recordSuccess("rerank");
			} catch (err) {
				componentHealth.recordFailure("rerank", err);
				const reason = err instanceof Error ? err.message : String(err);
				const { provider, model } = this.reranker.identity;
				if (err instanceof RerankSkippedError) {
					// The remote breaker warns once when it opens; skipped calls stay quiet.
					logger.log(
						`[activation] Rerank skipped — dense-only ordering. provider=${provider} model=${model} gate_mode=dense. Reason: ${reason}`,
					);
				} else {
					logger.warn(
						`[activation] Rerank failed — using dense-only ordering. provider=${provider} model=${model} gate_mode=dense. Reason: ${reason}`,
					);
				}
			}
		}

		// ── Precision gate: floor ───────────────────────────────────────────────
		// Runs before the cap, so the cap is spent on candidates that pass. Support
		// sources are gated when they are added below.
		const gateOn = config.activation.gate.enabled;
		const floor = gateOn
			? this.gateFloor(gateMode, scored, similarityThreshold)
			: null;
		if (floor !== null) {
			const passing = scored.filter((s) => s.gateScore >= floor);
			gateDropped += scored.length - passing.length;
			scored.splice(0, scored.length, ...passing);
		}

		// Sort, then drop what cannot be injected before the cap, so the cap is spent
		// on entries that can be shown: flagged conflicted entries with no contradicts
		// partner, and entries already shown in this session (session-dedupe.ts).
		scored.sort((a, b) => b.similarity - a.similarity);
		const conflictedIds = scored
			.filter(({ entry }) => entry.status === "conflicted")
			.map(({ entry }) => entry.id);
		const contradictPairs = mergeMaps(
			await this.fanOutSafe(
				this.readDbs,
				(db) => db.getContradictPairsForIds(conflictedIds),
				"contradict pairs lookup",
				slots,
			),
		);
		const beforeFlagCheck = scored.length;
		const unflagged = scored.filter(
			({ entry }) =>
				entry.status !== "conflicted" || contradictPairs.has(entry.id),
		);
		gateDropped += beforeFlagCheck - unflagged.length;
		scored.splice(0, scored.length, ...unflagged);

		const sessionKey = options?.session;
		let deduped = 0;
		if (sessionKey !== undefined) {
			const fresh = scored.filter((s) => {
				const suppressed = this.sessions.isSuppressed(
					sessionKey,
					{ id: s.entry.id, rawSimilarity: s.rawSimilarity },
					now,
				);
				if (suppressed) deduped++;
				return !suppressed;
			});
			scored.splice(0, scored.length, ...fresh);
		}
		scored.splice(maxResults);

		// ── Relation-aware activation: pull in supports sources ───────────────────
		// When a synthesized principle/pattern activates, surface its source entries
		// alongside it. This lets the agent see both the abstraction and the
		// concrete evidence it was distilled from.
		//
		// Design:
		// - Collect IDs of activated synthesized entries (principle or pattern type)
		// - Batch-fetch their supports source entries from the DB
		// - For each source entry not already in the scored set, compute its score
		//   as the synthesized entry's similarity × 0.9 (slightly lower to rank it
		//   below the principle itself, but still nearby)
		// - Gate: a source inherits no rerank score, so it is gated at 0.9× its
		//   parent's gate score (the same discount as its rank), against the same floor
		// - Add sources to the result (but do NOT exceed maxResults)
		const synthesizedIds = scored
			.filter(
				({ entry }) => entry.type === "principle" || entry.type === "pattern",
			)
			.map(({ entry }) => entry.id);

		const scoredIdSet = new Set(scored.map(({ entry }) => entry.id));

		if (synthesizedIds.length > 0) {
			// Fan out support sources lookup across read stores. A synthesized
			// entry's sources come from the first store that returns them (the same
			// precedence as mergeMaps), and each source keeps its owning store.
			const sourceMaps = await this.fanOutSafe(
				this.readDbs,
				async (db) => ({
					db,
					map: await db.getSupportSourcesForIds(synthesizedIds),
				}),
				"support sources lookup",
				slots,
			);
			const sourcesMap = new Map<
				string,
				Array<{ source: KnowledgeEntry; owner: IKnowledgeStore }>
			>();
			for (const { db, map } of sourceMaps) {
				for (const [synthId, sources] of map) {
					if (sourcesMap.has(synthId)) continue;
					sourcesMap.set(
						synthId,
						sources.map((source) => ({ source, owner: db })),
					);
				}
			}

			// Snapshot the array before iterating — we push source entries into `scored`
			// during the loop and don't want to visit newly-added entries (which could
			// cascade if a source is itself a principle with its own supports relations).
			// Conflicted sources need their contradicts partners for the flag check.
			const unfetched = [...sourcesMap.values()]
				.flat()
				.filter(
					({ source }) =>
						source.status === "conflicted" && !contradictPairs.has(source.id),
				)
				.map(({ source }) => source.id);
			if (unfetched.length > 0) {
				const more = mergeMaps(
					await this.fanOutSafe(
						this.readDbs,
						(db) => db.getContradictPairsForIds(unfetched),
						"contradict pairs lookup",
						slots,
					),
				);
				for (const [id, partner] of more) contradictPairs.set(id, partner);
			}
			const snapshot = [...scored];
			for (const parent of snapshot) {
				const { entry, rawSimilarity, similarity: synthSimilarity } = parent;
				if (entry.type !== "principle" && entry.type !== "pattern") continue;
				const sources = sourcesMap.get(entry.id);
				if (!sources) continue;

				for (const { source, owner } of sources) {
					if (scoredIdSet.has(source.id)) continue; // already activated
					if (scored.length >= maxResults) break;

					const sourceGate = parent.gateScore * SOURCE_GATE_DISCOUNT;
					if (floor !== null && sourceGate < floor) {
						gateDropped++;
						continue;
					}
					// Same rules as candidates: a flagged source and a source already shown in
					// this session are not injected, and do not use up the cap.
					if (
						source.status === "conflicted" &&
						!contradictPairs.has(source.id)
					) {
						gateDropped++;
						continue;
					}
					if (
						sessionKey !== undefined &&
						this.sessions.isSuppressed(
							sessionKey,
							{ id: source.id, rawSimilarity },
							now,
						)
					) {
						deduped++;
						continue;
					}

					const liveStrength = computeStrength(source, now);
					const ageDays = (now - source.createdAt) / DAY_MS;
					const lastAccessedDaysAgo = (now - source.lastAccessedAt) / DAY_MS;
					const halfLife =
						config.decay.typeHalfLife[source.type] ||
						config.decay.typeHalfLife.fact;

					// Use the synthesized entry's rawSimilarity as the source's signal.
					// This ensures source entries rank slightly below their synthesized principle.
					scored.push({
						entry: source,
						rawSimilarity,
						similarity: synthSimilarity * SOURCE_GATE_DISCOUNT * liveStrength,
						gateScore: sourceGate,
						owner,
						staleness: {
							ageDays: Math.round(ageDays),
							strength: liveStrength,
							lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
							mayBeStale: isMayBeStale(
								source,
								ageDays,
								lastAccessedDaysAgo,
								halfLife,
							),
						},
					});
					scoredIdSet.add(source.id);
				}

				// Respect maxResults cap for the outer loop too.
				if (scored.length >= maxResults) break;
			}
		}
		// ─────────────────────────────────────────────────────────────────────────

		// Build a map of activated entries for fast lookup during annotation.
		// Used to determine whether a conflicted entry's counterpart also activated
		// (we only annotate when both sides of the conflict are relevant to this query),
		// and to retrieve the counterpart's content without an extra DB round-trip.
		const scoredById = new Map(scored.map(({ entry }) => [entry.id, entry]));

		// Build contradiction annotations for conflicted entries whose counterpart
		// also activated in this query (both sides relevant — only then is the caveat useful).
		const contradictionMap = new Map<string, ContradictionAnnotation>();
		for (const { entry } of scored) {
			if (entry.status !== "conflicted") continue;

			const counterpartId = contradictPairs.get(entry.id);
			if (!counterpartId) continue;

			// Only annotate if the counterpart also activated in this same query.
			// The counterpart is already in scoredById if it activated, so no extra DB fetch needed.
			const counterpart = scoredById.get(counterpartId);
			if (!counterpart) continue;

			contradictionMap.set(entry.id, {
				conflictingEntryId: counterpartId,
				conflictingContent: counterpart.content,
				caveat:
					"This knowledge conflicts with another activated entry and has not been resolved. " +
					"Verify before relying on it; the conflicting view is also shown in this response.",
			});
		}

		// ── Token budget and render cap ─────────────────────────────────────────
		// One cap for everything that is counted, recorded and remembered. The block
		// cannot exceed RENDER_TOKEN_CAP, the formatter's own cap in the same estimate,
		// and the token budget is clamped to it. Each line is costed with the
		// formatter's newline, so the greedy prefix kept here is exactly what
		// formatActivatedKnowledge renders. The budget applies with the gate off too.
		const budget = gateOn
			? Math.min(config.activation.gate.tokenBudget, RENDER_TOKEN_CAP)
			: RENDER_TOKEN_CAP;
		const lineTokens = new Map(
			scored.map((s) => [
				s,
				estimateTokens(
					formatKnowledgeLine({
						entry: s.entry,
						staleness: s.staleness,
						contradiction: contradictionMap.get(s.entry.id),
					}).length + 1,
				),
			]),
		);
		const final = applyTokenBudget(
			scored,
			budget,
			(s) => lineTokens.get(s) ?? 0,
			(s) => s.rawSimilarity >= similarityThreshold,
		);
		const budgetDropped = scored.length - final.length;
		// A counterpart the budget dropped is no longer in the response, so its
		// annotation would point at nothing. Remove it.
		const finalIds = new Set(final.map(({ entry }) => entry.id));
		for (const [id, annotation] of contradictionMap) {
			if (!finalIds.has(annotation.conflictingEntryId)) {
				contradictionMap.delete(id);
			}
		}
		const injectedTokens = final.reduce(
			(n, s) => n + (lineTokens.get(s) ?? 0),
			0,
		);

		return {
			entries: final.map(
				({ entry, rawSimilarity, similarity, rerankScore, staleness }) => ({
					entry: { ...entry, embedding: undefined } as KnowledgeEntry,
					similarity,
					rawSimilarity,
					rerankScore,
					staleness,
					contradiction: contradictionMap.get(entry.id),
				}),
			),
			query: primaryQuery,
			totalActive,
			telemetry: {
				cueCount: queryList.length,
				nCandidates,
				gateMode,
				rerankModel,
				gateDropped,
				budgetDropped,
				deduped,
				injectedTokens,
			},
			injection: this.injectionPlan(final, { dryRun, sessionKey, now }),
		};
	}

	/**
	 * What one response showed: injection writes grouped by owning store (writable
	 * stores only, none for dryRun), and the session dedupe record. Undefined for a
	 * dryRun without a session, which has nothing to commit.
	 */
	private injectionPlan(
		items: ReadonlyArray<{
			entry: KnowledgeEntry;
			owner: IKnowledgeStore;
			rawSimilarity: number;
		}>,
		opts: { dryRun: boolean; sessionKey: string | undefined; now: number },
	): InjectionPlan | undefined {
		if (opts.dryRun && opts.sessionKey === undefined) return undefined;
		const byStore = new Map<string, string[]>();
		if (!opts.dryRun) {
			for (const { entry, owner } of items) {
				if (!this.writableDbs.some((db) => db.id === owner.id)) continue;
				const ids = byStore.get(owner.id);
				if (ids) ids.push(entry.id);
				else byStore.set(owner.id, [entry.id]);
			}
		}
		return {
			writes: [...byStore].map(([storeId, entryIds]) => ({
				storeId,
				entryIds,
			})),
			session:
				opts.sessionKey === undefined
					? undefined
					: {
							key: opts.sessionKey,
							at: opts.now,
							items: items.map(({ entry, rawSimilarity }) => ({
								id: entry.id,
								rawSimilarity,
							})),
						},
		};
	}

	/**
	 * Floor of the precision gate for this ordering mode, or null when no gate
	 * applies (no reranker configured).
	 */
	private gateFloor(
		gateMode: GateMode,
		scored: ReadonlyArray<{ gateScore: number }>,
		similarityThreshold: number,
	): number | null {
		const gate = config.activation.gate;
		if (gateMode === "rerank" && this.reranker) {
			let top = 0;
			for (const s of scored) top = Math.max(top, s.gateScore);
			return rerankFloor({
				identity: this.reranker.identity,
				floorOverride: gate.rerankFloor,
				relFrac: gate.rerankRelFrac,
				topScore: top,
			});
		}
		if (gateMode === "dense") {
			return denseFloor(similarityThreshold, gate.denseMargin);
		}
		return null;
	}

	/**
	 * Record one response's injections: the session dedupe record (in memory) and one
	 * batched write per owning writable store. Call it once the response has been
	 * delivered. The caller skips abandoned requests, so an entry the client never
	 * received is neither counted nor suppressed for the next prompt. Store writes
	 * are not awaited, so they add no latency. A failing store is logged once per
	 * failure streak and never reaches the caller.
	 */
	commitInjections(plan: InjectionPlan | undefined): void {
		if (!plan) return;
		if (plan.session) {
			this.sessions.remember(
				plan.session.key,
				plan.session.items,
				plan.session.at,
			);
		}
		const now = Date.now();
		for (const write of plan.writes) {
			const db = this.writableDbs.find((d) => d.id === write.storeId);
			if (!db) continue;
			db.recordInjections(write.entryIds, now).then(
				() => {
					this.injectionFailing.delete(db.id);
				},
				(err: unknown) => {
					if (this.injectionFailing.has(db.id)) return;
					this.injectionFailing.add(db.id);
					logger.warn(
						`[activation] Recording injections failed on store "${db.id}"; access stats are not updated until it recovers. Reason: ${describeError(err)}`,
					);
				},
			);
		}
	}

	/**
	 * Ensure all active and conflicted entries have embeddings.
	 * Called during consolidation (new entries) or on startup (migration).
	 *
	 * Conflicted entries are included because:
	 * - They participate in similarity search (getActiveEntriesWithEmbeddings includes them)
	 * - A merge resolution may clear their embedding (embedding = NULL)
	 * - Without re-embedding, they become permanently invisible to reconsolidation
	 *   and the contradiction scan, preventing automatic re-resolution.
	 *
	 * Uses a single DB query (getEntriesMissingEmbeddings) rather than two separate
	 * status queries to avoid any risk of duplicate entries in the result set.
	 */
	async ensureEmbeddings(): Promise<number> {
		let total = 0;
		for (const store of this.writableDbs) {
			total += await this.ensureEmbeddingsForStore(store);
		}
		return total;
	}

	private async ensureEmbeddingsForStore(db: IKnowledgeStore): Promise<number> {
		const needsEmbedding = await db.getEntriesMissingEmbeddings();

		if (needsEmbedding.length === 0) return 0;

		// Build embedding text using the shared formatter — same format as
		// reconsolidate() so vectors are consistent regardless of which path wrote them.
		const texts = needsEmbedding.map((e) =>
			formatEmbeddingText(e.type, e.content, e.topics),
		);

		const embeddings = await this.embeddings.embedBatch(
			texts,
			bulkEmbedOptions(),
		);

		for (let i = 0; i < needsEmbedding.length; i++) {
			await db.updateEntry(needsEmbedding[i].id, {
				embedding: embeddings[i],
			});
		}

		return needsEmbedding.length;
	}

	/**
	 * Check whether the configured embedding model differs from the model that
	 * produced the stored embeddings. If it has changed, re-embed every entry
	 * that has an embedding (any status except tombstoned) with the new model.
	 *
	 * Flow:
	 * 1. Read stored embedding metadata (model + dimensions) from DB
	 * 2. If no metadata exists (first run or pre-v8 upgrade):
	 *    - If entries with embeddings exist, probe the first one's dimension count
	 *      and record the current model as the baseline
	 *    - If no entries exist, just record the current model
	 * 3. If stored model matches configured model → no-op
	 * 4. If stored model differs:
	 *    a. Log the model change clearly
	 *    b. Clear embeddings of tombstoned entries (regenerated only on revival)
	 *    c. Re-embed the remaining entries in batches. The vector column is
	 *       reshaped before the first write, and the metadata is written LAST
	 *       (see inline comments below)
	 *
	 * Called at startup before consolidation so all vectors are consistent.
	 * Fans out across all writable stores. Returns true if any store was re-embedded.
	 */
	async checkAndReEmbed(): Promise<boolean> {
		let anyReEmbedded = false;
		for (const store of this.writableDbs) {
			let did: boolean;
			try {
				did = await this.checkAndReEmbedStore(store);
			} catch (e) {
				throw new Error(
					`Re-embed check for store "${store.id}" failed: ${describeError(e)}`,
					{ cause: e },
				);
			}
			if (did) anyReEmbedded = true;
		}
		return anyReEmbedded;
	}

	/**
	 * Per-store re-embed logic — factored out so checkAndReEmbed() can fan out
	 * across all writable stores. Returns true if a re-embed was performed.
	 */
	private async checkAndReEmbedStore(db: IKnowledgeStore): Promise<boolean> {
		const configuredModel = config.embedding.model;
		const stored = await db.getEmbeddingMetadata();

		if (!stored) {
			// First run or upgrade from pre-v8 schema — seed metadata.
			// Probe dimensions from an existing entry if available.
			const existing = await db.getActiveEntriesWithEmbeddings();
			if (existing.length > 0) {
				const dims = existing[0].embedding.length;
				logger.log(
					`[embedding] No embedding metadata found. Recording current model: ${configuredModel} (${dims} dimensions, from ${existing.length} existing entries).`,
				);
				await db.setEmbeddingMetadata(configuredModel, dims);
			} else {
				// No entries yet — probe the API for dimensions.
				logger.log(
					"[embedding] No embedding metadata found and no entries exist. Probing model for dimensions...",
				);
				try {
					const probe = await this.embeddings.embed("dimension probe");
					await db.setEmbeddingMetadata(configuredModel, probe.length);
					logger.log(
						`[embedding] Recorded embedding model: ${configuredModel} (${probe.length} dimensions).`,
					);
				} catch (e) {
					// Non-fatal at this stage — metadata will be recorded on first
					// successful consolidation or activation. Log and continue.
					logger.warn(
						`[embedding] Could not probe embedding dimensions: ${e instanceof Error ? e.message : String(e)}. Metadata will be recorded on first successful embedding call.`,
					);
				}
			}
			return false;
		}

		// Model matches — nothing to do.
		if (stored.model === configuredModel) {
			return false;
		}

		// ── Model has changed — re-embed every stored vector in place ──
		//
		// Every entry with an embedding is re-embedded, whatever its status: a
		// superseded, archived or conflicted row can be revived later (conflict
		// resolution, PATCH) and must not come back with an old-model vector.
		// Tombstoned rows are the exception: their vectors are cleared and
		// regenerated only if they are ever revived.
		//
		// Failure modes:
		// - embedBatch or a write throws: metadata is not stamped, so the next
		//   startup sees the mismatch again and resumes. Rows already written hold
		//   new-model vectors and are simply re-embedded on that run.
		// - Metadata is stamped only after the last batch, so it never claims a
		//   model that some rows do not yet match.
		logger.log(
			`[embedding] Model change detected: "${stored.model}" → "${configuredModel}".`,
		);

		const cleared = await db.clearTombstonedEmbeddings();
		if (cleared > 0) {
			logger.log(
				`[embedding] Cleared ${cleared} tombstoned embedding(s); they regenerate if revived.`,
			);
		}

		const reEmbedStart = Date.now();
		let cursor: string | null = null;
		let processed = 0;
		let dimensions: number | null = null;

		for (;;) {
			const batch = await db.getEntriesForReEmbed(cursor, RE_EMBED_BATCH_SIZE);
			if (batch.length === 0) break;

			const vectors = await this.embeddings.embedBatch(
				batch.map((e) => formatEmbeddingText(e.type, e.content, e.topics)),
				bulkEmbedOptions(),
			);

			if (dimensions === null) {
				// The new vector size is known only now. Reshape the vector column
				// before the first write so a new-size vector never meets an old-size
				// column.
				dimensions = vectors[0].length;
				await db.beginEmbeddingModelChange(dimensions);
			}

			for (let i = 0; i < batch.length; i++) {
				await db.updateEntry(batch[i].id, { embedding: vectors[i] });
			}

			processed += batch.length;
			cursor = batch[batch.length - 1].id;
			logger.log(
				`[embedding] Re-embedded ${processed} entries with "${configuredModel}"...`,
			);
		}

		if (dimensions === null) {
			// No entries with embeddings to re-embed — just update metadata.
			logger.log(
				"[embedding] No entries with embeddings to re-embed — updating metadata only.",
			);
			try {
				const probe = await this.embeddings.embed("dimension probe");
				await db.setEmbeddingMetadata(configuredModel, probe.length);
				logger.log(
					`[embedding] Recorded new embedding model: ${configuredModel} (${probe.length} dimensions).`,
				);
			} catch (e) {
				logger.warn(
					`[embedding] Could not probe new model dimensions: ${e instanceof Error ? e.message : String(e)}. Metadata will be recorded on first successful embedding call.`,
				);
			}
			return false;
		}

		await db.setEmbeddingMetadata(configuredModel, dimensions);

		const durationSec = ((Date.now() - reEmbedStart) / 1000).toFixed(1);

		logger.log(
			`[embedding] Re-embed complete: ${processed} entries re-embedded in ${durationSec}s ` +
				`(${stored.dimensions} → ${dimensions} dimensions).`,
		);

		return true;
	}
}

/** Entries per page when re-embedding after a model change. */
const RE_EMBED_BATCH_SIZE = 100;

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * True when a store can serve activation via DB-side ANN search (Path A):
 * findSimilarEntries is present (PostgresKnowledgeDB, not SQLite) and
 * isVectorSearchReady() is not false. Readiness can be false during a
 * model-change re-embed window, where findSimilarEntries would silently
 * return [] - such a store must use the full scan (Path B) instead.
 */
function storeSupportsAnn(db: IKnowledgeStore): boolean {
	return (
		typeof db.findSimilarEntries === "function" &&
		(typeof db.isVectorSearchReady !== "function" || db.isVectorSearchReady())
	);
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

/**
 * Merge an array of Maps into a single Map.
 * Earlier maps take precedence on key collisions (first-store-wins).
 */
function mergeMaps<K, V>(maps: Map<K, V>[]): Map<K, V> {
	const result = new Map<K, V>();
	for (const map of maps) {
		for (const [k, v] of map) {
			if (!result.has(k)) result.set(k, v);
		}
	}
	return result;
}
