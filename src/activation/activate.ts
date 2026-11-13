import { config } from "../config.js";
import type { IKnowledgeStore } from "../db/index.js";
import { logger } from "../logger.js";
import type {
	ActivationResult,
	InjectionPlan,
	KnowledgeEntry,
} from "../types.js";
import {
	EmbeddingClient,
	bulkEmbedOptions,
	describeError,
	formatEmbeddingText,
	prepareQuery,
} from "./embeddings.js";
import type { RerankScorer } from "./rerank.js";
import { SessionDedupe } from "./session-dedupe.js";
import {
	type AnnHit,
	type Candidate,
	type ScanHit,
	type ScanScore,
	type SourceExpansion,
	type SupportSource,
	appendSupportSources,
	applyPrecisionGate,
	applyRenderBudget,
	applySessionDedupe,
	buildContradictionAnnotations,
	dropUnflaggedConflicts,
	hasFlagWithoutPartner,
	isSynthesized,
	mergeCandidates,
	precisionFloor,
	scoreCandidates,
	scoreScanRows,
	sortByRank,
	toActivationEntries,
} from "./stages.js";

/** Session dedupe: retained sessions (LRU). */
const MAX_SESSIONS = 1000;
/** Raw-cosine gain that lets a suppressed session entry be injected again. */
const SESSION_RESCORE_MARGIN = 0.1;

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
	 * activation. A healthy store is not capped, so its cues fan out at once. After
	 * failures, a closed circuit allows threshold - failures calls at a time. An open
	 * or half-open circuit admits through admit() alone: one probe per window.
	 */
	budget(store: string, op: string): number {
		const s = this.stateOf(store, op);
		if (s.status !== "closed" || s.failures === 0) {
			return Number.POSITIVE_INFINITY;
		}
		return Math.max(1, this.threshold - s.failures);
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
	 * @param writableDb   The primary writable store.
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
	 * A store that has not failed is not capped: its cues fan out at once. After
	 * failures, a closed circuit admits at most threshold - failures calls at a time.
	 * An open or half-open circuit admits one probe per window through admit(). Calls
	 * the breaker refuses return at once: no timeout wait, no warning.
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
	 * The stages run in this order. stages.ts holds the ones that read no store:
	 * retrieval, rerank, precision gate, sort and drop what cannot be injected,
	 * session dedupe and rank cap, support sources, contradiction annotations,
	 * render budget.
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
			/**
			 * performance.now() when the request entered the server. Rerank starts only
			 * with enough of the budget left, and its timeout is clamped to what remains.
			 * Omitted: no deadline.
			 */
			requestStartedAt?: number;
			/**
			 * Time budget of the request, in ms, counted from requestStartedAt. Defaults
			 * to DEFAULT_ACTIVATION_BUDGET_MS; the server passes the client's own budget.
			 */
			budgetMs?: number;
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
		const sessionKey = options?.session;
		const maxResults = options?.limit ?? config.activation.maxResults;
		const similarityThreshold =
			options?.threshold ?? config.activation.similarityThreshold;
		const gateOn = config.activation.gate.enabled;

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

		const retrieved = await this.retrieveCandidates(queryEmbeddings, {
			fetchLimit,
			similarityThreshold,
			now,
			slots,
		});
		if (retrieved.candidates.length === 0) {
			return {
				entries: [],
				query: primaryQuery,
				totalActive: retrieved.totalActive,
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
		const nCandidates = retrieved.candidates.length;

		const reranked = await scoreCandidates(
			this.reranker,
			retrieved.candidates,
			primaryQuery,
			{
				requestStartedAt: options?.requestStartedAt,
				budgetMs: options?.budgetMs,
				now: performance.now(),
			},
		);

		const floor = gateOn
			? precisionFloor(
					this.reranker,
					reranked.gateMode,
					reranked.candidates,
					similarityThreshold,
				)
			: null;
		const gate = applyPrecisionGate(
			reranked.candidates,
			floor,
			reranked.gateMode,
		);
		let gateDropped = gate.gateDropped;

		// Sort, then drop what cannot be injected before the cap, so the cap is spent
		// on entries that can be shown: flagged conflicted entries with no contradicts
		// partner, and entries already shown in this session (session-dedupe.ts).
		const ranked = sortByRank(gate.candidates);
		const contradictPairs = await this.loadContradictPairs(
			ranked
				.filter(({ entry }) => entry.status === "conflicted")
				.map(({ entry }) => entry.id),
			slots,
		);
		const flagged = dropUnflaggedConflicts(ranked, contradictPairs);
		gateDropped += flagged.dropped;
		const fresh = applySessionDedupe(
			flagged.kept,
			this.sessions,
			sessionKey,
			now,
		);
		let deduped = fresh.deduped;
		const capped = fresh.kept;
		capped.splice(maxResults);

		// Relation-aware activation: the sources of a synthesized entry are pulled in
		// alongside it, so the agent sees the abstraction and its evidence.
		const expanded = await this.expandSupportSources(
			capped,
			contradictPairs,
			{ sourceFloor: gate.sourceFloor, maxResults, sessionKey, now },
			slots,
		);
		gateDropped += expanded.gateDropped;
		deduped += expanded.deduped;

		const contradictions = buildContradictionAnnotations(
			expanded.candidates,
			expanded.contradictPairs,
		);
		const rendered = applyRenderBudget(expanded.candidates, contradictions, {
			gateOn,
			similarityThreshold,
		});

		return {
			entries: toActivationEntries(rendered.final, rendered.contradictions),
			query: primaryQuery,
			totalActive: retrieved.totalActive,
			telemetry: {
				cueCount: queryList.length,
				nCandidates,
				gateMode: reranked.gateMode,
				rerankModel: reranked.rerankModel,
				gateDropped,
				budgetDropped: rendered.budgetDropped,
				deduped,
				injectedTokens: rendered.injectedTokens,
			},
			injection: this.injectionPlan(rendered.final, {
				dryRun,
				sessionKey,
				now,
			}),
		};
	}

	/**
	 * Candidate retrieval for one activation. The path is chosen per store (see
	 * storeSupportsAnn), and both paths run concurrently, so one scan-only store
	 * (e.g. a local SQLite) does not force every Postgres store into a full table
	 * load (#93).
	 *
	 * Path A — pgvector (findSimilarEntries, ready): one ANN query per query vector.
	 *   The HNSW index does the work; only candidates above the threshold return.
	 *   Similarity values are pre-computed by the DB (1 - cosine distance).
	 * Path B — in-process full scan (SQLite, or pgvector not ready): every active and
	 *   conflicted row is loaded and scored in JS. Correct at any scale; linear cost.
	 *
	 * fanOutSafe tolerates per-store failures and returns [] for an empty store list,
	 * so a topology without ANN or without scan stores makes no calls for that path.
	 * Breaker state is checked per call in callStore, so every store takes part.
	 */
	private async retrieveCandidates(
		queryEmbeddings: number[][],
		opts: {
			fetchLimit: number;
			similarityThreshold: number;
			now: number;
			slots: Map<string, StoreSlot>;
		},
	): Promise<{ candidates: Candidate[]; totalActive: number }> {
		const annDbs: IKnowledgeStore[] = [];
		const scanDbs: IKnowledgeStore[] = [];
		for (const db of this.readDbs) {
			(storeSupportsAnn(db) ? annDbs : scanDbs).push(db);
		}
		this.logRetrievalPath(annDbs, scanDbs);

		// COUNT per ANN store gives totalActive without loading entries. The full scan
		// loads every entry of every scan-only store.
		const [annResults, perStoreCounts, scanRows] = await Promise.all([
			this.annSearch(queryEmbeddings, annDbs, opts),
			this.fanOutSafe(
				annDbs,
				(db) =>
					typeof db.getActiveEntryCount === "function"
						? db.getActiveEntryCount()
						: Promise.resolve(0),
				"active entry count",
				opts.slots,
			),
			this.fanOutSafe(
				scanDbs,
				async (db) => ({ db, rows: await db.scanEmbeddings() }),
				"full-scan load",
				opts.slots,
			),
		]);

		const prepared = queryEmbeddings.map((q) => prepareQuery(q));
		const scan = scoreScanRows(scanRows, prepared, opts.similarityThreshold);
		this.recordDimMismatches(scan.mismatches, prepared[0].unit.length);
		const hydrated = await this.hydrateScanHits(scan.hits, opts.slots);

		return {
			candidates: mergeCandidates(annResults, scan.hits, hydrated, opts.now),
			// totalActive: per-store COUNT for ANN stores plus unique entries loaded from
			// scan stores. An ID present in both an ANN and a scan store is counted twice
			// (COUNT returns no IDs to dedupe against), the same as Path A already does
			// across several ANN stores.
			totalActive:
				perStoreCounts.reduce((sum, n) => sum + n, 0) + scan.seenIds.size,
		};
	}

	/**
	 * Path A: one fan-out per query vector, flattened across stores, so the result is
	 * [query][hit] rather than [query][store][hit].
	 */
	private annSearch(
		queryEmbeddings: number[][],
		annDbs: IKnowledgeStore[],
		opts: {
			fetchLimit: number;
			similarityThreshold: number;
			slots: Map<string, StoreSlot>;
		},
	): Promise<AnnHit[][]> {
		return Promise.all(
			queryEmbeddings.map(async (qEmb) =>
				(
					await this.fanOutSafe(
						annDbs,
						async (db) => {
							// Path A only runs for stores that pass storeSupportsAnn.
							// biome-ignore lint/style/noNonNullAssertion: guarded by storeSupportsAnn
							const hits = await db.findSimilarEntries!(
								qEmb,
								opts.fetchLimit,
								opts.similarityThreshold,
							);
							return hits.map((hit) => ({ db, ...hit }));
						},
						"ANN search",
						opts.slots,
					)
				).flat(),
			),
		);
	}

	/**
	 * Log the retrieval path per store, but only when the partition changes (first
	 * activation, or a vector search readiness flip).
	 */
	private logRetrievalPath(
		annDbs: IKnowledgeStore[],
		scanDbs: IKnowledgeStore[],
	): void {
		const pathPartition = `ann=${annDbs.map((d) => d.id).join(",")};scan=${scanDbs.map((d) => d.id).join(",")}`;
		if (pathPartition === this.lastPathPartition) return;
		this.lastPathPartition = pathPartition;
		const fmt = (dbs: IKnowledgeStore[]) =>
			dbs.length > 0 ? `[${dbs.map((d) => d.id).join(", ")}]` : "none";
		logger.log(
			`[activation] Retrieval path per store: ANN=${fmt(annDbs)}, full scan=${fmt(scanDbs)}`,
		);
	}

	/** Full entries for the scan hits that cleared the threshold. Nothing else is loaded. */
	private async hydrateScanHits(
		hits: readonly ScanHit[],
		slots: Map<string, StoreSlot>,
	): Promise<KnowledgeEntry[]> {
		const idsByStore = new Map<IKnowledgeStore, string[]>();
		for (const hit of hits) {
			const ids = idsByStore.get(hit.db);
			if (ids) ids.push(hit.id);
			else idsByStore.set(hit.db, [hit.id]);
		}
		const hydrated = await this.fanOutSafe(
			[...idsByStore.keys()],
			(db) => db.getEntriesByIds(idsByStore.get(db) ?? []),
			"entry hydrate",
			slots,
		);
		return hydrated.flat();
	}

	/**
	 * Warn once per streak per store. A scan with no mismatched rows ends the streak.
	 */
	private recordDimMismatches(
		mismatches: ScanScore["mismatches"],
		queryDims: number,
	): void {
		for (const { storeId, mismatchedDims } of mismatches) {
			if (mismatchedDims === undefined) {
				this.dimMismatchStores.delete(storeId);
			} else if (!this.dimMismatchStores.has(storeId)) {
				this.dimMismatchStores.add(storeId);
				logger.warn(
					`[activation] Store "${storeId}" has ${mismatchedDims}-dim embeddings but the query is ${queryDims}-dim — skipping those rows until they are re-embedded.`,
				);
			}
		}
	}

	/** Contradicts partner of each ID, across the read stores (first store wins). */
	private async loadContradictPairs(
		entryIds: string[],
		slots: Map<string, StoreSlot>,
	): Promise<Map<string, string>> {
		return mergeMaps(
			await this.fanOutSafe(
				this.readDbs,
				(db) => db.getContradictPairsForIds(entryIds),
				"contradict pairs lookup",
				slots,
			),
		);
	}

	/**
	 * Support sources of each synthesized parent. A parent's sources come from the
	 * first store that returns them (the same precedence as mergeMaps), and each
	 * source keeps its owning store.
	 */
	private async loadSupportSources(
		parentIds: string[],
		slots: Map<string, StoreSlot>,
	): Promise<Map<string, SupportSource[]>> {
		const lookups = await this.fanOutSafe(
			this.readDbs,
			async (db) => ({ db, map: await db.getSupportSourcesForIds(parentIds) }),
			"support sources lookup",
			slots,
		);
		const byParent = new Map<string, SupportSource[]>();
		for (const { db, map } of lookups) {
			for (const [parentId, sources] of map) {
				if (byParent.has(parentId)) continue;
				byParent.set(
					parentId,
					sources.map((source) => ({ source, owner: db })),
				);
			}
		}
		return byParent;
	}

	/**
	 * Add the support sources of the synthesized candidates (see appendSupportSources).
	 * Returns the contradicts partners the sources need as well.
	 */
	private async expandSupportSources(
		candidates: Candidate[],
		contradictPairs: ReadonlyMap<string, string>,
		plan: {
			sourceFloor: number | null;
			maxResults: number;
			sessionKey: string | undefined;
			now: number;
		},
		slots: Map<string, StoreSlot>,
	): Promise<
		SourceExpansion & { contradictPairs: ReadonlyMap<string, string> }
	> {
		const synthesizedIds = candidates
			.filter(({ entry }) => isSynthesized(entry))
			.map(({ entry }) => entry.id);
		if (synthesizedIds.length === 0) {
			return { candidates, gateDropped: 0, deduped: 0, contradictPairs };
		}
		const sourcesByParent = await this.loadSupportSources(
			synthesizedIds,
			slots,
		);
		// Conflicted sources need their contradicts partners for the flag check.
		const unfetched = [...sourcesByParent.values()]
			.flat()
			.filter(({ source }) => hasFlagWithoutPartner(source, contradictPairs))
			.map(({ source }) => source.id);
		const pairs =
			unfetched.length === 0
				? contradictPairs
				: new Map([
						...contradictPairs,
						...(await this.loadContradictPairs(unfetched, slots)),
					]);
		return {
			...appendSupportSources({
				candidates,
				sourcesByParent,
				contradictPairs: pairs,
				sourceFloor: plan.sourceFloor,
				maxResults: plan.maxResults,
				sessions: this.sessions,
				sessionKey: plan.sessionKey,
				now: plan.now,
			}),
			contradictPairs: pairs,
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
				await db.setEmbedding(batch[i].id, vectors[i]);
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
