import { config } from "../config.js";
import { computeStrength } from "../consolidation/decay.js";
import type { IKnowledgeStore } from "../db/index.js";
import { logger } from "../logger.js";
import type {
	ActivationResult,
	ContradictionAnnotation,
	GateMode,
	KnowledgeEntry,
} from "../types.js";
import {
	EmbeddingClient,
	bulkEmbedOptions,
	cosineSimilarity,
	describeError,
	formatEmbeddingText,
} from "./embeddings.js";
import type { RerankScorer } from "./rerank.js";

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

/** Per-store admission state within one activate() call (see callStore). */
interface StoreSlot {
	inFlight: number;
	/** The store has returned a result in this activation; the in-flight cap lifts. */
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
 * Per-store circuit breaker for the activation fan-out. State is keyed by store
 * id and lives as long as the ActivationEngine (one per process), so it is shared
 * across activations.
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

	private stateOf(id: string): BreakerState {
		let s = this.states.get(id);
		if (!s) {
			s = { status: "closed", failures: 0, openedAt: 0 };
			this.states.set(id, s);
		}
		return s;
	}

	/** True when a new activation should leave this store out. */
	isBlocked(id: string): boolean {
		const s = this.states.get(id);
		if (!s || s.status === "closed") return false;
		if (s.status === "half-open") return true;
		return this.now() - s.openedAt < this.windowMs;
	}

	/** Max calls in flight for this store before it has answered in an activation. */
	budget(id: string): number {
		const s = this.stateOf(id);
		return s.status === "closed" ? Math.max(1, this.threshold - s.failures) : 1;
	}

	/** Admit one call. After the window, the first caller becomes the probe. */
	admit(id: string): boolean {
		const s = this.stateOf(id);
		if (s.status === "closed") return true;
		if (s.status === "open" && this.now() - s.openedAt >= this.windowMs) {
			s.status = "half-open";
			return true;
		}
		return false;
	}

	/** Record the outcome of an admitted call. `detail` is used in the warning. */
	record(id: string, ok: boolean, detail: string): void {
		const s = this.stateOf(id);
		const windowS = this.windowMs / 1000;
		if (ok) {
			if (s.status === "half-open") {
				s.status = "closed";
				s.failures = 0;
				logger.warn(`[activation] Store "${id}" recovered — circuit closed.`);
			} else if (s.status === "closed") {
				s.failures = 0;
			}
			return;
		}
		if (s.status === "closed") {
			s.failures++;
			if (s.failures < this.threshold) return;
			this.open(s);
			logger.warn(
				`[activation] Store "${id}" failed ${this.threshold} times in a row ` +
					`(${detail}) — circuit open, skipping it for ${windowS}s.`,
			);
		} else if (s.status === "half-open") {
			this.open(s);
			logger.warn(
				`[activation] Store "${id}" probe failed (${detail}) — ` +
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

	/**
	 * Last logged ANN vs full-scan partition of reachable stores. Used to log the
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
	 */
	constructor(
		writableDb: IKnowledgeStore,
		readDbs?: IKnowledgeStore[],
		writableDbs?: IKnowledgeStore[],
		reranker?: RerankScorer | null,
	) {
		this.db = writableDb;
		this.readDbs = readDbs ?? [writableDb];
		this.writableDbs = writableDbs ?? [writableDb];
		this.embeddings = new EmbeddingClient();
		this.reranker = reranker ?? null;
		this.breaker = new StoreBreaker({
			threshold: ActivationEngine.STORE_FAILURE_THRESHOLD,
			windowMs: ActivationEngine.STORE_RETRY_INTERVAL_MS,
		});
	}

	/**
	 * Read stores that the circuit breaker does not block. A store is left out of
	 * a new activation while its circuit is open (or its half-open probe is in
	 * flight); once the window elapses it becomes eligible for one probe call.
	 */
	private reachableReadDbs(): IKnowledgeStore[] {
		return this.readDbs.filter((db) => !this.breaker.isBlocked(db.id));
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
		const slot: StoreSlot = slots.get(db.id) ?? {
			inFlight: 0,
			answered: false,
			waiting: [],
		};
		slots.set(db.id, slot);
		while (!slot.answered && slot.inFlight >= this.breaker.budget(db.id)) {
			await new Promise<void>((resolve) => slot.waiting.push(resolve));
		}
		if (!this.breaker.admit(db.id)) return { ok: false };

		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(
				() =>
					reject(
						new Error(
							`timeout after ${ActivationEngine.STORE_TIMEOUT_MS / 1000}s`,
						),
					),
				ActivationEngine.STORE_TIMEOUT_MS,
			);
		});
		slot.inFlight++;
		try {
			const value = await Promise.race([fn(db), timeout]);
			slot.answered = true;
			this.breaker.record(db.id, true, label);
			return { ok: true, value };
		} catch (err) {
			this.breaker.record(
				db.id,
				false,
				`${label}: ${err instanceof Error ? err.message : String(err)}`,
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
			 * Read-only: no access records (recordAccess), so measuring activation
			 * never changes the store. Used by the replay harness and the CLI.
			 */
			dryRun?: boolean;
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
			staleness: {
				ageDays: number;
				strength: number;
				lastAccessedDaysAgo: number;
				mayBeStale: boolean;
			};
		}> = [];

		// Split non-blocked read stores by retrieval path. Circuit-open stores are
		// already excluded by reachableReadDbs().
		const reachable = this.reachableReadDbs();
		const annDbs: IKnowledgeStore[] = [];
		const scanDbs: IKnowledgeStore[] = [];
		for (const db of reachable) {
			(storeSupportsAnn(db) ? annDbs : scanDbs).push(db);
		}

		// Log the retrieval path per store, but only when the partition changes
		// (first activation, store reachability, or vector search readiness flip).
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
								(db) =>
									// biome-ignore lint/style/noNonNullAssertion: guarded by storeSupportsAnn
									db.findSimilarEntries!(qEmb, fetchLimit, similarityThreshold),
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
					(db) => db.getActiveEntriesWithEmbeddings(),
					"full-scan load",
					slots,
				),
			]);

		// Scan stores: entries synced to several scan stores share an ID -
		// deduplicate among scan stores keeping the first occurrence.
		const scanSeenIds = new Set<string>();
		const scanEntries = scanEntriesPerStore.flat().filter((e) => {
			if (scanSeenIds.has(e.id)) return false;
			scanSeenIds.add(e.id);
			return true;
		});

		// Merge candidates from both paths: entry ID -> best rawSimilarity across
		// all query vectors and stores. ANN candidates are inserted first, so on a
		// tie the ANN copy is kept; otherwise the higher similarity wins and no
		// entry is lost when the same ID lives in an ANN and a scan store.
		const bestById = new Map<
			string,
			{
				entry: KnowledgeEntry;
				rawSimilarity: number;
			}
		>();
		const keepBest = (entry: KnowledgeEntry, rawSimilarity: number) => {
			const existing = bestById.get(entry.id);
			if (!existing || rawSimilarity > existing.rawSimilarity) {
				bestById.set(entry.id, { entry, rawSimilarity });
			}
		};
		for (const queryResults of annResultsPerQuery) {
			for (const { entry, similarity } of queryResults) {
				keepBest(entry, similarity);
			}
		}
		for (const entry of scanEntries) {
			const rawSimilarity = Math.max(
				...queryEmbeddings.map((qEmb) =>
					cosineSimilarity(qEmb, entry.embedding),
				),
			);
			// ANN stores filter by threshold in the DB; apply the same cut here.
			if (rawSimilarity < similarityThreshold) continue;
			keepBest(entry, rawSimilarity);
		}

		for (const { entry, rawSimilarity } of bestById.values()) {
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
				staleness: {
					ageDays: Math.round(ageDays),
					strength: liveStrength,
					lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
					mayBeStale: ageDays > halfLife && entry.accessCount < 3,
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
		let gateMode: GateMode = this.reranker ? "dense" : "off";
		if (this.reranker && scored.length > 1) {
			// Bound the rerank input: across multiple cues and stores the pool
			// can exceed `candidates` — keep the dense-best slice.
			const maxCandidates = config.activation.rerank.candidates;
			if (scored.length > maxCandidates) {
				scored.sort((a, b) => b.rawSimilarity - a.rawSimilarity);
				scored.length = maxCandidates;
			}

			try {
				const documents = scored.map((s) =>
					formatEmbeddingText(s.entry.type, s.entry.content, s.entry.topics),
				);
				const scores = await this.reranker.scoreBatch(primaryQuery, documents);
				for (let i = 0; i < scored.length; i++) {
					scored[i].rerankScore = scores[i];
					scored[i].similarity = scores[i] * scored[i].staleness.strength;
				}
				gateMode = "rerank";
			} catch (err) {
				logger.warn(
					`[activation] Rerank failed — using dense-only ordering. Reason: ${err instanceof Error ? err.message : String(err)}`,
				);
			}
		}

		// Sort and cap.
		scored.sort((a, b) => b.similarity - a.similarity);
		scored.splice(maxResults);

		// Record access for activated entries (reinforces their strength).
		// Skipped in dryRun so measuring never changes the store.
		if (!dryRun) {
			for (const { entry } of scored) {
				await this.db.recordAccess(entry.id);
			}
		}

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
		// - Add sources to the result (but do NOT exceed maxResults)
		const synthesizedIds = scored
			.filter(
				({ entry }) => entry.type === "principle" || entry.type === "pattern",
			)
			.map(({ entry }) => entry.id);

		const scoredIdSet = new Set(scored.map(({ entry }) => entry.id));

		if (synthesizedIds.length > 0) {
			// Fan out support sources lookup across reachable stores and merge maps.
			const sourceMaps = await this.fanOutSafe(
				this.reachableReadDbs(),
				(db) => db.getSupportSourcesForIds(synthesizedIds),
				"support sources lookup",
				slots,
			);
			const sourcesMap = mergeMaps(sourceMaps);

			// Snapshot the array before iterating — we push source entries into `scored`
			// during the loop and don't want to visit newly-added entries (which could
			// cascade if a source is itself a principle with its own supports relations).
			const snapshot = [...scored];
			for (const {
				entry,
				rawSimilarity,
				similarity: synthSimilarity,
			} of snapshot) {
				if (entry.type !== "principle" && entry.type !== "pattern") continue;
				const sources = sourcesMap.get(entry.id);
				if (!sources) continue;

				for (const source of sources) {
					if (scoredIdSet.has(source.id)) continue; // already activated
					if (scored.length >= maxResults) break;

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
						similarity: synthSimilarity * 0.9 * liveStrength,
						staleness: {
							ageDays: Math.round(ageDays),
							strength: liveStrength,
							lastAccessedDaysAgo: Math.round(lastAccessedDaysAgo),
							mayBeStale: ageDays > halfLife && source.accessCount < 3,
						},
					});
					scoredIdSet.add(source.id);
					if (!dryRun) await this.db.recordAccess(source.id);
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

		// Batch-fetch all contradicts relations for conflicted entries in a single query,
		// avoiding N+1 DB calls. Only fetch for entries with status 'conflicted'.
		const conflictedIds = scored
			.filter(({ entry }) => entry.status === "conflicted")
			.map(({ entry }) => entry.id);

		// Fan out contradiction pairs lookup across reachable stores and merge.
		const contradictMaps = await this.fanOutSafe(
			this.reachableReadDbs(),
			(db) => db.getContradictPairsForIds(conflictedIds),
			"contradict pairs lookup",
			slots,
		);
		const contradictPairs = mergeMaps(contradictMaps);

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

		return {
			entries: scored.map(
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
			telemetry: { cueCount: queryList.length, nCandidates, gateMode },
		};
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
	 * produced the stored embeddings. If it has changed, re-embed all active and
	 * conflicted entries with the new model.
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
	 *    b. Compute all new vectors (nothing is written if this fails)
	 *    c. Overwrite each entry's embedding vector with the new-model vector,
	 *       then write the metadata LAST (see inline comments on crash-recovery
	 *       trade-offs below)
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

		// ── Model has changed — re-embed all entries in-place ──

		const entriesWithEmbeddings = await db.getActiveEntriesWithEmbeddings();
		const totalEntries = entriesWithEmbeddings.length;

		logger.log(
			`[embedding] Model change detected: "${stored.model}" → "${configuredModel}".`,
		);

		if (totalEntries === 0) {
			// No entries to re-embed — just update metadata.
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

		logger.log(
			`[embedding] Re-embedding ${totalEntries} entries with new model "${configuredModel}"...`,
		);

		// Re-embed in-place: compute all new embeddings via a single batch call,
		// then overwrite each entry's vector individually, then stamp the metadata.
		//
		// Failure modes:
		// - embedBatch throws (after per-chunk retries for transient errors, or on a
		//   response that fails validation): nothing is written → metadata still
		//   points to the old model → next startup detects mismatch and retries in
		//   full. Safe.
		// - Response validation (count, vector shape) rejects truncated responses.
		//   Distinct indices are sorted; duplicate indices fall back to array order
		//   with a warning (#100).
		// - Process crashes mid-loop: entries updated so far have new-model
		//   embeddings; remaining entries still carry old-model embeddings (non-NULL).
		//   Because metadata is written AFTER the loop, next startup sees
		//   stored.model !== configuredModel and re-runs the full re-embed. The
		//   already-updated entries will be re-embedded redundantly but correctly.
		const reEmbedStart = Date.now();

		const texts = entriesWithEmbeddings.map((e) =>
			formatEmbeddingText(e.type, e.content, e.topics),
		);
		const newEmbeddings = await this.embeddings.embedBatch(
			texts,
			bulkEmbedOptions(),
		);

		const newDimensions =
			newEmbeddings.length > 0 ? newEmbeddings[0].length : 0;

		for (let i = 0; i < entriesWithEmbeddings.length; i++) {
			await db.updateEntry(entriesWithEmbeddings[i].id, {
				embedding: newEmbeddings[i],
			});
		}

		// Write metadata AFTER the loop so a mid-loop crash does not leave the DB
		// with mixed-dimension embeddings that look consistent to the next startup.
		await db.setEmbeddingMetadata(configuredModel, newDimensions);

		const durationSec = ((Date.now() - reEmbedStart) / 1000).toFixed(1);

		logger.log(
			`[embedding] Re-embed complete: ${totalEntries} entries re-embedded in ${durationSec}s ` +
				`(${stored.dimensions} → ${newDimensions} dimensions).`,
		);

		return true;
	}
}

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
