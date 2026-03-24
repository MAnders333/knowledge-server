import type { ActivationEngine } from "../activation/activate.js";
import { config } from "../config.js";
import type { IKnowledgeStore, IServerStateDB } from "../db/index.js";
import type { DomainResolution, DomainRouter } from "./domain-router.js";
import { logger } from "../logger.js";
import type {
	ConsolidationResult,
	Episode,
	IEpisodeReader,
	KnowledgeEntry,
} from "../types.js";
import { ContradictionScanner } from "./contradiction.js";
import { computeStrength } from "./decay.js";
import { ConsolidationLLM, formatEpisodes } from "./llm.js";
import type { ExtractedKnowledge } from "./llm.js";
import { approxTokens } from "../daemon/readers/shared.js";
import { Reconsolidator } from "./reconsolidate.js";

/**
 * Maximum tokens for the episode content portion of an extractKnowledge prompt.
 * System prompt overhead is ~2K tokens; output budget is 8K tokens.
 * This leaves 190K available, but we cap at 150K to give a comfortable safety margin
 * against token-counting imprecision (approxTokens uses chars/4, which underestimates
 * for non-ASCII content) and future system prompt growth.
 *
 * When a formatted chunk exceeds this limit, the chunk is split in half and each
 * half is processed independently. This is recursive — a single oversized episode
 * will ultimately be processed alone.
 */
const MAX_CHUNK_TOKENS = 150_000;

/**
 * The consolidation engine — the heart of the knowledge system.
 *
 * Orchestrates the full consolidation pipeline, delegating specialised work to:
 * - IEpisodeReader[]      — one PendingEpisodesReader draining the staging table
 * - ConsolidationLLM      — wraps all LLM calls (extract, merge, contradiction)
 * - Reconsolidator        — deduplicates extracted entries against existing knowledge
 * - ContradictionScanner  — detects and resolves contradictions in the mid-similarity band
 *
 * Models the human brain's sleep consolidation process:
 * 1. Read NEW episodes from pending_episodes (uploaded by the daemon)
 * 2. Load EXISTING knowledge (the current mental model)
 * 3. Extract new knowledge from episodes (what's worth remembering?)
 * 4. Reconsolidate — deduplicate/merge against existing knowledge
 * 5. Contradiction scan — detect and resolve conflicts in the mid-band
 * 6. Delete processed rows from pending_episodes (self-draining)
 * 7. After all readers: apply decay and generate embeddings
 */
export class ConsolidationEngine {
	private db: IKnowledgeStore;
	/** Server-local DB for staging tables (pending_episodes, consolidated_episode, etc.) */
	private serverStateDb: IServerStateDB;
	private activation: ActivationEngine;
	private readers: IEpisodeReader[];
	private llm: ConsolidationLLM;
	private reconsolidator: Reconsolidator;
	private contradictionScanner: ContradictionScanner;
	private domainRouter: DomainRouter | null;

	/**
	 * Stores that received inserts or content-changing updates during the most
	 * recent consolidation run. Accumulated across all batches in a drain loop.
	 * Reset at the start of each new consolidation run (_consolidate resets it).
	 * Used by runSynthesis() to limit synthesis to stores that had new knowledge.
	 * runSynthesis() iterates this set ∪ {this.db} and calls runKBSynthesis() per store.
	 *
	 * Invariant: if _consolidate() throws mid-run, this set is partial (only stores
	 * from completed readers). The exception propagates to the caller who must not
	 * call runSynthesis() after a failed consolidate(). The set is reset on the
	 * next run so partial state does not persist across runs.
	 */
	private _lastTouchedStores = new Set<IKnowledgeStore>();

	/**
	 * Concurrency guard — only one consolidation run at a time, regardless of
	 * whether it was triggered by the startup background loop or an API call.
	 * Lives here (not in the API closure) so both paths share the same flag.
	 *
	 * Use tryLock() / unlock() instead of mutating directly — this keeps external
	 * callers from accidentally bypassing or prematurely clearing the guard.
	 */
	private _isConsolidating = false;

	get isConsolidating(): boolean {
		return this._isConsolidating;
	}

	/**
	 * Atomically claim the consolidation lock.
	 * Returns true if the lock was acquired (caller should proceed),
	 * false if another run is already in progress (caller should abort).
	 *
	 * Must be called synchronously (no await before this call) to be race-free
	 * under Node/Bun's single-threaded event loop.
	 */
	tryLock(): boolean {
		if (this._isConsolidating) return false;
		this._isConsolidating = true;
		return true;
	}

	unlock(): void {
		this._isConsolidating = false;
	}

	/**
	 * @param db            Primary knowledge DB (for knowledge reads/writes and legacy compat).
	 * @param serverStateDb Server-local DB for staging tables (pending_episodes, etc.).
	 *                      Pass the same `db` for single-machine setups where one SQLite
	 *                      serves both roles. Pass a separate ServerStateDB in production.
	 * @param activation    ActivationEngine (provides shared EmbeddingClient)
	 * @param readers       Episode readers, one per source.
	 * @param domainRouter  Optional domain router for multi-store routing.
	 */
	constructor(
		db: IKnowledgeStore,
		serverStateDb: IServerStateDB,
		activation: ActivationEngine,
		readers: IEpisodeReader[] = [],
		domainRouter: DomainRouter | null = null,
	) {
		this.db = db;
		this.serverStateDb = serverStateDb;
		this.activation = activation;
		this.readers = readers;
		this.domainRouter = domainRouter;
		this.llm = new ConsolidationLLM();
		this.reconsolidator = new Reconsolidator(
			db,
			activation.embeddings,
			this.llm,
		);
		this.contradictionScanner = new ContradictionScanner(this.llm);
	}

	/**
	 * Check how many sessions are pending consolidation without running it.
	 * Used at startup to decide whether to kick off a background consolidation.
	 * Aggregates across all active readers.
	 */
	async checkPending(): Promise<{
		pendingSessions: number;
		lastConsolidatedAt: number;
	}> {
		const state = await this.serverStateDb.getConsolidationState();
		let pendingSessions = 0;

		// pending_episodes is self-draining — pass 0 to count all remaining rows.
		for (const reader of this.readers) {
			pendingSessions += reader.countNewSessions(0);
		}

		return { pendingSessions, lastConsolidatedAt: state.lastConsolidatedAt };
	}

	/**
	 * Public entry point. Acquires the DB-level advisory lock, delegates to
	 * _consolidate(), and releases the lock in a finally block.
	 *
	 * **Two-layer locking contract:**
	 *
	 * Layer 1 — Engine-level in-process lock (`tryLock` / `unlock`):
	 *   Owned by callers (index.ts drain loop, API server, CLI).
	 *   Prevents consolidate() and runSynthesis() from running concurrently
	 *   within the same process. Must be acquired *before* calling consolidate().
	 *
	 * Layer 2 — DB-level advisory lock (`tryAcquireConsolidationLock`):
	 *   Owned by consolidate() itself. Prevents concurrent consolidation across
	 *   *different processes* that share the same Postgres DB (team members each
	 *   running a local knowledge server against a shared remote DB).
	 *   Returns an empty result immediately if another process holds it — never waits.
	 *
	 * `runSynthesis` is intentionally not wrapped in the DB advisory lock:
	 *   Synthesis uses reconsolidate() which deduplicates via embedding similarity,
	 *   making concurrent synthesis runs safe (they produce near-duplicate entries
	 *   that the next synthesis pass will cluster and merge). The in-process lock
	 *   (Layer 1) ensures synthesis and consolidation never overlap on the same instance.
	 */
	async consolidate(): Promise<ConsolidationResult> {
		const lockAcquired = await this.serverStateDb.tryAcquireConsolidationLock();
		if (!lockAcquired) {
			logger.log(
				"[consolidation] Skipping — consolidation lock is already held (another process or concurrent call).",
			);
			return {
				sessionsProcessed: 0,
				segmentsProcessed: 0,
				entriesCreated: 0,
				entriesUpdated: 0,
				entriesArchived: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				duration: 0,
			};
		}

		try {
			return await this._consolidate();
		} finally {
			await this.serverStateDb.releaseConsolidationLock();
		}
	}

	/**
	 * Core consolidation logic. Called only from consolidate() after lock is held.
	 *
	 * Per-source per-run steps:
	 * 1. Fetch candidate sessions since the source cursor
	 * 2. Load already-processed episode ranges for this source
	 * 3. Segment sessions into episodes, skipping already-processed ranges
	 * 4. For each chunk: extract → reconsolidate → contradiction scan → record episodes
	 * 5. Advance the source cursor past all fetched candidates
	 *
	 * After all sources:
	 * 6. Apply decay to all active entries
	 * 7. Generate embeddings for new/updated entries
	 */
	private async _consolidate(): Promise<ConsolidationResult> {
		const startTime = Date.now();
		const state = await this.serverStateDb.getConsolidationState();

		logger.log(
			`[consolidation] Starting. Last run: ${state.lastConsolidatedAt ? new Date(state.lastConsolidatedAt).toISOString() : "never"}`,
		);

		// Reset touched stores for this run — accumulates across all source batches.
		this._lastTouchedStores = new Set();

		let totalSessionsProcessed = 0;
		let totalSegmentsProcessed = 0;
		let totalCreated = 0;
		let totalUpdated = 0;
		let totalConflictsDetected = 0;
		let totalConflictsResolved = 0;

		// Process each reader source independently with its own cursor.
		for (const reader of this.readers) {
			const sourceTotals = await this.consolidateSource(reader);
			totalSessionsProcessed += sourceTotals.sessionsProcessed;
			totalSegmentsProcessed += sourceTotals.segmentsProcessed;
			totalCreated += sourceTotals.entriesCreated;
			totalUpdated += sourceTotals.entriesUpdated;
			totalConflictsDetected += sourceTotals.conflictsDetected;
			totalConflictsResolved += sourceTotals.conflictsResolved;
			for (const s of sourceTotals.touchedStores)
				this._lastTouchedStores.add(s);
		}

		if (totalSessionsProcessed === 0) {
			logger.log("[consolidation] No new sessions to process.");
		}

		// Apply decay to ALL active entries across all configured stores.
		// Fan out to domain stores via the router; always include this.db as primary.
		// NOTE: decayStores is derived from domainRouter.allStores() (available stores
		// registered with the router), while activation.writableDbs is derived from
		// registry.writableStores() (all writable stores). Both exclude unavailable
		// stores and should be equivalent in practice — but they are two independent
		// derivation paths. If filtering logic changes in either, keep them in sync.
		const decayStores = this.domainRouter
			? new Set([this.db, ...this.domainRouter.allStores()])
			: new Set([this.db]);
		let archived = 0;
		for (const store of decayStores) {
			archived += await this.applyDecay(store);
		}

		// Generate embeddings for new entries across all writable stores.
		// ensureEmbeddings() fans out internally to this.activation.writableDbs.
		const embeddedCount = await this.activation.ensureEmbeddings();
		logger.log(
			`[consolidation] Generated embeddings for ${embeddedCount} entries.`,
		);

		// Seed embedding metadata for any store that has no metadata record yet
		// (e.g. first consolidation after a pre-v8 upgrade). Use decayStores as the
		// scope — all writable stores — to cover stores that may have received writes
		// in prior runs but never had metadata recorded.
		// Only seeds when metadata is absent — the model-change path is handled
		// exclusively by checkAndReEmbed() at startup.
		for (const store of decayStores) {
			if (!(await store.getEmbeddingMetadata())) {
				const sample = await store.getOneEntryWithEmbedding();
				if (sample) {
					await store.setEmbeddingMetadata(
						config.embedding.model,
						sample.embedding.length,
					);
					logger.log(
						`[embedding] Recorded embedding model: ${config.embedding.model} (${sample.embedding.length} dimensions).`,
					);
				}
			}
		}

		await this.serverStateDb.updateConsolidationState({
			lastConsolidatedAt: Date.now(),
			totalSessionsProcessed:
				state.totalSessionsProcessed + totalSessionsProcessed,
			totalEntriesCreated: state.totalEntriesCreated + totalCreated,
			totalEntriesUpdated: state.totalEntriesUpdated + totalUpdated,
		});

		const result: ConsolidationResult = {
			sessionsProcessed: totalSessionsProcessed,
			segmentsProcessed: totalSegmentsProcessed,
			entriesCreated: totalCreated,
			entriesUpdated: totalUpdated,
			entriesArchived: archived,
			conflictsDetected: totalConflictsDetected,
			conflictsResolved: totalConflictsResolved,
			duration: Date.now() - startTime,
		};

		logger.log(
			`[consolidation] Complete. ${result.sessionsProcessed} sessions (${result.segmentsProcessed} segments) -> ${result.entriesCreated} entries (${result.entriesArchived} archived, ${result.conflictsDetected} conflicts, ${result.conflictsResolved} resolved) in ${result.duration}ms`,
		);

		return result;
	}

	/**
	 * Run consolidation for a single source reader.
	 * Returns partial counts that the caller aggregates across all readers.
	 */
	private async consolidateSource(reader: IEpisodeReader): Promise<{
		sessionsProcessed: number;
		segmentsProcessed: number;
		entriesCreated: number;
		entriesUpdated: number;
		conflictsDetected: number;
		conflictsResolved: number;
		touchedStores: Set<IKnowledgeStore>;
	}> {
		// pending_episodes is self-draining — pass 0 so all remaining rows are candidates.
		const afterMessageTimeCreated = 0;

		// 0. Optional async preparation step (PendingEpisodesReader pre-loads its
		//    candidate list from pending_episodes before the sync getCandidateSessions call).
		//    Wrapped in try/catch so a prepare() failure (e.g. DB unreachable)
		//    skips this source rather than aborting the entire consolidation run.
		if (reader.prepare) {
			try {
				await reader.prepare(afterMessageTimeCreated);
			} catch (err) {
				logger.error(
					`[consolidation/${reader.source}] prepare() failed — skipping source this run:`,
					err,
				);
				return {
					sessionsProcessed: 0,
					segmentsProcessed: 0,
					entriesCreated: 0,
					entriesUpdated: 0,
					conflictsDetected: 0,
					conflictsResolved: 0,
					touchedStores: new Set(),
				};
			}
		}

		// 1. Fetch candidate sessions from pending_episodes.
		//    Returns session IDs plus the max message timestamp per session,
		//    ordered by max message time ASC for deterministic batching.
		const candidateSessions = reader.getCandidateSessions(
			afterMessageTimeCreated,
			config.consolidation.maxSessionsPerRun,
		);

		if (candidateSessions.length === 0) {
			return {
				sessionsProcessed: 0,
				segmentsProcessed: 0,
				entriesCreated: 0,
				entriesUpdated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				touchedStores: new Set(),
			};
		}

		const candidateIds = candidateSessions.map((s) => s.id);

		// 2. Load already-processed episode ranges for these sessions (all sources).
		const processedRanges =
			await this.serverStateDb.getProcessedEpisodeRanges(candidateIds);

		// 3. Segment sessions into episodes, skipping already-processed ranges.
		//    Wrapped in try/catch so a reader failure (e.g. corrupt JSONL file)
		//    skips this source rather than aborting the entire consolidation run.
		let episodes: ReturnType<typeof reader.getNewEpisodes>;
		try {
			episodes = reader.getNewEpisodes(candidateIds, processedRanges);
		} catch (err) {
			logger.error(
				`[consolidation/${reader.source}] getNewEpisodes failed — skipping source this run:`,
				err,
			);
			return {
				sessionsProcessed: 0,
				segmentsProcessed: 0,
				entriesCreated: 0,
				entriesUpdated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				touchedStores: new Set(),
			};
		}

		// Count unique sessions that produced at least one new episode
		const uniqueSessionIds = new Set(episodes.map((e) => e.sessionId));

		// Directly classify each skipped session rather than computing by subtraction
		// (subtraction can go negative for partially-processed sessions that still have new episodes).
		let alreadyDone = 0; // had prior episodes recorded, nothing new
		let tooFew = 0; // no prior episodes, didn't pass minSessionMessages filter
		for (const id of candidateIds) {
			if (uniqueSessionIds.has(id)) continue; // produced new episodes — not skipped
			if (processedRanges.has(id)) {
				alreadyDone++; // some episodes were previously recorded; no new tail
			} else {
				tooFew++; // never produced episodes — below minSessionMessages
			}
		}

		logger.log(
			`[consolidation/${reader.source}] Found ${episodes.length} episodes from ${uniqueSessionIds.size} sessions` +
				` (${tooFew} skipped — too few messages, ${alreadyDone} skipped — already processed).`,
		);

		// 4. Process episodes in chunks
		let totalCreated = 0;
		let totalUpdated = 0;
		let totalConflictsDetected = 0;
		let totalConflictsResolved = 0;
		const sourceTouchedStores = new Set<IKnowledgeStore>();

		const chunkSize = config.consolidation.chunkSize;

		// Group episodes by domain when domain routing is configured so all
		// episodes in a chunk share the same domain context. Without grouping,
		// a chunk spanning multiple project directories would use chunk[0].directory
		// as the domain hint for all entries — likely misrouting entries from the
		// other directories. When no domain router is configured, episodes are
		// processed in their original order (single-store mode).
		const episodeGroups: (typeof episodes)[] = this.domainRouter
			? groupByDomain(episodes, this.domainRouter)
			: [episodes];

		// Flatten groups into a single ordered sequence of chunks, preserving
		// within-group ordering while ensuring cross-domain episodes don't mix.
		const chunks: (typeof episodes)[] = [];
		for (const group of episodeGroups) {
			for (let i = 0; i < group.length; i += chunkSize) {
				chunks.push(group.slice(i, i + chunkSize));
			}
		}

		for (let ci = 0; ci < chunks.length; ci++) {
			const chunk = chunks[ci];
			const chunkLabel = `${ci + 1}/${chunks.length}`;
			const episodeTitles = [...new Set(chunk.map((ep) => ep.sessionTitle))]
				.map((t) => `"${t.length > 40 ? `${t.slice(0, 40)}…` : t}"`)
				.join(", ");

			logger.log(
				`[consolidation/${reader.source}] Chunk ${chunkLabel} (${chunk.length} episodes): ${episodeTitles}`,
			);

			// Use the first episode's source as the log label — all episodes in a
			// domain-grouped chunk share the same original source.
			const counts = await this.processChunk(
				chunk[0]?.source ?? reader.source,
				chunk,
			);
			totalCreated += counts.created;
			totalUpdated += counts.updated;
			totalConflictsDetected += counts.conflictsDetected;
			totalConflictsResolved += counts.conflictsResolved;
			for (const s of counts.touchedStores) sourceTouchedStores.add(s);
		}

		// 7. Post-consolidation hook — PendingEpisodesReader deletes processed rows from
		//    pending_episodes to keep the table lean.
		//    Use candidateIds (all processed candidates) rather than episodes.map(sessionId)
		//    so sessions where all episodes were already processed also get cleaned up.
		if (reader.afterConsolidated) {
			await reader.afterConsolidated(candidateIds);
		}

		return {
			sessionsProcessed: candidateSessions.length,
			segmentsProcessed: episodes.length,
			entriesCreated: totalCreated,
			entriesUpdated: totalUpdated,
			conflictsDetected: totalConflictsDetected,
			conflictsResolved: totalConflictsResolved,
			touchedStores: sourceTouchedStores,
		};
	}

	/**
	 * Process a single chunk of episodes through the two-pass pipeline:
	 *
	 * Pass 1 — Extract: call the LLM to extract knowledge entries from the episodes.
	 *   Token budget is checked here; oversized chunks are split and processed
	 *   recursively. This pass is pure LLM I/O — no DB reads or writes.
	 *
	 * Pass 2 — Dispatch per store: group the extracted entries by their resolved
	 *   domain store, then for each store:
	 *     a. Load `entriesMap` ONCE from that store (not once per recursive leaf).
	 *     b. Reconsolidate all entries for that store (dedup/merge against existing).
	 *     c. Contradiction scan within-store only (domain isolation by design).
	 *
	 * recordEpisode is called after both passes succeed so that a crash in Pass 2
	 * causes the chunk to be re-processed from scratch on the next run (safe because
	 * reconsolidate() is idempotent via embedding-similarity dedup).
	 */
	private async processChunk(
		source: string,
		chunk: ReturnType<IEpisodeReader["getNewEpisodes"]>,
	): Promise<{
		created: number;
		updated: number;
		conflictsDetected: number;
		conflictsResolved: number;
		touchedStores: Set<IKnowledgeStore>;
	}> {
		if (chunk.length === 0) {
			return {
				created: 0,
				updated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				touchedStores: new Set(),
			};
		}

		// ── Pass 1: Extract ──────────────────────────────────────────────────────
		// Domain routing: use the first episode's directory as the domain hint for
		// the extraction prompt. Episodes in a chunk share the same domain because
		// consolidateSource() groups them by domain before chunking.
		const chunkDirectory = chunk[0]?.directory ?? "";
		const domainResolution = this.domainRouter?.resolve(chunkDirectory) ?? null;

		// If the default store for this chunk's domain is unavailable, skip the whole
		// chunk before any LLM or DB work. Episodes remain in pending_episodes and
		// will be retried on the next consolidation run when the store may be back.
		if (domainResolution?.storeUnavailable) {
			logger.warn(
				`[consolidation/${source}] Skipping chunk — target store for domain "${domainResolution.domainId}" is unavailable. Episodes will be retried on next run.`,
			);
			return {
				created: 0,
				updated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
				touchedStores: new Set(),
			};
		}

		const extracted = await this.extractFromChunk(
			source,
			chunk,
			domainResolution,
		);

		// ── Pass 2: Dispatch per store ───────────────────────────────────────────
		// Group extracted entries by their resolved target store. Each entry may have
		// been assigned a different domain by the LLM — group them now so we load
		// entriesMap once per store rather than once per entry (or per recursive leaf).
		const storeGroups = this.groupExtractedByStore(extracted, domainResolution);

		// Use the max message time of the chunk as the session timestamp for new entries.
		const reduced = chunk.reduce(
			(max, e) => Math.max(max, e.maxMessageTime),
			Number.NEGATIVE_INFINITY,
		);
		const chunkSessionTimestamp = Number.isFinite(reduced)
			? reduced
			: Date.now();
		const sessionIds = [...new Set(chunk.map((e) => e.sessionId))];

		// Per-store consolidation runs concurrently — each store has independent state:
		// its own DB connection, its own entriesMap, its own contradictionCandidates.
		// There is no shared mutable state between stores within a single process.
		// SQLite stores are already serialised by their per-connection mutex; Postgres
		// uses a process-level advisory lock (held at consolidate() level) that prevents
		// cross-process races, not cross-store within-process parallelism.
		//
		// Promise.allSettled (not Promise.all) so a failure in one store does not
		// abort the others mid-flight. Each store's result is inspected independently:
		// - Successful stores contribute their counts and are recorded.
		// - Failed stores are logged; their episodes are NOT recorded as processed,
		//   so they will be re-processed on the next consolidation run.
		const settled = await Promise.allSettled(
			[...storeGroups.entries()].map(([store, entriesForStore]) =>
				this.consolidateExtractedToStore(
					source,
					store,
					entriesForStore,
					sessionIds,
					chunkSessionTimestamp,
				).then((counts) => ({ store, counts })),
			),
		);

		let chunkCreated = 0;
		let chunkUpdated = 0;
		let chunkConflictsDetected = 0;
		let chunkConflictsResolved = 0;
		const touchedStores = new Set<IKnowledgeStore>();
		let anyStoreFailed = false;

		for (const result of settled) {
			if (result.status === "fulfilled") {
				const { store, counts } = result.value;
				chunkCreated += counts.created;
				chunkUpdated += counts.updated;
				chunkConflictsDetected += counts.conflictsDetected;
				chunkConflictsResolved += counts.conflictsResolved;
				if (counts.created > 0 || counts.updated > 0) touchedStores.add(store);
			} else {
				anyStoreFailed = true;
				logger.error(
					`[consolidation/${source}] Store consolidation failed — episodes will be retried on next run:`,
					result.reason,
				);
			}
		}

		// Record episodes as processed only when all stores succeeded.
		// If any store failed, skip recording so the full chunk is retried on the
		// next run. reconsolidate() is idempotent via embedding-similarity dedup,
		// so re-processing the successful stores' entries is safe (no data corruption).
		if (!anyStoreFailed) {
			const entriesPerEp = Math.round(
				(chunkCreated + chunkUpdated) / chunk.length,
			);
			for (const ep of chunk) {
				await this.serverStateDb.recordEpisode(
					ep.source,
					ep.sessionId,
					ep.startMessageId,
					ep.endMessageId,
					ep.contentType,
					entriesPerEp,
				);
			}
		}

		return {
			created: chunkCreated,
			updated: chunkUpdated,
			conflictsDetected: chunkConflictsDetected,
			conflictsResolved: chunkConflictsResolved,
			touchedStores,
		};
	}

	/**
	 * Pass 1: extract knowledge entries from a chunk of episodes via LLM.
	 *
	 * Handles the token budget recursively — if the formatted chunk exceeds
	 * MAX_CHUNK_TOKENS the chunk is split in half and both halves are extracted
	 * independently, then their results are merged. A single oversized episode is
	 * processed alone (the LLM call may fail with a context-length error in that case).
	 *
	 * Returns all extracted entries with their LLM-assigned domain field, ready for
	 * Pass 2 to dispatch to the correct store.
	 */
	private async extractFromChunk(
		source: string,
		chunk: ReturnType<IEpisodeReader["getNewEpisodes"]>,
		domainResolution: DomainResolution | null,
	): Promise<ExtractedKnowledge[]> {
		const chunkSummary = formatEpisodes(chunk);
		const chunkTokens = approxTokens(chunkSummary);

		if (chunkTokens > MAX_CHUNK_TOKENS && chunk.length > 1) {
			logger.log(
				`[consolidation/${source}] Chunk too large (~${chunkTokens} tokens, limit ${MAX_CHUNK_TOKENS}) — splitting ${chunk.length} episodes into two halves.`,
			);
			const mid = Math.ceil(chunk.length / 2);
			const left = await this.extractFromChunk(
				source,
				chunk.slice(0, mid),
				domainResolution,
			);
			const right = await this.extractFromChunk(
				source,
				chunk.slice(mid),
				domainResolution,
			);
			return [...left, ...right];
		}

		if (chunkTokens > MAX_CHUNK_TOKENS) {
			logger.warn(
				`[consolidation/${source}] Single episode exceeds token limit (~${chunkTokens} tokens, limit ${MAX_CHUNK_TOKENS}) — processing alone.`,
			);
		}

		const extractStart = Date.now();
		const extracted = await this.llm.extractKnowledge(
			chunkSummary,
			domainResolution?.domainContext ?? undefined,
		);
		logger.log(
			`[consolidation/${source}] Extracted ${extracted.length} entries in ${((Date.now() - extractStart) / 1000).toFixed(1)}s.`,
		);
		return extracted;
	}

	/**
	 * Group extracted entries by their resolved target store.
	 *
	 * Each entry may carry an LLM-assigned domain that overrides the chunk default.
	 * Returns a Map<store, entries[]> preserving the order entries were assigned to
	 * each store. The chunk's default store (from domainResolution) is used for
	 * entries with no domain assignment.
	 */
	private groupExtractedByStore(
		extracted: ExtractedKnowledge[],
		domainResolution: DomainResolution | null,
	): Map<IKnowledgeStore, ExtractedKnowledge[]> {
		const groups = new Map<IKnowledgeStore, ExtractedKnowledge[]>();
		const domainRouter = this.domainRouter;

		for (const entry of extracted) {
			// Resolve the target store for this entry (same logic as before,
			// now centralised here instead of inside the reconsolidate callback).
			const targetStore =
				domainResolution && domainRouter
					? (domainRouter.resolveStore(
							entry.domain ?? domainResolution.domainId,
						) ?? domainResolution.store)
					: this.db;

			let group = groups.get(targetStore);
			if (!group) {
				group = [];
				groups.set(targetStore, group);
			}
			group.push(entry);
		}

		// In single-store mode (no domain routing), all entries go to this.db.
		if (groups.size === 0 && extracted.length > 0) {
			groups.set(this.db, [...extracted]);
		}

		return groups;
	}

	/**
	 * Pass 2: reconsolidate a batch of extracted entries into a single target store.
	 *
	 * All entries in this batch belong to the same store — groupExtractedByStore()
	 * guarantees this. Benefits over the old interleaved approach:
	 *
	 * - getActiveEntriesWithEmbeddings() is called ONCE for this store regardless
	 *   of how many recursive bisections happened in Pass 1.
	 * - entriesMap is shared across all entries in the batch, so within-store
	 *   cross-entry dedup works correctly for this entire call.
	 * - Contradiction scanning is trivially within-store — no filtering needed.
	 * - The store is explicit in the call signature, not inferred mid-callback.
	 */
	private async consolidateExtractedToStore(
		source: string,
		store: IKnowledgeStore,
		entries: ExtractedKnowledge[],
		sessionIds: string[],
		chunkSessionTimestamp: number,
	): Promise<{
		created: number;
		updated: number;
		conflictsDetected: number;
		conflictsResolved: number;
	}> {
		if (entries.length === 0) {
			return {
				created: 0,
				updated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
			};
		}

		// Load all active entries from this store once.
		// entriesMap is mutated in place as inserts/updates happen so that
		// subsequent entries in this batch can deduplicate against earlier ones.
		const allEntries = await store.getActiveEntriesWithEmbeddings();
		const entriesMap = new Map(allEntries.map((e) => [e.id, e]));

		let created = 0;
		let updated = 0;
		// Track IDs written to this store — eligible for within-store contradiction scan.
		const contradictionCandidates = new Set<string>();

		for (const entry of entries) {
			try {
				await this.reconsolidator.reconsolidate(
					entry,
					sessionIds,
					entriesMap,
					{
						onInsert: (inserted) => {
							created++;
							contradictionCandidates.add(inserted.id);
							logger.log(
								`[consolidation/${source}] + insert [${inserted.type}] ${entry.domain ? `domain:${entry.domain} ` : ""}${JSON.stringify(inserted.content)}`,
							);
							// Propagate into the cache so later entries in this batch
							// can deduplicate against entries inserted earlier.
							if (inserted.embedding) {
								entriesMap.set(
									inserted.id,
									inserted as KnowledgeEntry & { embedding: number[] },
								);
							}
						},
						onUpdate: (id, updatedFields, freshEmbedding) => {
							updated++;
							contradictionCandidates.add(id);
							const existing = entriesMap.get(id);
							logger.log(
								`[consolidation/${source}] ~ update [${updatedFields.type ?? existing?.type ?? "?"}] ${JSON.stringify(updatedFields.content ?? existing?.content ?? "")}`,
							);
							if (existing) {
								entriesMap.set(id, {
									...existing,
									content: updatedFields.content ?? existing.content,
									type:
										(updatedFields.type as KnowledgeEntry["type"]) ??
										existing.type,
									topics: updatedFields.topics ?? existing.topics,
									confidence: updatedFields.confidence ?? existing.confidence,
									embedding: freshEmbedding,
								});
							}
						},
						onKeep: () => {},
					},
					chunkSessionTimestamp,
					undefined, // precomputedEmbedding — let reconsolidate() compute it
					"consolidation",
					store, // targetDb: inserts land in this store
					store, // mergeDb: existing entries also live in this store
				);
			} catch (err) {
				// Log and skip — do NOT rethrow. Re-throwing would skip recordEpisode
				// for the whole chunk, causing all entries to be re-processed and
				// producing duplicates for entries already successfully inserted.
				logger.error(
					`[consolidation/${source}] Failed to reconsolidate entry ${JSON.stringify(entry.content ?? "")} — skipping:`,
					err,
				);
			}
		}

		// Contradiction scan — within-store only by construction (all entries in this
		// batch were routed to `store` by groupExtractedByStore). No filtering needed.
		const contradictions = await this.contradictionScanner.scan(
			store,
			entriesMap,
			contradictionCandidates,
		);

		return {
			created,
			updated,
			conflictsDetected: contradictions.detected,
			conflictsResolved: contradictions.resolved,
		};
	}

	/**
	 * Apply decay to all active entries.
	 * Returns the number of entries that were archived.
	 */
	private async applyDecay(db: IKnowledgeStore = this.db): Promise<number> {
		// Include conflicted entries — their strength must continue aging.
		// A conflicted entry whose strength falls to zero is effectively forgotten
		// regardless of the conflict, and should still be archived.
		// Single query avoids the TOCTOU window of two separate status queries.
		const entries = await db.getActiveAndConflictedEntries();
		let archived = 0;

		for (const entry of entries) {
			const newStrength = computeStrength(entry);

			if (newStrength < config.decay.archiveThreshold) {
				await db.updateEntry(entry.id, {
					status: "archived",
					strength: newStrength,
				});
				archived++;
				logger.log(
					`[decay] Archived: ${JSON.stringify(entry.content)} (strength: ${newStrength.toFixed(3)})`,
				);
			} else if (Math.abs(newStrength - entry.strength) > 0.01) {
				// Only update if strength changed meaningfully
				await db.updateStrength(entry.id, newStrength);
			}
		}

		// Tombstone long-archived entries
		const archivedEntries = await db.getEntriesByStatus("archived");
		const tombstoneThreshold =
			Date.now() - config.decay.tombstoneAfterDays * 24 * 60 * 60 * 1000;

		for (const entry of archivedEntries) {
			if (entry.updatedAt < tombstoneThreshold) {
				await db.updateEntry(entry.id, { status: "tombstoned" });
				logger.log(
					`[decay] Tombstoned: ${JSON.stringify(entry.content)} (archived for ${config.decay.tombstoneAfterDays}+ days)`,
				);
			}
		}

		return archived;
	}

	/**
	 * KB-wide synthesis pass: cluster all active entries and attempt to synthesize
	 * higher-order principles from each ripe cluster.
	 *
	 * Intentionally separated from consolidate() so callers can run all consolidation
	 * batches first and then synthesize once — rather than synthesizing after every
	 * batch, which re-clusters and re-synthesizes an ever-growing KB on each pass.
	 *
	 * Runs once per store that received inserts or content-changing updates during
	 * the most recent consolidate() call (_lastTouchedStores). Falls back to this.db
	 * if no stores were touched (e.g. no new entries, or called before consolidate()).
	 * The primary store (this.db) is always included in synthesis even when not
	 * explicitly touched — ripe clusters from prior runs may need synthesis.
	 *
	 * Must be called after ensureEmbeddings() has run (i.e. after consolidate()) so
	 * all entries have embeddings. Returns the total number of principles synthesized.
	 */
	async runSynthesis(): Promise<number> {
		const touched = this._lastTouchedStores;
		// Always include this.db — ripe clusters from prior runs may need synthesis
		// even when nothing was written to the primary store this run.
		const stores = new Set([...touched, this.db]);
		let total = 0;
		for (const store of stores) {
			total += await this.reconsolidator.runKBSynthesis(store);
		}
		return total;
	}

	close(): void {
		for (const reader of this.readers) {
			reader.close();
		}
	}
}

// ── Module helpers ────────────────────────────────────────────────────────────

/**
 * Group episodes by their resolved domain so episodes with different project
 * directories don't end up in the same chunk.
 *
 * Within each group, original episode order is preserved. Groups are ordered
 * by first-occurrence of their domain in the episode list.
 *
 * This ensures the DomainRouter receives a consistent directory for all episodes
 * in a given chunk, so the domain context injected into the LLM extraction prompt
 * accurately reflects the project the episodes actually came from.
 */
function groupByDomain(episodes: Episode[], router: DomainRouter): Episode[][] {
	const groups = new Map<string, Episode[]>();
	const order: string[] = [];

	for (const ep of episodes) {
		const resolution = router.resolve(ep.directory);
		// Use domainId as the grouping key; undefined (no-domain mode) falls
		// back to a single "__default__" group so behaviour degrades gracefully.
		const key = resolution.domainId ?? "__default__";
		let group = groups.get(key);
		if (!group) {
			group = [];
			groups.set(key, group);
			order.push(key);
		}
		group.push(ep);
	}

	return order.map((k) => groups.get(k) ?? []);
}
