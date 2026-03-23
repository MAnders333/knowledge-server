import type { ActivationEngine } from "../activation/activate.js";
import { config } from "../config.js";
import type { IKnowledgeStore, IServerStateDB } from "../db/index.js";
import type { DomainRouter } from "./domain-router.js";
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
	 * Reserved for the follow-up refactor that will scope runSynthesis() to only
	 * touched stores — currently runSynthesis() always operates on this.db.
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

		// Apply decay to ALL active entries (once, after all sources are processed).
		const archived = await this.applyDecay();

		// Generate embeddings for new entries (once, shared across all sources).
		const embeddedCount = await this.activation.ensureEmbeddings();
		logger.log(
			`[consolidation] Generated embeddings for ${embeddedCount} entries.`,
		);

		// If embedding metadata is missing (e.g. startup probe failed, or first
		// consolidation after a pre-v8 upgrade), record it now from a fresh entry.
		// Only seeds when metadata is absent — the model-change path is handled
		// exclusively by checkAndReEmbed() at startup to avoid overwriting metadata
		// with a stale model name while old-model vectors are still in the DB.
		if (embeddedCount > 0 && !(await this.db.getEmbeddingMetadata())) {
			const sample = await this.db.getOneEntryWithEmbedding();
			if (sample) {
				await this.db.setEmbeddingMetadata(
					config.embedding.model,
					sample.embedding.length,
				);
				logger.log(
					`[embedding] Recorded embedding model: ${config.embedding.model} (${sample.embedding.length} dimensions).`,
				);
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
	 * Process a single chunk of episodes through the full extraction → reconsolidation
	 * → contradiction scan pipeline.
	 *
	 * Pre-flight token guard: if the formatted chunk exceeds MAX_CHUNK_TOKENS, the chunk
	 * is split in half and each half is processed independently (recursively). This
	 * handles fat-tail episodes (e.g. Confluence pages near the 50K episode cap) without
	 * requiring a conservative global chunk size. A single oversized episode will
	 * ultimately be processed alone — the soft-limit in chunkByTokenBudget already
	 * ensures no individual episode is split further.
	 *
	 * recordEpisode is called here (not in the caller) so that each sub-chunk's episodes
	 * are marked processed immediately after their LLM call succeeds. This preserves
	 * idempotency: a crash mid-chunk only re-processes the remaining sub-chunks.
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

		// Pre-flight: check if the formatted chunk fits within the token budget.
		const chunkSummary = formatEpisodes(chunk);
		const chunkTokens = approxTokens(chunkSummary);

		if (chunkTokens > MAX_CHUNK_TOKENS && chunk.length > 1) {
			// Split in half and process each side independently.
			logger.log(
				`[consolidation/${source}] Chunk too large (~${chunkTokens} tokens, limit ${MAX_CHUNK_TOKENS}) — splitting ${chunk.length} episodes into two halves.`,
			);
			const mid = Math.ceil(chunk.length / 2);
			const left = await this.processChunk(source, chunk.slice(0, mid));
			const right = await this.processChunk(source, chunk.slice(mid));
			return {
				created: left.created + right.created,
				updated: left.updated + right.updated,
				conflictsDetected: left.conflictsDetected + right.conflictsDetected,
				conflictsResolved: left.conflictsResolved + right.conflictsResolved,
				touchedStores: new Set([...left.touchedStores, ...right.touchedStores]),
			};
		}

		if (chunkTokens > MAX_CHUNK_TOKENS) {
			// chunk.length === 1: single episode that can't be split further — log and proceed.
			// chunkByTokenBudget already applies a per-session soft limit, so this is an
			// unusual edge case (e.g. one enormous Confluence page). The LLM call may fail
			// with a context-length error; the caller's retry logic handles it.
			logger.warn(
				`[consolidation/${source}] Single episode exceeds token limit (~${chunkTokens} tokens, limit ${MAX_CHUNK_TOKENS}) — processing alone.`,
			);
		}

		// Fits within budget (or is a single episode that can't be split further).
		// Load entries from the primary writable store for reconsolidation/dedup.
		// Cross-store deduplication (against read-only domain stores) is not implemented —
		// entries in secondary stores may produce near-duplicates if the same knowledge
		// surfaces in a different domain session. Accepted limitation of v1 domain routing.
		const allEntriesForChunk = await this.db.getActiveEntriesWithEmbeddings();

		// Domain routing: resolve the domain for this chunk based on the episodes' directory.
		// Use the first episode's directory as the representative — episodes within a chunk
		// come from the same session source. The LLM may still classify individual entries
		// into different domains (e.g. a personal preference found in a work project).
		const chunkDirectory = chunk[0]?.directory ?? "";
		const domainResolution = this.domainRouter?.resolve(chunkDirectory) ?? null;

		// If the target store for this chunk's domain is currently unavailable, skip
		// the chunk entirely. The episodes remain in pending_episodes and will be
		// retried on the next consolidation run when the store may be reachable again.
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

		// Extract knowledge via LLM (episodes only — no existing knowledge context).
		const extractStart = Date.now();
		const extracted = await this.llm.extractKnowledge(
			chunkSummary,
			domainResolution?.domainContext ?? undefined,
		);
		logger.log(
			`[consolidation/${source}] Extracted ${extracted.length} entries in ${((Date.now() - extractStart) / 1000).toFixed(1)}s.`,
		);

		// Reconsolidate each extracted entry against existing knowledge.
		// Performance: load all entries with embeddings ONCE per chunk into an in-memory
		// Map. On insert/update, mutate the Map in place rather than reloading from DB.
		const sessionIds = [...new Set(chunk.map((e) => e.sessionId))];
		// Use the max message time of the chunk as the session timestamp for new entries.
		// This ensures entries extracted from old sessions start with the correct decay
		// already applied rather than appearing freshly created.
		// Defensive fallback: the empty-chunk guard at line 387 means chunk.length > 0
		// here, but an explicit fallback prevents Math.max(...[]) = -Infinity if that
		// guard is ever moved or removed in a future refactor.
		// Use reduce rather than Math.max(...spread) to avoid stack-size issues
		// on very large chunks. Use -Infinity as the neutral element so that a
		// chunk where every entry has maxMessageTime = 0 (epoch) correctly yields
		// 0, not -Infinity. -Infinity only arises for an empty chunk (already
		// guarded at line 387); isFinite() catches that fallback case.
		const reduced = chunk.reduce(
			(max, e) => Math.max(max, e.maxMessageTime),
			Number.NEGATIVE_INFINITY,
		);
		const chunkSessionTimestamp = Number.isFinite(reduced)
			? reduced
			: Date.now();
		// Reuse the entries already loaded above — no second DB read.
		const entriesMap = new Map(allEntriesForChunk.map((e) => [e.id, e]));
		let chunkCreated = 0;
		let chunkUpdated = 0;

		// Track IDs that were inserted or updated this chunk — only these are
		// eligible for the contradiction scan (pre-existing entries were already
		// checked in a previous consolidation run).
		const changedIds = new Set<string>();

		// Track which stores received inserts or content-changing updates this chunk.
		// Used after all chunks complete to run synthesis only on touched stores.
		// onKeep (reinforcement only) does not count — it doesn't affect cluster ripeness.
		const touchedStores = new Set<IKnowledgeStore>();

		for (const entry of extracted) {
			try {
				// Resolve the target store for this entry:
				// 1. If entry has an LLM-assigned domain, use that domain's store.
				// 2. Otherwise fall back to the chunk's resolved domain store.
				// 3. If no domain routing (domainResolution is null), pass undefined
				//    so reconsolidate() uses this.db as its insertDb fallback.
				//
				// this.domainRouter is non-null whenever domainResolution is non-null:
				// resolve() only returns non-null when domains are configured, and domains
				// being configured is the precondition for domainRouter being non-null.
				const domainRouter = this.domainRouter;
				// domainResolution.domainId is always a defined string here:
				// domainRouter is non-null only when config.domains.length > 0, which
				// means resolve() cannot hit its early-exit undefined path.
				const entryTargetStore =
					domainResolution && domainRouter
						? (domainRouter.resolveStore(
								entry.domain ?? domainResolution.domainId,
							) ?? domainResolution.store)
						: undefined;

				await this.reconsolidator.reconsolidate(
					entry,
					sessionIds,
					entriesMap,
					{
						onInsert: (inserted) => {
							chunkCreated++;
							changedIds.add(inserted.id);
							touchedStores.add(entryTargetStore ?? this.db);
							logger.log(
								// type is a validated KnowledgeType enum — bare interpolation is safe.
								// content is LLM-sourced — JSON.stringify escapes injected newlines/tokens.
								`[consolidation/${source}] + insert [${inserted.type}] ${entry.domain ? `domain:${entry.domain} ` : ""}${JSON.stringify(inserted.content)}`,
							);
							// Add to cache so subsequent entries in this chunk can deduplicate against it.
							// Embedding is available immediately since insertNewEntry stores it.
							if (inserted.embedding) {
								entriesMap.set(
									inserted.id,
									inserted as KnowledgeEntry & { embedding: number[] },
								);
							}
						},
						onUpdate: (id, updated, freshEmbedding) => {
							chunkUpdated++;
							changedIds.add(id);
							// Updates always go to this.db — existing entries live in this.db
							// regardless of the incoming entry's domain (entriesMap is loaded
							// exclusively from this.db). Only onInsert uses entryTargetStore.
							touchedStores.add(this.db);
							const existing = entriesMap.get(id);
							const contentForLog = updated.content ?? existing?.content ?? "";
							const typeForLog = updated.type ?? existing?.type ?? "?";
							logger.log(
								`[consolidation/${source}] ~ update [${typeForLog}] ${JSON.stringify(contentForLog)}`,
							);
							// Update the cache with the new content and fresh embedding.
							if (existing) {
								entriesMap.set(id, {
									...existing,
									content: updated.content ?? existing.content,
									type:
										(updated.type as KnowledgeEntry["type"]) ?? existing.type,
									topics: updated.topics ?? existing.topics,
									confidence: updated.confidence ?? existing.confidence,
									embedding: freshEmbedding,
								});
							}
						},
						onKeep: () => {},
					},
					chunkSessionTimestamp,
					undefined, // precomputedEmbedding
					"consolidation",
					entryTargetStore,
				);
			} catch (err) {
				// Log and skip this extracted entry — do NOT rethrow.
				// Rethrowing would skip recordEpisode for the whole chunk, causing all
				// entries in this chunk to be re-processed on the next run and producing
				// duplicates for the entries that were already successfully inserted.
				logger.error(
					`[consolidation/${source}] Failed to reconsolidate entry ${JSON.stringify(entry.content ?? "")} — skipping:`,
					err,
				);
			}
		}

		// Post-extraction contradiction scan.
		// Use the chunk's resolved domain store so that both candidate queries and
		// resolution writes target the same store the entries were written to.
		// Falls back to this.db in single-store mode (no domain routing).
		const contradictionDb = domainResolution?.store ?? this.db;
		const chunkContradictions = await this.contradictionScanner.scan(
			contradictionDb,
			entriesMap,
			changedIds,
		);

		// Record each episode in this chunk as processed using its original source.
		// Happens after the LLM call and DB writes succeed — makes consolidation
		// idempotent on crash/retry at the episode level.
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

		return {
			created: chunkCreated,
			updated: chunkUpdated,
			conflictsDetected: chunkContradictions.detected,
			conflictsResolved: chunkContradictions.resolved,
			touchedStores,
		};
	}

	/**
	 * Apply decay to all active entries.
	 * Returns the number of entries that were archived.
	 */
	private async applyDecay(): Promise<number> {
		// Include conflicted entries — their strength must continue aging.
		// A conflicted entry whose strength falls to zero is effectively forgotten
		// regardless of the conflict, and should still be archived.
		// Single query avoids the TOCTOU window of two separate status queries.
		const entries = await this.db.getActiveAndConflictedEntries();
		let archived = 0;

		for (const entry of entries) {
			const newStrength = computeStrength(entry);

			if (newStrength < config.decay.archiveThreshold) {
				await this.db.updateEntry(entry.id, {
					status: "archived",
					strength: newStrength,
				});
				archived++;
				logger.log(
					`[decay] Archived: ${JSON.stringify(entry.content)} (strength: ${newStrength.toFixed(3)})`,
				);
			} else if (Math.abs(newStrength - entry.strength) > 0.01) {
				// Only update if strength changed meaningfully
				await this.db.updateStrength(entry.id, newStrength);
			}
		}

		// Tombstone long-archived entries
		const archivedEntries = await this.db.getEntriesByStatus("archived");
		const tombstoneThreshold =
			Date.now() - config.decay.tombstoneAfterDays * 24 * 60 * 60 * 1000;

		for (const entry of archivedEntries) {
			if (entry.updatedAt < tombstoneThreshold) {
				await this.db.updateEntry(entry.id, { status: "tombstoned" });
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
	 * Must be called after ensureEmbeddings() has run (i.e. after consolidate()) so
	 * all entries have embeddings. Returns the total number of principles synthesized.
	 *
	 * NOTE: the `touchedStores` parameter and `_lastTouchedStores` accumulation are
	 * scaffolding for a follow-up refactor. Synthesis currently always runs on this.db
	 * only — running on domain stores is unsafe until reconsolidate() is parameterized
	 * for a mergeDb target (the inner mergeEntry call is hardcoded to this.db, so
	 * operating on a domain store would silently corrupt or no-op this.db entries).
	 * TODO: once reconsolidate() supports a mergeDb parameter, iterate over
	 * touchedStores (falling back to [this.db]) here instead of always using this.db.
	 */
	async runSynthesis(_touchedStores?: Set<IKnowledgeStore>): Promise<number> {
		// TODO: replace with per-store iteration once mergeDb refactor lands.
		return this.reconsolidator.runKBSynthesis(this.db);
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
