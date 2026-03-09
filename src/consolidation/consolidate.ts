import type { ActivationEngine } from "../activation/activate.js";
import { config } from "../config.js";
import type { KnowledgeDB } from "../db/database.js";
import { logger } from "../logger.js";
import type { IEpisodeReader, KnowledgeEntry } from "../types.js";
import type { ConsolidationResult } from "../types.js";
import { ContradictionScanner } from "./contradiction.js";
import { computeStrength } from "./decay.js";
import { ConsolidationLLM, formatEpisodes } from "./llm.js";
import { approxTokens } from "./readers/shared.js";
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
 * - IEpisodeReader[]      — one or more readers (OpenCode, Claude Code, …)
 * - ConsolidationLLM      — wraps all LLM calls (extract, merge, contradiction)
 * - Reconsolidator        — deduplicates extracted entries against existing knowledge
 * - ContradictionScanner  — detects and resolves contradictions in the mid-similarity band
 *
 * Models the human brain's sleep consolidation process:
 * 1. For each source reader, read NEW episodes (since that source's cursor)
 * 2. Load EXISTING knowledge (the current mental model)
 * 3. Extract new knowledge from episodes (what's worth remembering?)
 * 4. Reconsolidate — deduplicate/merge against existing knowledge
 * 5. Contradiction scan — detect and resolve conflicts in the mid-band
 * 6. Advance that source's cursor
 * 7. After all sources: apply decay and generate embeddings
 */
export class ConsolidationEngine {
	private db: KnowledgeDB;
	private activation: ActivationEngine;
	private readers: IEpisodeReader[];
	private llm: ConsolidationLLM;
	private reconsolidator: Reconsolidator;
	private contradictionScanner: ContradictionScanner;

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
	 * @param db         Knowledge DB instance
	 * @param activation ActivationEngine (provides shared EmbeddingClient)
	 * @param readers    Episode readers, one per source. Passed in by the caller
	 *                   (via createEpisodeReaders()) so construction is testable
	 *                   without real source DBs on disk.
	 */
	constructor(
		db: KnowledgeDB,
		activation: ActivationEngine,
		readers: IEpisodeReader[] = [],
	) {
		this.db = db;
		this.activation = activation;
		this.readers = readers;
		this.llm = new ConsolidationLLM();
		this.reconsolidator = new Reconsolidator(
			db,
			activation.embeddings,
			this.llm,
		);
		this.contradictionScanner = new ContradictionScanner(db, this.llm);
	}

	/**
	 * Check how many sessions are pending consolidation without running it.
	 * Used at startup to decide whether to kick off a background consolidation.
	 * Aggregates across all active readers.
	 */
	checkPending(): { pendingSessions: number; lastConsolidatedAt: number } {
		const state = this.db.getConsolidationState();
		let pendingSessions = 0;

		for (const reader of this.readers) {
			const cursor = this.db.getSourceCursor(reader.source);
			pendingSessions += reader.countNewSessions(cursor.lastMessageTimeCreated);
		}

		return { pendingSessions, lastConsolidatedAt: state.lastConsolidatedAt };
	}

	/**
	 * Run a consolidation cycle.
	 *
	 * This is the main entry point — called by HTTP API or CLI.
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
	async consolidate(): Promise<ConsolidationResult> {
		const startTime = Date.now();
		const state = this.db.getConsolidationState();

		logger.log(
			`[consolidation] Starting. Last run: ${state.lastConsolidatedAt ? new Date(state.lastConsolidatedAt).toISOString() : "never"}`,
		);

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
		}

		if (totalSessionsProcessed === 0) {
			logger.log("[consolidation] No new sessions to process.");
		}

		// Apply decay to ALL active entries (once, after all sources are processed).
		const archived = this.applyDecay();

		// Generate embeddings for new entries (once, shared across all sources).
		const embeddedCount = await this.activation.ensureEmbeddings();
		logger.log(
			`[consolidation] Generated embeddings for ${embeddedCount} entries.`,
		);

		// If embedding metadata is missing (e.g. startup probe failed, or first
		// consolidation after a pre-v7 upgrade) or stale (e.g. checkAndReEmbed
		// failed before updating metadata), record it now from a fresh entry.
		const embeddingMeta = this.db.getEmbeddingMetadata();
		const metadataStale = embeddingMeta && embeddingMeta.model !== config.embedding.model;
		if (embeddedCount > 0 && (!embeddingMeta || metadataStale)) {
			const sample = this.db.getActiveEntriesWithEmbeddings();
			if (sample.length > 0) {
				this.db.setEmbeddingMetadata(
					config.embedding.model,
					sample[0].embedding.length,
				);
				logger.log(
					`[embedding] Recorded embedding model: ${config.embedding.model} (${sample[0].embedding.length} dimensions).`,
				);
			}
		}

		this.db.updateConsolidationState({
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
	}> {
		const cursor = this.db.getSourceCursor(reader.source);

		// 1. Fetch candidate sessions: those with messages newer than this source's cursor.
		//    Returns session IDs plus the max message timestamp per session,
		//    ordered by max message time ASC for deterministic batching.
		const candidateSessions = reader.getCandidateSessions(
			cursor.lastMessageTimeCreated,
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
			};
		}

		const candidateIds = candidateSessions.map((s) => s.id);

		// 2. Load already-processed episode ranges for this source's batch of sessions.
		const processedRanges = this.db.getProcessedEpisodeRanges(
			reader.source,
			candidateIds,
		);

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

		const chunkSize = config.consolidation.chunkSize;

		for (let i = 0; i < episodes.length; i += chunkSize) {
			const chunk = episodes.slice(i, i + chunkSize);
			const chunkLabel = `${Math.floor(i / chunkSize) + 1}/${Math.ceil(episodes.length / chunkSize)}`;

			logger.log(
				`[consolidation/${reader.source}] Chunk ${chunkLabel} (${chunk.length} episodes)`,
			);

			const counts = await this.processChunk(reader.source, chunk);
			totalCreated += counts.created;
			totalUpdated += counts.updated;
			totalConflictsDetected += counts.conflictsDetected;
			totalConflictsResolved += counts.conflictsResolved;
		}

		// 7. Advance the source cursor past all fetched candidates.
		//    Same boundary-safety logic as before, but now per-source.
		const maxEpisodeMessageTime =
			episodes.length > 0
				? episodes.reduce(
						(max, ep) => (ep.maxMessageTime > max ? ep.maxMessageTime : max),
						0,
					)
				: 0;

		let newCursor = Math.max(
			maxEpisodeMessageTime,
			cursor.lastMessageTimeCreated,
		);

		const lastSession = candidateSessions[candidateSessions.length - 1];
		const hitBatchLimit =
			candidateSessions.length === config.consolidation.maxSessionsPerRun;

		// Boundary-timestamp safety: if the batch is full, there may be additional
		// unprocessed sessions beyond the boundary that share the exact same maxMessageTime.
		// The next query uses `>`, so cap the cursor just below the boundary to re-fetch them.
		if (hitBatchLimit) {
			const cap = lastSession.maxMessageTime - 1;
			if (cap > cursor.lastMessageTimeCreated) {
				newCursor = Math.min(newCursor, cap);
			}
		} else {
			// Batch is not full — advance past all candidates so sessions that produced
			// no episodes don't re-appear as candidates next run.
			newCursor = Math.max(newCursor, lastSession.maxMessageTime);
		}

		// Safety floor: never move the cursor backwards.
		newCursor = Math.max(newCursor, cursor.lastMessageTimeCreated);

		this.db.updateSourceCursor(reader.source, {
			lastMessageTimeCreated: newCursor,
			lastConsolidatedAt: Date.now(),
		});

		return {
			sessionsProcessed: candidateSessions.length,
			segmentsProcessed: episodes.length,
			entriesCreated: totalCreated,
			entriesUpdated: totalUpdated,
			conflictsDetected: totalConflictsDetected,
			conflictsResolved: totalConflictsResolved,
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
	}> {
		if (chunk.length === 0) {
			return {
				created: 0,
				updated: 0,
				conflictsDetected: 0,
				conflictsResolved: 0,
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
		// Load entries for reconsolidation. Existing knowledge is no longer passed to
		// the extraction LLM — deduplication is handled by the reconsolidation step
		// (embedding similarity + decideMerge).
		const allEntriesForChunk = this.db.getActiveEntriesWithEmbeddings();

		// Extract knowledge via LLM (episodes only — no existing knowledge context).
		const extractStart = Date.now();
		const extracted = await this.llm.extractKnowledge(chunkSummary);
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

		for (const entry of extracted) {
			try {
				await this.reconsolidator.reconsolidate(
					entry,
					sessionIds,
					entriesMap,
					{
						onInsert: (inserted) => {
							chunkCreated++;
							changedIds.add(inserted.id);
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
							// Update the cache with the new content and fresh embedding.
							const existing = entriesMap.get(id);
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
				);
			} catch (err) {
				// Log and skip this extracted entry — do NOT rethrow.
				// Rethrowing would skip recordEpisode for the whole chunk, causing all
				// entries in this chunk to be re-processed on the next run and producing
				// duplicates for the entries that were already successfully inserted.
				logger.error(
					`[consolidation/${source}] Failed to reconsolidate entry "${String(entry.content ?? "").slice(0, 60)}..." — skipping:`,
					err,
				);
			}
		}

		// Post-extraction contradiction scan.
		const chunkContradictions = await this.contradictionScanner.scan(
			entriesMap,
			changedIds,
		);

		// Record each episode in this chunk as processed.
		// Happens after the LLM call and DB writes succeed — makes consolidation
		// idempotent on crash/retry at the episode level.
		const entriesPerEp = Math.round(
			(chunkCreated + chunkUpdated) / chunk.length,
		);
		for (const ep of chunk) {
			this.db.recordEpisode(
				source,
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
		};
	}

	/**
	 * Apply decay to all active entries.
	 * Returns the number of entries that were archived.
	 */
	private applyDecay(): number {
		// Include conflicted entries — their strength must continue aging.
		// A conflicted entry whose strength falls to zero is effectively forgotten
		// regardless of the conflict, and should still be archived.
		// Single query avoids the TOCTOU window of two separate status queries.
		const entries = this.db.getActiveAndConflictedEntries();
		let archived = 0;

		for (const entry of entries) {
			const newStrength = computeStrength(entry);

			if (newStrength < config.decay.archiveThreshold) {
				this.db.updateEntry(entry.id, {
					status: "archived",
					strength: newStrength,
				});
				archived++;
				logger.log(
					`[decay] Archived: "${entry.content.slice(0, 60)}..." (strength: ${newStrength.toFixed(3)})`,
				);
			} else if (Math.abs(newStrength - entry.strength) > 0.01) {
				// Only update if strength changed meaningfully
				this.db.updateStrength(entry.id, newStrength);
			}
		}

		// Tombstone long-archived entries
		const archivedEntries = this.db.getEntriesByStatus("archived");
		const tombstoneThreshold =
			Date.now() - config.decay.tombstoneAfterDays * 24 * 60 * 60 * 1000;

		for (const entry of archivedEntries) {
			if (entry.updatedAt < tombstoneThreshold) {
				this.db.updateEntry(entry.id, { status: "tombstoned" });
				logger.log(
					`[decay] Tombstoned: "${entry.content.slice(0, 60)}..." (archived for ${config.decay.tombstoneAfterDays}+ days)`,
				);
			}
		}

		return archived;
	}

	close(): void {
		for (const reader of this.readers) {
			reader.close();
		}
	}
}
