import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import type { DaemonDB } from "../db/daemon/index.js";
import type { IServerStateDB } from "../db/interface.js";
import { logger } from "../logger.js";
import type { RedactionResult } from "../redact.js";
import {
	formatRedactionCounts,
	redactSecrets,
	totalRedactions,
} from "../redact.js";
import type {
	Episode,
	IEpisodeReader,
	PendingEpisode,
	ProcessedRange,
} from "../types.js";
import { DEFAULT_INITIAL_LOOKBACK_DAYS, initialCursorMs } from "./lookback.js";
import {
	SESSION_IDLE_FLUSH_MS,
	recordSkip,
	takeSkipCount,
} from "./readers/shared.js";

/** Result of one polling upload cycle, reported to the daemon's status file. */
interface CycleOutcome {
	ok: boolean;
	uploaded: number;
	error?: string;
}

/**
 * EpisodeUploader — the daemon's core upload loop.
 *
 * For each configured episode reader (OpenCode, Claude Code, Cursor, etc.):
 *   1. Reads the daemon cursor (from DaemonDB, always local) to find the
 *      high-water mark for this source. A source with no cursor yet is a first
 *      run: only the lookback window is considered (INITIAL_LOOKBACK_DAYS).
 *   2. Calls getCandidateSessions to find new sessions
 *   3. Loads already-uploaded ranges (server state + per-session daemon
 *      watermarks) and calls getNewEpisodes, which returns only the new tail
 *      of each session
 *   4. Writes each episode to pending_episodes via IServerStateDB (local SQLite
 *      or remote Postgres, depending on configuration) and advances the
 *      per-session watermark in DaemonDB
 *   5. Advances the daemon cursor
 *
 * The separation of DaemonDB (cursor, always local) from IServerStateDB
 * (pending_episodes) enables a fully remote consolidation server:
 * the daemon writes episodes to a shared Postgres staging DB while keeping
 * its own cursor in a local SQLite file.
 */
export class EpisodeUploader {
	private readonly readers: IEpisodeReader[];
	private readonly serverStateDb: IServerStateDB;
	private readonly daemonDb: DaemonDB;
	private readonly userId: string;
	private readonly initialLookbackDays: number;
	private readonly fullBackfillRequests: boolean;

	/**
	 * @param readers       Episode readers — one per AI tool source.
	 * @param serverStateDb The staging DB — holds pending_episodes.
	 *                      Can be local SQLite or remote Postgres.
	 * @param daemonDb      The daemon-local DB — holds daemon_cursor and the
	 *                      per-session watermarks. Always local SQLite, never remote.
	 * @param userId        Stable user identifier (KNOWLEDGE_USER_ID or hostname).
	 * @param options.initialLookbackDays  First-run window in days (0 = all
	 *                      history, the default). The daemon entry point passes
	 *                      INITIAL_LOOKBACK_DAYS; other callers keep full history.
	 * @param options.fullBackfillRequests  Honour the daemon-wide full re-ingest request
	 *                      (set by reinitialize) and clear it once every source has been
	 *                      re-read. Only the daemon passes true; process-inbox and other
	 *                      uploaders leave the request alone.
	 */
	constructor(
		readers: IEpisodeReader[],
		serverStateDb: IServerStateDB,
		daemonDb: DaemonDB,
		userId: string,
		options: {
			initialLookbackDays?: number;
			fullBackfillRequests?: boolean;
		} = {},
	) {
		this.readers = readers;
		this.serverStateDb = serverStateDb;
		this.daemonDb = daemonDb;
		this.userId = userId;
		this.initialLookbackDays = options.initialLookbackDays ?? 0;
		this.fullBackfillRequests = options.fullBackfillRequests ?? false;
	}

	/**
	 * Run one upload cycle — process all readers and upload new episodes.
	 * Returns a summary of what was uploaded.
	 */
	async upload(): Promise<{
		episodesUploaded: number;
		sessionsProcessed: number;
		/** True when any source's cursor advanced (episodes uploaded OR ineligible sessions skipped). */
		cursorAdvanced: boolean;
		/** Unreadable sessions and malformed or poison-pill items skipped this cycle, all sources. */
		itemsSkipped: number;
		sources: Array<{ source: string; episodes: number; sessions: number }>;
		/** Sources whose upload threw this cycle (skipped until the next cycle). */
		failedSources: Array<{ source: string; error: string }>;
	}> {
		// Sources are independent — upload all concurrently.
		// The daemon-wide re-ingest request is read and cleared only when the caller owns it.
		const fullBackfill =
			this.fullBackfillRequests &&
			(await this.daemonDb.hasFullBackfillRequest());
		const settled = await Promise.allSettled(
			this.readers.map((reader) => this.uploadSource(reader, fullBackfill)),
		);
		// A source is exhausted when this cycle reached the end of its candidates.
		let allExhausted = true;

		let totalEpisodes = 0;
		let totalSessions = 0;
		let totalSkipped = 0;
		let anyCursorAdvanced = false;
		const sources: Array<{
			source: string;
			episodes: number;
			sessions: number;
		}> = [];
		const failedSources: Array<{ source: string; error: string }> = [];

		for (let i = 0; i < settled.length; i++) {
			const result = settled[i];
			const reader = this.readers[i];
			if (result.status === "fulfilled") {
				totalEpisodes += result.value.episodes;
				totalSessions += result.value.sessions;
				totalSkipped += result.value.skipped;
				if (result.value.cursorAdvanced) anyCursorAdvanced = true;
				if (!result.value.exhausted) allExhausted = false;
				if (result.value.episodes > 0 || result.value.sessions > 0) {
					sources.push({
						source: reader.source,
						episodes: result.value.episodes,
						sessions: result.value.sessions,
					});
				}
				if (result.value.error !== undefined) {
					failedSources.push({
						source: reader.source,
						error: result.value.error,
					});
				}
			} else {
				// The source's skip counter is not drained here; it is reset on the next
				// cycle's uploadSource, and the reason is already logged there or here.
				takeSkipCount(reader.source);
				const error =
					result.reason instanceof Error
						? result.reason.message
						: String(result.reason);
				failedSources.push({ source: reader.source, error });
				allExhausted = false;
				logger.error(
					`[daemon/${reader.source}] Upload failed — skipping source this run:`,
					result.reason,
				);
			}
		}

		if (totalEpisodes > 0) {
			logger.log(
				`[daemon] Uploaded ${totalEpisodes} episodes from ${totalSessions} sessions.`,
			);
		}
		if (fullBackfill && allExhausted && failedSources.length === 0) {
			await this.daemonDb.clearFullBackfillRequest();
			logger.log("[daemon] Full re-ingest complete for every source.");
		}
		if (totalSkipped > 0) {
			logger.warn(
				`[daemon] Skipped ${totalSkipped} unreadable session(s) or malformed item(s) this cycle.`,
			);
		}

		return {
			episodesUploaded: totalEpisodes,
			sessionsProcessed: totalSessions,
			cursorAdvanced: anyCursorAdvanced,
			itemsSkipped: totalSkipped,
			sources,
			failedSources,
		};
	}

	private async uploadSource(
		reader: IEpisodeReader,
		fullBackfill: boolean,
	): Promise<{
		episodes: number;
		sessions: number;
		cursorAdvanced: boolean;
		skipped: number;
		/** This cycle read the source to its end (no batch limit hit, nothing aborted). */
		exhausted: boolean;
		/** Set when the source could not be read this cycle. */
		error?: string;
	}> {
		// Daemon cursor lives in DaemonDB (always local SQLite).
		const firstRun = !(await this.daemonDb.hasDaemonCursor(reader.source));
		// A re-ingest requested by reinitialize reads from the start of history.
		const since = firstRun
			? fullBackfill
				? 0
				: initialCursorMs(this.initialLookbackDays)
			: (await this.daemonDb.getDaemonCursor(reader.source))
					.lastMessageTimeCreated;

		const candidateSessions = reader.getCandidateSessions(
			since,
			config.consolidation.maxSessionsPerRun,
		);

		if (candidateSessions.length === 0) {
			return {
				episodes: 0,
				sessions: 0,
				cursorAdvanced: false,
				skipped: takeSkipCount(reader.source),
				exhausted: true,
			};
		}

		const candidateIds = candidateSessions.map((s) => s.id);

		// Already-uploaded ranges: consolidated and pending rows on the state DB,
		// plus this daemon's per-session watermark (in case a state row was
		// removed after upload). Each watermark is a degenerate range ending at
		// the last staged message, so the reader drops everything up to it.
		const processedRanges = await this.loadProcessedRanges(
			reader.source,
			candidateIds,
		);

		let newEpisodes: Episode[];
		try {
			newEpisodes = reader.getNewEpisodes(candidateIds, processedRanges);
		} catch (err) {
			logger.error(`[daemon/${reader.source}] getNewEpisodes failed:`, err);
			return {
				episodes: 0,
				sessions: 0,
				cursorAdvanced: false,
				skipped: takeSkipCount(reader.source),
				exhausted: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}

		if (firstRun) {
			this.logBackfillPreview(reader.source, newEpisodes, fullBackfill);
		}

		let uploadedCount = 0;
		const uploadedSessionIds = new Set<string>();
		// Latest maxMessageTime among episodes that are handled: staged, or skipped
		// on purpose (fail-closed redaction, NUL-only content). The cursor may move
		// up to here. A failed insert stops the loop before its own episode, so the
		// cursor stays behind it and the episode is retried next cycle.
		let progressMax = since;
		let aborted = false;
		const redactionCounts: Record<string, number> = {};
		let nulStripped = 0;

		for (const raw of newEpisodes) {
			const ep = stripNulFromEpisode(raw);
			if (ep !== raw) nulStripped++;

			// Every free-text field is scrubbed: the title, project and directory are
			// copied into the staged row and into each extraction prompt, not only the content.
			let scrubbed: [
				RedactionResult,
				RedactionResult,
				RedactionResult,
				RedactionResult,
			];
			try {
				scrubbed = [
					redactSecrets(ep.content),
					redactSecrets(ep.sessionTitle),
					redactSecrets(ep.projectName),
					redactSecrets(ep.directory),
				];
			} catch (err) {
				// Fail closed: an episode that cannot be scrubbed is never uploaded. It
				// is recorded as handled so it cannot hold the cursor back forever.
				recordSkip(
					reader.source,
					`session ${ep.sessionId}`,
					`redaction failed; episode not uploaded: ${err}`,
				);
				await this.daemonDb.setSessionCursor(
					reader.source,
					ep.sessionId,
					ep.endMessageId,
				);
				progressMax = Math.max(progressMax, ep.maxMessageTime);
				continue;
			}
			for (const result of scrubbed) {
				for (const [kind, n] of Object.entries(result.counts)) {
					redactionCounts[kind] = (redactionCounts[kind] ?? 0) + n;
				}
			}
			const [content, title, project, directory] = scrubbed;
			const pending: PendingEpisode = {
				id: randomUUID(),
				userId: this.userId,
				source: reader.source,
				sessionId: ep.sessionId,
				startMessageId: ep.startMessageId,
				endMessageId: ep.endMessageId,
				sessionTitle: title.text,
				projectName: project.text,
				directory: directory.text,
				timeCreated: ep.timeCreated,
				maxMessageTime: ep.maxMessageTime,
				content: content.text,
				contentType: ep.contentType,
				approxTokens: ep.approxTokens,
				uploadedAt: Date.now(),
			};
			try {
				await this.serverStateDb.insertPendingEpisode(pending);
			} catch (err) {
				logger.error(
					`[daemon/${reader.source}] Failed to insert episode ${pending.id}: ${err}`,
				);
				// Stop processing — cursor won't advance past this episode.
				aborted = true;
				break;
			}
			await this.daemonDb.setSessionCursor(
				reader.source,
				ep.sessionId,
				ep.endMessageId,
			);
			uploadedCount++;
			uploadedSessionIds.add(ep.sessionId);
			progressMax = Math.max(progressMax, ep.maxMessageTime);
		}

		const redactedTotal = totalRedactions(redactionCounts);
		if (redactedTotal > 0) {
			logger.log(
				`[redact] uploader: redacted ${redactedTotal} value(s) (${formatRedactionCounts(redactionCounts)})`,
			);
		}
		if (nulStripped > 0) {
			logger.warn(
				`[daemon/${reader.source}] Stripped NUL bytes from ${nulStripped} episode(s); Postgres rejects them.`,
			);
		}

		// Advance daemon cursor.
		//
		// 1. Insert failure: cursor moves only to progressMax, so the failed
		//    episode is retried on the next run.
		// 2. All episodes handled, none produced (e.g. sessions below
		//    minSessionMessages, or tails held back): advance past every examined
		//    session. Without this the cursor never moves past a block of
		//    ineligible sessions and the daemon re-fetches them forever.
		// 3. Batch limit hit: cap at lastSession.maxMessageTime - 1 so sessions that
		//    share the boundary timestamp are not skipped.
		// 4. Otherwise advance past every examined session.
		//
		// Then pin: a session with prior uploads that is still active (newest
		// message within the idle window) keeps the cursor behind it. Its held tail
		// is only flushed once it goes idle, and that needs a later cycle to still
		// list it as a candidate. Sessions never uploaded need no pin: below the
		// minimum they are ineligible, and growth moves them past the cursor.
		const lastSession = candidateSessions[candidateSessions.length - 1];
		const hitBatchLimit =
			candidateSessions.length === config.consolidation.maxSessionsPerRun;

		let newCursor = progressMax;

		// Case 1 (insert failure) keeps progressMax, so nothing else applies.
		if (!aborted) {
			if (newEpisodes.length === 0) {
				// Case 2.
				const reach = hitBatchLimit
					? lastSession.maxMessageTime - 1
					: lastSession.maxMessageTime;
				newCursor = Math.max(newCursor, reach);
			} else if (hitBatchLimit) {
				// Case 3.
				const cap = lastSession.maxMessageTime - 1;
				if (cap > since) newCursor = Math.min(newCursor, cap);
			} else {
				// Case 4.
				newCursor = Math.max(newCursor, lastSession.maxMessageTime);
			}
		}

		const now = Date.now();
		const activeSessions = candidateSessions.filter(
			(s) =>
				now - s.maxMessageTime < SESSION_IDLE_FLUSH_MS &&
				(processedRanges.get(s.id)?.length ?? 0) > 0,
		);
		if (activeSessions.length > 0) {
			const pin = Math.min(...activeSessions.map((s) => s.maxMessageTime)) - 1;
			newCursor = Math.min(newCursor, pin);
		}

		newCursor = Math.max(newCursor, since);

		await this.daemonDb.updateDaemonCursor(reader.source, {
			lastMessageTimeCreated: newCursor,
			lastUploadedAt: now,
		});

		if (uploadedCount > 0) {
			logger.log(
				`[daemon/${reader.source}] Uploaded ${uploadedCount} episodes from ${uploadedSessionIds.size} sessions.`,
			);
		}

		return {
			episodes: uploadedCount,
			sessions: uploadedSessionIds.size,
			cursorAdvanced: newCursor > since,
			skipped: takeSkipCount(reader.source),
			exhausted: !hitBatchLimit && !aborted,
		};
	}

	/**
	 * Ranges already covered for the candidate sessions: staged or consolidated
	 * episodes from the state DB, merged with this daemon's per-session watermarks.
	 */
	private async loadProcessedRanges(
		source: string,
		sessionIds: string[],
	): Promise<Map<string, ProcessedRange[]>> {
		const ranges =
			await this.serverStateDb.getUploadedEpisodeRanges(sessionIds);
		const watermarks = await this.daemonDb.getSessionCursors(
			source,
			sessionIds,
		);
		const merged = new Map(ranges);
		for (const [sessionId, lastMessageId] of watermarks) {
			merged.set(sessionId, [
				...(merged.get(sessionId) ?? []),
				{
					source,
					startMessageId: lastMessageId,
					endMessageId: lastMessageId,
				},
			]);
		}
		return merged;
	}

	/**
	 * First-run preview (PV-10): episode count and rough token total for this
	 * batch. No model price is configured here, so the cost is not estimated.
	 */
	private logBackfillPreview(
		source: string,
		episodes: Episode[],
		fullHistory: boolean,
	): void {
		const tokens = episodes.reduce((sum, ep) => sum + ep.approxTokens, 0);
		const window = fullHistory
			? "all history (full re-ingest requested by reinitialize)"
			: this.initialLookbackDays === 0
				? "all history (INITIAL_LOOKBACK_DAYS=0)"
				: `the last ${this.initialLookbackDays} days (INITIAL_LOOKBACK_DAYS; default ${DEFAULT_INITIAL_LOOKBACK_DAYS})`;
		logger.log(
			`[daemon/${source}] First run, ingesting ${window}. Preview: ${episodes.length} episodes, ~${tokens} tokens in this batch. LLM cost: not estimated (no model price configured).`,
		);
	}

	/**
	 * Run the daemon in polling mode — upload on interval until stopped.
	 *
	 * @param intervalMs  Upload interval in milliseconds (default: 30 minutes).
	 * @param onShutdown  Optional async cleanup callback called before process.exit.
	 *                    Use this to close DB connections and readers gracefully.
	 */
	async runPolling(
		intervalMs = 30 * 60 * 1000,
		onShutdown?: () => Promise<void>,
		onCycle?: (outcome: CycleOutcome) => void,
	): Promise<void> {
		let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
		let shuttingDown = false;

		const scheduleNext = (immediate: boolean) => {
			if (shuttingDown) return;
			timeoutHandle = setTimeout(
				() => void runCycle(),
				immediate ? 0 : intervalMs,
			);
		};

		const runCycle = async () => {
			if (shuttingDown) return;
			try {
				const result = await this.upload();
				onCycle?.(
					result.failedSources.length > 0
						? {
								ok: false,
								uploaded: result.episodesUploaded,
								error: result.failedSources
									.map((f) => `${f.source}: ${f.error}`)
									.join("; "),
							}
						: { ok: true, uploaded: result.episodesUploaded },
				);
				// Re-run immediately when the cursor advanced — either episodes were
				// uploaded or ineligible sessions were skipped past. Only sleep for
				// intervalMs when all sources are genuinely caught up with no new or
				// skippable sessions.
				scheduleNext(result.episodesUploaded > 0 || result.cursorAdvanced);
			} catch (err) {
				logger.error("[daemon] Upload cycle failed:", err);
				onCycle?.({
					ok: false,
					uploaded: 0,
					error: err instanceof Error ? err.message : String(err),
				});
				scheduleNext(false);
			}
		};

		// Register shutdown handlers before first run so a SIGTERM during
		// the initial upload cycle is handled cleanly.
		const cleanup = async () => {
			if (shuttingDown) return;
			shuttingDown = true;
			if (timeoutHandle !== null) clearTimeout(timeoutHandle);
			logger.log("[daemon] Stopping…");
			if (onShutdown) {
				await onShutdown().catch((err) => {
					logger.error("[daemon] Error during shutdown:", err);
				});
			}
			logger.log("[daemon] Stopped.");
			process.exit(0);
		};
		process.on("SIGTERM", () => void cleanup());
		process.on("SIGINT", () => void cleanup());

		// Announce only once the handlers exist: callers (and tests) treat this line
		// as "a SIGTERM from now on is handled and releases the daemon lock".
		logger.log(
			`[daemon] Starting. Upload interval: ${Math.round(intervalMs / 1000)}s. User: ${this.userId}`,
		);

		// Run immediately on start, then self-schedule via scheduleNext.
		await runCycle();
	}
}

/**
 * Remove NUL bytes from the text fields of an episode. Postgres rejects `\u0000`
 * in text columns, so one such byte would fail the insert and stall the source.
 * Returns the same object when nothing needed stripping.
 */
function stripNulFromEpisode(ep: Episode): Episode {
	const fields = [
		ep.content,
		ep.sessionTitle,
		ep.projectName,
		ep.directory,
	] as const;
	if (!fields.some((f) => f.includes("\u0000"))) return ep;
	return {
		...ep,
		content: stripNul(ep.content),
		sessionTitle: stripNul(ep.sessionTitle),
		projectName: stripNul(ep.projectName),
		directory: stripNul(ep.directory),
	};
}

function stripNul(value: string): string {
	return value.split("\u0000").join("");
}
