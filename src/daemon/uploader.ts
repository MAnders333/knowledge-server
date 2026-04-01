import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import type { IServerStateDB } from "../db/interface.js";
import { logger } from "../logger.js";
import type { Episode, IEpisodeReader, PendingEpisode } from "../types.js";

/**
 * EpisodeUploader — the daemon's core upload loop.
 *
 * For each configured episode reader (OpenCode, Claude Code, Cursor, etc.):
 *   1. Reads the daemon cursor to find the high-water mark for this source
 *   2. Calls getCandidateSessions to find new sessions
 *   3. Calls getNewEpisodes to get unprocessed episode content
 *   4. Writes each episode to the pending_episodes staging table
 *   5. Advances the daemon cursor
 *
 * The daemon writes pending_episodes to the server-local DB (state.db), which
 * is always co-located with the knowledge-server. The consolidation engine reads
 * from there, extracts knowledge via LLM, and routes entries to the appropriate
 * knowledge store (local SQLite or remote Postgres) based on domain configuration.
 *
 * The daemon cursor (tracking what has been uploaded) is stored in the same
 * server-local DB alongside pending_episodes.
 */
export class EpisodeUploader {
	private readonly readers: IEpisodeReader[];
	private readonly serverStateDb: IServerStateDB;
	private readonly userId: string;

	/**
	 * @param readers       Episode readers — one per AI tool source.
	 * @param serverStateDb The server-local DB — holds pending_episodes and daemon_cursor.
	 *                      This is always the DB on the same machine as the server.
	 * @param userId        Stable user identifier (KNOWLEDGE_USER_ID or hostname).
	 */
	constructor(
		readers: IEpisodeReader[],
		serverStateDb: IServerStateDB,
		userId: string,
	) {
		this.readers = readers;
		this.serverStateDb = serverStateDb;
		this.userId = userId;
	}

	/**
	 * Run one upload cycle — process all readers and upload new episodes.
	 * Returns a summary of what was uploaded.
	 */
	async upload(): Promise<{
		episodesUploaded: number;
		sessionsProcessed: number;
		cursorAdvanced: boolean;
		sources: Array<{ source: string; episodes: number; sessions: number }>;
	}> {
		let totalEpisodes = 0;
		let totalSessions = 0;
		let anyCursorAdvanced = false;
		const sources: Array<{
			source: string;
			episodes: number;
			sessions: number;
		}> = [];

		for (const reader of this.readers) {
			try {
				const result = await this.uploadSource(reader);
				totalEpisodes += result.episodes;
				totalSessions += result.sessions;
				if (result.cursorAdvanced) anyCursorAdvanced = true;
				if (result.episodes > 0 || result.sessions > 0) {
					sources.push({
						source: reader.source,
						episodes: result.episodes,
						sessions: result.sessions,
					});
				}
			} catch (err) {
				// Per-source failure: log and continue with other sources.
				// Same philosophy as the consolidation engine's per-source try/catch.
				logger.error(
					`[daemon/${reader.source}] Upload failed — skipping source this run:`,
					err,
				);
			}
		}

		if (totalEpisodes > 0) {
			logger.log(
				`[daemon] Uploaded ${totalEpisodes} episodes from ${totalSessions} sessions.`,
			);
		}

		return {
			episodesUploaded: totalEpisodes,
			sessionsProcessed: totalSessions,
			cursorAdvanced: anyCursorAdvanced,
			sources,
		};
	}

	private async uploadSource(
		reader: IEpisodeReader,
	): Promise<{ episodes: number; sessions: number; cursorAdvanced: boolean }> {
		// Daemon cursor and pending_episodes both live in the server-local DB.
		const cursor = await this.serverStateDb.getDaemonCursor(reader.source);

		const candidateSessions = reader.getCandidateSessions(
			cursor.lastMessageTimeCreated,
			config.consolidation.maxSessionsPerRun,
		);

		if (candidateSessions.length === 0) {
			return { episodes: 0, sessions: 0, cursorAdvanced: false };
		}

		const candidateIds = candidateSessions.map((s) => s.id);

		// Load already-uploaded/consolidated episodes to avoid re-uploading.
		const processedRanges =
			await this.serverStateDb.getProcessedEpisodeRanges(candidateIds);

		// Also check what's already pending (uploaded but not yet consolidated).
		const alreadyPending = await this.serverStateDb.getPendingEpisodes(
			cursor.lastMessageTimeCreated,
		);
		const pendingSet = new Set(
			alreadyPending
				.filter((ep) => ep.source === reader.source)
				.map((ep) => `${ep.sessionId}|${ep.startMessageId}|${ep.endMessageId}`),
		);

		let episodes: Episode[];
		try {
			episodes = reader.getNewEpisodes(candidateIds, processedRanges);
		} catch (err) {
			logger.error(`[daemon/${reader.source}] getNewEpisodes failed:`, err);
			return { episodes: 0, sessions: 0, cursorAdvanced: false };
		}

		const newEpisodes = episodes.filter(
			(ep) =>
				!pendingSet.has(
					`${ep.sessionId}|${ep.startMessageId}|${ep.endMessageId}`,
				),
		);

		let uploadedCount = 0;
		const uploadedSessionIds = new Set<string>();
		// Track the max message time of successfully uploaded episodes only.
		// If the loop exits via break (insert failure), the cursor won't advance
		// past the failed episode — it will be retried on the next daemon run.
		let lastSuccessMaxTime = cursor.lastMessageTimeCreated;

		for (const ep of newEpisodes) {
			const pending: PendingEpisode = {
				id: randomUUID(),
				userId: this.userId,
				source: reader.source,
				sessionId: ep.sessionId,
				startMessageId: ep.startMessageId,
				endMessageId: ep.endMessageId,
				sessionTitle: ep.sessionTitle,
				projectName: ep.projectName,
				directory: ep.directory,
				timeCreated: ep.timeCreated,
				maxMessageTime: ep.maxMessageTime,
				content: ep.content,
				contentType: ep.contentType,
				approxTokens: ep.approxTokens,
				uploadedAt: Date.now(),
			};
			try {
				await this.serverStateDb.insertPendingEpisode(pending);
				uploadedCount++;
				uploadedSessionIds.add(ep.sessionId);
				lastSuccessMaxTime = Math.max(lastSuccessMaxTime, ep.maxMessageTime);
			} catch (err) {
				logger.error(
					`[daemon/${reader.source}] Failed to insert episode ${pending.id}: ${err}`,
				);
				// Stop processing — cursor won't advance past this episode.
				break;
			}
		}

		// Advance daemon cursor.
		//
		// There are three cases to handle:
		//
		// 1. Some episodes were uploaded successfully → advance cursor to the max
		//    message time of the last uploaded episode (lastSuccessMaxTime).
		//    If we also hit the batch limit, cap at (lastSession.maxMessageTime - 1)
		//    to avoid skipping sessions that share the boundary timestamp.
		//
		// 2. No episodes were produced (all sessions skipped — e.g. too few
		//    messages, or all episodes already uploaded) and no insert failures →
		//    advance cursor past all examined sessions. Without this, the daemon
		//    would re-fetch the same batch of ineligible sessions every cycle and
		//    the cursor would never advance (starvation bug).
		//
		// 3. An insert failure caused the upload loop to break early → advance
		//    only to lastSuccessMaxTime, which stays at the old cursor if nothing
		//    was uploaded before the failure. The failed episode will be retried
		//    on the next daemon run.
		const lastSession = candidateSessions[candidateSessions.length - 1];
		const hitBatchLimit =
			candidateSessions.length === config.consolidation.maxSessionsPerRun;

		// Detect whether the upload loop completed without insert failures.
		// If it did, we've fully examined all candidate sessions and can safely
		// advance past them even if none produced episodes.
		const allUploadsSucceeded = uploadedCount === newEpisodes.length;

		let newCursor = lastSuccessMaxTime;
		if (allUploadsSucceeded && uploadedCount === 0) {
			// Case 2: all sessions examined, none produced episodes.
			// Advance past the entire batch so the next run picks up fresh sessions.
			if (hitBatchLimit) {
				// Cap at lastSession - 1 to avoid skipping a session that might
				// share the boundary timestamp with the next batch's first session.
				newCursor = Math.max(
					newCursor,
					lastSession.maxMessageTime - 1,
				);
			} else {
				newCursor = Math.max(newCursor, lastSession.maxMessageTime);
			}
		} else if (hitBatchLimit) {
			// Case 1 with batch limit: cap so we don't skip boundary sessions.
			const cap = lastSession.maxMessageTime - 1;
			if (cap > cursor.lastMessageTimeCreated) {
				newCursor = Math.min(newCursor, cap);
			}
		} else {
			// Case 1 without batch limit: advance past all examined sessions.
			newCursor = Math.max(newCursor, lastSession.maxMessageTime);
		}

		newCursor = Math.max(newCursor, cursor.lastMessageTimeCreated);

		await this.serverStateDb.updateDaemonCursor(reader.source, {
			lastMessageTimeCreated: newCursor,
			lastUploadedAt: Date.now(),
		});

		if (uploadedCount > 0) {
			logger.log(
				`[daemon/${reader.source}] Uploaded ${uploadedCount} episodes from ${uploadedSessionIds.size} sessions.`,
			);
		}

		return {
			episodes: uploadedCount,
			sessions: uploadedSessionIds.size,
			cursorAdvanced: newCursor > cursor.lastMessageTimeCreated,
		};
	}

	/**
	 * Run the daemon in polling mode — upload on interval until stopped.
	 *
	 * @param intervalMs  Upload interval in milliseconds (default: 5 minutes).
	 * @param onShutdown  Optional async cleanup callback called before process.exit.
	 *                    Use this to close DB connections and readers gracefully.
	 */
	async runPolling(
		intervalMs = 5 * 60 * 1000,
		onShutdown?: () => Promise<void>,
	): Promise<void> {
		logger.log(
			`[daemon] Starting. Upload interval: ${Math.round(intervalMs / 1000)}s. User: ${this.userId}`,
		);

		let timeoutHandle: ReturnType<typeof setTimeout> | null = null;

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
				// Run immediately if episodes were uploaded OR the cursor advanced
				// (e.g. past ineligible sessions). Only sleep for intervalMs when
				// all sources are genuinely caught up with no new sessions.
				scheduleNext(
					result.episodesUploaded > 0 || result.cursorAdvanced,
				);
			} catch (err) {
				logger.error("[daemon] Upload cycle failed:", err);
				scheduleNext(false);
			}
		};

		// Graceful shutdown: stop the timeout, run caller cleanup, then exit.
		// Uses process.on (not once) so both SIGTERM and SIGINT are always handled.
		// The re-entrancy guard prevents double-cleanup if both signals fire in rapid
		// succession before the async onShutdown resolves (process.once would leave
		// the second signal unhandled, causing a hard exit mid-onShutdown).
		let shuttingDown = false;
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

		// Register shutdown handlers before first run so a SIGTERM during
		// the initial upload cycle is handled cleanly.
		process.on("SIGTERM", () => void cleanup());
		process.on("SIGINT", () => void cleanup());

		// Run immediately on start, then self-schedule via scheduleNext.
		await runCycle();
	}
}
