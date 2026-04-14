import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import type { DaemonDB } from "../db/daemon/index.js";
import type { IServerStateDB } from "../db/interface.js";
import { logger } from "../logger.js";
import type { Episode, IEpisodeReader, PendingEpisode } from "../types.js";

/**
 * EpisodeUploader — the daemon's core upload loop.
 *
 * For each configured episode reader (OpenCode, Claude Code, Cursor, etc.):
 *   1. Reads the daemon cursor (from DaemonDB, always local) to find the
 *      high-water mark for this source
 *   2. Calls getCandidateSessions to find new sessions
 *   3. Calls getNewEpisodes to get unprocessed episode content
 *   4. Writes each episode to pending_episodes via IServerStateDB (local SQLite
 *      or remote Postgres, depending on configuration)
 *   5. Advances the daemon cursor
 *
 * The separation of DaemonDB (cursor, always local) from IServerStateDB
 * (pending_episodes, configurable) enables a fully remote consolidation server:
 * the daemon writes episodes to a shared Postgres staging DB while keeping
 * its own cursor in a local SQLite file.
 */
export class EpisodeUploader {
	private readonly readers: IEpisodeReader[];
	private readonly serverStateDb: IServerStateDB;
	private readonly daemonDb: DaemonDB;
	private readonly userId: string;

	/**
	 * @param readers       Episode readers — one per AI tool source.
	 * @param serverStateDb The staging DB — holds pending_episodes.
	 *                      Can be local SQLite or remote Postgres.
	 * @param daemonDb      The daemon-local DB — holds daemon_cursor.
	 *                      Always local SQLite, never remote.
	 * @param userId        Stable user identifier (KNOWLEDGE_USER_ID or hostname).
	 */
	constructor(
		readers: IEpisodeReader[],
		serverStateDb: IServerStateDB,
		daemonDb: DaemonDB,
		userId: string,
	) {
		this.readers = readers;
		this.serverStateDb = serverStateDb;
		this.daemonDb = daemonDb;
		this.userId = userId;
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
		sources: Array<{ source: string; episodes: number; sessions: number }>;
	}> {
		// Sources are independent — upload all concurrently.
		const settled = await Promise.allSettled(
			this.readers.map((reader) => this.uploadSource(reader)),
		);

		let totalEpisodes = 0;
		let totalSessions = 0;
		let anyCursorAdvanced = false;
		const sources: Array<{
			source: string;
			episodes: number;
			sessions: number;
		}> = [];

		for (let i = 0; i < settled.length; i++) {
			const result = settled[i];
			const reader = this.readers[i];
			if (result.status === "fulfilled") {
				totalEpisodes += result.value.episodes;
				totalSessions += result.value.sessions;
				if (result.value.cursorAdvanced) anyCursorAdvanced = true;
				if (result.value.episodes > 0 || result.value.sessions > 0) {
					sources.push({
						source: reader.source,
						episodes: result.value.episodes,
						sessions: result.value.sessions,
					});
				}
			} else {
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
		// Daemon cursor lives in DaemonDB (always local SQLite).
		const cursor = await this.daemonDb.getDaemonCursor(reader.source);

		const candidateSessions = reader.getCandidateSessions(
			cursor.lastMessageTimeCreated,
			config.consolidation.maxSessionsPerRun,
		);

		if (candidateSessions.length === 0) {
			return { episodes: 0, sessions: 0, cursorAdvanced: false };
		}

		const candidateIds = candidateSessions.map((s) => s.id);

		// getUploadedEpisodeRanges returns both consolidated and pending (staged)
		// episode ranges, so the reader's overlap check covers both cases and
		// the daemon won't re-upload episodes already in flight.
		const processedRanges =
			await this.serverStateDb.getUploadedEpisodeRanges(candidateIds);

		let newEpisodes: Episode[];
		try {
			newEpisodes = reader.getNewEpisodes(candidateIds, processedRanges);
		} catch (err) {
			logger.error(`[daemon/${reader.source}] getNewEpisodes failed:`, err);
			return { episodes: 0, sessions: 0, cursorAdvanced: false };
		}

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
		// Three cases:
		//
		// 1. Episodes uploaded successfully → advance to lastSuccessMaxTime.
		//    If the batch limit was hit, cap at (lastSession.maxMessageTime - 1)
		//    to avoid skipping sessions that share the boundary timestamp.
		//
		// 2. No episodes produced AND no insert failures (all sessions examined
		//    but ineligible — e.g. too few messages) → advance past all examined
		//    sessions. Without this, the cursor never moves when facing a block of
		//    ineligible sessions and the daemon re-fetches the same batch forever
		//    (starvation bug). Fix contributed by Dennis Paul (PR #68).
		//
		// 3. Insert failure before any episodes uploaded → keep cursor at old value
		//    so the failed episode is retried on the next daemon run.
		const lastSession = candidateSessions[candidateSessions.length - 1];
		const hitBatchLimit =
			candidateSessions.length === config.consolidation.maxSessionsPerRun;

		// True when the upload loop ran to completion without any insert failure.
		// (0 === 0 when newEpisodes is empty — intentional; it means all sessions
		// were examined and none produced episodes, not that we failed mid-loop.)
		const allExamined = uploadedCount === newEpisodes.length;

		let newCursor = lastSuccessMaxTime;

		if (!allExamined) {
			// Case 3: insert failure before all episodes were processed.
			// Keep cursor at lastSuccessMaxTime (= old cursor when nothing uploaded)
			// so the failed episode is retried on the next daemon run.
			// newCursor is already set to lastSuccessMaxTime — nothing to do.
		} else if (uploadedCount === 0) {
			// Case 2: all sessions examined, none produced episodes (e.g. all below
			// minSessionMessages). Advance past the entire batch so the daemon does
			// not re-fetch the same ineligible sessions on the next cycle (starvation
			// fix — contributed by Dennis Paul, PR #68).
			if (hitBatchLimit) {
				// Cap at lastSession - 1 to avoid skipping sessions that share
				// the boundary timestamp with the next batch's first session.
				newCursor = Math.max(newCursor, lastSession.maxMessageTime - 1);
			} else {
				newCursor = Math.max(newCursor, lastSession.maxMessageTime);
			}
		} else if (hitBatchLimit) {
			// Case 1 with batch limit: cap to avoid skipping boundary sessions.
			const cap = lastSession.maxMessageTime - 1;
			if (cap > cursor.lastMessageTimeCreated) {
				newCursor = Math.min(newCursor, cap);
			}
		} else {
			// Case 1 without batch limit: advance past all examined sessions.
			newCursor = Math.max(newCursor, lastSession.maxMessageTime);
		}

		newCursor = Math.max(newCursor, cursor.lastMessageTimeCreated);

		await this.daemonDb.updateDaemonCursor(reader.source, {
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
	 * @param intervalMs  Upload interval in milliseconds (default: 30 minutes).
	 * @param onShutdown  Optional async cleanup callback called before process.exit.
	 *                    Use this to close DB connections and readers gracefully.
	 */
	async runPolling(
		intervalMs = 30 * 60 * 1000,
		onShutdown?: () => Promise<void>,
	): Promise<void> {
		logger.log(
			`[daemon] Starting. Upload interval: ${Math.round(intervalMs / 1000)}s. User: ${this.userId}`,
		);

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
				// Re-run immediately when the cursor advanced — either episodes were
				// uploaded or ineligible sessions were skipped past. Only sleep for
				// intervalMs when all sources are genuinely caught up with no new or
				// skippable sessions.
				scheduleNext(result.episodesUploaded > 0 || result.cursorAdvanced);
			} catch (err) {
				logger.error("[daemon] Upload cycle failed:", err);
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

		// Run immediately on start, then self-schedule via scheduleNext.
		await runCycle();
	}
}
