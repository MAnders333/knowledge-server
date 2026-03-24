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
		sources: Array<{ source: string; episodes: number; sessions: number }>;
	}> {
		let totalEpisodes = 0;
		let totalSessions = 0;
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
			sources,
		};
	}

	private async uploadSource(
		reader: IEpisodeReader,
	): Promise<{ episodes: number; sessions: number }> {
		// Daemon cursor lives in DaemonDB (always local SQLite).
		const cursor = await this.daemonDb.getDaemonCursor(reader.source);

		const candidateSessions = reader.getCandidateSessions(
			cursor.lastMessageTimeCreated,
			config.consolidation.maxSessionsPerRun,
		);

		if (candidateSessions.length === 0) {
			return { episodes: 0, sessions: 0 };
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
			return { episodes: 0, sessions: 0 };
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

		// Advance daemon cursor — mirrors consolidation engine's boundary-safety logic.
		// Uses lastSuccessMaxTime (not all episodes) so a failed insert doesn't
		// silently advance the cursor past episodes that were never uploaded.
		const lastSession = candidateSessions[candidateSessions.length - 1];
		const hitBatchLimit =
			candidateSessions.length === config.consolidation.maxSessionsPerRun;

		const maxTime = lastSuccessMaxTime;

		let newCursor = maxTime;
		if (hitBatchLimit) {
			const cap = lastSession.maxMessageTime - 1;
			if (cap > cursor.lastMessageTimeCreated) {
				newCursor = Math.min(newCursor, cap);
			}
		} else {
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

		// Run immediately on start
		await this.upload();

		const interval = setInterval(async () => {
			await this.upload().catch((err) => {
				logger.error("[daemon] Upload cycle failed:", err);
			});
		}, intervalMs);

		// Graceful shutdown: stop the interval, run caller cleanup, then exit.
		// Uses process.on (not once) so both SIGTERM and SIGINT are always handled.
		// The re-entrancy guard prevents double-cleanup if both signals fire in rapid
		// succession before the async onShutdown resolves (process.once would leave
		// the second signal unhandled, causing a hard exit mid-onShutdown).
		let shuttingDown = false;
		const cleanup = async () => {
			if (shuttingDown) return;
			shuttingDown = true;
			clearInterval(interval);
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
	}
}
