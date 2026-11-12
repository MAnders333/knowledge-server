import {
	type IServerStateDB,
	PENDING_EPISODE_MAX_ATTEMPTS,
} from "../../db/interface.js";
import { logger } from "../../logger.js";
import type {
	Episode,
	EpisodeKey,
	EpisodeOutcome,
	IEpisodeReader,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";
import { episodeKeyId } from "../../types.js";

/**
 * PendingEpisodesReader — drains the `pending_episodes` staging table.
 *
 * The daemon writes episodes here; the consolidation engine drains them via
 * this single reader without ever touching local AI tool session files.
 *
 * A single instance covers all sources — source filtering is unnecessary
 * because pending_episodes is self-draining (rows deleted after consolidation)
 * and consolidated_episode already keys idempotency by (source, session_id, …).
 *
 * user_id is treated as provenance metadata only: all pending episodes are
 * drained regardless of which daemon wrote them.
 */
export class PendingEpisodesReader implements IEpisodeReader {
	readonly source = "pending";

	private readonly db: IServerStateDB;

	constructor(db: IServerStateDB) {
		this.db = db;
	}

	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit = 200,
	): Array<{ id: string; maxMessageTime: number }> {
		// Synchronous — data is pre-loaded by prepare() before this is called.
		return this._cachedCandidates
			.filter((s) => s.maxMessageTime > afterMessageTimeCreated)
			.slice(0, limit);
	}

	countNewSessions(afterMessageTimeCreated: number): number {
		// Before prepare() runs, return 1 as a conservative signal so checkPending()
		// doesn't skip consolidation. _prepared is reset at the start of each prepare()
		// so a DB error doesn't leave us stuck in a "prepared but empty" state.
		if (!this._prepared) return 1;
		return this._cachedCandidates.filter(
			(s) => s.maxMessageTime > afterMessageTimeCreated,
		).length;
	}

	/**
	 * Pre-load all pending episodes from the staging table.
	 * Called by ConsolidationEngine before getCandidateSessions.
	 */
	async prepare(
		afterMessageTimeCreated: number,
		isBlocked?: (episode: EpisodeKey & { directory: string }) => boolean,
	): Promise<void> {
		this._cachedRows = [];
		this._cachedCandidates = [];
		this._prepared = false;

		const rows = (
			await this.db.getPendingEpisodes(afterMessageTimeCreated, 2000)
		).filter((row) => !isBlocked?.(row));

		// Group by session_id, track max_message_time per session.
		const sessionMap = new Map<string, number>();
		for (const row of rows) {
			const existing = sessionMap.get(row.sessionId) ?? 0;
			if (row.maxMessageTime > existing) {
				sessionMap.set(row.sessionId, row.maxMessageTime);
			}
		}
		this._cachedRows = rows;
		this._cachedCandidates = Array.from(sessionMap.entries()).map(
			([id, maxMessageTime]) => ({ id, maxMessageTime }),
		);
		this._prepared = true;
	}

	getNewEpisodes(
		candidateSessionIds: string[],
		processedRanges: Map<string, ProcessedRange[]>,
	): Episode[] {
		const sessionIdSet = new Set(candidateSessionIds);
		const result: Episode[] = [];

		for (const row of this._cachedRows) {
			if (!sessionIdSet.has(row.sessionId)) continue;

			// Skip ranges already consolidated — matched by (source, start, end) so
			// episodes from different tools with the same session ID don't suppress each other.
			const ranges = processedRanges.get(row.sessionId);
			if (
				ranges?.some(
					(r) =>
						r.source === row.source &&
						r.startMessageId === row.startMessageId &&
						r.endMessageId === row.endMessageId,
				)
			)
				continue;

			result.push({
				source: row.source,
				sessionId: row.sessionId,
				startMessageId: row.startMessageId,
				endMessageId: row.endMessageId,
				sessionTitle: row.sessionTitle,
				projectName: row.projectName,
				directory: row.directory,
				timeCreated: row.timeCreated,
				maxMessageTime: row.maxMessageTime,
				content: row.content,
				contentType: row.contentType,
				approxTokens: row.approxTokens,
			});
		}

		return result;
	}

	/**
	 * Settle pending rows after a consolidation batch (see EpisodeOutcome):
	 * - completed episodes: rows are deleted.
	 * - failed episodes: rows are kept and failed_attempts is incremented. A row
	 *   that reaches PENDING_EPISODE_MAX_ATTEMPTS is dead-lettered (logged loudly
	 *   and excluded from future reads).
	 * Rows in neither list (lock held, store unavailable) are left untouched so
	 * they are retried on the next run.
	 */
	async afterConsolidated(outcome: EpisodeOutcome): Promise<void> {
		const rowsByKey = new Map<string, PendingEpisode[]>();
		for (const row of this._cachedRows) {
			const key = episodeKeyId(row);
			const bucket = rowsByKey.get(key);
			if (bucket) bucket.push(row);
			else rowsByKey.set(key, [row]);
		}

		const completedIds = outcome.completed.flatMap(
			(key) => rowsByKey.get(episodeKeyId(key))?.map((row) => row.id) ?? [],
		);

		const failedIdsByError = new Map<string, string[]>();
		for (const { episode, error } of outcome.failed) {
			for (const row of rowsByKey.get(episodeKeyId(episode)) ?? []) {
				const ids = failedIdsByError.get(error) ?? [];
				ids.push(row.id);
				failedIdsByError.set(error, ids);
				if ((row.failedAttempts ?? 0) + 1 >= PENDING_EPISODE_MAX_ATTEMPTS) {
					logger.error(
						`[consolidation/pending] Dead-lettering episode ${episode.source}/${episode.sessionId} ` +
							`[${episode.startMessageId}..${episode.endMessageId}] after ${PENDING_EPISODE_MAX_ATTEMPTS} failed attempts. ` +
							`It will not be retried; the row is kept in pending_episodes for inspection. Last error: ${error}`,
					);
				}
			}
		}

		await this.db.deletePendingEpisodes(completedIds);
		for (const [error, ids] of failedIdsByError) {
			await this.db.recordPendingEpisodeFailures(ids, error.slice(0, 1000));
		}

		const settled = new Set([
			...completedIds,
			...[...failedIdsByError.values()].flat(),
		]);
		this._cachedRows = this._cachedRows.filter((r) => !settled.has(r.id));
	}

	close(): void {
		// No resources to release — DB is owned by the caller.
	}

	private _prepared = false;
	private _cachedRows: PendingEpisode[] = [];
	private _cachedCandidates: Array<{ id: string; maxMessageTime: number }> = [];
}
