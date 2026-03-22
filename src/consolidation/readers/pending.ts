import type { IKnowledgeDB } from "../../db/interface.js";
import type {
	Episode,
	IEpisodeReader,
	PendingEpisode,
	ProcessedRange,
} from "../../types.js";

/**
 * PendingEpisodesReader — reads episodes from the `pending_episodes` staging table.
 *
 * This reader enables the daemon architecture: episodes are uploaded by the
 * knowledge-daemon process (running on the user's machine) into `pending_episodes`,
 * and the consolidation engine drains them via this reader without ever reading
 * local AI tool session files directly.
 *
 * The source name uses the pattern "pending:<original-source>" so the consolidation
 * engine tracks cursor and processed-episode state per original source, allowing
 * smooth migration from direct file readers to daemon-uploaded episodes.
 *
 * After successful consolidation, processed rows are deleted from `pending_episodes`
 * to keep the staging table small.
 */
export class PendingEpisodesReader implements IEpisodeReader {
	readonly source: string;
	private readonly db: IKnowledgeDB;
	private readonly userId: string;
	private readonly originalSource: string;

	/**
	 * @param originalSource  The source name as set by the daemon (e.g. "opencode").
	 * @param userId          User ID to filter pending episodes by.
	 * @param db              The knowledge DB (shared Postgres or local SQLite).
	 */
	constructor(originalSource: string, userId: string, db: IKnowledgeDB) {
		this.originalSource = originalSource;
		this.userId = userId;
		this.db = db;
		// Prefix with "pending:" so this reader's cursor is independent from any
		// legacy direct-file reader cursor for the same source.
		this.source = `pending:${originalSource}`;
	}

	/**
	 * Return candidate sessions from pending_episodes newer than the cursor.
	 * Groups by session_id, returns maxMessageTime per session.
	 */
	getCandidateSessions(
		afterMessageTimeCreated: number,
		limit = 200,
	): Array<{ id: string; maxMessageTime: number }> {
		// This is synchronous in the IEpisodeReader interface but we need async DB
		// access. We use a sync-over-async pattern by pre-loading in getNewEpisodes.
		// For getCandidateSessions/countNewSessions we return from the cached result.
		// The actual fetch happens lazily — safe because consolidateSource always
		// calls getCandidateSessions then getNewEpisodes in sequence.
		return this._cachedCandidates
			.filter((s) => s.maxMessageTime > afterMessageTimeCreated)
			.slice(0, limit);
	}

	countNewSessions(afterMessageTimeCreated: number): number {
		// If prepare() hasn't been called yet, return 1 as a conservative
		// "there may be pending episodes" signal so checkPending() doesn't skip
		// consolidation. The real count is determined after prepare() runs inside
		// consolidateSource. We use an explicit _prepared flag (not _cachedCandidates.length)
		// so that a genuine empty result after prepare() correctly returns 0.
		if (!this._prepared) return 1;
		return this._cachedCandidates.filter(
			(s) => s.maxMessageTime > afterMessageTimeCreated,
		).length;
	}

	/**
	 * Pre-load candidate sessions from pending_episodes.
	 * Called by ConsolidationEngine before getCandidateSessions (optional hook).
	 */
	async prepare(afterMessageTimeCreated: number): Promise<void> {
		// Reset cache state before the async fetch so stale data from the previous
		// run doesn't persist. _prepared stays false until the fetch completes so
		// a DB error doesn't leave the reader in a "prepared but empty" state that
		// would suppress the conservative return-1 in countNewSessions.
		this._cachedRows = [];
		this._cachedCandidates = [];
		this._prepared = false;
		const rows = await this.db.getPendingEpisodes(
			this.originalSource,
			this.userId,
			afterMessageTimeCreated,
			2000,
		);
		// Group by session_id, track max_message_time per session
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
		// Set _prepared only after successful cache population.
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

			// Skip if this (start, end) range was already consolidated
			const ranges = processedRanges.get(row.sessionId);
			if (ranges) {
				const alreadyDone = ranges.some(
					(r) =>
						r.startMessageId === row.startMessageId &&
						r.endMessageId === row.endMessageId,
				);
				if (alreadyDone) continue;
			}

			result.push({
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
	 * Delete consolidated pending_episodes rows to keep the staging table lean.
	 * Called by ConsolidationEngine after cursor is advanced (afterConsolidated hook).
	 */
	async afterConsolidated(sessionIds: string[]): Promise<void> {
		const sessionIdSet = new Set(sessionIds);
		const idsToDelete = this._cachedRows
			.filter((r) => sessionIdSet.has(r.sessionId))
			.map((r) => r.id);
		await this.db.deletePendingEpisodes(idsToDelete);
		// Remove from cache so they don't reappear in the same run
		this._cachedRows = this._cachedRows.filter(
			(r) => !sessionIdSet.has(r.sessionId),
		);
	}

	close(): void {
		// No resources to release — DB is owned by the caller
	}

	private _prepared = false;
	private _cachedRows: PendingEpisode[] = [];
	private _cachedCandidates: Array<{ id: string; maxMessageTime: number }> = [];
}
