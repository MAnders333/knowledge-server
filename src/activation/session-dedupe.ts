/**
 * Per-session dedupe of injected entries, in server memory.
 *
 * A client that sends a session id (hook `session_id`, `x-knowledge-session`
 * header) gets entries it already received in that session suppressed, so the
 * same block is not re-injected on every prompt. The key is client-scoped by the
 * caller. Requests without a session id bypass the dedupe entirely.
 *
 * An entry stays suppressed for `ttlMs` after its last injection, unless it now
 * scores at least `rescoreMargin` higher on raw cosine than it did then. The TTL
 * is measured from the injection, not from the last suppressed prompt, because
 * the injected copy stays in the agent's context until compaction. `ttlMs` of 0
 * disables suppression.
 *
 * Failure modes (accepted):
 * - Compaction removes earlier injections from the context, and no compaction
 *   signal reaches the server, so the TTL is the only reset. Needed entries can
 *   be suppressed for up to `ttlMs` after a compaction.
 * - A restart clears the map, so each entry can repeat once.
 * - Memory is bounded: at most `maxSessions` sessions (LRU), and expired entries
 *   are pruned when a session is written.
 */

interface Injected {
	/** Epoch ms of the injection. */
	at: number;
	/** Raw cosine at the time of the injection. */
	rawSimilarity: number;
}

interface SessionDedupeOptions {
	ttlMs: number;
	maxSessions: number;
	/** Raw-cosine increase that makes a suppressed entry worth re-injecting. */
	rescoreMargin: number;
}

export class SessionDedupe {
	/** Insertion order is recency: the first key is the least recently used. */
	private readonly sessions = new Map<string, Map<string, Injected>>();

	constructor(private readonly opts: SessionDedupeOptions) {}

	/** True when `entry` was injected in this session within the TTL and is not much better now. */
	isSuppressed(
		key: string,
		entry: { id: string; rawSimilarity: number },
		now: number,
	): boolean {
		if (this.opts.ttlMs <= 0) return false;
		const session = this.touch(key);
		const prior = session?.get(entry.id);
		if (!session || !prior) return false;
		if (now - prior.at > this.opts.ttlMs) {
			session.delete(entry.id);
			return false;
		}
		return entry.rawSimilarity < prior.rawSimilarity + this.opts.rescoreMargin;
	}

	/** Record entries that were actually injected. Expired entries in the session are pruned. */
	remember(
		key: string,
		injected: ReadonlyArray<{ id: string; rawSimilarity: number }>,
		now: number,
	): void {
		if (injected.length === 0 || this.opts.ttlMs <= 0) return;
		let session = this.touch(key);
		if (!session) {
			session = new Map();
			this.sessions.set(key, session);
			while (this.sessions.size > this.opts.maxSessions) {
				const oldest = this.sessions.keys().next().value;
				if (oldest === undefined) break;
				this.sessions.delete(oldest);
			}
		}
		for (const [id, prior] of session) {
			if (now - prior.at > this.opts.ttlMs) session.delete(id);
		}
		for (const entry of injected) {
			session.set(entry.id, { at: now, rawSimilarity: entry.rawSimilarity });
		}
	}

	/** Number of sessions held. For tests and status. */
	get size(): number {
		return this.sessions.size;
	}

	/** Returns the session map and moves it to the most-recently-used position. */
	private touch(key: string): Map<string, Injected> | undefined {
		const session = this.sessions.get(key);
		if (session) {
			this.sessions.delete(key);
			this.sessions.set(key, session);
		}
		return session;
	}
}
