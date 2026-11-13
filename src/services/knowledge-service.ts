import {
	EmbeddingClient,
	formatEmbeddingText,
} from "../activation/embeddings.js";
import { config } from "../config.js";
import type { IKnowledgeStore } from "../db/interface.js";
import type { KnowledgeEntry } from "../types.js";

/**
 * KnowledgeService — application-layer wrapper around IKnowledgeStore.
 *
 * Adds embedding-aware logic that belongs above the storage layer:
 *
 *   updateEntry(id, updates)
 *     Works identically to IKnowledgeStore.updateEntry for non-semantic fields
 *     (strength, confidence, isSynthesized, etc.).
 *     When `content` or `topics` are included in the update, it automatically
 *     re-computes and stores the embedding — no caller needs to remember to do
 *     this manually.
 *     When `status` moves an entry to active or conflicted, its vector must be
 *     current: an entry that was archived, superseded or tombstoned may carry a
 *     vector from an older model. If it has no embedding, or the store's
 *     recorded model is not the configured one, the embedding is regenerated
 *     before the entry becomes visible to similarity search.
 *
 * The service is intentionally narrow — only `updateEntry` and `close` are
 * exposed. All other DB operations go directly through `IKnowledgeStore`.
 *
 * Backward compatibility: no schema changes, no migrations. Users who update
 * the binary automatically get the safer update path.
 */
export class KnowledgeService {
	private embedder: EmbeddingClient;

	/**
	 * @param db      - The knowledge DB instance to wrap.
	 * @param embedder - Optional EmbeddingClient to use. Defaults to a new instance.
	 *                   Pass an existing client (e.g. from ActivationEngine) to share
	 *                   the same instance and allow test spying.
	 */
	constructor(
		private readonly db: IKnowledgeStore,
		embedder?: EmbeddingClient,
	) {
		this.embedder = embedder ?? new EmbeddingClient();
	}

	/**
	 * Update fields on a knowledge entry.
	 *
	 * If `content` or `topics` are present in the update, the embedding is
	 * automatically recomputed using the canonical `formatEmbeddingText` format
	 * before writing to the DB. If `status` becomes active or conflicted and the
	 * stored vector is missing or not from the configured model, it is
	 * recomputed the same way.
	 *
	 * For all other fields the update is passed through to the DB directly.
	 */
	async updateEntry(
		id: string,
		updates: Partial<KnowledgeEntry>,
	): Promise<void> {
		const needsReEmbed =
			updates.content !== undefined || updates.topics !== undefined;
		const revives =
			updates.status === "active" || updates.status === "conflicted";

		if (!needsReEmbed && !revives) {
			return this.db.updateEntry(id, updates);
		}

		// Fetch the current entry to fill in whichever semantic field isn't changing.
		const current = await this.db.getEntryWithEmbedding(id);
		if (!current) {
			throw new Error(`KnowledgeService.updateEntry: entry not found: ${id}`);
		}

		if (needsReEmbed) {
			const nextContent = updates.content ?? current.content;
			const nextTopics = updates.topics ?? current.topics;
			const embedding = await this.embed(current.type, nextContent, nextTopics);
			return this.db.updateEntry(id, { ...updates, embedding });
		}

		if (await this.needsFreshEmbedding(current)) {
			const embedding = await this.embed(
				current.type,
				current.content,
				current.topics,
			);
			return this.db.updateEntry(id, { ...updates, embedding });
		}

		return this.db.updateEntry(id, updates);
	}

	/**
	 * True when an entry's stored vector cannot be trusted for search: it is
	 * missing, or the store's embedding metadata does not record the configured
	 * model (no metadata, or a different model, means the store's vectors are
	 * from another model).
	 */
	private async needsFreshEmbedding(entry: KnowledgeEntry): Promise<boolean> {
		if (!entry.embedding) return true;
		const meta = await this.db.getEmbeddingMetadata();
		return !meta || meta.model !== config.embedding.model;
	}

	private embed(
		type: KnowledgeEntry["type"],
		content: string,
		topics: string[],
	): Promise<number[]> {
		return this.embedder.embed(formatEmbeddingText(type, content, topics));
	}

	/**
	 * Close the underlying DB connection.
	 */
	close(): Promise<void> {
		return this.db.close();
	}
}
