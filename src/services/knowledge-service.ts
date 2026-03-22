import {
	EmbeddingClient,
	formatEmbeddingText,
} from "../activation/embeddings.js";
import type { IKnowledgeDB } from "../db/interface.js";
import type { KnowledgeEntry } from "../types.js";

/**
 * KnowledgeService — application-layer wrapper around IKnowledgeDB.
 *
 * Adds embedding-aware logic that belongs above the storage layer:
 *
 *   updateEntry(id, updates)
 *     Works identically to IKnowledgeDB.updateEntry for non-semantic fields
 *     (status, strength, confidence, scope, isSynthesized, etc.).
 *     When `content` or `topics` are included in the update, it automatically
 *     re-computes and stores the embedding — no caller needs to remember to do
 *     this manually.
 *
 * The service is intentionally narrow — only `updateEntry` and `close` are
 * exposed. All other DB operations go directly through `IKnowledgeDB`.
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
		private readonly db: IKnowledgeDB,
		embedder?: EmbeddingClient,
	) {
		this.embedder = embedder ?? new EmbeddingClient();
	}

	/**
	 * Update fields on a knowledge entry.
	 *
	 * If `content` or `topics` are present in the update, the embedding is
	 * automatically recomputed using the canonical `formatEmbeddingText` format
	 * before writing to the DB.
	 *
	 * For all other fields the update is passed through to the DB directly.
	 */
	async updateEntry(
		id: string,
		updates: Partial<KnowledgeEntry>,
	): Promise<void> {
		const needsReEmbed =
			updates.content !== undefined || updates.topics !== undefined;

		if (!needsReEmbed) {
			return this.db.updateEntry(id, updates);
		}

		// Fetch the current entry to fill in whichever semantic field isn't changing.
		const current = await this.db.getEntry(id);
		if (!current) {
			throw new Error(`KnowledgeService.updateEntry: entry not found: ${id}`);
		}

		const nextContent = updates.content ?? current.content;
		const nextTopics = updates.topics ?? current.topics;

		const embeddingText = formatEmbeddingText(
			current.type,
			nextContent,
			nextTopics,
		);
		const embedding = await this.embedder.embed(embeddingText);

		return this.db.updateEntry(id, { ...updates, embedding });
	}

	/**
	 * Close the underlying DB connection.
	 */
	close(): Promise<void> {
		return this.db.close();
	}
}
