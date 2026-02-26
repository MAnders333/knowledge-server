import { config } from "../config.js";
import type { KnowledgeType } from "../types.js";

/**
 * Embedding client.
 *
 * Embeddings always use the OpenAI-compatible API format
 * (the unified endpoint exposes embeddings via /openai/v1 regardless of model vendor).
 */
export class EmbeddingClient {
  private endpoint: string;
  private apiKey: string;
  private model: string;
  private dimensions: number | undefined;

  constructor() {
    // Embeddings always go through the OpenAI-compatible path
    this.endpoint = `${config.llm.baseEndpoint}/openai/v1`;
    this.apiKey = config.llm.apiKey;
    this.model = config.embedding.model;
    this.dimensions = config.embedding.dimensions;
  }

  /**
   * Generate an embedding for a single text.
   */
  async embed(text: string): Promise<number[]> {
    const results = await this.embedBatch([text]);
    return results[0];
  }

  /**
   * Generate embeddings for multiple texts in a single API call.
   * The API may have limits on batch size — we chunk at 100.
   */
  async embedBatch(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) return [];

    const results: number[][] = [];
    const chunkSize = 100;

    for (let i = 0; i < texts.length; i += chunkSize) {
      const chunk = texts.slice(i, i + chunkSize);
      const response = await fetch(`${this.endpoint}/embeddings`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.apiKey}`,
        },
        body: JSON.stringify({
          model: this.model,
          input: chunk,
          // Only sent when explicitly configured — `dimensions` is only valid for
          // text-embedding-3-* models; omitting it lets the model use its default.
          ...(this.dimensions !== undefined && { dimensions: this.dimensions }),
        }),
      });

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`Embedding API error ${response.status}: ${body}`);
      }

      const data = (await response.json()) as {
        data: Array<{ embedding: number[]; index: number }>;
      };

      // Sort by index to maintain order
      const sorted = data.data.sort((a, b) => a.index - b.index);
      results.push(...sorted.map((d) => d.embedding));
    }

    return results;
  }
}

/**
 * Format an entry's fields into the canonical text string used for embedding.
 *
 * Single source of truth — used by both:
 * - `reconsolidate()` when embedding immediately after a merge
 * - `ensureEmbeddings()` when regenerating embeddings for entries missing one
 *
 * Keeping both paths in sync here prevents silent vector drift between a
 * freshly-merged entry and the same entry re-embedded on a later run.
 */
export function formatEmbeddingText(type: KnowledgeType, content: string, topics: string[]): string {
  return `[${type}] ${content} (topics: ${topics.join(", ")})`;
}

/**
 * Compute cosine similarity between two vectors.
 * Returns a value between -1 and 1 (1 = identical, 0 = orthogonal, -1 = opposite).
 */
export function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(
      `Vector dimension mismatch: ${a.length} vs ${b.length}`
    );
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  if (denominator === 0) return 0;

  return dotProduct / denominator;
}
