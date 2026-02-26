import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export const config = {
  // Server
  port: Number.parseInt(process.env.KNOWLEDGE_PORT || "3179", 10),
  host: process.env.KNOWLEDGE_HOST || "127.0.0.1",
  // Optional fixed admin token — set KNOWLEDGE_ADMIN_TOKEN to use a stable token
  // instead of a random one generated at startup. Useful for scripted/automated use.
  // Leave unset in production for better security (random token per process lifetime).
  adminToken: process.env.KNOWLEDGE_ADMIN_TOKEN || null,

  // Database
  dbPath:
    process.env.KNOWLEDGE_DB_PATH ||
    join(homedir(), ".local", "share", "knowledge-server", "knowledge.db"),

  // OpenCode episode source
  opencodeDbPath:
    process.env.OPENCODE_DB_PATH ||
    join(homedir(), ".local", "share", "opencode", "opencode.db"),

  // Unified endpoint — single API key, base URL routes by provider.
  // Set LLM_BASE_ENDPOINT in .env. No default is provided since this is
  // deployment-specific; the server will fail config validation if not set
  // when LLM_API_KEY is present but the endpoint is wrong.
  //
  // Three model slots with independent defaults — tune cost vs quality per task:
  //   extractionModel   — full episode → knowledge extraction (complex reasoning)
  //   mergeModel        — decideMerge near-duplicate comparison (structured, cheap)
  //   contradictionModel — detect + resolve contradictions (nuanced, fires rarely)
  llm: {
    baseEndpoint: process.env.LLM_BASE_ENDPOINT || "",
    apiKey: process.env.LLM_API_KEY || "",
    extractionModel: process.env.LLM_EXTRACTION_MODEL || "anthropic/claude-sonnet-4-6",
    mergeModel: process.env.LLM_MERGE_MODEL || "anthropic/claude-haiku-4-5",
    contradictionModel: process.env.LLM_CONTRADICTION_MODEL || "anthropic/claude-sonnet-4-6",
  },

  // Embedding (always OpenAI-compatible, always through /openai/v1)
  embedding: {
    model: process.env.EMBEDDING_MODEL || "text-embedding-3-large",
    // Only defined when EMBEDDING_DIMENSIONS is explicitly set.
    // Forwarded to the API only when present — the `dimensions` parameter is
    // only valid for text-embedding-3-* models; sending it to other models
    // (ada-002, Ollama, etc.) causes a 400 error.
    dimensions: process.env.EMBEDDING_DIMENSIONS
      ? Number.parseInt(process.env.EMBEDDING_DIMENSIONS, 10)
      : undefined,
  },

  // Decay parameters
  decay: {
    archiveThreshold: Number.parseFloat(
      process.env.DECAY_ARCHIVE_THRESHOLD || "0.15"
    ),
    tombstoneAfterDays: Number.parseInt(
      process.env.DECAY_TOMBSTONE_DAYS || "180", 10
    ),
    // Type-specific decay rates (higher = slower decay)
    typeHalfLife: {
      fact: 30, // facts go stale in ~30 days
      principle: 180, // principles last ~6 months
      pattern: 90, // patterns last ~3 months
      decision: 120, // decisions last ~4 months
      procedure: 365, // procedures are very stable
    } as Record<string, number>,
  },

  // Consolidation
  consolidation: {
    chunkSize: Number.parseInt(process.env.CONSOLIDATION_CHUNK_SIZE || "10", 10),
    maxSessionsPerRun: Number.parseInt(
      process.env.CONSOLIDATION_MAX_SESSIONS || "50", 10
    ),
    minSessionMessages: Number.parseInt(
      process.env.CONSOLIDATION_MIN_MESSAGES || "4", 10
    ),
    // Similarity band for post-extraction contradiction scan.
    // Entries above RECONSOLIDATION_THRESHOLD (0.82) are already handled by decideMerge.
    // Entries below contradictionMinSimilarity are too dissimilar to plausibly contradict.
    // The band in between gets the contradiction LLM call.
    contradictionMinSimilarity: Number.parseFloat(
      process.env.CONTRADICTION_MIN_SIMILARITY || "0.4"
    ),
  },

  // Activation
  activation: {
    // Top-N entries returned per activation call.
    // Default is 10 — a generous ceiling for the MCP tool (deliberate active recall).
    // The passive plugin explicitly overrides this to 5 via ?limit=5 to keep
    // injected context tight. ACTIVATION_MAX_RESULTS overrides the server default.
    maxResults: Number.parseInt(process.env.ACTIVATION_MAX_RESULTS || "10", 10),
    // Minimum strength-weighted cosine similarity to activate an entry.
    // 0.4 is more discriminating than the old 0.3 default — at 0.3, weakly
    // related entries fired too readily. 0.4 cuts noise while keeping
    // genuinely relevant entries (text-embedding-3-large at 0.4 is still a
    // meaningful topical match).
    similarityThreshold: Number.parseFloat(
      process.env.ACTIVATION_SIMILARITY_THRESHOLD || "0.4"
    ),
  },
} as const;

export function validateConfig(): string[] {
  const errors: string[] = [];

  if (!config.llm.apiKey) {
    errors.push("LLM_API_KEY is required. Set it in .env or environment.");
  }

  if (!config.llm.baseEndpoint) {
    errors.push("LLM_BASE_ENDPOINT is required. Set it in .env or environment.");
  }

  const loopbackHosts = ["127.0.0.1", "::1", "localhost"];
  if (!loopbackHosts.includes(config.host)) {
    errors.push(
      `KNOWLEDGE_HOST is set to "${config.host}", which exposes the server on non-loopback interfaces with no authentication. Only use 127.0.0.1 unless you have added authentication and understand the security implications.`
    );
  }

  if (!existsSync(config.opencodeDbPath)) {
    errors.push(
      `OpenCode database not found at ${config.opencodeDbPath}. Set OPENCODE_DB_PATH if it's elsewhere.`
    );
  }

  return errors;
}
