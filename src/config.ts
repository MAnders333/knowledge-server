import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export const config = {
  // Server
  port: Number.parseInt(process.env.KNOWLEDGE_PORT || "3179"),
  host: process.env.KNOWLEDGE_HOST || "127.0.0.1",

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
  llm: {
    baseEndpoint: process.env.LLM_BASE_ENDPOINT || "",
    apiKey: process.env.LLM_API_KEY || "",
    model: process.env.LLM_MODEL || "anthropic/claude-sonnet-4-6",
  },

  // Embedding (always OpenAI-compatible, always through /openai/v1)
  embedding: {
    model: process.env.EMBEDDING_MODEL || "text-embedding-3-large",
    dimensions: Number.parseInt(process.env.EMBEDDING_DIMENSIONS || "3072"),
  },

  // Decay parameters
  decay: {
    archiveThreshold: Number.parseFloat(
      process.env.DECAY_ARCHIVE_THRESHOLD || "0.15"
    ),
    tombstoneAfterDays: Number.parseInt(
      process.env.DECAY_TOMBSTONE_DAYS || "180"
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
    chunkSize: Number.parseInt(process.env.CONSOLIDATION_CHUNK_SIZE || "10"),
    maxSessionsPerRun: Number.parseInt(
      process.env.CONSOLIDATION_MAX_SESSIONS || "50"
    ),
    minSessionMessages: Number.parseInt(
      process.env.CONSOLIDATION_MIN_MESSAGES || "4"
    ),
  },

  // Activation
  activation: {
    maxResults: Number.parseInt(process.env.ACTIVATION_MAX_RESULTS || "10"),
    similarityThreshold: Number.parseFloat(
      process.env.ACTIVATION_SIMILARITY_THRESHOLD || "0.3"
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

  if (!existsSync(config.opencodeDbPath)) {
    errors.push(
      `OpenCode database not found at ${config.opencodeDbPath}. Set OPENCODE_DB_PATH if it's elsewhere.`
    );
  }

  return errors;
}
