import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { RECONSOLIDATION_THRESHOLD } from "./types.js";

/**
 * Parse an integer environment variable with a fallback default and optional
 * minimum clamp. Returns `defaultVal` when the variable is absent, empty, or
 * not a valid integer. NaN-safe: a `Number.isNaN` guard ensures NaN never reaches
 * `Math.max` — an invalid string always yields `defaultVal`, never NaN.
 */
function parseIntEnv(envVar: string | undefined, defaultVal: number, min?: number): number {
  const parsed = Number.parseInt(envVar ?? "", 10);
  const value = Number.isNaN(parsed) ? defaultVal : parsed;
  return min !== undefined ? Math.max(min, value) : value;
}

/**
 * Parse a float environment variable with a fallback default and optional
 * minimum clamp. Returns `defaultVal` when the variable is absent, empty, or
 * not a valid number. NaN-safe: a `Number.isNaN` guard ensures NaN never reaches
 * `Math.max` — an invalid string always yields `defaultVal`, never NaN.
 */
function parseFloatEnv(envVar: string | undefined, defaultVal: number, min?: number): number {
  const parsed = Number.parseFloat(envVar ?? "");
  const value = Number.isNaN(parsed) ? defaultVal : parsed;
  return min !== undefined ? Math.max(min, value) : value;
}

export const config = {
  // Server
  port: parseIntEnv(process.env.KNOWLEDGE_PORT, 3179, 1),
  host: process.env.KNOWLEDGE_HOST || "127.0.0.1",
  // Optional fixed admin token — set KNOWLEDGE_ADMIN_TOKEN to use a stable token
  // instead of a random one generated at startup. Useful for scripted/automated use.
  // Leave unset in production for better security (random token per process lifetime).
  adminToken: process.env.KNOWLEDGE_ADMIN_TOKEN || null,

  // Database
  dbPath:
    process.env.KNOWLEDGE_DB_PATH ||
    join(homedir(), ".local", "share", "knowledge-server", "knowledge.db"),

  // Log file — all operational output is tee'd here in addition to stdout.
  // Set KNOWLEDGE_LOG_PATH to override; set to "" to disable file logging.
  // Uses ?? (not ||) so that KNOWLEDGE_LOG_PATH="" is respected as an explicit
  // "disable" signal — || would treat "" as falsy and fall back to the default path.
  logPath:
    process.env.KNOWLEDGE_LOG_PATH ??
    join(homedir(), ".local", "share", "knowledge-server", "server.log"),

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
    // Per-call timeout in milliseconds. Applied per attempt (not across all retries).
    // Default: 5 minutes. Large contradiction batches (50+ candidates) can take
    // 2–3 minutes for a complex Sonnet response; 5 minutes gives headroom while
    // still bounding a true hang (network stall, rate-limit loop, etc.).
    // Minimum 1 ms enforced — set a high value rather than 0 to effectively disable.
    timeoutMs: parseIntEnv(process.env.LLM_TIMEOUT_MS, 5 * 60 * 1000, 1),
    // Per-call retry budget. On timeout or transient error, complete() retries up
    // to this many additional times before throwing to the caller.
    // Retries use exponential backoff starting at retryBaseDelayMs (capped at 60s).
    // Set to 0 to disable retries entirely.
    maxRetries: parseIntEnv(process.env.LLM_MAX_RETRIES, 2, 0),
    retryBaseDelayMs: parseIntEnv(process.env.LLM_RETRY_BASE_DELAY_MS, 3000, 0),
  },

  // Embedding (always OpenAI-compatible, always through /openai/v1)
  embedding: {
    model: process.env.EMBEDDING_MODEL || "text-embedding-3-large",
    // Only defined when EMBEDDING_DIMENSIONS is explicitly set.
    // Forwarded to the API only when present — the `dimensions` parameter is
    // only valid for text-embedding-3-* models; sending it to other models
    // (ada-002, Ollama, etc.) causes a 400 error.
    // Validated in validateConfig() — a non-positive integer is rejected at startup
    // rather than silently forwarded to the API causing a confusing 400.
    // Note: "0" is intentionally falsy here so the ternary gives undefined for that
    // case too; validateConfig() checks the raw env var and catches it as an error.
    dimensions: process.env.EMBEDDING_DIMENSIONS
      ? parseIntEnv(process.env.EMBEDDING_DIMENSIONS, 1, 1)
      : undefined,
  },

  // Decay parameters
  decay: {
    archiveThreshold: parseFloatEnv(process.env.DECAY_ARCHIVE_THRESHOLD, 0.15),
    tombstoneAfterDays: parseIntEnv(process.env.DECAY_TOMBSTONE_DAYS, 180, 1),
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
    chunkSize: parseIntEnv(process.env.CONSOLIDATION_CHUNK_SIZE, 10, 1),
    maxSessionsPerRun: parseIntEnv(process.env.CONSOLIDATION_MAX_SESSIONS, 50, 1),
    minSessionMessages: parseIntEnv(process.env.CONSOLIDATION_MIN_MESSAGES, 4, 1),
    // Similarity band for post-extraction contradiction scan.
    // Entries above RECONSOLIDATION_THRESHOLD are already handled by decideMerge.
    // Entries below contradictionMinSimilarity are too dissimilar to plausibly contradict.
    // The band in between gets the contradiction LLM call.
    contradictionMinSimilarity: parseFloatEnv(process.env.CONTRADICTION_MIN_SIMILARITY, 0.4),
  },

  // Activation
  activation: {
    // Top-N entries returned per activation call.
    // Default is 10 — a generous ceiling for the MCP tool (deliberate active recall).
    // The passive plugin explicitly overrides this to 5 via ?limit=5 to keep
    // injected context tight. ACTIVATION_MAX_RESULTS overrides the server default.
    maxResults: parseIntEnv(process.env.ACTIVATION_MAX_RESULTS, 10, 1),
    // Minimum raw cosine similarity (NOT decay-weighted) to activate an entry.
    // Filtering on rawSimilarity means entry age/staleness never prevents a
    // semantically relevant entry from activating — decay only affects ranking.
    // 0.4 is more discriminating than the old 0.3 default — at 0.3, weakly
    // related entries fired too readily. 0.4 cuts noise while keeping
    // genuinely relevant entries (text-embedding-3-large at 0.4 is still a
    // meaningful topical match).
    similarityThreshold: parseFloatEnv(process.env.ACTIVATION_SIMILARITY_THRESHOLD, 0.4),
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

  // Similarity/threshold floats must be in (0, 1] — 0 is semantically invalid
  // (zero archive threshold means nothing ever archives; zero similarity threshold
  // activates every entry regardless of relevance).
  // Validate against the raw env var so the error message reflects what the user
  // actually typed — parse-time defaults would otherwise corrupt the "got X" value.
  // Interval helpers: lo is always exclusive (values must be > lo).
  // hi is inclusive by default; pass hiExclusive=true for a strict upper bound.
  function validateFloatRange(
    envVar: string | undefined,
    envName: string,
    lo: number,
    hi: number,
    hint: string,
    hiExclusive = false
  ): void {
    if (envVar === undefined) return; // not set — default is always valid
    const raw = Number.parseFloat(envVar);
    const hiViolation = hiExclusive ? raw >= hi : raw > hi;
    const intervalStr = hiExclusive ? `(${lo}, ${hi})` : `(${lo}, ${hi}]`;
    if (Number.isNaN(raw) || raw <= lo || hiViolation) {
      errors.push(
        `${envName} must be in ${intervalStr} (got "${envVar}"). ${hint}`
      );
    }
  }

  validateFloatRange(
    process.env.DECAY_ARCHIVE_THRESHOLD,
    "DECAY_ARCHIVE_THRESHOLD", 0, 1,
    `Default is ${config.decay.archiveThreshold}.`
  );
  // Upper bound is RECONSOLIDATION_THRESHOLD (exclusive) — a value at or above it
  // collapses the contradiction scan band to empty since decideMerge already handles
  // entries above that ceiling.
  validateFloatRange(
    process.env.CONTRADICTION_MIN_SIMILARITY,
    "CONTRADICTION_MIN_SIMILARITY", 0, RECONSOLIDATION_THRESHOLD,
    `Must be strictly below the ${RECONSOLIDATION_THRESHOLD} reconsolidation threshold. Default is ${config.consolidation.contradictionMinSimilarity}.`,
    true // hiExclusive
  );
  validateFloatRange(
    process.env.ACTIVATION_SIMILARITY_THRESHOLD,
    "ACTIVATION_SIMILARITY_THRESHOLD", 0, 1,
    `Default is ${config.activation.similarityThreshold}.`
  );

  // Validate EMBEDDING_DIMENSIONS early — a non-positive or non-integer value would
  // be clamped to 1 by parseIntEnv and forwarded to the API, causing a confusing 400.
  // Note: check the raw env var, not config.embedding.dimensions — "0" is falsy so
  // the config ternary would set dimensions=undefined and silently skip this check.
  if (process.env.EMBEDDING_DIMENSIONS !== undefined) {
    const raw = Number.parseInt(process.env.EMBEDDING_DIMENSIONS, 10);
    if (Number.isNaN(raw) || raw < 1) {
      errors.push(
        `EMBEDDING_DIMENSIONS must be a positive integer (got "${process.env.EMBEDDING_DIMENSIONS}"). Remove it to use the model's default dimensions.`
      );
    }
  }

  return errors;
}
