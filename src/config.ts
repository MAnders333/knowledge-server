import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import { RECONSOLIDATION_THRESHOLD } from "./types.js";

/**
 * Parse an integer environment variable with a fallback default and optional
 * minimum clamp. Returns `defaultVal` when the variable is absent, empty, or
 * not a valid integer. NaN-safe: a `Number.isNaN` guard ensures NaN never reaches
 * `Math.max` — an invalid string always yields `defaultVal`, never NaN.
 */
function parseIntEnv(
	envVar: string | undefined,
	defaultVal: number,
	min?: number,
): number {
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
function parseFloatEnv(
	envVar: string | undefined,
	defaultVal: number,
	min?: number,
): number {
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

	// PID file — written on startup, deleted on clean shutdown.
	// Used by `knowledge-server stop` to signal the running server.
	// Override with KNOWLEDGE_PID_PATH; set to "" to disable PID file management.
	pidPath:
		process.env.KNOWLEDGE_PID_PATH ??
		join(homedir(), ".local", "share", "knowledge-server", "server.pid"),

	// Episode sources
	//
	// opencodeDbPath: path to OpenCode's SQLite DB.
	//   Auto-detected via `opencode db path` or a probe list; override with OPENCODE_DB_PATH.
	//   A missing path is non-fatal — the source is simply disabled at startup.
	//   If explicitly set via env var and missing, validateConfig() will error.
	opencodeDbPath:
		process.env.OPENCODE_DB_PATH ||
		join(homedir(), ".local", "share", "opencode", "opencode.db"),

	// claudeDbPath: root directory for Claude Code session JSONL files.
	//   Defaults to CLAUDE_CONFIG_DIR env var (what Claude Code itself uses), then ~/.claude.
	//   Override with CLAUDE_DB_PATH (takes precedence over CLAUDE_CONFIG_DIR).
	claudeDbPath:
		process.env.CLAUDE_DB_PATH ||
		process.env.CLAUDE_CONFIG_DIR ||
		join(homedir(), ".claude"),

	// cursorDbPath: explicit override for Cursor's state.vscdb path.
	//   Auto-detected by resolveCursorDbPath() in cursor.ts using a platform-specific
	//   probe list (macOS: ~/Library/Application Support/…, Linux: ~/.config/…).
	//   Only set here when CURSOR_DB_PATH is explicitly provided — an empty string
	//   means "auto-detect" rather than "path was not found".
	cursorDbPath: process.env.CURSOR_DB_PATH || "",

	// codexSessionsDir: root directory for Codex CLI JSONL rollout files.
	//   Layout: <sessionsDir>/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl
	//   Auto-detected by resolveCodexSessionsDir() in codex.ts:
	//     1. CODEX_SESSIONS_DIR env var (this field, when non-empty)
	//     2. $CODEX_HOME/sessions (or ~/.codex/sessions when CODEX_HOME is unset)
	//   An empty string means "auto-detect".
	codexSessionsDir: process.env.CODEX_SESSIONS_DIR || "",

	// vscodeDataDir: root data directory for VSCode.
	//   Auto-detected by resolveVSCodeDataDir() in vscode.ts using a platform-specific
	//   probe list (macOS: ~/Library/Application Support/Code, Linux: ~/.config/Code).
	//   Only set here when VSCODE_DATA_DIR is explicitly provided — an empty string
	//   means "auto-detect" rather than "path was not found".
	vscodeDataDir: process.env.VSCODE_DATA_DIR || "",

	// Explicit source enable/disable.
	// All default to true (auto-detect); set to "false" to hard-disable a source.
	opencodeEnabled: process.env.OPENCODE_ENABLED !== "false",
	claudeEnabled: process.env.CLAUDE_ENABLED !== "false",
	cursorEnabled: process.env.CURSOR_ENABLED !== "false",
	codexEnabled: process.env.CODEX_ENABLED !== "false",
	vscodeEnabled: process.env.VSCODE_ENABLED !== "false",

	// LLM credentials — three tiers, evaluated per-provider at call time:
	//
	//   Tier 1 — per-provider direct credentials (no proxy needed):
	//     ANTHROPIC_BASE_URL / ANTHROPIC_API_KEY   → Anthropic models
	//     OPENAI_BASE_URL    / OPENAI_API_KEY       → OpenAI-compatible models
	//     GOOGLE_BASE_URL    / GOOGLE_API_KEY        → Google models
	//
	//   Tier 2 — unified proxy (existing behaviour, backwards compatible):
	//     LLM_BASE_ENDPOINT  / LLM_API_KEY
	//     Routes by model prefix: anthropic/→/anthropic/v1, google/→/gemini/v1beta, else /openai/v1
	//
	// Per-provider credentials take precedence over the unified endpoint.
	// At least one credential source must be configured; validateConfig() checks this.
	//
	// Four model slots with independent defaults — tune cost vs quality per task:
	//   extractionModel    — full episode → knowledge extraction (complex reasoning)
	//   mergeModel         — decideMerge near-duplicate comparison (structured, cheap)
	//   contradictionModel — detect + resolve contradictions (nuanced, fires rarely)
	//   synthesisModel     — cross-session principle synthesis (rare, high-quality)
	llm: {
		// Unified proxy fallback (backwards compatible)
		baseEndpoint: process.env.LLM_BASE_ENDPOINT || "",
		apiKey: process.env.LLM_API_KEY || "",
		// Per-provider overrides — take precedence over unified endpoint when set
		anthropic: {
			baseURL: process.env.ANTHROPIC_BASE_URL || "",
			apiKey: process.env.ANTHROPIC_API_KEY || "",
		},
		openai: {
			baseURL: process.env.OPENAI_BASE_URL || "",
			apiKey: process.env.OPENAI_API_KEY || "",
		},
		google: {
			baseURL: process.env.GOOGLE_BASE_URL || "",
			apiKey: process.env.GOOGLE_API_KEY || "",
		},
		extractionModel:
			process.env.LLM_EXTRACTION_MODEL || "anthropic/claude-sonnet-4-6",
		mergeModel: process.env.LLM_MERGE_MODEL || "anthropic/claude-haiku-4-5",
		contradictionModel:
			process.env.LLM_CONTRADICTION_MODEL || "anthropic/claude-sonnet-4-6",
		// Synthesis fires rarely (at obs=10, 20, 30, ...) and produces abstract
		// principles — worth using a capable model. Defaults to extractionModel
		// so existing deployments are unaffected without LLM_SYNTHESIS_MODEL set.
		synthesisModel:
			process.env.LLM_SYNTHESIS_MODEL ||
			process.env.LLM_EXTRACTION_MODEL ||
			"anthropic/claude-sonnet-4-6",
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

	// Embedding endpoint — separate from LLM so local models (Ollama, etc.) can
	// be used for embeddings while a cloud LLM handles extraction/merge/contradiction.
	//
	// Priority order:
	//   1. EMBEDDING_BASE_URL / EMBEDDING_API_KEY  — dedicated embedding endpoint
	//   2. OPENAI_BASE_URL    / OPENAI_API_KEY      — reuse OpenAI-compatible provider if set
	//   3. LLM_BASE_ENDPOINT  / LLM_API_KEY         — unified proxy fallback
	//
	// All embedding APIs must be OpenAI-compatible (/v1/embeddings).
	// Example for Ollama: EMBEDDING_BASE_URL=http://localhost:11434/v1
	embedding: {
		baseURL: process.env.EMBEDDING_BASE_URL || "",
		apiKey: process.env.EMBEDDING_API_KEY || "",
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
		maxSessionsPerRun: parseIntEnv(
			process.env.CONSOLIDATION_MAX_SESSIONS,
			50,
			1,
		),
		minSessionMessages: parseIntEnv(
			process.env.CONSOLIDATION_MIN_MESSAGES,
			4,
			1,
		),
		// Comma-separated list of tool names whose outputs should be included in
		// knowledge extraction. Empty by default (no tool outputs included).
		// Example: atlassian_confluence_get_page,atlassian_confluence_search
		includeToolOutputs: process.env.CONSOLIDATION_INCLUDE_TOOL_OUTPUTS
			? process.env.CONSOLIDATION_INCLUDE_TOOL_OUTPUTS.split(",")
					.map((s) => s.trim())
					.filter(Boolean)
			: [],
		// Similarity band for post-extraction contradiction scan.
		// Entries above RECONSOLIDATION_THRESHOLD are already handled by decideMerge.
		// Entries below contradictionMinSimilarity are too dissimilar to plausibly contradict.
		// The band in between gets the contradiction LLM call.
		contradictionMinSimilarity: parseFloatEnv(
			process.env.CONTRADICTION_MIN_SIMILARITY,
			0.4,
		),
		// Observation count threshold for triggering cross-session knowledge synthesis.
		// When an entry's observation_count reaches this value (or the next multiple of it),
		// the system looks at its KB neighbors and attempts to synthesize a higher-order
		// principle. Set to 0 to disable synthesis entirely.
		// Default: 10 (fires at obs=10, 20, 30, ... — 9 reinforcements for the first trigger).
		// observationCount starts at 1 (insertion), so threshold=10 means 9 reinforcements.
		// Lower values fire more aggressively but with weaker evidence — 3 fires after only
		// 2 confirmations, which is too weak a signal for reliable principle extraction.
		synthesisObservationThreshold: parseIntEnv(
			process.env.CONSOLIDATION_SYNTHESIS_THRESHOLD,
			10,
			0,
		),
		// Polling interval for background auto-consolidation while the server is running.
		// 0 (default) disables polling — consolidation only runs on startup and via API.
		// Example: 1800000 = 30 minutes. Only triggers when pending sessions exist.
		pollIntervalMs: parseIntEnv(
			process.env.CONSOLIDATION_POLL_INTERVAL_MS,
			0,
			0,
		),
	},

	// Activation
	activation: {
		// Top-N entries returned per activation call.
		// Default is 10 — a generous ceiling for the MCP tool (deliberate active recall).
		// The passive plugin explicitly overrides this to 8 via ?limit=8 to balance
		// recall against injected context size. ACTIVATION_MAX_RESULTS overrides the server default.
		maxResults: parseIntEnv(process.env.ACTIVATION_MAX_RESULTS, 10, 1),
		// Minimum raw cosine similarity (NOT decay-weighted) to activate an entry.
		// Filtering on rawSimilarity means entry age/staleness never prevents a
		// semantically relevant entry from activating — decay only affects ranking.
		// 0.30 favors recall over precision: text-embedding-3-large has a low noise
		// floor at this range, so the risk of irrelevant entries is acceptable compared
		// to the cost of silently missing useful knowledge. Raise to 0.35 if noise
		// becomes a problem in practice.
		similarityThreshold: parseFloatEnv(
			process.env.ACTIVATION_SIMILARITY_THRESHOLD,
			0.3,
		),
	},
} as const;

/**
 * Resolve the .env file path for config — used in error messages and as the
 * canonical location hint for binary installs.
 *
 * Search order (first existing file wins):
 *   1. $KNOWLEDGE_CONFIG_HOME/.env          — explicit override
 *   2. $XDG_CONFIG_HOME/knowledge-server/.env — XDG standard
 *   3. ~/.config/knowledge-server/.env      — XDG default (when XDG_CONFIG_HOME unset)
 *   4. ~/.local/share/knowledge-server/.env — legacy location (backwards compat)
 *
 * Source install: returns a prose description — Bun auto-loads .env from the
 * project root, so no path resolution is needed.
 *
 * Used only in error messages — we don't load the file here (Bun handles
 * source installs; the launcher wrapper handles binary installs).
 */
export function resolveEnvFilePath(): string {
	const execPath = process.execPath;
	// Source install: Bun runs .ts files directly; the exec path is the bun binary.
	// Detect by checking that the bun binary's basename starts with "bun"
	// (covers bun, bun-debug, bun-1.x, etc.).
	if (basename(execPath).startsWith("bun")) {
		return "the .env file in the project root";
	}

	// Priority-ordered candidate list for binary installs.
	const candidates: string[] = [];

	// 1. Explicit override
	if (process.env.KNOWLEDGE_CONFIG_HOME) {
		candidates.push(join(process.env.KNOWLEDGE_CONFIG_HOME, ".env"));
	}

	// 2 & 3. XDG_CONFIG_HOME (falls back to ~/.config per XDG spec)
	const xdgConfigHome =
		process.env.XDG_CONFIG_HOME || join(homedir(), ".config");
	candidates.push(join(xdgConfigHome, "knowledge-server", ".env"));

	// 4. Legacy location — backwards compat for existing installs
	candidates.push(
		join(homedir(), ".local", "share", "knowledge-server", ".env"),
	);

	for (const candidate of candidates) {
		if (existsSync(candidate)) return candidate;
	}

	// Nothing found — return the preferred new location as the hint
	return join(xdgConfigHome, "knowledge-server", ".env");
}

// Placeholder values shipped in the .env template — used to detect unconfigured installs.
// Update these if the template values in install.sh change.
const PLACEHOLDER_API_KEY = "your-api-key-here";
const PLACEHOLDER_ENDPOINT = "https://your-llm-endpoint.example.com";

export function validateConfig(): string[] {
	const errors: string[] = [];

	const envPath = resolveEnvFilePath();

	// Validate that at least one LLM credential source is configured.
	// Per-provider credentials (ANTHROPIC_API_KEY etc.) take precedence over the
	// unified endpoint (LLM_BASE_ENDPOINT + LLM_API_KEY). Having any one of them
	// configured is sufficient — the runtime routing will use whichever applies.
	const hasAnthropic = !!(
		config.llm.anthropic.apiKey?.trim() && config.llm.anthropic.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const hasOpenAI = !!(
		config.llm.openai.apiKey?.trim() && config.llm.openai.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const hasGoogle = !!(
		config.llm.google.apiKey?.trim() && config.llm.google.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const unifiedApiKey = config.llm.apiKey?.trim() ?? "";
	const unifiedEndpoint = config.llm.baseEndpoint?.trim() ?? "";
	const hasUnified =
		!!(unifiedApiKey && unifiedApiKey !== PLACEHOLDER_API_KEY) &&
		!!(unifiedEndpoint && unifiedEndpoint !== PLACEHOLDER_ENDPOINT);

	if (!hasAnthropic && !hasOpenAI && !hasGoogle && !hasUnified) {
		// Provide a more specific hint when the user has set half of the unified pair.
		// Guard against placeholder values — a key equal to PLACEHOLDER_API_KEY means
		// the .env was never edited, so it should not trigger the "key set but endpoint
		// missing" hint (the user would see a confusing message about a key they didn't
		// actually configure).
		const unifiedApiKeyReal =
			unifiedApiKey && unifiedApiKey !== PLACEHOLDER_API_KEY;
		const unifiedEndpointReal =
			unifiedEndpoint && unifiedEndpoint !== PLACEHOLDER_ENDPOINT;
		if (unifiedApiKeyReal && !unifiedEndpointReal) {
			errors.push(
				`LLM_API_KEY is set but LLM_BASE_ENDPOINT is missing. Edit ${envPath} and set LLM_BASE_ENDPOINT, or use a per-provider key (ANTHROPIC_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY) instead.`,
			);
		} else if (unifiedEndpointReal && !unifiedApiKeyReal) {
			errors.push(
				`LLM_BASE_ENDPOINT is set but LLM_API_KEY is missing. Edit ${envPath} and set LLM_API_KEY.`,
			);
		} else {
			errors.push(
				`No LLM credentials configured. Edit ${envPath} and set one of:\n    ANTHROPIC_API_KEY (direct Anthropic)\n    OPENAI_API_KEY (direct OpenAI-compatible)\n    GOOGLE_API_KEY (direct Google)\n    LLM_BASE_ENDPOINT + LLM_API_KEY (unified proxy)`,
			);
		}
	}

	// Warn when a dedicated embedding endpoint is configured but no API key is
	// available. This silently produces empty-string credentials and causes
	// authentication failures at runtime — surface it early at startup instead.
	// Affected setup: Ollama embeddings + cloud LLM where OPENAI_API_KEY is not set
	// and LLM_API_KEY is the cloud provider key (not an OpenAI-compatible key).
	if (config.embedding.baseURL) {
		const embeddingApiKey =
			config.embedding.apiKey?.trim() ||
			config.llm.openai.apiKey?.trim() ||
			config.llm.apiKey?.trim() ||
			"";
		if (!embeddingApiKey || embeddingApiKey === PLACEHOLDER_API_KEY) {
			// Warn rather than error — local endpoints (Ollama, llama.cpp) don't
			// require authentication and will work without a key. Cloud-hosted
			// OpenAI-compatible endpoints do require one, so surface the hint.
			console.warn(
				"[config] EMBEDDING_BASE_URL is set but no embedding API key was found. " +
					"If your endpoint requires authentication, set EMBEDDING_API_KEY. " +
					"Local endpoints (Ollama, llama.cpp) work without a key — this warning can be ignored.",
			);
		}
	}

	// Enforce a minimum token length when a fixed admin token is configured.
	// A short token is trivial to brute-force over a loopback connection — 16 chars
	// of a random hex string gives ~64 bits of entropy, a reasonable lower bound.
	// Random startup-generated tokens (24 bytes → 48 hex chars, ~192 bits) always pass.
	// Prefer at least 24 hex chars (openssl rand -hex 24) for a stronger guarantee.
	if (config.adminToken !== null && config.adminToken.length < 16) {
		errors.push(
			"KNOWLEDGE_ADMIN_TOKEN must be at least 16 characters. Use a long random value (e.g. openssl rand -hex 24).",
		);
	}

	const loopbackHosts = ["127.0.0.1", "::1", "localhost"];
	if (!loopbackHosts.includes(config.host)) {
		errors.push(
			`KNOWLEDGE_HOST is set to "${config.host}", which exposes the server on non-loopback interfaces with no authentication. Only use 127.0.0.1 unless you have added authentication and understand the security implications.`,
		);
	}

	// Error only when the user explicitly configured OPENCODE_DB_PATH but the file
	// doesn't exist — a typo or stale path is worth surfacing as a hard error.
	// When using the default auto-detected path, a missing file is non-fatal: the
	// OpenCode source will be disabled at startup with a warning instead.
	if (process.env.OPENCODE_DB_PATH && !existsSync(config.opencodeDbPath)) {
		errors.push(
			`OPENCODE_DB_PATH is set but OpenCode database not found at ${config.opencodeDbPath}.`,
		);
	}

	// Same pattern for CURSOR_DB_PATH: explicit env var pointing to missing file = hard error.
	// No explicit env var = auto-detect at startup (non-fatal if Cursor isn't installed).
	if (process.env.CURSOR_DB_PATH && !existsSync(process.env.CURSOR_DB_PATH)) {
		errors.push(
			`CURSOR_DB_PATH is set but Cursor database not found at ${process.env.CURSOR_DB_PATH}.`,
		);
	}

	// Same pattern for CODEX_SESSIONS_DIR.
	if (
		process.env.CODEX_SESSIONS_DIR &&
		!existsSync(process.env.CODEX_SESSIONS_DIR)
	) {
		errors.push(
			`CODEX_SESSIONS_DIR is set but directory not found at ${process.env.CODEX_SESSIONS_DIR}.`,
		);
	}

	// Same pattern for VSCODE_DATA_DIR.
	if (process.env.VSCODE_DATA_DIR && !existsSync(process.env.VSCODE_DATA_DIR)) {
		errors.push(
			`VSCODE_DATA_DIR is set but directory not found at ${process.env.VSCODE_DATA_DIR}.`,
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
		hiExclusive = false,
	): void {
		if (envVar === undefined) return; // not set — default is always valid
		const raw = Number.parseFloat(envVar);
		const hiViolation = hiExclusive ? raw >= hi : raw > hi;
		const intervalStr = hiExclusive ? `(${lo}, ${hi})` : `(${lo}, ${hi}]`;
		if (Number.isNaN(raw) || raw <= lo || hiViolation) {
			errors.push(
				`${envName} must be in ${intervalStr} (got "${envVar}"). ${hint}`,
			);
		}
	}

	validateFloatRange(
		process.env.DECAY_ARCHIVE_THRESHOLD,
		"DECAY_ARCHIVE_THRESHOLD",
		0,
		1,
		`Default is ${config.decay.archiveThreshold}.`,
	);
	// Upper bound is RECONSOLIDATION_THRESHOLD (exclusive) — a value at or above it
	// collapses the contradiction scan band to empty since decideMerge already handles
	// entries above that ceiling.
	validateFloatRange(
		process.env.CONTRADICTION_MIN_SIMILARITY,
		"CONTRADICTION_MIN_SIMILARITY",
		0,
		RECONSOLIDATION_THRESHOLD,
		`Must be strictly below the ${RECONSOLIDATION_THRESHOLD} reconsolidation threshold. Default is ${config.consolidation.contradictionMinSimilarity}.`,
		true, // hiExclusive
	);
	validateFloatRange(
		process.env.ACTIVATION_SIMILARITY_THRESHOLD,
		"ACTIVATION_SIMILARITY_THRESHOLD",
		0,
		1,
		`Default is ${config.activation.similarityThreshold}.`,
	);

	// Validate EMBEDDING_DIMENSIONS early — a non-positive or non-integer value would
	// be clamped to 1 by parseIntEnv and forwarded to the API, causing a confusing 400.
	// Note: check the raw env var, not config.embedding.dimensions — "0" is falsy so
	// the config ternary would set dimensions=undefined and silently skip this check.
	if (process.env.EMBEDDING_DIMENSIONS !== undefined) {
		const raw = Number.parseInt(process.env.EMBEDDING_DIMENSIONS, 10);
		if (Number.isNaN(raw) || raw < 1) {
			errors.push(
				`EMBEDDING_DIMENSIONS must be a positive integer (got "${process.env.EMBEDDING_DIMENSIONS}"). Remove it to use the model's default dimensions.`,
			);
		}
	}

	return errors;
}
