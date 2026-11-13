import { existsSync } from "node:fs";
import { basename, join } from "node:path";
import { isPlainHttpRemote } from "./activation/key-transport.js";
import { hasOAuthTokens } from "./auth/claude-oauth.js";
import { DEFAULT_CONFIG_PATH, resolveDataDir } from "./config-file.js";
import { envFileCandidates, preferredEnvFile } from "./env-file.js";
import { homeDir } from "./home.js";
import { DEFAULT_LOG_MAX_BYTES } from "./logger.js";

/** The environment a config is parsed from. */
type Env = Readonly<Record<string, string | undefined>>;

const TRUE_SPELLINGS = ["true", "1", "on", "yes"];
const FALSE_SPELLINGS = ["false", "0", "off", "no"];

/**
 * Reads environment variables while the config is parsed. An unset or empty
 * variable means "use the default". A value that is not a number, not a
 * spelling of a flag, not one of the allowed choices, or outside its range is
 * recorded as an issue and the default is used; validateConfig() reports the
 * issues. Each variable is therefore parsed once, and validated where it is
 * parsed.
 */
class EnvReader {
	readonly issues: string[] = [];

	constructor(private readonly env: Env) {}

	private raw(name: string): string | undefined {
		const value = this.env[name];
		return value === undefined || value.trim() === "" ? undefined : value;
	}

	private issue(message: string): void {
		if (!this.issues.includes(message)) this.issues.push(message);
	}

	/** Integer ≥ min. Non-numeric or smaller input is an issue. */
	int(
		name: string,
		fallback: number,
		min: number,
		options: { hint?: string } = {},
	): number {
		const raw = this.raw(name);
		if (raw === undefined) return fallback;
		const value = Number.parseInt(raw, 10);
		if (Number.isNaN(value) || value < min) {
			this.issue(
				`${name} must be an integer ≥ ${min} (got "${raw}").${options.hint ? ` ${options.hint}` : ""}`,
			);
			return fallback;
		}
		return value;
	}

	/** Number in [min, max], or in (min, max] when `openMin` is set. */
	float(
		name: string,
		fallback: number,
		min: number,
		max: number,
		options: { openMin?: boolean; hint?: string } = {},
	): number {
		const raw = this.raw(name);
		if (raw === undefined) return fallback;
		const value = Number.parseFloat(raw);
		const belowMin = options.openMin ? value <= min : value < min;
		if (Number.isNaN(value) || belowMin || value > max) {
			const left = options.openMin ? "(" : "[";
			this.issue(
				`${name} must be a number in ${left}${min}, ${max}] (got "${raw}").${options.hint ? ` ${options.hint}` : ""}`,
			);
			return fallback;
		}
		return value;
	}

	/** Integer in [min, max] when set; undefined when unset or invalid. */
	optionalInt(
		name: string,
		min: number,
		options: { hint?: string } = {},
	): number | undefined {
		if (this.raw(name) === undefined) return undefined;
		const value = this.int(name, Number.NaN, min, options);
		return Number.isNaN(value) ? undefined : value;
	}

	/** Number in [min, max] when set; null when unset or invalid. */
	optionalFloat(name: string, min: number, max: number): number | null {
		if (this.raw(name) === undefined) return null;
		const value = this.float(name, Number.NaN, min, max);
		return Number.isNaN(value) ? null : value;
	}

	/** Boolean flag: true/false, on/off, yes/no or 1/0 (case-insensitive). */
	flag(name: string, fallback: boolean): boolean {
		const raw = this.raw(name);
		if (raw === undefined) return fallback;
		const word = raw.trim().toLowerCase();
		if (TRUE_SPELLINGS.includes(word)) return true;
		if (FALSE_SPELLINGS.includes(word)) return false;
		this.issue(
			`${name} must be one of ${[...TRUE_SPELLINGS, ...FALSE_SPELLINGS].map((s) => `"${s}"`).join(", ")} (got "${raw}").`,
		);
		return fallback;
	}

	/** One of `choices`. */
	choice<T extends string>(
		name: string,
		choices: readonly T[],
		fallback: T,
	): T {
		const raw = this.raw(name);
		if (raw === undefined) return fallback;
		const found = choices.find((c) => c === raw.trim());
		if (found !== undefined) return found;
		this.issue(
			`${name} must be ${choices.map((c) => `"${c}"`).join(" or ")} (got "${raw}").`,
		);
		return fallback;
	}
}

/**
 * Parse KNOWLEDGE_ALLOWED_HOSTS: comma-separated Host header values
 * (`name` or `name:port`). Entries are trimmed and lower-cased; empty entries
 * are dropped. Format is validated where the allow-list is built (api/host-allowlist).
 */
export function parseAllowedHosts(raw: string | undefined): string[] {
	return (raw ?? "")
		.split(",")
		.map((h) => h.trim().toLowerCase())
		.filter((h) => h.length > 0);
}

function originOf(url: string | undefined): string | null {
	if (!url) return null;
	try {
		return new URL(url).origin;
	} catch {
		return null;
	}
}

/**
 * Choose the API key for a dedicated endpoint (EMBEDDING_BASE_URL, RERANK_REMOTE_URL).
 *
 * The dedicated key (EMBEDDING_API_KEY, RERANK_REMOTE_API_KEY) is used when set.
 * LLM_API_KEY is forwarded only when the endpoint has the same origin as
 * LLM_BASE_ENDPOINT (the same trust domain). Any other endpoint gets no key, so the
 * generic LLM credential never leaves its configured provider.
 */
export function resolveScopedApiKey(env: {
	endpointUrl?: string;
	dedicatedApiKey?: string;
	llmBaseEndpoint?: string;
	llmApiKey?: string;
}): string {
	const dedicated = env.dedicatedApiKey?.trim();
	if (dedicated) return dedicated;
	const llmKey = env.llmApiKey?.trim();
	if (!env.endpointUrl || !llmKey) return "";
	const endpointOrigin = originOf(env.endpointUrl);
	return endpointOrigin !== null &&
		endpointOrigin === originOf(env.llmBaseEndpoint)
		? llmKey
		: "";
}

// ── Defaults ─────────────────────────────────────────────────────────────────

/** Local cross-encoder model. Its checksum pin lives in rerank.ts (PINNED_MODELS). */
export const DEFAULT_RERANK_MODEL = "Xenova/ms-marco-MiniLM-L-6-v2";

/**
 * Default decision model. A versioned ID: aliases move under the same name, and
 * the calibrated floors are keyed by model name.
 */
const DEFAULT_RERANK_REMOTE_MODEL = "jev-1.14.0";

/** Candidate pool for the local reranker (RERANK_CANDIDATES unset). */
const DEFAULT_LOCAL_RERANK_CANDIDATES = 16;

/** Candidate pool for the remote reranker (RERANK_CANDIDATES unset). */
const DEFAULT_REMOTE_RERANK_CANDIDATES = 30;

/**
 * Candidates one remote rerank call carries. The remote reranker never splits the
 * pool, so a larger RERANK_CANDIDATES is a startup error (see validateConfig).
 */
export const REMOTE_RERANK_MAX_CANDIDATES = 40;

/**
 * Conservative character budget for one remote rerank request: the query tail, the
 * head of every document and the question text. hev's reference state budget is
 * 100k characters; the gateway's own limits are far higher.
 */
export const REMOTE_RERANK_REQUEST_CHAR_BUDGET = 100_000;

/** Upper bound, in characters, on the question text sent with each candidate. */
const REMOTE_RERANK_QUESTION_CHARS = 512;

/** Default remote rerank budget per call, retry included. */
const DEFAULT_RERANK_REMOTE_TIMEOUT_MS = 2_500;

/** Placeholder values shipped in the .env template — used to detect unconfigured installs. */
export const PLACEHOLDER_API_KEY = "your-api-key-here";
export const PLACEHOLDER_ENDPOINT = "https://your-llm-endpoint.example.com";
const PLACEHOLDER_ADMIN_TOKEN = "your-stable-token-here";

// ── Threshold defaults ───────────────────────────────────────────────────────
// Single source of truth for all similarity-threshold defaults. Calibrated for
// text-embedding-3-large. Run `knowledge-server calibrate` to find the right
// values for a different embedding model.
//
// Override at runtime via the corresponding env vars:
//   RECONSOLIDATION_SIMILARITY_THRESHOLD, CONTRADICTION_MIN_SIMILARITY,
//   ACTIVATION_SIMILARITY_THRESHOLD, CLUSTER_ASSIGNMENT_THRESHOLD,
//   CLUSTER_MIN_MEMBERS.

/**
 * Strength below which an active entry is considered stale and surfaced in
 * `GET /review` and `knowledge-server review`.
 * Distinct from DECAY_ARCHIVE_THRESHOLD (0.15) — entries here are active but
 * fading and warrant human attention before they decay further.
 */
export const REVIEW_STALE_STRENGTH_THRESHOLD = 0.3;

/** Near-duplicate merge cutoff (upper bound of contradiction scan band). */
const DEFAULT_RECONSOLIDATION_THRESHOLD = 0.78;
/** Lower bound of the contradiction scan band. */
const DEFAULT_CONTRADICTION_MIN_SIMILARITY = 0.4;
/**
 * Most candidates checked per entry and per scan path, ranked by similarity.
 * Unbounded by default: a lower cap has not been validated for recall.
 */
const DEFAULT_CONTRADICTION_MAX_CANDIDATES = Number.POSITIVE_INFINITY;
/** Minimum raw cosine similarity to activate an entry. */
const DEFAULT_ACTIVATION_SIMILARITY_THRESHOLD = 0.42;
// Precision gate defaults. Conservative placeholders, NOT replay-calibrated (see gate.ts).
const DEFAULT_RERANK_REL_FRAC = 0.35;
const DEFAULT_DENSE_MARGIN = 0;
/**
 * Token budget of the injected block: the formatter's 6,000-character cap in
 * tokens. A larger value is clamped to that cap (activate.ts RENDER_TOKEN_CAP).
 */
const DEFAULT_TOKEN_BUDGET = 1500;
/**
 * Cosine similarity threshold for assigning an entry to an existing cluster
 * during greedy clustering. Below this threshold a new cluster is started.
 * Kept broad (0.5) — heterogeneous cluster members give the LLM more
 * cross-domain material to generalize from. Synthesis quality is instead
 * governed by CLUSTER_MIN_MEMBERS (minimum size before synthesis fires).
 */
const DEFAULT_CLUSTER_ASSIGNMENT_THRESHOLD = 0.5;
/**
 * Minimum cluster size required before synthesis is attempted.
 * Requires enough distinct observations that a structural invariant is likely
 * to have genuinely emerged, rather than being a coincidence of a few entries.
 */
const DEFAULT_CLUSTER_MIN_MEMBERS = 5;

// ── Remote reranker endpoint ─────────────────────────────────────────────────

/** RERANK_PROVIDER as the server runs it: "remote" only when set explicitly. */
function rerankProviderFromEnv(env: Env): "local" | "remote" {
	return env.RERANK_PROVIDER?.trim().toLowerCase() === "remote"
		? "remote"
		: "local";
}

/**
 * Remote endpoint and its key: RERANK_REMOTE_URL, or the System One route on the
 * LLM gateway. The URL is blank when neither LLM_BASE_ENDPOINT nor RERANK_REMOTE_URL
 * is set; the reranker then stays off.
 */
function resolveRerankRemoteEndpoint(env: Env): {
	url: string;
	apiKey: string;
} {
	const llmBaseEndpoint = env.LLM_BASE_ENDPOINT ?? "";
	const gateway = llmBaseEndpoint.trim().replace(/\/+$/, "");
	const url =
		env.RERANK_REMOTE_URL?.trim() ||
		(gateway ? `${gateway}/decisions/v1/systemone` : "");
	return {
		url,
		apiKey: resolveScopedApiKey({
			endpointUrl: url,
			dedicatedApiKey: env.RERANK_REMOTE_API_KEY,
			llmBaseEndpoint,
			llmApiKey: env.LLM_API_KEY,
		}),
	};
}

/**
 * True when the remote reranker is the scorer the server will use: reranking is on,
 * RERANK_PROVIDER=remote, and the endpoint and key are set and not template
 * placeholders. A missing endpoint or key falls back to local, so the remote
 * defaults and checks do not apply then.
 */
function isRemoteRerankUsable(rerank: {
	enabled: boolean;
	provider: "local" | "remote";
	url: string;
	apiKey: string;
}): boolean {
	return (
		rerank.enabled &&
		rerank.provider === "remote" &&
		rerank.url !== "" &&
		rerank.apiKey !== "" &&
		!rerank.url.startsWith(PLACEHOLDER_ENDPOINT) &&
		rerank.apiKey !== PLACEHOLDER_API_KEY
	);
}

/** True for an absolute http or https URL. */
function isHttpUrl(url: string): boolean {
	try {
		const { protocol } = new URL(url);
		return protocol === "http:" || protocol === "https:";
	} catch {
		return false;
	}
}

/**
 * Startup warning for an endpoint that receives an API key over plain http to a
 * non-loopback host. It is a warning, not an error: LAN gateways often run plain http.
 */
function warnKeyOverPlainHttp(
	setting: string,
	url: string,
	apiKey: string,
): void {
	if (!apiKey.trim() || !isPlainHttpRemote(url)) return;
	console.warn(
		`[config] ${setting} uses plain http on a non-loopback host, so its API key is sent unencrypted. Use https.`,
	);
}

// ── Parsing ──────────────────────────────────────────────────────────────────

/** Path variables that the user set explicitly; validateConfig() checks they exist. */
const EXPLICIT_PATH_VARS = [
	{ name: "OPENCODE_DB_PATH", missing: "OpenCode database" },
	{ name: "CURSOR_DB_PATH", missing: "Cursor database" },
	{ name: "CODEX_SESSIONS_DIR", missing: "directory" },
	{ name: "VSCODE_DATA_DIR", missing: "directory" },
] as const;

/**
 * Parse the environment into the config object. Pure: the same env always gives
 * the same result. The module-level `config` is parseEnv(process.env).config.
 */
export function parseEnv(env: Env, configPath = DEFAULT_CONFIG_PATH) {
	const r = new EnvReader(env);
	const dataDir = resolveDataDir(configPath);
	const explicitPidPath = env.KNOWLEDGE_PID_PATH;
	const pidPath = explicitPidPath ?? join(dataDir, "server.pid");
	const legacyPidDefault = join(
		homeDir(),
		".local",
		"share",
		"knowledge-server",
		"server.pid",
	);

	const rerankEnabled = env.RERANK_ENABLED !== "false";
	const rerankProvider = rerankProviderFromEnv(env);
	const rawRerankProvider = env.RERANK_PROVIDER?.trim().toLowerCase();
	if (
		rawRerankProvider &&
		rawRerankProvider !== "local" &&
		rawRerankProvider !== "remote"
	) {
		r.issues.push(
			`RERANK_PROVIDER must be "local" or "remote" (got "${env.RERANK_PROVIDER}").`,
		);
	}
	const remoteEndpoint = resolveRerankRemoteEndpoint(env);
	const remoteUsable = isRemoteRerankUsable({
		enabled: rerankEnabled,
		provider: rerankProvider,
		url: remoteEndpoint.url,
		apiKey: remoteEndpoint.apiKey,
	});

	const config = {
		// Server bind address (port/host) is resolved in config-file.ts (resolveServerAddress).
		// Optional fixed admin token — set KNOWLEDGE_ADMIN_TOKEN to use a stable token
		// instead of a random one generated at startup. Useful for scripted/automated use.
		// Leave unset in production for better security (random token per process lifetime).
		adminToken: env.KNOWLEDGE_ADMIN_TOKEN || null,
		// Extra Host names accepted by the HTTP API (comma-separated, optional :port).
		// localhost, 127.0.0.1, [::1] and KNOWLEDGE_HOST are always accepted.
		allowedHosts: parseAllowedHosts(env.KNOWLEDGE_ALLOWED_HOSTS),
		// Scrub credentials (API keys, tokens, private keys, DB passwords) from
		// transcript text before it is staged, sent to an LLM/embedding API, or stored.
		// Set REDACT_SECRETS=false to disable.
		redactSecrets: env.REDACT_SECRETS !== "false",

		// Log file — all operational output is tee'd here in addition to stdout.
		// Set KNOWLEDGE_LOG_PATH to override; set to "" to disable file logging.
		// Uses ?? (not ||) so that KNOWLEDGE_LOG_PATH="" is respected as an explicit
		// "disable" signal — || would treat "" as falsy and fall back to the default path.
		// The default lives in the data directory (see resolveDataDir).
		logPath: env.KNOWLEDGE_LOG_PATH ?? join(dataDir, "server.log"),
		// Size at which the active log rotates (server.log.1 … .3 are kept).
		// LOG_MAX_BYTES=0 disables rotation.
		logMaxBytes: r.int("LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES, 0),

		// PID file — written on startup, deleted on clean shutdown.
		// Used by `knowledge-server stop` to signal the running server.
		// Override with KNOWLEDGE_PID_PATH; set to "" to disable PID file management.
		pidPath,
		// Where earlier versions wrote the PID file: the default before the data
		// directory followed stateDb.path. Set only when that differs from pidPath and
		// KNOWLEDGE_PID_PATH is unset, so a server started by an earlier version can
		// still be found after an upgrade. Null otherwise.
		legacyPidPath:
			explicitPidPath === undefined && legacyPidDefault !== pidPath
				? legacyPidDefault
				: null,

		// Upper bound on each knowledge store's connect at startup (store-registry.ts).
		storeInitTimeoutMs: r.int("STORE_INIT_TIMEOUT_MS", 10_000, 1),

		// Upload interval of the daemon the server spawns (DAEMON_UPLOAD_INTERVAL_SECONDS),
		// or null when unset. Read through spawnedDaemonIntervalArgs().
		daemon: {
			uploadIntervalSeconds:
				r.optionalInt("DAEMON_UPLOAD_INTERVAL_SECONDS", 1) ?? null,
		},

		// Episode sources
		// Note: daemonAutoSpawn is resolved by StoreRegistry via config-file.ts
		// (config.jsonc "daemonAutoSpawn" field with DAEMON_AUTO_SPAWN env var override).
		//
		// opencodeDbPath: path to OpenCode's SQLite DB.
		//   Auto-detected via `opencode db path` or a probe list; override with OPENCODE_DB_PATH.
		//   A missing path is non-fatal — the source is simply disabled at startup.
		//   If explicitly set via env var and missing, validateConfig() will error.
		opencodeDbPath:
			env.OPENCODE_DB_PATH ||
			join(homeDir(), ".local", "share", "opencode", "opencode.db"),

		// claudeDbPath: root directory for Claude Code session JSONL files.
		//   Defaults to CLAUDE_CONFIG_DIR env var (what Claude Code itself uses), then ~/.claude.
		//   Override with CLAUDE_DB_PATH (takes precedence over CLAUDE_CONFIG_DIR).
		claudeDbPath:
			env.CLAUDE_DB_PATH || env.CLAUDE_CONFIG_DIR || join(homeDir(), ".claude"),

		// cursorDbPath: explicit override for Cursor's state.vscdb path.
		//   Auto-detected by resolveCursorDbPath() in cursor.ts using a platform-specific
		//   probe list (macOS: ~/Library/Application Support/…, Linux: ~/.config/…).
		//   Only set here when CURSOR_DB_PATH is explicitly provided — an empty string
		//   means "auto-detect" rather than "path was not found".
		cursorDbPath: env.CURSOR_DB_PATH || "",

		// codexSessionsDir: root directory for Codex CLI JSONL rollout files.
		//   Layout: <sessionsDir>/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl
		//   Auto-detected by resolveCodexSessionsDir() in codex.ts:
		//     1. CODEX_SESSIONS_DIR env var (this field, when non-empty)
		//     2. $CODEX_HOME/sessions (or ~/.codex/sessions when CODEX_HOME is unset)
		//   An empty string means "auto-detect".
		codexSessionsDir: env.CODEX_SESSIONS_DIR || "",

		// vscodeDataDir: root data directory for VSCode.
		//   Auto-detected by resolveVSCodeDataDir() in vscode.ts using a platform-specific
		//   probe list (macOS: ~/Library/Application Support/Code, Linux: ~/.config/Code).
		//   Only set here when VSCODE_DATA_DIR is explicitly provided — an empty string
		//   means "auto-detect" rather than "path was not found".
		vscodeDataDir: env.VSCODE_DATA_DIR || "",

		// localFilesDir: directory containing Markdown files to ingest as a knowledge source.
		//   Default: ~/knowledge (user-visible, memorable location).
		//   Override with LOCAL_FILES_DIR env var.
		//   If the directory does not exist, the source is silently skipped (no warning,
		//   no error) — the feature is opt-in by creating the directory.
		localFilesDir: env.LOCAL_FILES_DIR || join(homeDir(), "knowledge"),

		// piSessionsRoot: root directory scanned for pi session JSONL files.
		//   pi stores sessions at <agentDir>/sessions/, where the agent dir defaults to
		//   ~/.pi/agent but can be swapped per-invocation via PI_CODING_AGENT_DIR
		//   (e.g. separate work/personal layers). The reader therefore scans every
		//   <piSessionsRoot>/<dir>/sessions it finds — with the default ~/.pi root this
		//   covers the standard `agent` layer plus any custom layers automatically.
		//   Override with PI_SESSIONS_ROOT (point at ~/.pi or directly at an agent dir).
		piSessionsRoot: env.PI_SESSIONS_ROOT || join(homeDir(), ".pi"),

		// ompSessionsRoot: root directory scanned for oh-my-pi (the `omp` harness,
		//   package @oh-my-pi/pi-coding-agent) session JSONL files. oh-my-pi keeps a
		//   shared <root>/agent layer plus per-mode layers at
		//   <root>/profiles/<mode>/agent — the reader scans <root>/agent/sessions and
		//   every <root>/profiles/<mode>/agent/sessions. Same JSONL format as pi.
		//   Override with OMP_SESSIONS_ROOT.
		ompSessionsRoot: env.OMP_SESSIONS_ROOT || join(homeDir(), ".omp"),

		// Explicit source enable/disable.
		// All default to true (auto-detect); set to "false" to hard-disable a source.
		opencodeEnabled: env.OPENCODE_ENABLED !== "false",
		claudeEnabled: env.CLAUDE_ENABLED !== "false",
		cursorEnabled: env.CURSOR_ENABLED !== "false",
		codexEnabled: env.CODEX_ENABLED !== "false",
		vscodeEnabled: env.VSCODE_ENABLED !== "false",
		localFilesEnabled: env.LOCAL_FILES_ENABLED !== "false",
		piEnabled: env.PI_ENABLED !== "false",
		ompEnabled: env.OMP_ENABLED !== "false",

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
			baseEndpoint: env.LLM_BASE_ENDPOINT || "",
			apiKey: env.LLM_API_KEY || "",
			// Per-provider overrides — take precedence over unified endpoint when set
			anthropic: {
				baseURL: env.ANTHROPIC_BASE_URL || "",
				apiKey: env.ANTHROPIC_API_KEY || "",
			},
			openai: {
				baseURL: env.OPENAI_BASE_URL || "",
				apiKey: env.OPENAI_API_KEY || "",
				// Name of the request parameter carrying the output-token limit on
				// OpenAI-compatible chat calls. The AI SDK always emits `max_tokens`;
				// newer OpenAI models (gpt-5.x, o-series) reject it and require
				// `max_completion_tokens` instead. Set OPENAI_MAX_TOKENS_PARAM to
				// "max_completion_tokens" for those; anything else keeps the legacy
				// default.
				maxTokensParam: r.choice(
					"OPENAI_MAX_TOKENS_PARAM",
					["max_tokens", "max_completion_tokens"] as const,
					"max_tokens",
				),
				// Comma-separated request parameters to omit from OpenAI-compatible
				// chat calls. Some models reject non-default sampling parameters
				// outright — gpt-5.x/o-series reasoning models only accept the default
				// temperature (1), so the hardcoded temperature in the request fails
				// with HTTP 400. Typical usage: OPENAI_DROP_PARAMS=temperature
				dropParams: env.OPENAI_DROP_PARAMS
					? env.OPENAI_DROP_PARAMS.split(",")
							.map((s) => s.trim())
							.filter(Boolean)
					: [],
			},
			google: {
				baseURL: env.GOOGLE_BASE_URL || "",
				apiKey: env.GOOGLE_API_KEY || "",
			},
			extractionModel:
				env.LLM_EXTRACTION_MODEL || "anthropic/claude-sonnet-4-6",
			mergeModel: env.LLM_MERGE_MODEL || "anthropic/claude-haiku-4-5",
			contradictionModel:
				env.LLM_CONTRADICTION_MODEL || "anthropic/claude-haiku-4-5",
			// Synthesis fires per cluster when membership changes since last synthesis.
			// Defaults to extractionModel so existing deployments are unaffected without
			// LLM_SYNTHESIS_MODEL set.
			synthesisModel:
				env.LLM_SYNTHESIS_MODEL ||
				env.LLM_EXTRACTION_MODEL ||
				"anthropic/claude-sonnet-4-6",
			// Per-call-type timeouts in ms: total wall-clock budget for one attempt
			// (not across retries). A timed-out attempt is not retried. Defaults suit
			// hosted models; slow local models (Ollama) should raise the value for the
			// affected call types. Precedence per type: LLM_TIMEOUT_<TYPE>_MS >
			// LLM_TIMEOUT_MS > default. A value below 1 ms or not a number is reported by validateConfig().
			timeoutsMs: {
				contradiction: r.int(
					"LLM_TIMEOUT_CONTRADICTION_MS",
					r.int("LLM_TIMEOUT_MS", 60_000, 1),
					1,
				),
				merge: r.int(
					"LLM_TIMEOUT_MERGE_MS",
					r.int("LLM_TIMEOUT_MS", 90_000, 1),
					1,
				),
				extraction: r.int(
					"LLM_TIMEOUT_EXTRACTION_MS",
					r.int("LLM_TIMEOUT_MS", 300_000, 1),
					1,
				),
				synthesis: r.int(
					"LLM_TIMEOUT_SYNTHESIS_MS",
					r.int("LLM_TIMEOUT_MS", 300_000, 1),
					1,
				),
			},
			// Per-call retry budget. On transient error, complete() retries up to this many
			// additional times before throwing to the caller. A timed-out attempt is never
			// retried (its budget is treated as a hang).
			// Retries use exponential backoff starting at retryBaseDelayMs (capped at 60s).
			// Set to 0 to disable retries entirely.
			maxRetries: r.int("LLM_MAX_RETRIES", 2, 0),
			retryBaseDelayMs: r.int("LLM_RETRY_BASE_DELAY_MS", 3000, 0),
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
			baseURL: env.EMBEDDING_BASE_URL || "",
			apiKey: resolveScopedApiKey({
				endpointUrl: env.EMBEDDING_BASE_URL,
				dedicatedApiKey: env.EMBEDDING_API_KEY,
				llmBaseEndpoint: env.LLM_BASE_ENDPOINT,
				llmApiKey: env.LLM_API_KEY,
			}),
			model: env.EMBEDDING_MODEL || "text-embedding-3-large",
			// Only defined when EMBEDDING_DIMENSIONS is explicitly set.
			// Forwarded to the API only when present — the `dimensions` parameter is
			// only valid for text-embedding-3-* models; sending it to other models
			// (ada-002, Ollama, etc.) causes a 400 error. A non-positive or non-integer
			// value is an issue reported by validateConfig().
			dimensions: r.optionalInt("EMBEDDING_DIMENSIONS", 1, {
				hint: "Remove it to use the model's default dimensions.",
			}),
			// Hard per-call timeout for the embedding fetch. Without this, a slow or
			// rate-limited embedding proxy (e.g. the unified LLM proxy) causes the
			// request to hang indefinitely and holds the TCP connection open until
			// the OS-level keepalive eventually reaps it — silently breaking both
			// the /activate HTTP route and the MCP `activate` tool.
			//
			// Override via EMBEDDING_TIMEOUT_MS env var. Must be a positive integer.
			// Background bulk embedding uses max(this, BULK_EMBED_MIN_TIMEOUT_MS = 60s).
			timeoutMs: r.int("EMBEDDING_TIMEOUT_MS", 20_000, 1),
			// Retry budget per 100-text chunk for background bulk embedding only
			// (startup re-embed after an EMBEDDING_MODEL change, post-consolidation
			// backfill of missing embeddings). Retries transient failures: timeouts,
			// network errors, HTTP 408/429/5xx and malformed/truncated responses,
			// with exponential backoff from 1s. Bulk calls also use a per-attempt
			// timeout of at least 60s. Interactive activation never retries.
			// Set to 0 to disable.
			maxRetries: r.int("EMBEDDING_MAX_RETRIES", 2, 0),
		},

		// State database (server-side pending_episodes staging and bookkeeping).
		// Its location is config.jsonc stateDb (see config-file.ts).
		postgres: {
			// Maximum pool connections for each knowledge store on Postgres. Useful for
			// hosted services with tight connection limits (Supabase free, Railway, Neon).
			poolMax: r.int("POSTGRES_POOL_MAX", 10, 1),
		},

		// Decay parameters
		decay: {
			archiveThreshold: r.float("DECAY_ARCHIVE_THRESHOLD", 0.15, 0, 1, {
				openMin: true,
			}),
			tombstoneAfterDays: r.int("DECAY_TOMBSTONE_DAYS", 180, 1),
			// Decay runs on its own schedule, independent of pending work, so an idle
			// install still archives stale entries. Not run per consolidation batch.
			// Set to 0 to disable.
			intervalMs: r.int("DECAY_INTERVAL_MS", 86_400_000, 0),
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
			// Cosine similarity threshold above which two entries are considered near-duplicates
			// and routed to decideMerge (LLM merge decision), rather than inserted as novel.
			// Also serves as the exclusive upper bound of the contradiction scan band — entries
			// at or above this are already handled by decideMerge and excluded from the scan.
			//
			// This value is sensitive to the embedding model in use. Models with a broader
			// similarity distribution (e.g. small local models) may need a lower value to avoid
			// over-merging; high-quality dense models (e.g. text-embedding-3-large) work well at
			// the default. Run `knowledge-server calibrate` to find the right value for a
			// different embedding model.
			reconsolidationThreshold: r.float(
				"RECONSOLIDATION_SIMILARITY_THRESHOLD",
				DEFAULT_RECONSOLIDATION_THRESHOLD,
				0,
				1,
				{ openMin: true },
			),
			chunkSize: r.int("CONSOLIDATION_CHUNK_SIZE", 10, 1),
			maxSessionsPerRun: r.int("CONSOLIDATION_MAX_SESSIONS", 50, 1),
			minSessionMessages: r.int("CONSOLIDATION_MIN_MESSAGES", 4, 1),
			// Comma-separated list of tool names whose outputs should be included in
			// knowledge extraction. Empty by default (no tool outputs included).
			// Example: atlassian_confluence_get_page,atlassian_confluence_search
			includeToolOutputs: env.CONSOLIDATION_INCLUDE_TOOL_OUTPUTS
				? env.CONSOLIDATION_INCLUDE_TOOL_OUTPUTS.split(",")
						.map((s) => s.trim())
						.filter(Boolean)
				: [],
			// Similarity band for post-extraction contradiction scan.
			// Entries above reconsolidationThreshold are already handled by decideMerge.
			// Entries below contradictionMinSimilarity are too dissimilar to plausibly contradict.
			// The band in between gets the contradiction LLM call.
			contradictionMinSimilarity: r.float(
				"CONTRADICTION_MIN_SIMILARITY",
				DEFAULT_CONTRADICTION_MIN_SIMILARITY,
				0,
				1,
				{ openMin: true },
			),
			// Most candidates sent to the contradiction check per entry and scan path,
			// highest similarity first. Unset = no cap. Lower values cut LLM calls but
			// lose detections past the cap.
			contradictionMaxCandidates: r.int(
				"CONTRADICTION_MAX_CANDIDATES",
				DEFAULT_CONTRADICTION_MAX_CANDIDATES,
				1,
			),
			// Contradiction reply format. false (default): one verdict per candidate, the
			// format the scan was tuned on. true: list conflicts only, which saves about
			// 65% of output tokens per call but is not validated for recall; enable only
			// after an online spot-check.
			contradictionConflictsOnly: r.flag("CONTRADICTION_CONFLICTS_ONLY", false),
			// An entry whose best candidate is below this similarity is not scanned.
			// 0 disables the floor (every in-band candidate is checked).
			contradictionMinTopSimilarity: r.float(
				"CONTRADICTION_MIN_TOP_SIMILARITY",
				0,
				0,
				1,
			),
			// Cosine similarity threshold for assigning an entry to an existing cluster
			// during greedy clustering. Kept broad by default — heterogeneous members give
			// the LLM more cross-domain material to generalize from.
			clusterAssignmentThreshold: r.float(
				"CLUSTER_ASSIGNMENT_THRESHOLD",
				DEFAULT_CLUSTER_ASSIGNMENT_THRESHOLD,
				0,
				1,
				{ openMin: true },
			),
			// Minimum cluster size required before synthesis is attempted.
			// Higher values require more evidence that a structural invariant has emerged.
			clusterMinMembers: r.int(
				"CLUSTER_MIN_MEMBERS",
				DEFAULT_CLUSTER_MIN_MEMBERS,
				2,
			),
			// Maximum cluster members sent to synthesizePrinciple per call. Members are
			// chosen closest-to-centroid first.
			synthesisMaxPeers: r.int("SYNTHESIS_MAX_PEERS", 50, 1),
			// Approximate token budget for the members sent per synthesis prompt; members
			// are dropped furthest-first until the estimate fits.
			synthesisMaxPromptTokens: r.int(
				"SYNTHESIS_MAX_PROMPT_TOKENS",
				30_000,
				1000,
			),
			// Re-synthesizing an already-synthesized cluster requires a material change: at
			// least SYNTHESIS_MIN_NEW_MEMBERS members joined since its last synthesis, or at
			// least SYNTHESIS_MIN_NEW_FRACTION (0..1) of its current members did. Counting is
			// cumulative across clustering passes until the cluster synthesizes.
			synthesisMinNewMembers: r.int("SYNTHESIS_MIN_NEW_MEMBERS", 3, 1),
			synthesisMinNewFraction: r.float("SYNTHESIS_MIN_NEW_FRACTION", 0.2, 0, 1),
			// Polling interval for background auto-consolidation while the server is running.
			// Default: 8 hours (28_800_000 ms). Only triggers when pending sessions exist,
			// so a quiet period costs nothing. Matches the sleep-consolidation metaphor —
			// once per day / overnight is the natural cadence.
			// Set to 0 to disable.
			pollIntervalMs: r.int("CONSOLIDATION_POLL_INTERVAL_MS", 28_800_000, 0),
		},

		// Activation
		activation: {
			// Top-N entries returned per activation call.
			// Default is 10 — a generous ceiling for the MCP tool (deliberate active recall).
			// The passive plugin explicitly overrides this to 8 via ?limit=8 to balance
			// recall against injected context size.
			maxResults: r.int("ACTIVATION_MAX_RESULTS", 10, 1),
			// Minimum raw cosine similarity (NOT decay-weighted) to activate an entry.
			// Filtering on rawSimilarity means entry age/staleness never prevents a
			// semantically relevant entry from activating — decay only affects ranking.
			// The default favors recall over precision: text-embedding-3-large has a low
			// noise floor at this range, so the risk of irrelevant entries is acceptable
			// compared to the cost of silently missing useful knowledge.
			similarityThreshold: r.float(
				"ACTIVATION_SIMILARITY_THRESHOLD",
				DEFAULT_ACTIVATION_SIMILARITY_THRESHOLD,
				0,
				1,
				{ openMin: true },
			),
			// Local cross-encoder reranker (second stage of retrieve & re-rank).
			// Dense retrieval overfetches `candidates` entries for recall; the
			// in-process cross-encoder (onnxruntime-web WASM, no network/vendor)
			// then re-scores them for precision. On any failure the engine falls
			// back to dense-only ordering — reranking never breaks activation.
			rerank: {
				// Default on. RERANK_ENABLED=false restores pure dense ordering.
				enabled: rerankEnabled,
				// "local" (default) runs the in-process cross-encoder below. "remote"
				// scores candidates with the System One decision model at RERANK_REMOTE_URL.
				// Remote sends the query tail and candidate text off-machine on every
				// activation, so it runs only when set explicitly. See rerank-remote.ts.
				// Unknown values are reported by validateConfig() and run as local.
				provider: rerankProvider,
				// Hugging Face model ID. Default is the classic English MS MARCO
				// cross-encoder (int8 ONNX, ~23MB, ~40ms/pair on Apple Silicon WASM).
				// For multilingual knowledge bases use a multilingual ONNX
				// cross-encoder instead (e.g. a jina/bge multilingual reranker export).
				model: env.RERANK_MODEL || DEFAULT_RERANK_MODEL,
				// Cache directory for the downloaded model (one-time ~23MB fetch).
				modelsDir: env.RERANK_MODELS_DIR || join(dataDir, "models"),
				// Candidate pool size fetched by dense retrieval when reranking is
				// active (overfetch). Must be >= activation.maxResults to be useful.
				// The default follows the effective provider: remote only when it is
				// usable (endpoint and key present), so a fallback to local keeps the
				// local pool. Local: 16 keeps a pool of 256-token documents inside the
				// timeout on the hardware measured for this default. Remote: one call
				// carries the whole pool, and 30 was the benchmark's quality point
				// (at most REMOTE_RERANK_MAX_CANDIDATES; see README).
				candidates: r.int(
					"RERANK_CANDIDATES",
					remoteUsable
						? DEFAULT_REMOTE_RERANK_CANDIDATES
						: DEFAULT_LOCAL_RERANK_CANDIDATES,
					1,
				),
				// Hard timeout per local rerank call; on expiry the result is abandoned
				// and dense-only ordering is used. Kept well under the 5 s plugin
				// budget, which also covers the embedding call and store reads.
				timeoutMs: r.int("RERANK_TIMEOUT_MS", 3_000, 100),
				// Query side of each pair: the last N tokens of the prompt (remote:
				// N x 4 characters). Capped so a long prompt cannot squeeze documents
				// out of the model window.
				maxQueryTokens: r.int("RERANK_MAX_QUERY_TOKENS", 128, 1),
				// Document side of each pair: the first N tokens of the candidate
				// (remote: N x 4 characters). A 128-token cap scored below BM25 on
				// SciFact abstracts; memory entries are mostly short, so 256 keeps
				// nearly all of them whole.
				maxDocTokens: r.int("RERANK_MAX_DOC_TOKENS", 256, 1),
				// Remote System One reranker, used only when provider is "remote": one call
				// per activation. The URL defaults to the systemone route on LLM_BASE_ENDPOINT;
				// direct TypeSafe (https://api.typesafe.ai/v1/systemone) needs RERANK_REMOTE_API_KEY.
				remote: {
					...remoteEndpoint,
					model: env.RERANK_REMOTE_MODEL || DEFAULT_RERANK_REMOTE_MODEL,
					// Budget for the whole gateway request, retry included. The clock starts
					// after the query embedding and store reads, and the Codex hook gives up
					// after 4 s, so this stays well under that. On expiry the batch uses dense ordering.
					timeoutMs: r.int(
						"RERANK_REMOTE_TIMEOUT_MS",
						DEFAULT_RERANK_REMOTE_TIMEOUT_MS,
						100,
					),
				},
			},
			// Precision gate: decides how many candidates are injected (see gate.ts).
			gate: {
				// ACTIVATION_PRECISION_GATE=off (or false, 0, no) disables the floors and the token budget.
				enabled: r.flag("ACTIVATION_PRECISION_GATE", true),
				// RERANK_FLOOR overrides the absolute rerank floor of the configured scorer.
				// Scores are sigmoids in [0, 1]. Unset: the per-model default in gate.ts.
				// A model without one uses the relative rule only.
				rerankFloor: r.optionalFloat("RERANK_FLOOR", 0, 1),
				// A candidate must reach this fraction of the best rerank score.
				rerankRelFrac: r.float(
					"RERANK_REL_FRAC",
					DEFAULT_RERANK_REL_FRAC,
					0,
					1,
				),
				// Dense-only ordering (rerank did not run): raw cosine must clear the similarity
				// threshold by this margin.
				denseMargin: r.float(
					"ACTIVATION_DENSE_MARGIN",
					DEFAULT_DENSE_MARGIN,
					0,
					1,
				),
				// Token budget for the injected entries (estimate: characters / 4). The top
				// entry is always kept. Codex truncates hook output near 2,500 tokens, so the
				// default stays at or below 2,000.
				tokenBudget: r.int("ACTIVATION_TOKEN_BUDGET", DEFAULT_TOKEN_BUDGET, 1),
				// Per-session dedupe TTL, measured from each injection. 0 disables suppression.
				sessionTtlMs: r.int("ACTIVATION_SESSION_TTL_MS", 1_800_000, 0),
				// Rank-time freshness dampening: rerank × (0.5 + 0.5·strength) instead of
				// rerank × strength. Off by default.
				strengthDampening: r.flag("ACTIVATION_STRENGTH_DAMPENING", false),
			},
		},
	} as const;

	// ── Cross-field checks on the parsed values ───────────────────────────────

	// The contradiction scan band is [contradictionMinSimilarity, reconsolidationThreshold).
	// An explicit CONTRADICTION_MIN_SIMILARITY at or above the reconsolidation
	// threshold leaves the band empty, which silently disables the scan, so it is
	// an error. An unset one is not checked: a deployment may lower the
	// reconsolidation threshold (for example for a local embedding model) and never
	// set the band.
	const { reconsolidationThreshold, contradictionMinSimilarity } =
		config.consolidation;
	if (
		env.CONTRADICTION_MIN_SIMILARITY?.trim() &&
		contradictionMinSimilarity >= reconsolidationThreshold
	) {
		r.issues.push(
			`CONTRADICTION_MIN_SIMILARITY (${contradictionMinSimilarity}) must be strictly below RECONSOLIDATION_SIMILARITY_THRESHOLD (${reconsolidationThreshold}): the contradiction scan band is empty otherwise.`,
		);
	}

	const explicitPaths = EXPLICIT_PATH_VARS.filter(
		({ name }) => env[name] !== undefined && env[name] !== "",
	).map(({ name, missing }) => ({ name, path: env[name] ?? "", missing }));

	return { config, issues: r.issues, explicitPaths };
}

const parsed = parseEnv(process.env);

export const config = parsed.config;

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

	// Nothing found — return the preferred new location as the hint.
	return envFileCandidates().find((p) => existsSync(p)) ?? preferredEnvFile();
}

/**
 * Arguments the server adds when it spawns the daemon. The spawned daemon uploads
 * every 5 minutes unless DAEMON_UPLOAD_INTERVAL_SECONDS is set: the daemon's
 * --interval flag would override that variable, so the flag is passed only when
 * the variable is not set.
 */
export function spawnedDaemonIntervalArgs(
	uploadIntervalSeconds: number | null,
): string[] {
	return uploadIntervalSeconds === null ? ["--interval=300"] : [];
}

/**
 * True when the remote reranker is the scorer the server will use (see
 * isRemoteRerankUsable). Reads the parsed config, so tests may adjust it.
 */
export function remoteRerankIsActive(): boolean {
	const { enabled, provider, remote } = config.activation.rerank;
	return isRemoteRerankUsable({
		enabled,
		provider,
		url: remote.url,
		apiKey: remote.apiKey,
	});
}

/**
 * Every configuration error, as messages for the operator. The environment is
 * parsed once, at import (parseEnv); its issues come first, followed by checks
 * on the parsed config and on the credentials.
 */
export function validateConfig(): string[] {
	const errors: string[] = [...parsed.issues];

	const envPath = resolveEnvFilePath();

	// Validate that at least one LLM credential source is configured.
	// Per-provider credentials (ANTHROPIC_API_KEY etc.) take precedence over the
	// unified endpoint (LLM_BASE_ENDPOINT + LLM_API_KEY). Having any one of them
	// configured is sufficient — the runtime routing will use whichever applies.
	const hasAnthropic = !!(
		config.llm.anthropic.apiKey?.trim() &&
		config.llm.anthropic.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const hasOpenAI = !!(
		config.llm.openai.apiKey?.trim() &&
		config.llm.openai.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const hasGoogle = !!(
		config.llm.google.apiKey?.trim() &&
		config.llm.google.apiKey.trim() !== PLACEHOLDER_API_KEY
	);
	const unifiedApiKey = config.llm.apiKey?.trim() ?? "";
	const unifiedEndpoint = config.llm.baseEndpoint?.trim() ?? "";
	const hasUnified =
		!!(unifiedApiKey && unifiedApiKey !== PLACEHOLDER_API_KEY) &&
		!!(unifiedEndpoint && unifiedEndpoint !== PLACEHOLDER_ENDPOINT);

	// Claude subscription OAuth tokens count as a valid Anthropic credential.
	// hasOAuthTokens() does a full JSON parse so a corrupt token file does not
	// suppress the startup error while runtime LLM calls still fail.
	const hasAnthropicOAuth = hasOAuthTokens();

	if (
		!hasAnthropic &&
		!hasOpenAI &&
		!hasGoogle &&
		!hasUnified &&
		!hasAnthropicOAuth
	) {
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
				`No LLM credentials configured. Edit ${envPath} and set one of:\n    ANTHROPIC_API_KEY (direct Anthropic)\n    OPENAI_API_KEY (direct OpenAI-compatible)\n    GOOGLE_API_KEY (direct Google)\n    LLM_BASE_ENDPOINT + LLM_API_KEY (unified proxy)\n  Or run \`knowledge-server claude-auth\` to authenticate with a Claude Pro/Max subscription.`,
			);
		}
	}

	// Warn when a dedicated embedding endpoint is configured but no API key is
	// available. This silently produces empty-string credentials and causes
	// authentication failures at runtime — surface it early at startup instead.
	// Affected setup: Ollama embeddings + cloud LLM where OPENAI_API_KEY is not set
	// and LLM_API_KEY is the cloud provider key (not an OpenAI-compatible key).
	if (config.embedding.baseURL) {
		const embeddingApiKey = config.embedding.apiKey?.trim() || "";
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
	// A key sent over plain http to a non-loopback host crosses the network unencrypted.
	warnKeyOverPlainHttp(
		"LLM_BASE_ENDPOINT",
		config.llm.baseEndpoint,
		config.llm.apiKey,
	);
	warnKeyOverPlainHttp(
		"OPENAI_BASE_URL",
		config.llm.openai.baseURL,
		config.llm.openai.apiKey || config.llm.apiKey,
	);
	warnKeyOverPlainHttp(
		"EMBEDDING_BASE_URL",
		config.embedding.baseURL,
		config.embedding.apiKey,
	);

	// Enforce a minimum token length when a fixed admin token is configured.
	// A short token is trivial to brute-force over a loopback connection — 16 chars
	// of a random hex string gives ~64 bits of entropy, a reasonable lower bound.
	// Random startup-generated tokens (24 bytes → 48 hex chars, ~192 bits) always pass.
	// Prefer at least 24 hex chars (openssl rand -hex 24) for a stronger guarantee.
	if (config.adminToken !== null) {
		if (config.adminToken.length < 16) {
			errors.push(
				"KNOWLEDGE_ADMIN_TOKEN must be at least 16 characters. Use a long random value (e.g. openssl rand -hex 24).",
			);
		} else if (config.adminToken === PLACEHOLDER_ADMIN_TOKEN) {
			errors.push(
				"KNOWLEDGE_ADMIN_TOKEN is still the template placeholder. Replace it with a long random value (e.g. openssl rand -hex 24).",
			);
		}
	}

	// Note: loopback-host validation is done post-registry in index.ts because
	// the resolved host may come from config.jsonc (not just KNOWLEDGE_HOST env var),
	// and the registry is created after validateConfig() runs.

	// Remote reranking sends the query tail and candidate text to its endpoint, so its
	// settings are checked only when the remote reranker is the active scorer.
	if (remoteRerankIsActive()) {
		const { remote, candidates, maxDocTokens, maxQueryTokens } =
			config.activation.rerank;
		if (remote.url && !isHttpUrl(remote.url)) {
			errors.push(
				"The remote reranker URL (RERANK_REMOTE_URL, or LLM_BASE_ENDPOINT + /decisions/v1/systemone) must be an absolute http or https URL.",
			);
		}
		if (remote.url) {
			warnKeyOverPlainHttp("RERANK_REMOTE_URL", remote.url, remote.apiKey);
		}
		if (candidates > REMOTE_RERANK_MAX_CANDIDATES) {
			errors.push(
				`RERANK_CANDIDATES must be at most ${REMOTE_RERANK_MAX_CANDIDATES} with RERANK_PROVIDER=remote (got ${candidates}): the remote reranker scores the whole candidate pool in one call.`,
			);
		}
		const requestChars =
			candidates * (maxDocTokens * 4 + REMOTE_RERANK_QUESTION_CHARS) +
			maxQueryTokens * 4;
		if (requestChars > REMOTE_RERANK_REQUEST_CHAR_BUDGET) {
			errors.push(
				`The remote rerank request could reach ${requestChars} characters (RERANK_CANDIDATES × (RERANK_MAX_DOC_TOKENS × 4 + ${REMOTE_RERANK_QUESTION_CHARS}) + RERANK_MAX_QUERY_TOKENS × 4), above the ${REMOTE_RERANK_REQUEST_CHAR_BUDGET}-character budget. Lower RERANK_CANDIDATES or RERANK_MAX_DOC_TOKENS.`,
			);
		}
	}

	// Error only when the user explicitly configured a path variable but the file
	// doesn't exist — a typo or stale path is worth surfacing as a hard error.
	// When using the default auto-detected path, a missing file is non-fatal: the
	// source is disabled at startup with a warning instead.
	for (const { name, path, missing } of parsed.explicitPaths) {
		if (!existsSync(path)) {
			errors.push(`${name} is set but ${missing} not found at ${path}.`);
		}
	}

	return errors;
}
