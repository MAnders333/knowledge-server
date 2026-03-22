import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/**
 * Store configuration — one entry per knowledge database.
 */
export interface StoreConfig {
	/** Unique identifier for this store — referenced by domain config (future). */
	id: string;
	/** Storage backend. */
	kind: "sqlite" | "postgres";
	/**
	 * Whether this store accepts consolidation writes.
	 * Exactly one store must be writable. All others are read-only for activation.
	 */
	writable: boolean;
	/**
	 * SQLite only: path to the .db file.
	 * Defaults to ~/.local/share/knowledge-server/knowledge.db
	 */
	path?: string;
	/**
	 * Postgres only: connection URI (postgres://user:pass@host:5432/dbname).
	 * Can also be set via the STORE_<ID>_URI environment variable, which
	 * takes precedence over this field.
	 */
	uri?: string;
}

/**
 * Domain configuration — a semantic category of knowledge.
 *
 * Each domain maps to exactly one store (by store id) and has a description
 * that the LLM uses to classify knowledge entries during consolidation.
 */
export interface DomainConfig {
	/** Unique domain identifier — used in routing and as a tag on entries. */
	id: string;
	/**
	 * Human-readable description of what belongs in this domain.
	 * Injected into the LLM extraction prompt to guide classification.
	 * Example: "Personal preferences, individual workflows, user-specific setups"
	 */
	description: string;
	/** ID of the store that receives consolidation writes for this domain. */
	store: string;
}

/**
 * Project configuration — maps a local directory path to a default domain.
 *
 * When a session's working directory matches a project path prefix, that
 * project's default_domain is used as the prior for LLM classification.
 * This is a hint, not a hard constraint — the LLM may still classify an
 * individual entry differently (e.g. a personal preference found in a work project).
 */
export interface ProjectConfig {
	/**
	 * Absolute path prefix (~ is expanded to the home directory).
	 * Sessions whose working directory starts with this path match this project.
	 */
	path: string;
	/** Domain id to use as the default for sessions in this project. */
	default_domain: string;
}

/**
 * Parsed and validated config.jsonc file.
 */
export interface KnowledgeServerConfig {
	stores: StoreConfig[];
	domains: DomainConfig[];
	projects: ProjectConfig[];
}

// ── Paths ─────────────────────────────────────────────────────────────────────

export const DEFAULT_CONFIG_PATH = join(
	homedir(),
	".config",
	"knowledge-server",
	"config.jsonc",
);

export const DEFAULT_SQLITE_PATH = join(
	homedir(),
	".local",
	"share",
	"knowledge-server",
	"knowledge.db",
);

// ── JSONC strip-comments ──────────────────────────────────────────────────────

/**
 * Strip // line comments and /* block comments from a JSONC string.
 * Handles comments inside strings correctly (does not strip them).
 */
function stripJsoncComments(jsonc: string): string {
	let result = "";
	let i = 0;
	let inString = false;

	while (i < jsonc.length) {
		const ch = jsonc[i];

		if (inString) {
			result += ch;
			if (ch === "\\" && i + 1 < jsonc.length) {
				// Escaped character inside string — include both chars, skip stripping logic
				result += jsonc[i + 1];
				i += 2;
				continue;
			}
			if (ch === '"') inString = false;
			i++;
			continue;
		}

		if (ch === '"') {
			inString = true;
			result += ch;
			i++;
			continue;
		}

		// Line comment
		if (ch === "/" && jsonc[i + 1] === "/") {
			while (i < jsonc.length && jsonc[i] !== "\n") i++;
			continue;
		}

		// Block comment
		if (ch === "/" && jsonc[i + 1] === "*") {
			const start = i;
			i += 2;
			while (i < jsonc.length && !(jsonc[i] === "*" && jsonc[i + 1] === "/")) {
				i++;
			}
			if (i >= jsonc.length) {
				throw new Error(
					`Unterminated block comment in config file starting at position ${start}`,
				);
			}
			i += 2; // skip closing */
			continue;
		}

		result += ch;
		i++;
	}

	return result;
}

// ── Loader ────────────────────────────────────────────────────────────────────

/**
 * Load and parse the config.jsonc file.
 *
 * Returns null if the file does not exist (caller falls back to defaults).
 * Throws a descriptive error if the file exists but is invalid.
 *
 * @param configPath Override path — used in tests and by migrate-config.
 */
export function loadConfigFile(
	configPath = DEFAULT_CONFIG_PATH,
): KnowledgeServerConfig | null {
	if (!existsSync(configPath)) {
		return null;
	}

	let raw: string;
	try {
		raw = readFileSync(configPath, "utf8");
	} catch (e) {
		throw new Error(
			`Failed to read config file at ${configPath}: ${e instanceof Error ? e.message : String(e)}`,
		);
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(stripJsoncComments(raw));
	} catch (e) {
		throw new Error(
			`Failed to parse config file at ${configPath}: ${e instanceof Error ? e.message : String(e)}`,
		);
	}

	return validateConfigFile(parsed, configPath);
}

// ── Validator ─────────────────────────────────────────────────────────────────

function validateConfigFile(
	raw: unknown,
	configPath: string,
): KnowledgeServerConfig {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		throw new Error(`config.jsonc must be a JSON object (got ${typeof raw})`);
	}

	const obj = raw as Record<string, unknown>;

	// stores
	if (!("stores" in obj)) {
		throw new Error(
			`config.jsonc at ${configPath} is missing required field "stores"`,
		);
	}
	if (!Array.isArray(obj.stores)) {
		throw new Error(`config.jsonc "stores" must be an array`);
	}
	if (obj.stores.length === 0) {
		throw new Error(`config.jsonc "stores" must contain at least one store`);
	}

	const stores: StoreConfig[] = obj.stores.map((s, i) =>
		validateStore(s, i, configPath),
	);

	// Duplicate store IDs would silently clobber each other in StoreRegistry's Map.
	const ids = stores.map((s) => s.id);
	const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
	if (dupes.length > 0) {
		throw new Error(
			`config.jsonc "stores" contains duplicate ids: ${[...new Set(dupes)].join(", ")}`,
		);
	}

	// Exactly one writable store required
	const writableStores = stores.filter((s) => s.writable);
	if (writableStores.length === 0) {
		throw new Error(
			`config.jsonc "stores" must have exactly one store with "writable": true`,
		);
	}
	if (writableStores.length > 1) {
		throw new Error(
			`config.jsonc "stores" has ${writableStores.length} writable stores — exactly one is required. ` +
				`Writable store ids: ${writableStores.map((s) => s.id).join(", ")}`,
		);
	}

	// domains — optional, defaults to empty (single-store, no routing)
	const domains: DomainConfig[] = [];
	if ("domains" in obj) {
		if (!Array.isArray(obj.domains)) {
			throw new Error(`config.jsonc "domains" must be an array`);
		}
		const storeIds = new Set(stores.map((s) => s.id));
		for (let i = 0; i < obj.domains.length; i++) {
			domains.push(validateDomain(obj.domains[i], i, storeIds));
		}
		// Validate no duplicate domain ids
		const domainIds = domains.map((d) => d.id);
		const domainDupes = domainIds.filter(
			(id, i) => domainIds.indexOf(id) !== i,
		);
		if (domainDupes.length > 0) {
			throw new Error(
				`config.jsonc "domains" contains duplicate ids: ${[...new Set(domainDupes)].join(", ")}`,
			);
		}
	}

	// projects — optional, defaults to empty
	const projects: ProjectConfig[] = [];
	if ("projects" in obj) {
		if (!Array.isArray(obj.projects)) {
			throw new Error(`config.jsonc "projects" must be an array`);
		}
		const domainIds = new Set(domains.map((d) => d.id));
		for (let i = 0; i < obj.projects.length; i++) {
			projects.push(validateProject(obj.projects[i], i, domainIds));
		}
	}

	return { stores, domains, projects };
}

function validateDomain(
	raw: unknown,
	index: number,
	storeIds: Set<string>,
): DomainConfig {
	const loc = `config.jsonc domains[${index}]`;
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		throw new Error(`${loc} must be an object`);
	}
	const d = raw as Record<string, unknown>;

	if (typeof d.id !== "string" || !d.id.trim()) {
		throw new Error(`${loc} must have a non-empty string "id"`);
	}
	if (!/^[a-z0-9_-]+$/.test(d.id)) {
		throw new Error(
			`${loc} id "${d.id}" is invalid — use only lowercase letters, digits, hyphens, and underscores`,
		);
	}
	if (typeof d.description !== "string" || !d.description.trim()) {
		throw new Error(`${loc} must have a non-empty string "description"`);
	}
	if (typeof d.store !== "string" || !d.store.trim()) {
		throw new Error(`${loc} must have a non-empty string "store"`);
	}
	if (!storeIds.has(d.store)) {
		throw new Error(
			`${loc} references unknown store "${d.store}" — must be one of: ${[...storeIds].join(", ")}`,
		);
	}

	return {
		id: d.id as string,
		description: d.description as string,
		store: d.store as string,
	};
}

function validateProject(
	raw: unknown,
	index: number,
	domainIds: Set<string>,
): ProjectConfig {
	const loc = `config.jsonc projects[${index}]`;
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		throw new Error(`${loc} must be an object`);
	}
	const p = raw as Record<string, unknown>;

	if (typeof p.path !== "string" || !p.path.trim()) {
		throw new Error(`${loc} must have a non-empty string "path"`);
	}
	if (typeof p.default_domain !== "string" || !p.default_domain.trim()) {
		throw new Error(`${loc} must have a non-empty string "default_domain"`);
	}
	if (domainIds.size > 0 && !domainIds.has(p.default_domain)) {
		throw new Error(
			`${loc} references unknown domain "${p.default_domain}" — must be one of: ${[...domainIds].join(", ")}`,
		);
	}

	// Expand ~ to home directory
	const expandedPath = (p.path as string).replace(/^~/, homedir());

	return {
		path: expandedPath,
		default_domain: p.default_domain as string,
	};
}

function validateStore(
	raw: unknown,
	index: number,
	configPath: string,
): StoreConfig {
	const loc = `config.jsonc stores[${index}]`;

	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		throw new Error(`${loc} must be an object`);
	}

	const s = raw as Record<string, unknown>;

	// id
	if (typeof s.id !== "string" || !s.id.trim()) {
		throw new Error(`${loc} must have a non-empty string "id"`);
	}
	if (!/^[a-z0-9_-]+$/.test(s.id)) {
		throw new Error(
			`${loc} id "${s.id}" is invalid — use only lowercase letters, digits, hyphens, and underscores`,
		);
	}

	// kind
	if (s.kind !== "sqlite" && s.kind !== "postgres") {
		throw new Error(
			`${loc} "kind" must be "sqlite" or "postgres" (got "${s.kind}")`,
		);
	}

	// writable
	if (typeof s.writable !== "boolean") {
		throw new Error(`${loc} "writable" must be a boolean`);
	}

	// kind-specific validation
	if (s.kind === "sqlite") {
		if (s.path !== undefined && typeof s.path !== "string") {
			throw new Error(`${loc} sqlite "path" must be a string if provided`);
		}
	}

	if (s.kind === "postgres") {
		// URI can be omitted if STORE_<ID>_URI env var is set — validated at runtime
		if (s.uri !== undefined && typeof s.uri !== "string") {
			throw new Error(`${loc} postgres "uri" must be a string if provided`);
		}
	}

	return {
		id: s.id as string,
		kind: s.kind as "sqlite" | "postgres",
		writable: s.writable as boolean,
		...(s.path !== undefined && { path: s.path as string }),
		...(s.uri !== undefined && { uri: s.uri as string }),
	};
}

// ── Effective config resolver ─────────────────────────────────────────────────

/**
 * Resolve the effective URI for a postgres store.
 *
 * Priority:
 *   1. STORE_<ID>_URI env var (uppercase ID, hyphens → underscores)
 *   2. "uri" field in config.jsonc
 *
 * Throws if neither is set.
 */
export function resolvePostgresUri(store: StoreConfig): string {
	const envKey = `STORE_${store.id.toUpperCase().replace(/-/g, "_")}_URI`;
	const fromEnv = process.env[envKey];
	if (fromEnv) return fromEnv;
	if (store.uri) return store.uri;
	throw new Error(
		`Postgres store "${store.id}" has no URI. ` +
			`Set "${envKey}" environment variable or add "uri" to the store config.`,
	);
}

/**
 * Resolve the effective SQLite path for a sqlite store.
 * Falls back to the default path if not specified.
 */
export function resolveSqlitePath(store: StoreConfig): string {
	return store.path ?? DEFAULT_SQLITE_PATH;
}

// ── Default config (no config file) ──────────────────────────────────────────

/**
 * Default config used when no config.jsonc exists.
 * Single SQLite store at the default path — zero-config UX for new users.
 */
export const DEFAULT_CONFIG: KnowledgeServerConfig = {
	stores: [
		{
			id: "default",
			kind: "sqlite",
			writable: true,
		},
	],
	domains: [],
	projects: [],
};
