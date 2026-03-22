import {
	DEFAULT_CONFIG,
	DEFAULT_CONFIG_PATH,
	loadConfigFile,
	resolvePostgresUri,
	resolveSqlitePath,
} from "../config-file.js";
import type { KnowledgeServerConfig, StoreConfig } from "../config-file.js";
import { DomainRouter } from "../consolidation/domain-router.js";
import { logger } from "../logger.js";
import { KnowledgeDB } from "./database.js";
import type { IKnowledgeDB } from "./interface.js";
import { PostgresKnowledgeDB } from "./pg-database.js";

/**
 * StoreRegistry — manages a configured set of IKnowledgeDB instances.
 *
 * Replaces the old createKnowledgeDB() factory. Instead of a single DB,
 * the registry holds 1..N stores:
 *
 *   writableStore() — the one store that accepts consolidation writes
 *   readStores()    — all stores (including writable) used for activation reads
 *
 * The writable store is always included in readStores() so activation draws
 * from the full knowledge corpus.
 *
 * Configuration is loaded from ~/.config/knowledge-server/config.jsonc.
 * If no config file exists, a single default SQLite store is used (zero-config).
 */
export class StoreRegistry {
	private stores: Map<string, IKnowledgeDB>;
	private writable: IKnowledgeDB;
	private readable: IKnowledgeDB[];
	/** Domain router for consolidation routing. Null when no domains are configured. */
	readonly domainRouter: DomainRouter | null;
	/**
	 * Stable user identifier for multi-user shared DB setups.
	 * Scopes the consolidation cursor and episode log per user.
	 * Resolved from KNOWLEDGE_USER_ID env var → config.jsonc userId → hostname → "default".
	 */
	readonly userId: string;

	private constructor(
		stores: Map<string, IKnowledgeDB>,
		writableId: string,
		config: KnowledgeServerConfig,
	) {
		this.stores = stores;
		const writable = stores.get(writableId);
		if (!writable) {
			throw new Error(
				`StoreRegistry: writable store "${writableId}" not found`,
			);
		}
		this.writable = writable;
		this.readable = Array.from(stores.values());
		this.domainRouter =
			config.domains.length > 0
				? new DomainRouter(config, stores, writable)
				: null;
		this.userId = config.userId;
	}

	/** The store that receives consolidation writes. */
	writableStore(): IKnowledgeDB {
		return this.writable;
	}

	/**
	 * All stores used for activation reads.
	 * Includes the writable store — activation draws from the full corpus.
	 * Returns a shallow copy — callers cannot mutate the registry's internal list.
	 */
	readStores(): IKnowledgeDB[] {
		return [...this.readable];
	}

	/** Close all store connections. */
	async close(): Promise<void> {
		await Promise.all(Array.from(this.stores.values()).map((db) => db.close()));
	}

	// ── Factory ─────────────────────────────────────────────────────────────

	/**
	 * Create and initialize a StoreRegistry from config.jsonc.
	 *
	 * Falls back to a single default SQLite store if no config file exists.
	 *
	 * @param configPath Override config file path — used in tests.
	 */
	static async create(
		configPath = DEFAULT_CONFIG_PATH,
	): Promise<StoreRegistry> {
		let fileConfig: KnowledgeServerConfig | null;
		try {
			fileConfig = loadConfigFile(configPath);
		} catch (e) {
			throw new Error(
				`Failed to load config file: ${e instanceof Error ? e.message : String(e)}`,
			);
		}

		const config = fileConfig ?? DEFAULT_CONFIG;

		if (!fileConfig) {
			logger.log("[db] No config.jsonc found — using default SQLite store.");
		}

		// Resolve writableId before any I/O — it comes from config, not from DB init.
		const writableId = config.stores.find((s) => s.writable)?.id ?? null;
		if (!writableId) {
			// Validated in loadConfigFile — should never happen, but guard anyway
			throw new Error("StoreRegistry: no writable store configured");
		}

		// Initialise all stores in parallel — no ordering dependency between them.
		const initialized = await Promise.all(
			config.stores.map(async (storeConfig) => ({
				id: storeConfig.id,
				db: await initStore(storeConfig),
			})),
		);

		const stores = new Map<string, IKnowledgeDB>(
			initialized.map(({ id, db }) => [id, db]),
		);

		const registry = new StoreRegistry(stores, writableId, config);

		// Warn if writable SQLite stores coexist with remote Postgres stores.
		// In this topology, a remote consolidation server cannot write to local SQLite —
		// any entries the LLM classifies as belonging to the SQLite domain will be
		// misrouted to the fallback store instead. This is a configuration smell.
		warnIfMixedTopology(config);

		return registry;
	}
}

// ── Store initializer ────────────────────────────────────────────────────────

async function initStore(storeConfig: StoreConfig): Promise<IKnowledgeDB> {
	if (storeConfig.kind === "sqlite") {
		const path = resolveSqlitePath(storeConfig);
		logger.log(`[db] Store "${storeConfig.id}": SQLite at ${path}`);
		return new KnowledgeDB(path);
	}

	if (storeConfig.kind === "postgres") {
		let uri: string;
		try {
			uri = resolvePostgresUri(storeConfig);
		} catch (e) {
			throw new Error(
				`Store "${storeConfig.id}": ${e instanceof Error ? e.message : String(e)}`,
			);
		}

		logger.log(
			`[db] Store "${storeConfig.id}": PostgreSQL at ${redactUri(uri)}`,
		);
		const db = new PostgresKnowledgeDB(uri);
		try {
			await db.initialize();
		} catch (e) {
			const msg = e instanceof Error ? e.message : String(e);
			throw new Error(
				`Store "${storeConfig.id}": failed to connect to PostgreSQL. Check that the server is reachable and credentials are correct. Original error: ${msg}`,
			);
		}
		return db;
	}

	// TypeScript exhaustiveness — kind is validated in config-file.ts
	throw new Error(`Unknown store kind: ${(storeConfig as StoreConfig).kind}`);
}

/**
 * Warn if the config has writable SQLite stores alongside remote Postgres stores.
 *
 * This topology is problematic for remote consolidation: a knowledge server
 * running on a different machine can connect to remote Postgres but cannot
 * write to a local SQLite file. Entries classified as belonging to the SQLite
 * domain would be misrouted to the fallback store rather than silently lost,
 * but the operator should be aware of the limitation.
 *
 * Detection: a Postgres URI is considered "remote" if its host is not localhost
 * or 127.0.0.1 and is not a Unix socket path.
 */
function warnIfMixedTopology(config: KnowledgeServerConfig): void {
	const hasSqliteWritable = config.stores.some(
		(s) => s.kind === "sqlite" && s.writable,
	);
	if (!hasSqliteWritable) return;

	const hasRemotePostgres = config.stores.some((s) => {
		if (s.kind !== "postgres") return false;
		// Use resolvePostgresUri to match the actual URI used during initStore,
		// avoiding silent divergence if the env-var naming convention changes.
		let uri: string;
		try {
			uri = resolvePostgresUri(s);
		} catch {
			return false; // URI not configured — not remote
		}
		try {
			const url = new URL(uri);
			const host = url.hostname;
			// Unix socket connections have an empty or path-like hostname.
			if (!host || host.startsWith("/")) return false;
			return host !== "localhost" && host !== "127.0.0.1" && host !== "::1";
		} catch {
			return false;
		}
	});

	if (hasRemotePostgres) {
		logger.warn(
			"[config] Mixed topology detected: writable SQLite store(s) alongside remote Postgres store(s). " +
				"If this knowledge server is accessed remotely (e.g. via the managed product or a team setup), " +
				"entries classified as belonging to SQLite-backed domains cannot be written from the remote context " +
				"and will fall back to the default store. " +
				"Consider using Postgres for all writable stores in remote deployments.",
		);
	}
}

/** Redact password from a postgres URI for safe logging. */
function redactUri(uri: string): string {
	try {
		const url = new URL(uri);
		if (url.password) url.password = "***";
		return url.toString();
	} catch {
		return uri.replace(/:([^@/]+)@/, ":***@");
	}
}
