import { homedir } from "node:os";
import { normalize } from "node:path";
import type {
	DomainConfig,
	KnowledgeServerConfig,
	ProjectConfig,
} from "../config-file.js";
import type { IKnowledgeDB } from "../db/interface.js";
import { logger } from "../logger.js";

/**
 * The result of resolving a domain for a set of episodes.
 */
export interface DomainResolution {
	/** The resolved domain id, or undefined if no domains are configured. */
	domainId: string | undefined;
	/** The store that should receive writes for this domain. */
	store: IKnowledgeDB;
	/**
	 * Whether the resolved store is currently unavailable (failed to connect at startup).
	 * When true, the consolidation engine should skip this episode group and leave
	 * the rows in pending_episodes for retry on the next run.
	 */
	storeUnavailable: boolean;
	/**
	 * Domain context to inject into the LLM extraction prompt.
	 * Null when no domains are configured (single-store mode).
	 */
	domainContext: DomainContext | null;
}

/**
 * Domain context passed to the LLM extraction prompt.
 * Tells the LLM which domains exist and which one is the default for this session.
 */
export interface DomainContext {
	/** All configured domains — injected so the LLM knows the valid assignment targets. */
	domains: Array<{ id: string; description: string }>;
	/** The default domain for this session's project (from path matching). */
	defaultDomain: string;
}

/**
 * DomainRouter — resolves a session directory to a domain and its backing store.
 *
 * Resolution order:
 * 1. If no domains are configured → return the writable store, no domain routing.
 * 2. Match the session directory against project path prefixes (longest match wins).
 * 3. If a project matches → use its default_domain as the domain.
 * 4. If no project matches → use the first domain as the fallback.
 *
 * The resolved domain tells the consolidation engine:
 * - Which store to write extracted entries to (domain.store)
 * - What domain context to inject into the LLM extraction prompt (for per-entry
 *   domain assignment within the chunk — entries may belong to any domain)
 *
 * Note: domain routing determines the *default* and provides context to the LLM.
 * The LLM assigns each extracted entry to a domain individually — a work session
 * can still produce personal entries (e.g. personal tool preferences).
 */
export class DomainRouter {
	private domains: DomainConfig[];
	private projects: Array<ProjectConfig & { normalizedPath: string }>;
	private stores: Map<string, IKnowledgeDB>;
	private fallbackStore: IKnowledgeDB;
	private unavailableStoreIds: ReadonlySet<string>;
	/** ID of the fallback (primary writable) store — used for unavailability checks. */
	private fallbackStoreId: string;

	constructor(
		config: KnowledgeServerConfig,
		stores: Map<string, IKnowledgeDB>,
		fallbackStore: IKnowledgeDB,
		unavailableStoreIds: ReadonlySet<string> = new Set(),
	) {
		this.domains = config.domains;
		// Pre-normalize project paths once at construction so bestProjectMatch
		// doesn't re-normalize on every resolve() call.
		// Note: ~ is already expanded by config-file.ts validateProject at load time.
		this.projects = config.projects.map((p) => ({
			...p,
			normalizedPath: normalize(p.path),
		}));
		this.stores = stores;
		this.fallbackStore = fallbackStore;
		this.unavailableStoreIds = unavailableStoreIds;
		// Derive fallback store ID from the stores map — avoids hardcoding a name,
		// since users can name their stores anything in config.jsonc. Without this,
		// a store named "personal" (not "default") would never be detected as
		// unavailable in single-store mode, silently suppressing the skip signal.
		let fallbackStoreId = "";
		for (const [id, db] of stores) {
			if (db === fallbackStore) {
				fallbackStoreId = id;
				break;
			}
		}
		if (!fallbackStoreId) {
			logger.warn(
				"[domain-router] fallbackStore not found in stores map — the no-domain fallback path will not report store unavailability. This should not happen in normal operation.",
			);
		}
		this.fallbackStoreId = fallbackStoreId;
	}

	/**
	 * Resolve the domain and store for a given session directory.
	 *
	 * @param directory The working directory of the session being consolidated.
	 *                  May be empty or undefined for sessions without path context.
	 */
	resolve(directory: string): DomainResolution {
		// No domains configured — single-store mode, no routing needed.
		if (this.domains.length === 0) {
			return {
				domainId: undefined,
				store: this.fallbackStore,
				storeUnavailable: this.unavailableStoreIds.has(this.fallbackStoreId),
				domainContext: null,
			};
		}

		// Find the best matching project (longest path prefix match).
		const normalizedDir = directory
			? normalize(directory.replace(/^~/, homedir()))
			: "";
		const matchedProject = this.bestProjectMatch(normalizedDir);

		// Determine default domain: matched project's domain, or first domain as fallback.
		const defaultDomainId =
			matchedProject?.default_domain ?? this.domains[0].id;
		const defaultDomain = this.domains.find((d) => d.id === defaultDomainId);

		if (!defaultDomain) {
			// Shouldn't happen — validated at config load time.
			logger.warn(
				`[domain-router] Default domain "${defaultDomainId}" not found — falling back to first domain.`,
			);
		}

		const resolvedDomain = defaultDomain ?? this.domains[0];
		const store = this.stores.get(resolvedDomain.store) ?? this.fallbackStore;

		if (matchedProject) {
			logger.log(
				`[domain-router] "${normalizedDir}" → project match → domain "${resolvedDomain.id}" → store "${resolvedDomain.store}"`,
			);
		} else {
			logger.log(
				`[domain-router] "${normalizedDir}" → no project match → default domain "${resolvedDomain.id}" → store "${resolvedDomain.store}"`,
			);
		}

		const storeUnavailable = this.unavailableStoreIds.has(resolvedDomain.store);

		return {
			domainId: resolvedDomain.id,
			store,
			storeUnavailable,
			domainContext: {
				domains: this.domains.map((d) => ({
					id: d.id,
					description: d.description,
				})),
				defaultDomain: resolvedDomain.id,
			},
		};
	}

	/**
	 * Resolve the store for a specific domain id.
	 * Used for per-entry routing when the LLM assigns a domain to an extracted entry.
	 *
	 * Returns undefined when domainId is undefined or not found —
	 * callers fall back to the chunk's default resolved store.
	 */
	resolveStore(domainId: string | undefined): IKnowledgeDB | undefined {
		if (!domainId) return undefined;
		const domain = this.domains.find((d) => d.id === domainId);
		if (!domain) return undefined;
		return this.stores.get(domain.store);
	}

	/**
	 * Find the project whose path prefix best matches the given directory.
	 * Longest match wins — more specific project paths take precedence.
	 */
	private bestProjectMatch(directory: string): ProjectConfig | null {
		if (!directory) return null;

		let best: ProjectConfig | null = null;
		let bestLen = 0;

		for (const project of this.projects) {
			// Use pre-normalized path from constructor (avoids re-normalizing per call).
			const projectPath = project.normalizedPath;
			// Match if directory starts with the project path
			// (add trailing separator to avoid partial directory name matches)
			const prefix = projectPath.endsWith("/")
				? projectPath
				: `${projectPath}/`;
			if (
				(directory === projectPath || directory.startsWith(prefix)) &&
				projectPath.length > bestLen
			) {
				best = project;
				bestLen = projectPath.length;
			}
		}

		return best;
	}
}
