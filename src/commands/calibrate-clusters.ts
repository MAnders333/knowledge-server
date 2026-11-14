/**
 * `knowledge-server calibrate --clusters [--thresholds <list>] [--samples <t>]`
 *
 * Read-only simulation of the synthesis clustering on the real entries of every
 * writable store. Runs the same greedy clustering core (greedyClusterAsync) as
 * production, so the threshold the operator picks is the one consolidation uses.
 * Makes no writes and no LLM or embedding calls.
 *
 * Stores are opened without StoreRegistry (which runs migrations and can drop
 * tables) and without logging paths or URIs.
 */
import {
	DEFAULT_CONFIG,
	defaultConfigPath,
	loadConfigFile,
	resolvePostgresUri,
	resolveSqlitePath,
} from "../config-file.js";
import type { StoreConfig } from "../config-file.js";
import { config } from "../config.js";
import {
	estimateSynthesisPeerTokens,
	greedyClusterAsync,
} from "../consolidation/cluster.js";
import type {
	ClusterCentroidMode,
	GreedyCluster,
} from "../consolidation/cluster.js";
import type { IKnowledgeStore } from "../db/index.js";
import { PostgresKnowledgeDB } from "../db/postgres/index.js";
import { KnowledgeDB } from "../db/sqlite/index.js";
import type { KnowledgeEntry } from "../types.js";
import { errCode } from "../utils.js";
import { percentile } from "./calibrate.js";

/** Default sweep. Step 0.04 (not 0.02) keeps the sweep short: high thresholds are
 *  O(n^2 x dim), so at ~10k entries one run takes on the order of a minute and the
 *  default sweep (9 thresholds (+1 if the current threshold is not in the list) x 2
 *  modes) can take
 *  15-30 minutes. Use --thresholds for a narrower or finer sweep. */
export const DEFAULT_CLUSTER_SWEEP = [
	0.6, 0.64, 0.68, 0.72, 0.76, 0.8, 0.84, 0.88, 0.92,
];
export const CENTROID_MODES: ClusterCentroidMode[] = ["running", "seed"];

const SAMPLE_CLUSTERS = 3;
const SAMPLE_MEMBERS = 5;
const SNIPPET_CHARS = 100;

export type SimEntry = KnowledgeEntry & { embedding: number[] };

export interface ClusterRunSummary {
	clusterCount: number;
	singletons: number;
	/** Cluster size percentiles over ALL clusters (incl. singletons). */
	p50: number;
	p90: number;
	max: number;
	/** max / entries.length, 0 when no entries. */
	largestShare: number;
	/** Sum of estimateSynthesisPeerTokens over the largest cluster's members (all members, before the prompt cap). */
	largestTokens: number;
	/** Clusters with size >= minMembers. */
	eligible: number;
	/** Eligible clusters whose prompt upstream's cap would trim: more than maxPeers
	 *  members, or more than maxPromptTokens estimated tokens. */
	capped: number;
}

/**
 * Parse a comma-separated list of thresholds, e.g. "0.7,0.72,0.8".
 * Returns de-duplicated, ascending values. Throws on any value outside (0, 1].
 */
export function parseThresholdList(
	raw: string,
	flag = "--thresholds",
): number[] {
	// Number() rejects trailing junk ("0.7abc") that parseFloat would accept as 0.7.
	// Empty items are rejected explicitly because Number("") is 0.
	const values = raw
		.split(",")
		.map((s) => (s.trim() === "" ? Number.NaN : Number(s.trim())));
	const invalid =
		values.length === 0 ||
		values.some((v) => Number.isNaN(v) || v <= 0 || v > 1);
	if (raw.trim() === "" || invalid) {
		throw new Error(
			`${flag} must be a comma-separated list of numbers in (0, 1] (got "${raw}")`,
		);
	}
	return [...new Set(values)].sort((a, b) => a - b);
}

export function summarizeClusters(
	entries: ReadonlyArray<SimEntry>,
	clusters: ReadonlyArray<GreedyCluster>,
	opts: { minMembers: number; maxPeers: number; maxPromptTokens: number },
): ClusterRunSummary {
	if (clusters.length === 0) {
		return {
			clusterCount: 0,
			singletons: 0,
			p50: 0,
			p90: 0,
			max: 0,
			largestShare: 0,
			largestTokens: 0,
			eligible: 0,
			capped: 0,
		};
	}

	const byId = new Map(entries.map((e) => [e.id, e]));
	const sizes = clusters.map((c) => c.memberIds.length).sort((a, b) => a - b);
	const max = sizes[sizes.length - 1];
	const largest = clusters.reduce((a, b) =>
		b.memberIds.length > a.memberIds.length ? b : a,
	);

	let eligible = 0;
	let capped = 0;
	for (const cluster of clusters) {
		if (cluster.memberIds.length < opts.minMembers) continue;
		eligible++;
		const members = cluster.memberIds
			.map((id) => byId.get(id))
			.filter((e): e is SimEntry => e !== undefined);
		const tokens = members.reduce(
			(sum, e) => sum + estimateSynthesisPeerTokens(e),
			0,
		);
		if (members.length > opts.maxPeers || tokens > opts.maxPromptTokens) {
			capped++;
		}
	}

	const largestTokens = largest.memberIds.reduce((sum, id) => {
		const e = byId.get(id);
		return e ? sum + estimateSynthesisPeerTokens(e) : sum;
	}, 0);

	return {
		clusterCount: clusters.length,
		singletons: sizes.filter((s) => s === 1).length,
		p50: Math.round(percentile(sizes, 50)),
		p90: Math.round(percentile(sizes, 90)),
		max,
		largestShare: entries.length === 0 ? 0 : max / entries.length,
		largestTokens,
		eligible,
		capped,
	};
}

/** Open a store read-only. Never prints paths or URIs. */
async function openStoreReadOnly(store: StoreConfig): Promise<IKnowledgeStore> {
	if (store.kind === "sqlite") {
		const db = new KnowledgeDB(resolveSqlitePath(store), { readOnly: true });
		db.id = store.id;
		return db;
	}
	// No initialize(): the constructor does not connect or migrate; only SELECTs run.
	// readOnly makes the server reject writes as a second line of defence.
	const db = new PostgresKnowledgeDB(resolvePostgresUri(store), 1, {
		readOnly: true,
	});
	db.id = store.id;
	return db;
}

function describeOpenError(e: unknown): string {
	return errCode(e) ?? (e instanceof Error ? e.name : "error");
}

function requireValue(args: string[], flag: string): string {
	const idx = args.indexOf(flag);
	const value = args[idx + 1];
	if (value === undefined || value.startsWith("--")) {
		console.error(
			`${flag} requires a value, e.g. ${flag === "--thresholds" ? "--thresholds 0.7,0.75,0.8" : "--samples 0.72"}`,
		);
		process.exit(1);
	}
	return value;
}

/** Fixed-width cell: right-aligned unless `side` is "left". */
function pad(
	text: string,
	width: number,
	side: "left" | "right" = "right",
): string {
	return side === "left" ? text.padEnd(width) : text.padStart(width);
}

function snippet(content: string): string {
	const flat = content.replace(/\s+/g, " ").trim();
	return flat.length > SNIPPET_CHARS
		? `${flat.slice(0, SNIPPET_CHARS)}...`
		: flat;
}

function printSamples(
	entries: ReadonlyArray<SimEntry>,
	clusters: ReadonlyArray<GreedyCluster>,
	mode: ClusterCentroidMode,
	threshold: number,
): void {
	const byId = new Map(entries.map((e) => [e.id, e]));
	const ranked = [...clusters]
		.sort((a, b) => b.memberIds.length - a.memberIds.length)
		.slice(0, SAMPLE_CLUSTERS);
	ranked.forEach((cluster, i) => {
		const members = cluster.memberIds
			.map((id) => byId.get(id))
			.filter((e): e is SimEntry => e !== undefined);
		const tokens = members.reduce(
			(s, e) => s + estimateSynthesisPeerTokens(e),
			0,
		);
		console.log(
			`  [${mode} @ ${threshold.toFixed(2)}] cluster #${i + 1}: ${cluster.memberIds.length} entries, ~${tokens} tokens`,
		);
		for (const m of members.slice(0, SAMPLE_MEMBERS)) {
			console.log(`    - ${snippet(m.content)}`);
		}
	});
	console.log("");
}

export async function runCalibrateClusters(
	args: string[],
	configPath: string = defaultConfigPath(),
): Promise<void> {
	// The current threshold is always in the default sweep so the "<- current" row
	// (the incident's threshold, if it is still configured) is shown.
	let thresholds = [
		...new Set([
			...DEFAULT_CLUSTER_SWEEP,
			config.consolidation.clusterAssignmentThreshold,
		]),
	].sort((a, b) => a - b);
	if (args.includes("--thresholds")) {
		const raw = requireValue(args, "--thresholds");
		try {
			thresholds = parseThresholdList(raw);
		} catch (e) {
			console.error(e instanceof Error ? e.message : String(e));
			process.exit(1);
		}
	}

	let sampleThreshold: number | undefined;
	if (args.includes("--samples")) {
		const raw = requireValue(args, "--samples");
		try {
			const parsed = parseThresholdList(raw, "--samples");
			if (parsed.length !== 1)
				throw new Error("--samples takes a single threshold");
			sampleThreshold = parsed[0];
		} catch (e) {
			console.error(e instanceof Error ? e.message : String(e));
			process.exit(1);
		}
	}

	const { consolidation, embedding } = config;
	const currentThreshold = consolidation.clusterAssignmentThreshold;
	const currentMode = consolidation.clusterCentroidMode;
	const minMembers = consolidation.clusterMinMembers;
	const maxPeers = consolidation.synthesisMaxPeers;
	const maxPromptTokens = consolidation.synthesisMaxPromptTokens;

	console.log("Cluster Calibration (read-only simulation)");
	console.log("───────────────────────────────────────");
	console.log(`  Embedding model:               ${embedding.model}`);
	console.log(`  CLUSTER_ASSIGNMENT_THRESHOLD:  ${currentThreshold}`);
	console.log(`  CLUSTER_CENTROID_MODE:         ${currentMode}`);
	console.log(`  CLUSTER_MIN_MEMBERS:           ${minMembers}`);
	console.log(
		`  SYNTHESIS_MAX_PEERS / _PROMPT_TOKENS: ${maxPeers} peers / ${maxPromptTokens} tokens`,
	);
	console.log(
		`  Synthesis calls per run (cap): ${consolidation.clusterSynthesisMaxCallsPerRun}`,
	);
	console.log("");

	let fileConfig: typeof DEFAULT_CONFIG;
	try {
		fileConfig = loadConfigFile(configPath) ?? DEFAULT_CONFIG;
	} catch (e) {
		// Do not print e.message: it contains the config file path.
		console.error(`Failed to load config file (${describeOpenError(e)})`);
		process.exit(1);
	}

	// Synthesis only runs on writable stores, so the simulation covers those.
	const stores = fileConfig.stores.filter((s) => s.writable);
	const header = `  ${"mode".padEnd(8)}${"thresh".padStart(7)}${"clusters".padStart(10)}${"singletons".padStart(12)}${"p50".padStart(6)}${"p90".padStart(6)}${"max".padStart(7)}${"max%".padStart(7)}${"max~tok".padStart(10)}${"eligible".padStart(10)}${"capped".padStart(8)}${"time".padStart(8)}`;

	for (const store of stores) {
		let db: IKnowledgeStore | undefined;
		try {
			db = await openStoreReadOnly(store);
			const entries = (await db.getActiveEntriesWithEmbeddings()) as SimEntry[];
			console.log(
				`Store "${store.id}" (${store.kind}): ${entries.length} active/conflicted entries with embeddings`,
			);
			if (entries.length < minMembers) {
				console.log("  too few entries for synthesis - skipped");
				console.log("");
				continue;
			}

			console.log(header);
			for (const mode of CENTROID_MODES) {
				for (const threshold of thresholds) {
					const started = performance.now();
					const clusters = await greedyClusterAsync(entries, {
						threshold,
						centroidMode: mode,
					});
					const ms = performance.now() - started;
					const s = summarizeClusters(entries, clusters, {
						minMembers,
						maxPeers,
						maxPromptTokens,
					});
					const isCurrent =
						threshold === currentThreshold && mode === currentMode;
					const cells = [
						pad(mode, 8, "left"),
						pad(threshold.toFixed(2), 7),
						pad(String(s.clusterCount), 10),
						pad(String(s.singletons), 12),
						pad(String(s.p50), 6),
						pad(String(s.p90), 6),
						pad(String(s.max), 7),
						pad(`${(s.largestShare * 100).toFixed(1)}%`, 7),
						pad(String(s.largestTokens), 10),
						pad(String(s.eligible), 10),
						pad(String(s.capped), 8),
						pad(`${(ms / 1000).toFixed(1)}s`, 8),
					];
					console.log(`  ${cells.join("")}${isCurrent ? "  <- current" : ""}`);
				}
			}
			console.log("");

			if (sampleThreshold !== undefined) {
				for (const mode of CENTROID_MODES) {
					const clusters = await greedyClusterAsync(entries, {
						threshold: sampleThreshold,
						centroidMode: mode,
					});
					printSamples(entries, clusters, mode, sampleThreshold);
				}
			}
		} catch (e) {
			// greedyClusterAsync throws this for mixed-dimension embeddings (e.g. a re-embed
			// in progress). The message carries only the two dimensions, no store data.
			if (
				e instanceof Error &&
				e.message.startsWith("Vector dimension mismatch")
			) {
				console.log(
					`  Store "${store.id}": embeddings have mixed dimensions (re-embed in progress?) - skipped`,
				);
			} else {
				console.log(
					`  Store "${store.id}" (${store.kind}): unavailable - skipped (${describeOpenError(e)})`,
				);
			}
			console.log("");
		} finally {
			if (db) await db.close();
		}
	}

	console.log(
		"Pick the lowest threshold where max% is small (no cluster dominates; synthesis skips a cluster holding more than 50% of entries and at least 100) and eligible is reasonable, then set CLUSTER_ASSIGNMENT_THRESHOLD (and optionally CLUSTER_CENTROID_MODE) in .env. Changing the threshold re-keys most clusters: they all become ripe once, and CLUSTER_SYNTHESIS_MAX_CALLS_PER_RUN throttles that backlog over several runs. Switching the centroid mode keeps most cluster IDs via member overlap.",
	);
	console.log("Nothing was written; no LLM or embedding calls were made.");
}
