#!/usr/bin/env bun
/**
 * Replay harness for activation metrics (MR-M).
 *
 * Replays prompts against the configured knowledge stores with `dryRun`, so no
 * injection writes happen and no activation_event rows are written. It prints the
 * metrics table used by plan-review §3.1 (cap-hit, zero-hit by prompt bucket,
 * n_returned, tokens, hub concentration, repeat-in-session, latency).
 *
 * Usage:
 *   bun run bench/replay-activation.ts <prompts.jsonl> [--limit 8] [--json]
 *                                      [--sessions] [--dump <out.jsonl>]
 *
 * Input: one JSON object per line with a `prompt` string and an optional `ts`
 * (ISO time or epoch ms):
 *   {"prompt": "how do I rotate the admin token?", "ts": "2026-03-23T13:32:01Z"}
 * Prompts are supplied by you. None ship with the repo. Keep the file outside
 * the repo; it may contain private text. The harness prints aggregates only.
 *
 * --sessions  groups prompts into 30-minute windows by `ts` and passes each window
 *             as a session key, so the server's per-session dedupe applies and
 *             repeat-in-session is measured the way production sees it.
 * --dump      writes one JSON line per prompt with the candidate scores (entry id,
 *             raw cosine, rerank score, returned or not). Private output: keep it
 *             outside the repo. Use with --limit 16 and the gate off to see the
 *             whole rerank pool.
 *
 * Cues are built the same way as the claude-code hook: splitIntoCues(prompt).
 * Stores and reranker come from config, as for the server.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { ActivationEngine, splitIntoCues } from "../src/activation/activate.js";
import { formatKnowledgeLine } from "../src/activation/format.js";
import { createRerankerFromConfig } from "../src/activation/rerank.js";
import { StoreRegistry } from "../src/db/store-registry.js";
import { logger } from "../src/logger.js";

interface Args {
	file: string;
	limit: number;
	json: boolean;
	sessions: boolean;
	dump: string | null;
}

const VALUE_FLAGS = new Set(["--limit", "--dump"]);

function parseArgs(argv: string[]): Args {
	const positional = argv.filter(
		(a, i) => !a.startsWith("--") && !VALUE_FLAGS.has(argv[i - 1] ?? ""),
	);
	const file = positional[0];
	if (!file) {
		console.error(
			"Usage: bun run bench/replay-activation.ts <prompts.jsonl> [--limit 8] [--json] [--sessions] [--dump out.jsonl]",
		);
		process.exit(1);
	}
	const limitIdx = argv.indexOf("--limit");
	const limit =
		limitIdx >= 0 ? Number.parseInt(argv[limitIdx + 1] ?? "", 10) : 8;
	if (!Number.isFinite(limit) || limit < 1) {
		console.error("--limit must be a positive integer");
		process.exit(1);
	}
	const dumpIdx = argv.indexOf("--dump");
	return {
		file,
		limit,
		json: argv.includes("--json"),
		sessions: argv.includes("--sessions"),
		dump: dumpIdx >= 0 ? (argv[dumpIdx + 1] ?? null) : null,
	};
}

export interface PromptRecord {
	prompt: string;
	/** Epoch ms, when the prompt file carries a timestamp. */
	ts?: number;
}

export function loadPromptRecords(path: string): PromptRecord[] {
	const out: PromptRecord[] = [];
	for (const [i, line] of readFileSync(path, "utf8").split("\n").entries()) {
		if (!line.trim()) continue;
		let parsed: { prompt?: unknown; ts?: unknown };
		try {
			parsed = JSON.parse(line) as { prompt?: unknown; ts?: unknown };
		} catch {
			throw new Error(`line ${i + 1}: not JSON`);
		}
		const prompt = parsed.prompt;
		if (typeof prompt !== "string")
			throw new Error(`line ${i + 1}: missing "prompt" string`);
		if (!prompt.trim()) continue;
		const ts =
			typeof parsed.ts === "string"
				? Date.parse(parsed.ts)
				: typeof parsed.ts === "number"
					? parsed.ts
					: Number.NaN;
		out.push(Number.isFinite(ts) ? { prompt, ts } : { prompt });
	}
	return out;
}

export function loadPrompts(path: string): string[] {
	return loadPromptRecords(path).map((r) => r.prompt);
}

export const PROMPT_BUCKETS = [
	{ name: "<40", lo: 0, hi: 40 },
	{ name: "40-199", lo: 40, hi: 200 },
	{ name: "200-999", lo: 200, hi: 1000 },
	{ name: ">=1000", lo: 1000, hi: Number.POSITIVE_INFINITY },
] as const;

export function bucketOf(chars: number): string {
	const b = PROMPT_BUCKETS.find((x) => chars >= x.lo && chars < x.hi);
	return b?.name ?? "unknown";
}

/** Session window for --sessions: prompts within the same 30-minute slot share a session. */
export const SESSION_WINDOW_MS = 30 * 60 * 1000;

export interface ReplayRecord {
	promptChars: number;
	nReturned: number;
	entryIds: string[];
	latencyMs: number;
	gateMode: string;
	/** Estimated tokens of the injected entry lines. */
	injectedTokens?: number;
	gateDropped?: number;
	budgetDropped?: number;
	deduped?: number;
	/** Session key (only with --sessions). Records sharing a key are one session. */
	session?: string;
}

/** Metrics from replay records. Pure, so it can be tested on synthetic data. */
export function summarize(records: ReplayRecord[], limit: number) {
	const n = records.length;
	const pct = (x: number, d: number) =>
		d === 0 ? null : Math.round((x / d) * 1000) / 10;
	// Linear interpolation between closest ranks, same as analyze_baseline.py.
	const quantile = (xs: number[], p: number) => {
		if (xs.length === 0) return null;
		const s = [...xs].sort((a, b) => a - b);
		const k = (s.length - 1) * p;
		const lo = Math.floor(k);
		const hi = Math.min(lo + 1, s.length - 1);
		return Math.round((s[lo] + (s[hi] - s[lo]) * (k - lo)) * 10) / 10;
	};
	const mean = (xs: number[]) =>
		xs.length === 0
			? null
			: Math.round((xs.reduce((a, b) => a + b, 0) / xs.length) * 100) / 100;
	const slots = new Map<string, number>();
	let totalSlots = 0;
	for (const r of records) {
		for (const id of r.entryIds) {
			slots.set(id, (slots.get(id) ?? 0) + 1);
			totalSlots++;
		}
	}
	const counts = [...slots.values()].sort((a, b) => b - a);
	const topShare = (k: number) =>
		totalSlots === 0
			? null
			: pct(
					counts.slice(0, k).reduce((a, b) => a + b, 0),
					totalSlots,
				);
	const buckets = PROMPT_BUCKETS.map((b) => {
		const inB = records.filter((r) => bucketOf(r.promptChars) === b.name);
		return {
			bucket: b.name,
			activations: inB.length,
			zeroHitPct: pct(inB.filter((r) => r.nReturned === 0).length, inB.length),
		};
	});
	// Repeat-in-session: an activation shares an entry with an earlier activation of
	// the same session key.
	const seenBySession = new Map<string, Set<string>>();
	let repeats = 0;
	let sessioned = 0;
	for (const r of records) {
		if (r.session === undefined) continue;
		sessioned++;
		let seen = seenBySession.get(r.session);
		if (!seen) {
			seen = new Set();
			seenBySession.set(r.session, seen);
		}
		if (r.entryIds.some((id) => seen?.has(id))) repeats++;
		for (const id of r.entryIds) seen.add(id);
	}
	const latencies = records.map((r) => r.latencyMs);
	const gate: Record<string, number> = {};
	for (const r of records) gate[r.gateMode] = (gate[r.gateMode] ?? 0) + 1;
	const tokens = records
		.map((r) => r.injectedTokens)
		.filter((t): t is number => t !== undefined);
	return {
		activations: n,
		capHitPct: pct(records.filter((r) => r.nReturned === limit).length, n),
		zeroHitPct: pct(records.filter((r) => r.nReturned === 0).length, n),
		meanReturned:
			n === 0
				? null
				: Math.round((records.reduce((a, r) => a + r.nReturned, 0) / n) * 100) /
					100,
		tokensMean: mean(tokens),
		tokensP50: quantile(tokens, 0.5),
		top1SlotPct: topShare(1),
		top20SlotPct: topShare(20),
		distinctEntries: slots.size,
		repeatInSessionPct: sessioned === 0 ? null : pct(repeats, sessioned),
		gateDroppedMean: mean(
			records
				.map((r) => r.gateDropped)
				.filter((x): x is number => x !== undefined),
		),
		budgetDroppedMean: mean(
			records
				.map((r) => r.budgetDropped)
				.filter((x): x is number => x !== undefined),
		),
		dedupedMean: mean(
			records.map((r) => r.deduped).filter((x): x is number => x !== undefined),
		),
		latencyMsP50: quantile(latencies, 0.5),
		latencyMsP95: quantile(latencies, 0.95),
		gateMode: gate,
		buckets,
	};
}

export function renderTable(
	s: ReturnType<typeof summarize>,
	limit: number,
): string {
	const rows = [
		"| Metric | Value |",
		"|---|---|",
		`| Activations | ${s.activations} |`,
		`| Cap-hit (n == ${limit}) % | ${s.capHitPct ?? "n/a"} |`,
		`| Zero-hit % | ${s.zeroHitPct ?? "n/a"} |`,
		`| Mean n_returned | ${s.meanReturned ?? "n/a"} |`,
		`| Tokens per activation mean / p50 (est.) | ${s.tokensMean ?? "n/a"} / ${s.tokensP50 ?? "n/a"} |`,
		`| Top-1 share of slots % | ${s.top1SlotPct ?? "n/a"} |`,
		`| Top-20 share of slots % | ${s.top20SlotPct ?? "n/a"} |`,
		`| Distinct entries injected | ${s.distinctEntries} |`,
		`| Repeat-in-session % | ${s.repeatInSessionPct ?? "n/a"} |`,
		`| Mean gate-dropped / budget-dropped / deduped | ${s.gateDroppedMean ?? "n/a"} / ${s.budgetDroppedMean ?? "n/a"} / ${s.dedupedMean ?? "n/a"} |`,
		`| Latency p50 / p95 (ms) | ${s.latencyMsP50 ?? "n/a"} / ${s.latencyMsP95 ?? "n/a"} |`,
		`| Gate mode | ${JSON.stringify(s.gateMode)} |`,
		"",
		"| Prompt bucket (chars) | Activations | Zero-hit % |",
		"|---|---|---|",
		...s.buckets.map(
			(b) => `| ${b.bucket} | ${b.activations} | ${b.zeroHitPct ?? "n/a"} |`,
		),
	];
	return rows.join("\n");
}

async function main(): Promise<void> {
	const args = parseArgs(process.argv.slice(2));
	// Keep stdout to the table: only warnings and errors from the stores.
	logger.init("", "warn");
	const prompts = loadPromptRecords(args.file);
	const registry = await StoreRegistry.create();
	const dumpLines: string[] = [];
	try {
		const engine = new ActivationEngine(
			registry.writableStore(),
			registry.readStores(),
			registry.writableStores(),
			createRerankerFromConfig(),
		);
		const records: ReplayRecord[] = [];
		for (const { prompt, ts } of prompts) {
			const session =
				args.sessions && ts !== undefined
					? `replay\u0000${Math.floor(ts / SESSION_WINDOW_MS)}`
					: undefined;
			const started = performance.now();
			const result = await engine.activate(splitIntoCues(prompt), {
				limit: args.limit,
				dryRun: true,
				session,
			});
			// Replay shows the entries to nobody, so the session state is committed
			// explicitly. dryRun plans carry no store writes, only the dedupe record.
			engine.commitInjections(result.injection);
			records.push({
				promptChars: prompt.length,
				nReturned: result.entries.length,
				entryIds: result.entries.map((e) => e.entry.id),
				latencyMs: performance.now() - started,
				gateMode: result.telemetry?.gateMode ?? "off",
				injectedTokens: result.telemetry?.injectedTokens,
				gateDropped: result.telemetry?.gateDropped,
				budgetDropped: result.telemetry?.budgetDropped,
				deduped: result.telemetry?.deduped,
				session,
			});
			if (args.dump) {
				dumpLines.push(
					JSON.stringify({
						promptChars: prompt.length,
						gateMode: result.telemetry?.gateMode ?? "off",
						candidates: result.entries.map((e) => ({
							id: e.entry.id,
							type: e.entry.type,
							raw: e.rawSimilarity,
							rerank: e.rerankScore ?? null,
							tokens: formatKnowledgeLine({
								entry: e.entry,
								staleness: e.staleness,
								contradiction: e.contradiction,
							}).length,
						})),
					}),
				);
			}
		}
		if (args.dump)
			writeFileSync(args.dump, `${dumpLines.join("\n")}\n`, { mode: 0o600 });
		const summary = summarize(records, args.limit);
		console.log(
			args.json
				? JSON.stringify(summary, null, 2)
				: renderTable(summary, args.limit),
		);
	} finally {
		await registry.close();
	}
}

if (import.meta.main) {
	main().catch((err) => {
		console.error(err instanceof Error ? err.message : err);
		process.exit(1);
	});
}
