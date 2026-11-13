#!/usr/bin/env bun
/**
 * Replay harness for activation metrics (MR-M).
 *
 * Replays prompts against the configured knowledge stores with `dryRun`, so no
 * access counts change and no activation_event rows are written. It prints the
 * metrics table used by plan-review §3.1 (cap-hit, zero-hit by prompt bucket,
 * n_returned, hub concentration, latency).
 *
 * Usage:
 *   bun run bench/replay-activation.ts <prompts.jsonl> [--limit 8] [--json]
 *
 * Input: one JSON object per line with a `prompt` string:
 *   {"prompt": "how do I rotate the admin token?"}
 * Prompts are supplied by you. None ship with the repo. Keep the file outside
 * the repo; it may contain private text. The harness prints aggregates only.
 *
 * Cues are built the same way as the claude-code hook: splitIntoCues(prompt).
 * Stores and reranker come from config, as for the server. Activation runs with
 * dryRun, so this does not record access or write telemetry.
 */
import { readFileSync } from "node:fs";
import { ActivationEngine, splitIntoCues } from "../src/activation/activate.js";
import { createRerankerFromConfig } from "../src/activation/rerank.js";
import { StoreRegistry } from "../src/db/store-registry.js";
import { logger } from "../src/logger.js";

interface Args {
	file: string;
	limit: number;
	json: boolean;
}

function parseArgs(argv: string[]): Args {
	const positional = argv.filter(
		(a, i) => !a.startsWith("--") && argv[i - 1] !== "--limit",
	);
	const file = positional[0];
	if (!file) {
		console.error(
			"Usage: bun run bench/replay-activation.ts <prompts.jsonl> [--limit 8] [--json]",
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
	return { file, limit, json: argv.includes("--json") };
}

export function loadPrompts(path: string): string[] {
	const out: string[] = [];
	for (const [i, line] of readFileSync(path, "utf8").split("\n").entries()) {
		if (!line.trim()) continue;
		let prompt: unknown;
		try {
			prompt = (JSON.parse(line) as { prompt?: unknown }).prompt;
		} catch {
			throw new Error(`line ${i + 1}: not JSON`);
		}
		if (typeof prompt !== "string")
			throw new Error(`line ${i + 1}: missing "prompt" string`);
		if (prompt.trim()) out.push(prompt);
	}
	return out;
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

export interface ReplayRecord {
	promptChars: number;
	nReturned: number;
	entryIds: string[];
	latencyMs: number;
	gateMode: string;
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
	const latencies = records.map((r) => r.latencyMs);
	const gate: Record<string, number> = {};
	for (const r of records) gate[r.gateMode] = (gate[r.gateMode] ?? 0) + 1;
	return {
		activations: n,
		capHitPct: pct(records.filter((r) => r.nReturned === limit).length, n),
		zeroHitPct: pct(records.filter((r) => r.nReturned === 0).length, n),
		meanReturned:
			n === 0
				? null
				: Math.round((records.reduce((a, r) => a + r.nReturned, 0) / n) * 100) /
					100,
		top1SlotPct: topShare(1),
		top20SlotPct: topShare(20),
		distinctEntries: slots.size,
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
		`| Top-1 share of slots % | ${s.top1SlotPct ?? "n/a"} |`,
		`| Top-20 share of slots % | ${s.top20SlotPct ?? "n/a"} |`,
		`| Distinct entries injected | ${s.distinctEntries} |`,
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
	const prompts = loadPrompts(args.file);
	const registry = await StoreRegistry.create();
	try {
		const engine = new ActivationEngine(
			registry.writableStore(),
			registry.readStores(),
			registry.writableStores(),
			createRerankerFromConfig(),
		);
		const records: ReplayRecord[] = [];
		for (const prompt of prompts) {
			const started = performance.now();
			const result = await engine.activate(splitIntoCues(prompt), {
				limit: args.limit,
				dryRun: true,
			});
			records.push({
				promptChars: prompt.length,
				nReturned: result.entries.length,
				entryIds: result.entries.map((e) => e.entry.id),
				latencyMs: performance.now() - started,
				gateMode: result.telemetry?.gateMode ?? "off",
			});
		}
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
