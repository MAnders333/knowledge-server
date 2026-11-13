/**
 * Component health for `/status`.
 *
 * Two pieces:
 *  - OutcomeTracker records success/failure per named component (a knowledge
 *    store, an embedding or rerank provider). It keeps the last success, the
 *    last error and the consecutive-failure count, and reports a circuit as open
 *    after `openAfter` consecutive failures, for `openMs` after the latest one.
 *  - evaluateHealth() rolls the observed inputs up to `ok | degraded | down` and
 *    lists the human-readable reasons. It is pure so the rules are unit-tested
 *    without a server.
 */

type HealthLevel = "ok" | "degraded" | "down";

/** Backlog above this many pending sessions degrades health. */
const PENDING_BACKLOG_DEGRADED = 500;

/**
 * LLM outage thresholds, counted in consecutive consolidation runs that made no
 * progress because of transient provider errors (rate limits, timeouts, rejected
 * credentials, 5xx). Degraded from the third run; down once the outage has lasted
 * ten runs and more than an hour.
 */
export const LLM_DEGRADED_FAILING_RUNS = 3;
export const LLM_DOWN_FAILING_RUNS = 10;
export const LLM_DOWN_AFTER_MS = 60 * 60 * 1000;

interface OutcomeSnapshot {
	name: string;
	lastSuccessAt: number | null;
	lastErrorAt: number | null;
	lastError: string | null;
	consecutiveFailures: number;
	/** When the current run of failures began; null after a success. */
	failingSince: number | null;
	/** The last outcome was a failure. */
	failing: boolean;
	/** Circuit open: `openAfter`+ consecutive failures, latest one inside `openMs`. */
	open: boolean;
}

interface OutcomeTrackerOptions {
	openAfter?: number;
	openMs?: number;
	now?: () => number;
}

interface MutableOutcome {
	lastSuccessAt: number | null;
	lastErrorAt: number | null;
	lastError: string | null;
	consecutiveFailures: number;
	failingSince: number | null;
	/** The most recent call failed. */
	lastFailed: boolean;
}

const NO_OUTCOME: MutableOutcome = {
	lastSuccessAt: null,
	lastErrorAt: null,
	lastError: null,
	consecutiveFailures: 0,
	failingSince: null,
	lastFailed: false,
};

export class OutcomeTracker {
	private readonly outcomes = new Map<string, MutableOutcome>();
	private readonly openAfter: number;
	private readonly openMs: number;
	private readonly now: () => number;

	constructor(options: OutcomeTrackerOptions = {}) {
		this.openAfter = options.openAfter ?? 3;
		this.openMs = options.openMs ?? 60_000;
		this.now = options.now ?? Date.now;
	}

	recordSuccess(name: string): void {
		const o = this.entry(name);
		o.lastSuccessAt = this.now();
		o.consecutiveFailures = 0;
		o.failingSince = null;
		o.lastFailed = false;
	}

	recordFailure(name: string, error: unknown): void {
		const o = this.entry(name);
		o.lastErrorAt = this.now();
		o.lastError = error instanceof Error ? error.message : String(error);
		if (o.consecutiveFailures === 0) o.failingSince = o.lastErrorAt;
		o.consecutiveFailures++;
		o.lastFailed = true;
	}

	snapshot(name: string): OutcomeSnapshot {
		const { lastFailed, ...o } = this.outcomes.get(name) ?? NO_OUTCOME;
		const open =
			o.consecutiveFailures >= this.openAfter &&
			o.lastErrorAt !== null &&
			this.now() - o.lastErrorAt < this.openMs;
		return { name, ...o, failing: lastFailed, open };
	}

	names(): string[] {
		return [...this.outcomes.keys()];
	}

	/** Forget every recorded outcome. Test seam: production never resets. */
	clear(): void {
		this.outcomes.clear();
	}

	private entry(name: string): MutableOutcome {
		let o = this.outcomes.get(name);
		if (!o) {
			o = { ...NO_OUTCOME };
			this.outcomes.set(name, o);
		}
		return o;
	}
}

/** Process-wide store and provider outcomes. Read by /status. */
export const componentHealth = new OutcomeTracker();

export interface HealthInput {
	/** The server state DB answered. False means `down`. */
	stateDbReachable: boolean;
	/** `failing`: the latest read failed, or the store was unavailable at startup. */
	stores: Array<{ id: string; failing: boolean }>;
	/** Providers whose circuit is open. */
	providers: Array<{ name: string; open: boolean }>;
	daemon: {
		/** A daemon is expected to run (auto-spawn on, or it has run before). */
		expected: boolean;
		running: boolean;
		consecutiveFailures: number;
	};
	consolidation: {
		consecutiveFailures: number;
		deadLettered: number;
		pendingSessions: number;
	};
	/** The LLM provider used for extraction. Counts runs without progress, not calls. */
	llm: {
		failingRuns: number;
		/** Milliseconds since the first failing run of the current streak (0 if none). */
		failingForMs: number;
		/** Secret-free failure class of the latest failing run, e.g. "rate limited (HTTP 429)". */
		failureClass: string | null;
	};
}

interface HealthReport {
	health: HealthLevel;
	reasons: string[];
}

/** Roll the observed inputs up to one health level, with a reason per problem. */
export function evaluateHealth(input: HealthInput): HealthReport {
	const reasons: string[] = [];
	const failingStores = input.stores.filter((s) => s.failing);
	const allStoresFailing =
		input.stores.length > 0 && failingStores.length === input.stores.length;

	if (!input.stateDbReachable) {
		reasons.push("state database unreachable");
	}
	if (allStoresFailing) {
		reasons.push("all knowledge stores unreachable");
	} else {
		for (const store of failingStores) {
			reasons.push(`knowledge store "${store.id}" unreachable`);
		}
	}

	if (input.daemon.expected && !input.daemon.running) {
		reasons.push(
			"knowledge-daemon not running; sessions are not being collected",
		);
	}
	if (input.daemon.running && input.daemon.consecutiveFailures > 0) {
		reasons.push(
			`knowledge-daemon upload cycle failing (${input.daemon.consecutiveFailures} in a row)`,
		);
	}

	if (input.consolidation.consecutiveFailures > 0) {
		reasons.push(
			`consolidation failing (${input.consolidation.consecutiveFailures} consecutive errors)`,
		);
	}
	if (input.consolidation.deadLettered > 0) {
		reasons.push(
			`${input.consolidation.deadLettered} episode(s) dead-lettered; run consolidate --retry-dead`,
		);
	}
	if (input.consolidation.pendingSessions > PENDING_BACKLOG_DEGRADED) {
		reasons.push(
			`consolidation backlog: ${input.consolidation.pendingSessions} pending sessions`,
		);
	}

	for (const provider of input.providers) {
		if (provider.open) {
			reasons.push(
				`${provider.name} provider circuit open (repeated failures)`,
			);
		}
	}

	const { llm } = input;
	if (llm.failingRuns >= LLM_DEGRADED_FAILING_RUNS) {
		reasons.push(
			`LLM provider failing for ${llm.failingRuns} consecutive consolidation runs (${llm.failureClass ?? "provider call failed"}); no episodes are being consolidated`,
		);
	}
	const llmDown =
		llm.failingRuns >= LLM_DOWN_FAILING_RUNS &&
		llm.failingForMs > LLM_DOWN_AFTER_MS;

	const down = !input.stateDbReachable || allStoresFailing || llmDown;
	const health: HealthLevel = down
		? "down"
		: reasons.length > 0
			? "degraded"
			: "ok";
	return { health, reasons };
}
