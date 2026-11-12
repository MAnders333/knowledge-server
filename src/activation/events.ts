import { createHash } from "node:crypto";
import { logger } from "../logger.js";
import type { ActivationEventRow } from "../types.js";

/**
 * Content-free activation telemetry: one activation_event row per /activate call
 * that produced a result. Rows hold ids, counts, scores, latency and a client
 * tag, never prompt or entry text.
 *
 * Off the hot path: `record()` only pushes onto an in-memory queue and schedules
 * a flush. Flushes write one batch. A failing write is logged once per failure
 * streak; rows that cannot be stored are dropped once the queue is full, so a
 * dead state DB never grows memory without bound.
 */

/** How long activation_event rows are kept. */
export const ACTIVATION_EVENT_RETENTION_MS = 90 * 24 * 60 * 60 * 1000;

/** Hash a session id so rows can be grouped without storing the id. */
export function hashSessionId(
	sessionId: string | null | undefined,
): string | null {
	const v = sessionId?.trim();
	if (!v) return null;
	return createHash("sha256").update(v).digest("hex").slice(0, 16);
}

interface EventStore {
	insertActivationEvents(rows: ActivationEventRow[]): Promise<void>;
	pruneActivationEvents(cutoffMs: number): Promise<number>;
}

export interface ActivationEventRecorderOptions {
	batchSize?: number;
	flushDelayMs?: number;
	maxQueued?: number;
	retentionMs?: number;
	pruneIntervalMs?: number;
}

export class ActivationEventRecorder {
	private queue: ActivationEventRow[] = [];
	private flushTimer: ReturnType<typeof setTimeout> | null = null;
	private pruneTimer: ReturnType<typeof setInterval> | null = null;
	private inFlight: Promise<void> = Promise.resolve();
	private failing = false;
	private droppedRows = 0;
	private readonly batchSize: number;
	private readonly flushDelayMs: number;
	private readonly maxQueued: number;
	private readonly retentionMs: number;
	private readonly pruneIntervalMs: number;

	constructor(
		private readonly store: EventStore,
		options: ActivationEventRecorderOptions = {},
	) {
		this.batchSize = options.batchSize ?? 50;
		this.flushDelayMs = options.flushDelayMs ?? 1000;
		this.maxQueued = options.maxQueued ?? 1000;
		this.retentionMs = options.retentionMs ?? ACTIVATION_EVENT_RETENTION_MS;
		this.pruneIntervalMs = options.pruneIntervalMs ?? 24 * 60 * 60 * 1000;
	}

	/** Queue a row. Never throws and never waits for the database. */
	record(row: ActivationEventRow): void {
		if (this.queue.length >= this.maxQueued) {
			this.queue.shift();
			this.droppedRows++;
		}
		this.queue.push(row);
		if (this.queue.length >= this.batchSize) {
			void this.flush();
		} else if (!this.flushTimer) {
			this.flushTimer = setTimeout(() => {
				this.flushTimer = null;
				void this.flush();
			}, this.flushDelayMs);
			this.flushTimer.unref?.();
		}
	}

	/** Write everything queued now. Serialised, so overlapping calls never reorder. */
	flush(): Promise<void> {
		this.inFlight = this.inFlight.then(() => this.writeBatches());
		return this.inFlight;
	}

	private async writeBatches(): Promise<void> {
		while (this.queue.length > 0) {
			const batch = this.queue.splice(0, this.batchSize);
			try {
				await this.store.insertActivationEvents(batch);
				if (this.failing) {
					logger.log("[activation-events] Writes recovered.");
					this.failing = false;
				}
			} catch (err) {
				if (!this.failing) {
					this.failing = true;
					const reason = err instanceof Error ? err.message : String(err);
					logger.warn(
						`[activation-events] Could not write activation_event rows; metrics are incomplete until this clears. Reason: ${reason}`,
					);
				}
				return;
			}
		}
	}

	/** Delete rows older than the retention window. Errors are logged, not thrown. */
	async prune(now = Date.now()): Promise<void> {
		try {
			const deleted = await this.store.pruneActivationEvents(
				now - this.retentionMs,
			);
			if (deleted > 0) {
				logger.log(
					`[activation-events] Pruned ${deleted} rows older than retention.`,
				);
			}
		} catch (err) {
			logger.warn(
				`[activation-events] Prune failed: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	/** Prune now and then daily. Idempotent. */
	start(): void {
		void this.prune();
		if (!this.pruneTimer) {
			this.pruneTimer = setInterval(
				() => void this.prune(),
				this.pruneIntervalMs,
			);
			this.pruneTimer.unref?.();
		}
	}

	/** Stop timers and write what is queued. */
	async stop(): Promise<void> {
		if (this.flushTimer) clearTimeout(this.flushTimer);
		if (this.pruneTimer) clearInterval(this.pruneTimer);
		this.flushTimer = null;
		this.pruneTimer = null;
		await this.flush();
		if (this.droppedRows > 0) {
			logger.warn(
				`[activation-events] Dropped ${this.droppedRows} rows while the queue was full.`,
			);
		}
	}

	/** Rows waiting to be written. For tests and status. */
	get pending(): number {
		return this.queue.length;
	}
}
