import { logger } from "../logger.js";
import type { ConsolidationEngine } from "./consolidate.js";

export type DecayTickResult =
	| { status: "busy" }
	| { status: "done"; archived: number }
	| { status: "failed" };

/**
 * One tick of the decay schedule: archive and tombstone entries under the
 * consolidation lock. Runs whether or not sessions are pending.
 *
 * `busy` means another run holds the lock; the caller retries soon. Never rejects.
 */
export async function runDecayTick(
	consolidation: Pick<ConsolidationEngine, "runDecay" | "tryLock" | "unlock">,
): Promise<DecayTickResult> {
	if (!consolidation.tryLock()) return { status: "busy" };
	try {
		const archived = await consolidation.runDecay();
		logger.log(`[decay] Scheduled decay complete: ${archived} archived.`);
		return { status: "done", archived };
	} catch (err) {
		logger.error(
			"[decay] Scheduled decay failed — retrying next interval:",
			err,
		);
		return { status: "failed" };
	} finally {
		consolidation.unlock();
	}
}

/**
 * One tick of the server's polling loop: start a consolidation drain when
 * sessions are pending.
 *
 * Never rejects. A transient failure (e.g. the state DB is briefly unreachable)
 * is logged and the next tick retries. An escaped rejection would otherwise
 * crash the server and orphan the auto-spawned daemon.
 */
export async function runPollTick(
	consolidation: Pick<ConsolidationEngine, "checkPending">,
	drain: (label: string) => Promise<void>,
): Promise<void> {
	try {
		const { pendingSessions } = await consolidation.checkPending();
		if (pendingSessions === 0) return;
		logger.log(
			`[poll] ${pendingSessions} pending sessions — starting consolidation.`,
		);
		await drain("poll");
	} catch (err) {
		logger.error("[poll] Poll tick failed — retrying next interval:", err);
	}
}
