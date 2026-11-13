import { logger } from "../logger.js";
import type { ConsolidationEngine } from "./consolidate.js";

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
