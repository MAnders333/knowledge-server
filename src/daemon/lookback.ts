import { logger } from "../logger.js";

/**
 * First-install lookback (PV-10). On the first run of a source the daemon only
 * ingests sessions with activity in the last N days, instead of the full history.
 * INITIAL_LOOKBACK_DAYS=0 disables the window and ingests everything.
 */
export const DEFAULT_INITIAL_LOOKBACK_DAYS = 30;

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Parse INITIAL_LOOKBACK_DAYS: a non-negative integer, 0 meaning all history.
 * Unset or empty uses the default. Anything else warns and uses the default.
 */
export function parseInitialLookbackDays(raw: string | undefined): number {
	if (raw === undefined || raw.trim() === "") {
		return DEFAULT_INITIAL_LOOKBACK_DAYS;
	}
	const days = Number(raw.trim());
	if (!Number.isInteger(days) || days < 0) {
		logger.warn(
			`[daemon] INITIAL_LOOKBACK_DAYS=${raw} is not a non-negative integer; using ${DEFAULT_INITIAL_LOOKBACK_DAYS}.`,
		);
		return DEFAULT_INITIAL_LOOKBACK_DAYS;
	}
	return days;
}

/**
 * Starting message-time cursor for a source with no daemon_cursor row:
 * 0 (all history) or now minus the lookback window.
 */
export function initialCursorMs(days: number, now = Date.now()): number {
	return days === 0 ? 0 : now - days * DAY_MS;
}
