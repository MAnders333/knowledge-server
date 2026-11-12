/**
 * The server's polling tick must survive a failing state DB: the error is logged
 * and the next tick retries, instead of rejecting the loop (which crashes the
 * server and orphans the auto-spawned daemon).
 */
import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { runPollTick } from "../src/consolidation/poll";
import { logger } from "../src/logger";

afterEach(() => {
	mock.restore();
});

describe("runPollTick", () => {
	it("logs a failing checkPending and resolves, so the loop keeps running", async () => {
		const errorLog = spyOn(logger, "error").mockImplementation(() => {});
		const drain = mock(async (_label: string) => {});

		await expect(
			runPollTick(
				{
					checkPending: async () => {
						throw new Error("connection reset by peer");
					},
				},
				drain,
			),
		).resolves.toBeUndefined();

		expect(drain).not.toHaveBeenCalled();
		expect(errorLog).toHaveBeenCalledTimes(1);
		expect(String(errorLog.mock.calls[0][0])).toContain("[poll]");
	});

	it("starts a poll-labelled drain when sessions are pending", async () => {
		const drain = mock(async (_label: string) => {});

		await runPollTick(
			{
				checkPending: async () => ({
					pendingSessions: 2,
					lastConsolidatedAt: 0,
				}),
			},
			drain,
		);

		expect(drain).toHaveBeenCalledWith("poll");
	});

	it("retries on the next tick after a failed one", async () => {
		spyOn(logger, "error").mockImplementation(() => {});
		const drain = mock(async (_label: string) => {});
		let calls = 0;
		const consolidation = {
			checkPending: async () => {
				calls++;
				if (calls === 1) throw new Error("transient");
				return { pendingSessions: 1, lastConsolidatedAt: 0 };
			},
		};

		await runPollTick(consolidation, drain);
		await runPollTick(consolidation, drain);

		expect(drain).toHaveBeenCalledTimes(1);
	});
});
