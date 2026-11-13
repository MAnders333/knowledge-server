/**
 * The server's polling tick must survive a failing state DB: the error is logged
 * and the next tick retries, instead of rejecting the loop (which crashes the
 * server and orphans the auto-spawned daemon).
 */
import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { runDecayTick, runPollTick } from "../src/consolidation/poll";
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

describe("runDecayTick", () => {
	function stub(opts: { locked?: boolean; fail?: boolean } = {}) {
		let locked = opts.locked ?? false;
		const calls = { decay: 0, unlock: 0 };
		return {
			calls,
			engine: {
				tryLock: () => {
					if (locked) return false;
					locked = true;
					return true;
				},
				unlock: () => {
					calls.unlock++;
					locked = false;
				},
				runDecay: async () => {
					calls.decay++;
					if (opts.fail) throw new Error("state db unreachable");
					return 2;
				},
			},
		};
	}

	it("runs decay with nothing pending and releases the lock", async () => {
		const { engine, calls } = stub();
		const result = await runDecayTick(engine);
		expect(result).toEqual({ status: "done", archived: 2 });
		expect(calls.decay).toBe(1);
		expect(calls.unlock).toBe(1);
	});

	it("reports busy without decaying when another run holds the lock", async () => {
		const { engine, calls } = stub({ locked: true });
		expect(await runDecayTick(engine)).toEqual({ status: "busy" });
		expect(calls.decay).toBe(0);
	});

	it("reports failure instead of rejecting, and still releases the lock", async () => {
		const { engine, calls } = stub({ fail: true });
		const errorSpy = spyOn(logger, "error").mockImplementation(() => {});
		try {
			expect(await runDecayTick(engine)).toEqual({ status: "failed" });
			expect(calls.unlock).toBe(1);
		} finally {
			errorSpy.mockRestore();
		}
	});
});
