/**
 * In-memory throttle for failed Bearer attempts against a stable admin token.
 *
 * Fixed windows, one per client address. Once a client has failed `maxFailures`
 * times in a window it is refused (429) until the window ends. Other clients are
 * unaffected. The map is bounded: when it is full, the least recently created
 * window is dropped, so a flood of addresses cannot grow it without limit.
 */
export class AuthFailureThrottle {
	private readonly windows = new Map<
		string,
		{ startedAt: number; failures: number }
	>();

	constructor(
		private readonly maxFailures = 10,
		private readonly windowMs = 60_000,
		private readonly maxClients = 1024,
	) {}

	/** Milliseconds until `client` may try again, or 0 when it may try now. */
	blockedForMs(client: string, now = Date.now()): number {
		const window = this.windows.get(client);
		if (!window || now - window.startedAt >= this.windowMs) return 0;
		if (window.failures < this.maxFailures) return 0;
		return window.startedAt + this.windowMs - now;
	}

	recordFailure(client: string, now = Date.now()): void {
		let window = this.windows.get(client);
		if (!window || now - window.startedAt >= this.windowMs) {
			if (!window && this.windows.size >= this.maxClients) {
				const oldest = this.windows.keys().next().value;
				if (oldest !== undefined) this.windows.delete(oldest);
			}
			window = { startedAt: now, failures: 0 };
			this.windows.set(client, window);
		}
		window.failures += 1;
	}
}
