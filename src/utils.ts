/**
 * Extract the `code` property from an unknown thrown value.
 *
 * Node/Bun system errors (ENOENT, EPERM, ESRCH, EADDRINUSE, …) carry a
 * `code` string on the error object. This helper avoids repeating the
 * `(e as { code?: string }).code` inline cast — the preferred pattern in
 * this codebase over `(e as NodeJS.ErrnoException).code` because the project
 * uses `"types": ["bun"]` without `@types/node`.
 */
export function errCode(e: unknown): string | undefined {
	return (e as { code?: string }).code;
}

/**
 * Race a promise against a timeout. Resolves with the promise's value if it
 * completes in time, or rejects with a timeout error otherwise. The underlying
 * promise is NOT cancelled — it will eventually settle in the background.
 *
 * Used to guard sequential per-store calls (e.g. for…of loops in server.ts
 * that search for a single entry across stores) against dead stores that would
 * otherwise hang indefinitely on a broken TCP connection.
 */
export function withTimeout<T>(
	promise: Promise<T>,
	timeoutMs: number,
	label = "operation",
): Promise<T> {
	return Promise.race([
		promise,
		new Promise<never>((_, reject) =>
			setTimeout(
				() => reject(new Error(`${label} timeout after ${timeoutMs / 1000}s`)),
				timeoutMs,
			),
		),
	]);
}
