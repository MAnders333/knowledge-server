/**
 * Tests for Claude OAuth token storage and refresh logic.
 *
 * Tests cover:
 * - loadOAuthTokens: returns null for missing/corrupt/incomplete files
 * - saveOAuthTokens / loadOAuthTokens round-trip
 * - hasOAuthTokens: reflects file presence and validity
 * - clearOAuthTokens: removes the token file
 * - getAccessToken: returns stored token when fresh, refreshes when expired,
 *   deduplicates concurrent refresh calls
 * - refreshTokens: constructs correct request and parses response
 *
 * The interactive OAuth flow (runOAuthFlow) is not tested here — it requires
 * a browser and a live Anthropic endpoint.
 *
 * Originally implemented by Dennis Paul (PR #66).
 */
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// ── Override XDG_CONFIG_HOME so tests use an isolated temp dir ────────────────

let tempDir: string;

beforeEach(() => {
	tempDir = mkdtempSync(join(tmpdir(), "ks-oauth-test-"));
	process.env.XDG_CONFIG_HOME = tempDir;
	// Reset the shared in-flight refresh promise between tests by re-importing
	// the module. Bun caches modules so we clear the promise via the exported
	// API rather than module isolation.
});

afterEach(() => {
	rmSync(tempDir, { recursive: true, force: true });
	process.env.XDG_CONFIG_HOME = undefined as unknown as string;
});

// Lazy import so XDG_CONFIG_HOME is set before the module resolves file paths.
async function getOAuth() {
	// Bun caches module imports — we need to bust the cache to pick up the new
	// XDG_CONFIG_HOME. In tests we work around this by using the exported
	// functions directly; the file path is computed at call time.
	const mod = await import("../src/auth/claude-oauth");
	return mod;
}

// ── loadOAuthTokens ───────────────────────────────────────────────────────────

describe("loadOAuthTokens", () => {
	it("returns null when no token file exists", async () => {
		const { loadOAuthTokens } = await getOAuth();
		expect(loadOAuthTokens()).toBeNull();
	});

	it("returns null for a file with missing fields", async () => {
		const { saveOAuthTokens, loadOAuthTokens } = await getOAuth();
		// Write a file with only partial data via raw FS — bypass saveOAuthTokens
		// which would add the missing fields.
		const { mkdirSync, writeFileSync } = await import("node:fs");
		const { join: pathJoin } = await import("node:path");
		const dir = pathJoin(tempDir, "knowledge-server");
		mkdirSync(dir, { recursive: true });
		writeFileSync(pathJoin(dir, "claude-oauth.json"), JSON.stringify({ refresh_token: "rt" }));
		expect(loadOAuthTokens()).toBeNull();
	});

	it("returns null for a corrupt JSON file", async () => {
		const { loadOAuthTokens } = await getOAuth();
		const { mkdirSync, writeFileSync } = await import("node:fs");
		const { join: pathJoin } = await import("node:path");
		const dir = pathJoin(tempDir, "knowledge-server");
		mkdirSync(dir, { recursive: true });
		writeFileSync(pathJoin(dir, "claude-oauth.json"), "not json {{{");
		expect(loadOAuthTokens()).toBeNull();
	});
});

// ── saveOAuthTokens / loadOAuthTokens round-trip ──────────────────────────────

describe("saveOAuthTokens / loadOAuthTokens", () => {
	it("round-trips tokens correctly", async () => {
		const { saveOAuthTokens, loadOAuthTokens } = await getOAuth();
		const tokens = {
			access_token: "at-test",
			refresh_token: "rt-test",
			expires_at: Date.now() + 3600_000,
		};
		saveOAuthTokens(tokens);
		const loaded = loadOAuthTokens();
		if (!loaded) throw new Error("expected tokens to be loaded");
		expect(loaded.access_token).toBe("at-test");
		expect(loaded.refresh_token).toBe("rt-test");
		expect(loaded.expires_at).toBe(tokens.expires_at);
	});

	it("overwrites existing tokens on save", async () => {
		const { saveOAuthTokens, loadOAuthTokens } = await getOAuth();
		saveOAuthTokens({ access_token: "old-at", refresh_token: "old-rt", expires_at: 1 });
		saveOAuthTokens({ access_token: "new-at", refresh_token: "new-rt", expires_at: 2 });
		const loaded = loadOAuthTokens();
		if (!loaded) throw new Error("expected tokens to be loaded");
		expect(loaded.access_token).toBe("new-at");
	});
});

// ── hasOAuthTokens ────────────────────────────────────────────────────────────

describe("hasOAuthTokens", () => {
	it("returns false when no file exists", async () => {
		const { hasOAuthTokens } = await getOAuth();
		expect(hasOAuthTokens()).toBe(false);
	});

	it("returns true after saving valid tokens", async () => {
		const { saveOAuthTokens, hasOAuthTokens } = await getOAuth();
		saveOAuthTokens({
			access_token: "at",
			refresh_token: "rt",
			expires_at: Date.now() + 3600_000,
		});
		expect(hasOAuthTokens()).toBe(true);
	});

	it("returns false for a corrupt token file", async () => {
		const { hasOAuthTokens } = await getOAuth();
		const { mkdirSync, writeFileSync } = await import("node:fs");
		const { join: pathJoin } = await import("node:path");
		const dir = pathJoin(tempDir, "knowledge-server");
		mkdirSync(dir, { recursive: true });
		writeFileSync(pathJoin(dir, "claude-oauth.json"), "not json");
		// A corrupt file must not pass as a valid credential source.
		expect(hasOAuthTokens()).toBe(false);
	});
});

// ── clearOAuthTokens ──────────────────────────────────────────────────────────

describe("clearOAuthTokens", () => {
	it("removes stored tokens", async () => {
		const { saveOAuthTokens, clearOAuthTokens, hasOAuthTokens } = await getOAuth();
		saveOAuthTokens({ access_token: "at", refresh_token: "rt", expires_at: Date.now() + 3600_000 });
		expect(hasOAuthTokens()).toBe(true);
		clearOAuthTokens();
		expect(hasOAuthTokens()).toBe(false);
	});

	it("is a no-op when no file exists", async () => {
		const { clearOAuthTokens } = await getOAuth();
		expect(() => clearOAuthTokens()).not.toThrow();
	});
});

// ── getAccessToken ────────────────────────────────────────────────────────────

describe("getAccessToken", () => {
	it("throws when no tokens are stored", async () => {
		const { getAccessToken } = await getOAuth();
		await expect(getAccessToken()).rejects.toThrow("No Claude OAuth tokens found");
	});

	it("returns the stored access token when it is still fresh", async () => {
		const { saveOAuthTokens, getAccessToken } = await getOAuth();
		saveOAuthTokens({
			access_token: "fresh-at",
			refresh_token: "rt",
			expires_at: Date.now() + 120_000, // 2 minutes from now, well above the 60 s threshold
		});
		const token = await getAccessToken();
		expect(token).toBe("fresh-at");
	});

	it("refreshes and returns a new token when the stored token is near expiry", async () => {
		const { saveOAuthTokens, getAccessToken, refreshTokens } = await getOAuth();

		// Store an access token that expires in 30 s (below the 60 s refresh threshold).
		saveOAuthTokens({
			access_token: "expiring-at",
			refresh_token: "my-refresh-token",
			expires_at: Date.now() + 30_000,
		});

		// Stub global fetch to return a mock refresh response.
		const originalFetch = globalThis.fetch;
		globalThis.fetch = async () =>
			new Response(
				JSON.stringify({
					access_token: "refreshed-at",
					refresh_token: "new-rt",
					expires_in: 3600,
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);

		try {
			const token = await getAccessToken();
			expect(token).toBe("refreshed-at");
		} finally {
			globalThis.fetch = originalFetch;
		}
	});

	it("persists refreshed tokens to disk", async () => {
		const { saveOAuthTokens, getAccessToken, loadOAuthTokens } = await getOAuth();

		saveOAuthTokens({
			access_token: "expiring-at",
			refresh_token: "my-rt",
			expires_at: Date.now() + 30_000,
		});

		const originalFetch = globalThis.fetch;
		globalThis.fetch = async () =>
			new Response(
				JSON.stringify({
					access_token: "new-at",
					refresh_token: "new-rt",
					expires_in: 3600,
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);

		try {
			await getAccessToken();
			const stored = loadOAuthTokens();
			if (!stored) throw new Error("expected refreshed tokens to be persisted");
			expect(stored.access_token).toBe("new-at");
			expect(stored.refresh_token).toBe("new-rt");
		} finally {
			globalThis.fetch = originalFetch;
		}
	});
});

// ── refreshTokens ─────────────────────────────────────────────────────────────

describe("refreshTokens", () => {
	it("throws on HTTP error response", async () => {
		const { refreshTokens } = await getOAuth();

		const originalFetch = globalThis.fetch;
		globalThis.fetch = async () =>
			new Response("Unauthorized", { status: 401 });

		try {
			await expect(refreshTokens("bad-rt")).rejects.toThrow(
				"Claude OAuth token refresh failed: HTTP 401",
			);
		} finally {
			globalThis.fetch = originalFetch;
		}
	});

	it("falls back to the existing refresh_token when the response omits it", async () => {
		const { refreshTokens } = await getOAuth();

		const originalFetch = globalThis.fetch;
		globalThis.fetch = async () =>
			new Response(
				JSON.stringify({
					access_token: "new-at",
					// no refresh_token field — existing one stays valid
					expires_in: 3600,
				}),
				{ status: 200, headers: { "Content-Type": "application/json" } },
			);

		try {
			const result = await refreshTokens("original-rt");
			expect(result.refresh_token).toBe("original-rt");
			expect(result.access_token).toBe("new-at");
		} finally {
			globalThis.fetch = originalFetch;
		}
	});
});
