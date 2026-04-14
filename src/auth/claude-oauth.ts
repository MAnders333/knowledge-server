/**
 * Claude subscription OAuth flow for knowledge-server.
 *
 * Implements the same PKCE-based web authentication flow used by the official
 * Claude CLI and opencode plugin, allowing Claude Pro/Max subscribers to use
 * their subscription instead of an API key.
 *
 * Tokens are stored at:
 *   $XDG_CONFIG_HOME/knowledge-server/claude-oauth.json  (default)
 *   ~/.config/knowledge-server/claude-oauth.json
 *
 * Usage:
 *   1. Run `knowledge-server claude-auth` to authenticate interactively.
 *   2. Tokens are auto-refreshed on each LLM call — no manual renewal needed.
 *   3. Run `knowledge-server claude-auth --revoke` to remove stored tokens.
 *
 * This is entirely optional. ANTHROPIC_API_KEY (and other provider keys) take
 * precedence. OAuth is only used when no Anthropic API key is configured and
 * stored tokens exist.
 *
 * Originally implemented by Dennis Paul (PR #66).
 */

import { createHash, randomBytes } from "node:crypto";
import { createServer } from "node:http";
import {
	existsSync,
	mkdirSync,
	readFileSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

// ── OAuth constants (same as opencode plugin) ─────────────────────────────────

export const ANTHROPIC_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";

export const ANTHROPIC_AUTHORIZE_URL = "https://claude.ai/oauth/authorize";

export const ANTHROPIC_TOKEN_URL =
	"https://platform.claude.com/v1/oauth/token";

export const ANTHROPIC_OAUTH_SCOPES =
	"org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";

/** Beta feature flags required on every API call made with OAuth credentials. */
export const ANTHROPIC_BETA_HEADERS = [
	"oauth-2025-04-20",
	"interleaved-thinking-2025-05-14",
];

/**
 * User-Agent that Anthropic's OAuth-gated token endpoint expects.
 *
 * This value was determined empirically by matching what the official Claude
 * CLI sends. If requests start returning 4xx, Anthropic may have changed the
 * expected value — update to match the current Claude CLI release.
 */
export const ANTHROPIC_OAUTH_USER_AGENT = "claude-cli/2.1.2 (external, cli)";

/**
 * User-Agent for token endpoint requests specifically.
 * The platform.claude.com token endpoint requires this value.
 */
const TOKEN_ENDPOINT_USER_AGENT = "axios/1.13.6";

const CALLBACK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

// ── Token file ────────────────────────────────────────────────────────────────

export interface OAuthTokens {
	refresh_token: string;
	access_token: string;
	/** Millisecond Unix timestamp when the access token expires. */
	expires_at: number;
}

function getOAuthFilePath(): string {
	const xdgConfigHome =
		process.env.XDG_CONFIG_HOME || join(homedir(), ".config");
	return join(xdgConfigHome, "knowledge-server", "claude-oauth.json");
}

/** Load stored OAuth tokens. Returns null if no tokens exist or the file is invalid. */
export function loadOAuthTokens(): OAuthTokens | null {
	const filePath = getOAuthFilePath();
	if (!existsSync(filePath)) return null;
	try {
		const data = JSON.parse(readFileSync(filePath, "utf-8")) as Partial<OAuthTokens>;
		if (
			typeof data.refresh_token === "string" &&
			typeof data.access_token === "string" &&
			typeof data.expires_at === "number"
		) {
			return data as OAuthTokens;
		}
		return null;
	} catch {
		return null;
	}
}

/** Persist OAuth tokens to disk. File is created with mode 0o600 (user-only). */
export function saveOAuthTokens(tokens: OAuthTokens): void {
	const filePath = getOAuthFilePath();
	mkdirSync(dirname(filePath), { recursive: true });
	writeFileSync(filePath, JSON.stringify(tokens, null, 2), { mode: 0o600 });
}

/** Remove stored OAuth tokens. No-op if the file does not exist. */
export function clearOAuthTokens(): void {
	const filePath = getOAuthFilePath();
	if (existsSync(filePath)) unlinkSync(filePath);
}

/** Returns true when valid-looking (parseable) OAuth tokens exist on disk. */
export function hasOAuthTokens(): boolean {
	return loadOAuthTokens() !== null;
}

// ── PKCE helpers ──────────────────────────────────────────────────────────────

function base64urlEncode(buffer: Buffer): string {
	return buffer.toString("base64url");
}

function generateVerifier(): string {
	return base64urlEncode(randomBytes(32));
}

function generateChallenge(verifier: string): string {
	return base64urlEncode(createHash("sha256").update(verifier).digest());
}

// ── Token refresh ─────────────────────────────────────────────────────────────

/**
 * Exchange a refresh token for a new access token.
 * Throws on HTTP errors — the caller is responsible for retry logic.
 */
export async function refreshTokens(refreshToken: string): Promise<OAuthTokens> {
	const response = await fetch(ANTHROPIC_TOKEN_URL, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"User-Agent": TOKEN_ENDPOINT_USER_AGENT,
		},
		body: JSON.stringify({
			grant_type: "refresh_token",
			refresh_token: refreshToken,
			client_id: ANTHROPIC_CLIENT_ID,
		}),
	});

	if (!response.ok) {
		const body = await response.text().catch(() => "");
		throw new Error(
			`Claude OAuth token refresh failed: HTTP ${response.status} — ${body.slice(0, 300)}`,
		);
	}

	const data = (await response.json()) as {
		access_token: string;
		refresh_token?: string;
		expires_in: number;
	};

	return {
		access_token: data.access_token,
		// Some responses omit refresh_token (the existing one stays valid per RFC 6749 §6).
		refresh_token: data.refresh_token ?? refreshToken,
		expires_at: Date.now() + data.expires_in * 1000,
	};
}

/**
 * In-flight refresh promise — shared across concurrent callers so only one
 * refresh request is made at a time.
 *
 * Anthropic's token endpoint uses rotating refresh tokens (RFC 6749 §10.4):
 * a second concurrent exchange would invalidate the refresh token returned by
 * the first, causing subsequent calls to fail. Sharing this promise ensures
 * all concurrent callers get the same refreshed token.
 */
let _refreshPromise: Promise<OAuthTokens> | null = null;

/**
 * Return a valid access token, refreshing from disk when the stored token
 * is about to expire (within 60 s). Updated tokens are persisted to disk.
 *
 * Concurrent callers share the same in-flight promise to avoid issuing
 * parallel refresh requests (which would invalidate each other's tokens).
 *
 * Throws if no tokens are stored or the refresh fails.
 */
export async function getAccessToken(): Promise<string> {
	const tokens = loadOAuthTokens();
	if (!tokens) {
		throw new Error(
			"No Claude OAuth tokens found. Run `knowledge-server claude-auth` to authenticate.",
		);
	}

	// Return the stored token if it's still fresh.
	if (tokens.expires_at > Date.now() + 60_000) {
		return tokens.access_token;
	}

	// Deduplicate concurrent refreshes.
	if (!_refreshPromise) {
		_refreshPromise = refreshTokens(tokens.refresh_token)
			.then((refreshed) => {
				saveOAuthTokens(refreshed);
				return refreshed;
			})
			.finally(() => {
				_refreshPromise = null;
			});
	}

	const refreshed = await _refreshPromise;
	return refreshed.access_token;
}

// ── OAuth flow (interactive) ──────────────────────────────────────────────────

/**
 * Run the full PKCE OAuth flow interactively.
 *
 * Prints the authorization URL for the user to open, spins up a local HTTP
 * callback server, then exchanges the received code for tokens.
 *
 * Returns the raw token response — the caller should persist them with
 * `saveOAuthTokens()`.
 */
export async function runOAuthFlow(): Promise<OAuthTokens> {
	const verifier = generateVerifier();
	const challenge = generateChallenge(verifier);
	const state = randomBytes(16).toString("hex");

	// ── Local callback server ────────────────────────────────────────────────
	let resolveCallback!: (url: string) => void;
	let rejectCallback!: (err: Error) => void;
	const callbackPromise = new Promise<string>((resolve, reject) => {
		resolveCallback = resolve;
		rejectCallback = reject;
	});

	// Box so the server handler closure can reference the timeout handle
	// that is assigned after server.listen().
	const timerRef = { current: undefined as ReturnType<typeof setTimeout> | undefined };

	const server = createServer((req, res) => {
		const url = new URL(req.url ?? "/", "http://localhost");

		if (url.pathname !== "/callback") {
			res.writeHead(404);
			res.end();
			return;
		}

		const returnedState = url.searchParams.get("state");
		if (returnedState !== state) {
			res.writeHead(400, { "Content-Type": "text/plain" });
			res.end("Invalid state parameter — possible CSRF attempt.");
			clearTimeout(timerRef.current);
			server.close();
			rejectCallback(
				new Error("OAuth state mismatch — the callback state did not match."),
			);
			return;
		}

		res.writeHead(200, { "Content-Type": "text/html" });
		res.end(
			`<!DOCTYPE html><html><body style="font-family:sans-serif;text-align:center;padding:60px">
				<h2 style="color:#1a1a1a">Authorization complete</h2>
				<p style="color:#555">You can close this window and return to knowledge-server.</p>
			</body></html>`,
		);

		resolveCallback(req.url ?? "");
		setTimeout(() => server.close(), 500);
	});

	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as { port: number };
	const redirectUri = `http://localhost:${port}/callback`;

	timerRef.current = setTimeout(() => {
		server.close();
		rejectCallback(new Error("OAuth callback timed out after 5 minutes."));
	}, CALLBACK_TIMEOUT_MS);

	// ── Authorization URL ────────────────────────────────────────────────────
	const authUrl = new URL(ANTHROPIC_AUTHORIZE_URL);
	authUrl.searchParams.set("code", "true");
	authUrl.searchParams.set("client_id", ANTHROPIC_CLIENT_ID);
	authUrl.searchParams.set("response_type", "code");
	authUrl.searchParams.set("redirect_uri", redirectUri);
	authUrl.searchParams.set("scope", ANTHROPIC_OAUTH_SCOPES);
	authUrl.searchParams.set("code_challenge", challenge);
	authUrl.searchParams.set("code_challenge_method", "S256");
	authUrl.searchParams.set("state", state);

	console.log(
		"Open this URL in your browser to authenticate with your Claude subscription:\n",
	);
	console.log(`  ${authUrl.toString()}\n`);
	console.log("Waiting for browser callback (timeout: 5 minutes)...");

	// ── Wait for callback ────────────────────────────────────────────────────
	const callbackUrl = await callbackPromise;
	clearTimeout(timerRef.current);

	const callbackParams = new URL(callbackUrl, "http://localhost").searchParams;
	const code = callbackParams.get("code");
	if (!code) {
		throw new Error("No authorization code received in OAuth callback.");
	}

	// ── Code exchange ────────────────────────────────────────────────────────
	const response = await fetch(ANTHROPIC_TOKEN_URL, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"User-Agent": TOKEN_ENDPOINT_USER_AGENT,
		},
		body: JSON.stringify({
			code,
			state,
			grant_type: "authorization_code",
			client_id: ANTHROPIC_CLIENT_ID,
			redirect_uri: redirectUri,
			code_verifier: verifier,
		}),
	});

	if (!response.ok) {
		const body = await response.text().catch(() => "");
		throw new Error(
			`Claude OAuth code exchange failed: HTTP ${response.status} — ${body.slice(0, 300)}`,
		);
	}

	const data = (await response.json()) as {
		access_token: string;
		refresh_token: string;
		expires_in: number;
	};

	return {
		access_token: data.access_token,
		refresh_token: data.refresh_token,
		expires_at: Date.now() + data.expires_in * 1000,
	};
}
