/**
 * `knowledge-server claude-auth` — Claude subscription web authentication.
 *
 * Allows Claude Pro/Max subscribers to authenticate via the browser-based
 * OAuth flow instead of supplying an API key. Tokens are stored at
 * ~/.config/knowledge-server/claude-oauth.json and auto-refreshed as needed.
 *
 * This is entirely optional — ANTHROPIC_API_KEY (and other provider keys)
 * continue to work as before and take precedence over OAuth tokens.
 *
 * Usage:
 *   knowledge-server claude-auth              Authenticate (opens browser)
 *   knowledge-server claude-auth --status     Show current auth state
 *   knowledge-server claude-auth --revoke     Remove stored tokens
 *
 * Originally implemented by Dennis Paul (PR #66).
 */

import {
	clearOAuthTokens,
	loadOAuthTokens,
	runOAuthFlow,
	saveOAuthTokens,
} from "../auth/claude-oauth.js";

export async function runClaudeAuth(args: string[]): Promise<void> {
	const revoke = args.includes("--revoke");
	const status = args.includes("--status");

	// ── --revoke ─────────────────────────────────────────────────────────────
	if (revoke) {
		clearOAuthTokens();
		console.log("Claude OAuth tokens removed.");
		return;
	}

	// ── --status ─────────────────────────────────────────────────────────────
	if (status) {
		const tokens = loadOAuthTokens();
		if (!tokens) {
			console.log("No Claude OAuth tokens stored.");
			console.log(
				"Run `knowledge-server claude-auth` to authenticate with your Claude subscription.",
			);
		} else {
			const secondsUntilExpiry = Math.floor(
				(tokens.expires_at - Date.now()) / 1000,
			);
			if (secondsUntilExpiry > 0) {
				console.log(
					`Claude OAuth tokens found. Access token expires in ${secondsUntilExpiry}s.`,
				);
			} else {
				// Access token expired — it will be refreshed automatically on the
				// next LLM call via the refresh token. However if the refresh token
				// has also expired (uncommon; they are long-lived), the next call will
				// fail and re-authentication will be required.
				console.log(
					"Claude OAuth tokens found. Access token has expired and will be refreshed automatically on the next LLM call.",
				);
				console.log(
					"If LLM calls fail with an authentication error, run `knowledge-server claude-auth` to re-authenticate.",
				);
			}
		}
		return;
	}

	// ── Interactive auth flow ─────────────────────────────────────────────────
	console.log("┌─────────────────────────────────────┐");
	console.log("│  Claude Subscription Auth            │");
	console.log("└─────────────────────────────────────┘");
	console.log("");
	console.log(
		"Authenticate with your Claude Pro/Max subscription via the browser.",
	);
	console.log(
		"This is optional — ANTHROPIC_API_KEY and other provider keys also work.",
	);
	console.log("");

	try {
		const tokens = await runOAuthFlow();
		saveOAuthTokens(tokens);
		console.log("\nAuthentication successful. Tokens saved.");
		console.log(
			"knowledge-server will now use your Claude subscription for LLM calls.",
		);
		console.log(
			"\nRun `knowledge-server claude-auth --status` to verify at any time.",
		);
	} catch (err) {
		console.error(
			`\nAuthentication failed: ${err instanceof Error ? err.message : String(err)}`,
		);
		process.exit(1);
	}
}
