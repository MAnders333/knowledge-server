import { spawn } from "node:child_process";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../../package.json" with { type: "json" };
import { formatActivatedKnowledge } from "../activation/format.js";
import { resolveServerAddress } from "../config-file.js";
import { config } from "../config.js";
import type { ActivationResult } from "../types.js";

/**
 * Zod input schema for the `activate` MCP tool.
 * Exported so tests can validate schema constraints without starting the server.
 */
export const activateInputSchema = {
	cues: z
		.string()
		.describe(
			"One or more cues to activate associated knowledge. Can be a question, topic description, or comma-separated keywords. Example: 'churn analysis, segment X, onboarding'",
		),
	limit: z
		.number()
		.int()
		.min(1)
		.max(50)
		.optional()
		.describe(
			`Maximum number of entries to return (default: ${config.activation.maxResults}). Increase when broad topic recall is needed.`,
		),
	threshold: z
		.number()
		.min(0)
		.max(1)
		.optional()
		.describe(
			`Minimum cosine similarity score to include an entry (default: ${config.activation.similarityThreshold}). Lower to cast a wider net (e.g. 0.25), raise to require a tighter match (e.g. 0.45).`,
		),
};

/**
 * Zod input schema for the `knowledge_flag` MCP tool. `id` is the full entry id or
 * the short id shown in injected lines, e.g. "(id 1a2b3c4d)".
 */
export const flagInputSchema = {
	id: z
		.string()
		.regex(/^[0-9a-f-]{8,36}$/i)
		.describe(
			"Entry id as shown in an injected line, e.g. '1a2b3c4d' from '(id 1a2b3c4d)'.",
		),
	reason: z
		.enum(["wrong", "outdated", "irrelevant"])
		.describe(
			"Why the entry should not be used: wrong (factually incorrect), outdated (was true, no longer), irrelevant (does not apply here).",
		),
};

/**
 * Base URL of the knowledge HTTP server at `host:port`. The proxy sends cues over
 * plain HTTP, so only loopback hosts are accepted, the same rule the plugins apply
 * to KNOWLEDGE_SERVER_URL.
 */
export function loopbackBaseUrl(host: string, port: number): string {
	const bare = host.replace(/^\[(.*)\]$/, "$1");
	if (bare !== "127.0.0.1" && bare !== "localhost" && bare !== "::1") {
		throw new Error(
			`the knowledge server host "${host}" is not loopback (127.0.0.1, localhost or ::1). The MCP proxy refuses other hosts because it sends prompts over plain HTTP. Set KNOWLEDGE_HOST or "host" in config.jsonc to a loopback address.`,
		);
	}
	return `http://${bare.includes(":") ? `[${bare}]` : bare}:${port}`;
}

/** Base URL of the knowledge HTTP server (config.jsonc / KNOWLEDGE_PORT, see resolveServerAddress). */
function serverBaseUrl(): string {
	const { host, port } = resolveServerAddress();
	return loopbackBaseUrl(host, port);
}

/**
 * Check whether the knowledge HTTP server is reachable.
 * Returns true if it responds to GET /status within the given timeout.
 */
async function isServerReachable(
	baseUrl: string,
	timeoutMs = 2000,
): Promise<boolean> {
	try {
		const res = await fetch(`${baseUrl}/status`, {
			signal: AbortSignal.timeout(timeoutMs),
		});
		return res.ok;
	} catch {
		return false;
	}
}

/**
 * Attempt to auto-start the knowledge HTTP server as a background process.
 *
 * Uses the same binary that is currently running (`process.execPath`) when
 * invoked as a compiled binary, or falls back to `bun run src/index.ts` when
 * running from source. The server process is detached so it outlives the MCP
 * subprocess.
 *
 * Returns true if the server came up within the timeout, false otherwise.
 */
async function tryAutoStart(timeoutMs = 8000): Promise<boolean> {
	// Determine how to launch the server. When running as a compiled binary,
	// process.execPath points to `knowledge-server` — run it with no args to
	// start the HTTP server. When running via `bun run src/mcp/index.ts`,
	// execPath is the bun binary — run `bun run src/index.ts` instead.
	let cmd: string;
	let args: string[];
	const execName = process.execPath.split("/").pop() ?? "";
	if (execName === "knowledge-server") {
		cmd = process.execPath;
		args = [];
	} else {
		cmd = process.execPath; // bun
		args = ["run", new URL("../index.ts", import.meta.url).pathname];
	}

	try {
		const child = spawn(cmd, args, {
			detached: true,
			stdio: "ignore",
			// Inherit env so .env-loaded vars (KNOWLEDGE_PORT, etc.) are available
			// if the launcher script has already exported them.
			env: process.env,
		});
		child.unref(); // don't keep the MCP process alive waiting for the child
	} catch {
		return false;
	}

	// Poll until the server responds or we time out
	const pollIntervalMs = 200;
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, pollIntervalMs));
		if (await isServerReachable(serverBaseUrl(), 1000)) return true;
	}
	return false;
}

/**
 * MCP server interface for the knowledge system.
 *
 * Exposes two tools: `activate` (recall) and `knowledge_flag` (report a wrong entry)
 *
 * Rather than embedding the ActivationEngine directly (which requires DB access
 * and LLM credentials for embedding), this server delegates to the already-running
 * knowledge HTTP server via GET /activate. This keeps the MCP process lightweight:
 * it only needs KNOWLEDGE_HOST / KNOWLEDGE_PORT to find the server.
 *
 * On startup, if the HTTP server is not reachable, this process will attempt to
 * auto-start it as a background daemon before connecting the stdio transport.
 *
 * Usage: Agent sends cues (keywords, topics, questions)
 * and receives associated knowledge entries ranked by relevance.
 */
export async function main() {
	// No validateConfig() call — LLM credentials are not needed here.
	// The knowledge HTTP server holds those; this process is a thin proxy.
	// Connection errors are surfaced per-call with a clear ECONNREFUSED message.
	let baseUrl: string;
	try {
		baseUrl = serverBaseUrl();
	} catch (e) {
		console.error(`[knowledge-server-mcp] ${(e as Error).message}`);
		process.exit(1);
	}

	// Auto-start: if the HTTP server isn't reachable, try to launch it.
	// This lets users skip the manual "start the server" step — the MCP process
	// handles it transparently when the IDE starts.
	// Failures are silent: if auto-start doesn't work (e.g. .env not configured),
	// the activate tool will surface the ECONNREFUSED error on first use instead.
	if (!(await isServerReachable(baseUrl))) {
		await tryAutoStart();
		// Don't error if it didn't come up — the activate handler will explain.
	}

	const server = new McpServer({
		name: "knowledge-server",
		version: pkg.version,
	});

	server.tool(
		"activate",
		"Activate associated knowledge by providing cues. Returns knowledge entries that are semantically related to the provided cues. Use this when you need to recall what has been learned from prior sessions about a specific topic. Provide descriptive cues — topics, questions, or keywords — and receive relevant knowledge entries ranked by association strength.",
		activateInputSchema,
		async ({ cues, limit, threshold }) => {
			try {
				const url = new URL(`${baseUrl}/activate`);
				url.searchParams.set("q", cues);
				if (limit !== undefined) url.searchParams.set("limit", String(limit));
				if (threshold !== undefined)
					url.searchParams.set("threshold", String(threshold));

				const response = await fetch(url, {
					headers: { "x-knowledge-client": "mcp" },
					signal: AbortSignal.timeout(15_000),
				});

				if (!response.ok) {
					const body = await response.text();
					return {
						content: [
							{
								type: "text" as const,
								text: `Knowledge server error (${response.status}): ${body}`,
							},
						],
						isError: true,
					};
				}

				const result = (await response.json()) as ActivationResult;

				if (result.entries.length === 0) {
					return {
						content: [
							{
								type: "text" as const,
								text: "No relevant knowledge found for these cues.",
							},
						],
					};
				}

				const formatted = formatActivatedKnowledge(result.entries);

				const conflictCount = result.entries.filter(
					(r) => r.contradiction,
				).length;
				const conflictNote =
					conflictCount > 0
						? ` — ${conflictCount} conflicted, do not act on those without clarifying which version is correct`
						: "";

				return {
					content: [
						{
							type: "text" as const,
							text: `## Activated Knowledge (${result.entries.length} entries, ${result.totalActive} total active${conflictNote})\n\n${formatted}`,
						},
					],
				};
			} catch (e) {
				const isConnRefused =
					String(e).includes("ECONNREFUSED") ||
					String(e).includes("Connection refused");
				const msg = isConnRefused
					? `Cannot reach knowledge server at ${baseUrl}. Is it running? Start it with: knowledge-server`
					: `Error activating knowledge: ${e}`;
				return {
					content: [{ type: "text" as const, text: msg }],
					isError: true,
				};
			}
		},
	);

	server.tool(
		"knowledge_flag",
		"Report a recalled knowledge entry that is wrong, outdated or irrelevant, using the id shown in its injected line. The entry is queued for review and is no longer injected into sessions; nothing is deleted. Requires KNOWLEDGE_ADMIN_TOKEN in this process's environment.",
		flagInputSchema,
		async ({ id, reason }) => {
			if (!config.adminToken) {
				return {
					content: [
						{
							type: "text" as const,
							text: "knowledge_flag needs KNOWLEDGE_ADMIN_TOKEN set in the environment of the knowledge MCP process (the same stable token the server runs with). Nothing was flagged.",
						},
					],
					isError: true,
				};
			}
			try {
				const response = await fetch(
					`${baseUrl}/entries/${encodeURIComponent(id)}/flag`,
					{
						method: "POST",
						headers: {
							Authorization: `Bearer ${config.adminToken}`,
							"Content-Type": "application/json",
						},
						body: JSON.stringify({ reason }),
						signal: AbortSignal.timeout(15_000),
					},
				);
				if (!response.ok) {
					const body = await response.text();
					return {
						content: [
							{
								type: "text" as const,
								text: `Knowledge server error (${response.status}): ${body}`,
							},
						],
						isError: true,
					};
				}
				return {
					content: [
						{
							type: "text" as const,
							text: `Flagged entry ${id} (${reason}). It is queued for review and is no longer injected into sessions. Nothing was deleted.`,
						},
					],
				};
			} catch (e) {
				return {
					content: [
						{ type: "text" as const, text: `Error flagging entry: ${e}` },
					],
					isError: true,
				};
			}
		},
	);

	const transport = new StdioServerTransport();
	await server.connect(transport);
}

if (import.meta.main) {
	main().catch((err) => {
		console.error("[knowledge-server-mcp] fatal:", err);
		process.exit(1);
	});
}
