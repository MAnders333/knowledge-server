import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
/**
 * End-to-end tests for the MCP stdio proxy (src/mcp/index.ts). The proxy runs as
 * a child process against a stub knowledge server on loopback, so the tests see
 * exactly which Authorization header each call sends.
 */
import type { Server } from "bun";

const MCP_ENTRY = join(import.meta.dir, "..", "src", "mcp", "index.ts");
const ADMIN_TOKEN = "stable-admin-token-0123456789";
const ENTRY_ID = "0f1e2d3c-4b5a-4968-8777-665544332211";

interface Seen {
	method: string;
	path: string;
	authorization: string | null;
}
let server: Server<undefined>;
let seen: Seen[];
let home: string;

beforeEach(() => {
	seen = [];
	home = mkdtempSync(join(tmpdir(), "ks-mcp-home-"));
	server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(req) {
			const { pathname } = new URL(req.url);
			seen.push({
				method: req.method,
				path: pathname,
				authorization: req.headers.get("authorization"),
			});
			if (pathname === "/status") {
				return Response.json({ knowledge: { active: 0 } });
			}
			if (pathname === "/activate") {
				return Response.json({ entries: [], totalActive: 0 });
			}
			if (/^\/entries\/[0-9a-f-]+\/flag$/.test(pathname)) {
				return Response.json({ ok: true });
			}
			return new Response("not found", { status: 404 });
		},
	});
});

afterEach(() => {
	server.stop(true);
	rmSync(home, { recursive: true, force: true });
});

/** Starts the proxy as a child process with a clean environment plus `env`. */
async function connect(env: Record<string, string>): Promise<Client> {
	const transport = new StdioClientTransport({
		command: process.execPath,
		args: ["run", MCP_ENTRY],
		cwd: home,
		// The SDK merges this over its own safe defaults, so no KNOWLEDGE_* leaks in.
		env: {
			HOME: home,
			XDG_CONFIG_HOME: join(home, ".config"),
			KNOWLEDGE_HOST: "127.0.0.1",
			KNOWLEDGE_PORT: String(server.port),
			...env,
		},
		stderr: "ignore",
	});
	const client = new Client({ name: "mcp-proxy-test", version: "0.0.0" });
	await client.connect(transport);
	return client;
}

describe("MCP proxy admin token", () => {
	it("flags an entry without any admin token and sends no Authorization", async () => {
		const client = await connect({});
		try {
			const result = await client.callTool({
				name: "knowledge_flag",
				arguments: { id: ENTRY_ID, reason: "wrong" },
			});
			expect(result.isError).toBeFalsy();
		} finally {
			await client.close();
		}

		const flag = seen.find((r) => r.method === "POST");
		expect(flag?.path).toBe(`/entries/${ENTRY_ID}/flag`);
		expect(flag?.authorization).toBeNull();
	}, 20_000);

	it("sends the admin bearer on read calls and on flagging when the token is set", async () => {
		const client = await connect({ KNOWLEDGE_ADMIN_TOKEN: ADMIN_TOKEN });
		try {
			await client.callTool({
				name: "activate",
				arguments: { cues: "churn analysis" },
			});
			await client.callTool({
				name: "knowledge_flag",
				arguments: { id: ENTRY_ID, reason: "outdated" },
			});
		} finally {
			await client.close();
		}

		expect(seen.map((r) => r.path)).toEqual(
			expect.arrayContaining([
				"/status",
				"/activate",
				`/entries/${ENTRY_ID}/flag`,
			]),
		);
		for (const request of seen) {
			expect(request.authorization).toBe(`Bearer ${ADMIN_TOKEN}`);
		}
	}, 20_000);

	it("sends no Authorization on read calls when the token is unset", async () => {
		const client = await connect({});
		try {
			await client.callTool({
				name: "activate",
				arguments: { cues: "churn analysis" },
			});
		} finally {
			await client.close();
		}

		expect(seen.length).toBeGreaterThan(0);
		for (const request of seen) {
			expect(request.authorization).toBeNull();
		}
	}, 20_000);
});

describe("MCP proxy server host", () => {
	it("exits with the reason when KNOWLEDGE_HOST is not loopback", () => {
		const proc = Bun.spawnSync([process.execPath, "run", MCP_ENTRY], {
			cwd: home,
			env: {
				HOME: home,
				XDG_CONFIG_HOME: join(home, ".config"),
				KNOWLEDGE_HOST: "0.0.0.0",
			},
			stdout: "ignore",
			stderr: "pipe",
		});
		expect(proc.exitCode).toBe(1);
		expect(proc.stderr.toString()).toContain("is not loopback");
	});
});
