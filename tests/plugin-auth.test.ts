/**
 * The plugins send `Authorization: Bearer $KNOWLEDGE_ADMIN_TOKEN` on read calls
 * only when that variable is set (a stable token makes the server require it).
 * The plugins read the variable once, at module load, so each case runs in a
 * child Bun process whose environment is set before the plugin loads.
 */
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const PLUGIN_DIR = join(import.meta.dir, "..", "plugin");
const TOKEN = "stable-admin-token-0123456789";

interface Seen {
	path: string;
	authorization: string | null;
}

/**
 * Loads one plugin with a mocked fetch, runs its startup probe and one
 * activation, and prints every request it made. The mock answers both routes
 * with empty but valid bodies. The probe's `import()` is deliberate: the plugin
 * must load after its environment is set.
 */
function probeScript(pluginFile: string): string {
	const pluginUrl = JSON.stringify(join(PLUGIN_DIR, pluginFile));
	return `
const seen: Array<{ path: string; authorization: string | null }> = [];
globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
	const url = new URL(String(input));
	seen.push({
		path: url.pathname,
		authorization: new Headers(init?.headers).get("authorization"),
	});
	return Response.json(
		url.pathname === "/status" ? { knowledge: { active: 1 } } : { entries: [] },
	);
}) as typeof fetch;
const mod = await import(${pluginUrl});
const prompt = "Please remind me which port the knowledge server uses.";
if (mod.KnowledgePlugin) {
	const hooks = await mod.KnowledgePlugin({ client: { app: { log: async () => {} } } });
	await hooks["chat.message"](
		{ sessionID: "s1" },
		{
			message: { sessionID: "s1", id: "m1" },
			parts: [{ type: "text", text: prompt }],
		},
	);
} else {
	const handlers: Record<string, (...args: unknown[]) => unknown> = {};
	mod.default({
		on: (event: string, handler: (...args: unknown[]) => unknown) => {
			handlers[event] = handler;
		},
	});
	await handlers.session_start?.({}, { hasUI: false, ui: {} });
	await handlers.before_agent_start?.({ reason: "prompt", prompt }, {});
}
console.log(JSON.stringify(seen));
`;
}

/** Runs one plugin in a child process and returns the requests it made. */
function requestsFrom(pluginFile: string, token: string | null): Seen[] {
	const home = mkdtempSync(join(tmpdir(), "ks-plugin-auth-"));
	try {
		const probe = join(home, "probe.ts");
		writeFileSync(probe, probeScript(pluginFile));

		const env: Record<string, string> = {};
		for (const [key, value] of Object.entries(process.env)) {
			if (value !== undefined && !key.startsWith("KNOWLEDGE_")) {
				env[key] = value;
			}
		}
		env.HOME = home;
		env.XDG_CONFIG_HOME = join(home, ".config");
		env.KNOWLEDGE_SERVER_URL = "http://127.0.0.1:3179";
		if (token !== null) env.KNOWLEDGE_ADMIN_TOKEN = token;

		const proc = Bun.spawnSync([process.execPath, probe], {
			cwd: home,
			env,
			stdout: "pipe",
			stderr: "pipe",
		});
		if (proc.exitCode !== 0) {
			throw new Error(`probe failed (${proc.exitCode}): ${proc.stderr}`);
		}
		const lines = proc.stdout.toString().trim().split("\n");
		return JSON.parse(lines[lines.length - 1]) as Seen[];
	} finally {
		rmSync(home, { recursive: true, force: true });
	}
}

describe.each(["knowledge.ts", "pi-knowledge.ts", "omp-knowledge.ts"] as const)(
	"%s read calls",
	(pluginFile) => {
		it("send the admin bearer on every read when KNOWLEDGE_ADMIN_TOKEN is set", () => {
			const seen = requestsFrom(pluginFile, TOKEN);
			expect(seen.map((r) => r.path)).toEqual(
				expect.arrayContaining(["/status", "/activate"]),
			);
			for (const request of seen) {
				expect(request.authorization).toBe(`Bearer ${TOKEN}`);
			}
		});

		it("send no Authorization header when KNOWLEDGE_ADMIN_TOKEN is unset", () => {
			const seen = requestsFrom(pluginFile, null);
			expect(seen.map((r) => r.path)).toEqual(
				expect.arrayContaining(["/status", "/activate"]),
			);
			for (const request of seen) {
				expect(request.authorization).toBeNull();
			}
		});
	},
);
