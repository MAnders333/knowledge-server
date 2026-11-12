import { describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * The plugins are standalone single-file modules (they cannot import src/), and
 * os.homedir() is fixed when the process starts. Each probe therefore runs in a
 * child Bun process with HOME pointing at a temp dir holding a config.jsonc.
 * The child must load the plugin at runtime (after HOME is set), so its
 * `await import()` is an intentional module-loading boundary.
 */
const PLUGIN_DIR = join(import.meta.dir, "..", "plugin");
/** Run a plugin's startup health probe in a child process and return the fetched URLs. */
async function startupProbeUrls(
	pluginFile: "knowledge.ts" | "pi-knowledge.ts" | "omp-knowledge.ts",
	configJsonc: string,
): Promise<string[]> {
	const home = mkdtempSync(join(tmpdir(), "ks-plugin-home-"));
	try {
		const configDir = join(home, ".config", "knowledge-server");
		mkdirSync(configDir, { recursive: true });
		writeFileSync(join(configDir, "config.jsonc"), configJsonc);

		const probe = join(home, "probe.ts");
		const pluginUrl = join(PLUGIN_DIR, pluginFile);
		writeFileSync(
			probe,
			`
const urls: string[] = [];
globalThis.fetch = (async (input: unknown) => {
	urls.push(String(input));
	throw new Error("offline");
}) as typeof fetch;
const mod = await import(${JSON.stringify(pluginUrl)});
const silentLog = async () => {};
if (mod.KnowledgePlugin) {
	await mod.KnowledgePlugin({ client: { app: { log: silentLog } } });
} else {
	const handlers: Record<string, (...args: unknown[]) => unknown> = {};
	const pi = new Proxy({}, {
		get: (_t, name) =>
			name === "on"
				? (event: string, handler: (...args: unknown[]) => unknown) => {
						handlers[event] = handler;
					}
				: () => {},
	});
	mod.default(pi);
	await handlers.session_start?.({}, { hasUI: false, ui: {} });
}
console.log(JSON.stringify(urls));
`,
		);

		const env: Record<string, string> = {};
		for (const [k, v] of Object.entries(process.env)) {
			if (v !== undefined && !k.startsWith("KNOWLEDGE_")) env[k] = v;
		}
		env.HOME = home;

		const proc = Bun.spawnSync([process.execPath, probe], {
			env,
			stdout: "pipe",
			stderr: "pipe",
		});
		if (proc.exitCode !== 0) {
			throw new Error(
				`probe failed (${proc.exitCode}): ${proc.stderr.toString()}`,
			);
		}
		const lines = proc.stdout.toString().trim().split("\n");
		return JSON.parse(lines[lines.length - 1]) as string[];
	} finally {
		rmSync(home, { recursive: true, force: true });
	}
}

describe.each(["knowledge.ts", "pi-knowledge.ts", "omp-knowledge.ts"] as const)(
	"%s server URL",
	(pluginFile) => {
		it("uses the port from config.jsonc", async () => {
			const urls = await startupProbeUrls(
				pluginFile,
				`{ "stores": [{ "id": "main", "kind": "sqlite", "writable": true }], "port": 4321 }`,
			);
			expect(urls[0]).toBe("http://127.0.0.1:4321/status");
		});

		it("falls back to 3179 when config.jsonc has no port", async () => {
			const urls = await startupProbeUrls(
				pluginFile,
				`{ "stores": [{ "id": "main", "kind": "sqlite", "writable": true }] }`,
			);
			expect(urls[0]).toBe("http://127.0.0.1:3179/status");
		});
	},
);
