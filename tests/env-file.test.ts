import { describe, expect, it } from "bun:test";
import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
	envFileCandidates,
	findEnvFile,
	loadEnvFile,
	parseEnvText,
} from "../src/env-file";

// Reproduces ARC-01: the launcher's old parser kept `   # comment` in unquoted
// values, so the admin token and model names carried the comment text.
const FIXTURE = [
	"# knowledge-server credentials",
	"KNOWLEDGE_ADMIN_TOKEN=abcdef0123456789abcd   # fixes the token across restarts",
	"LLM_EXTRACTION_MODEL=anthropic/claude-sonnet-4-6    # episode → knowledge extraction",
	'LLM_MERGE_MODEL="anthropic/claude-haiku-4-5"  # quoted value keeps its text',
	"PATH=/attacker/bin",
	"LD_PRELOAD=/attacker/evil.so",
	"DYLD_INSERT_LIBRARIES=/attacker/evil.dylib",
	"IFS=evil",
	"",
].join("\n");

describe("parseEnvText (single .env parser)", () => {
	it("strips inline comments from unquoted values, as dotenv does", () => {
		const env = parseEnvText(FIXTURE);
		expect(env.KNOWLEDGE_ADMIN_TOKEN).toBe("abcdef0123456789abcd");
		expect(env.LLM_EXTRACTION_MODEL).toBe("anthropic/claude-sonnet-4-6");
		expect(env.LLM_MERGE_MODEL).toBe("anthropic/claude-haiku-4-5");
	});

	it("keeps a # inside quotes", () => {
		expect(
			parseEnvText('EMBEDDING_MODEL="text#3"  # note').EMBEDDING_MODEL,
		).toBe("text#3");
	});

	it("drops process-control keys that a .env must not set", () => {
		const env = parseEnvText(FIXTURE);
		expect(env.PATH).toBeUndefined();
		expect(env.LD_PRELOAD).toBeUndefined();
		expect(env.DYLD_INSERT_LIBRARIES).toBeUndefined();
		expect(env.IFS).toBeUndefined();
	});
});

describe("parseEnvText: proxy and CA keys", () => {
	it("keeps proxy and CA variables in every letter case", () => {
		const text = [
			"HTTPS_PROXY=http://proxy.example:3128",
			"HTTP_PROXY=http://proxy.example:3128",
			"ALL_PROXY=socks5://proxy.example:1080",
			"NO_PROXY=localhost,.corp.example",
			"https_proxy=http://proxy.example:3128",
			"http_proxy=http://proxy.example:3128",
			"all_proxy=socks5://proxy.example:1080",
			"no_proxy=localhost",
			"NODE_EXTRA_CA_CERTS=/etc/ssl/corp-ca.pem",
			"CURL_CA_BUNDLE=/etc/ssl/corp-ca.pem",
			"SSL_CERT_FILE=/etc/ssl/corp-ca.pem",
		].join("\n");
		const env = parseEnvText(text);
		expect(Object.keys(env)).toEqual([
			"HTTPS_PROXY",
			"HTTP_PROXY",
			"ALL_PROXY",
			"NO_PROXY",
			"https_proxy",
			"http_proxy",
			"all_proxy",
			"no_proxy",
			"NODE_EXTRA_CA_CERTS",
			"CURL_CA_BUNDLE",
			"SSL_CERT_FILE",
		]);
		expect(env.NODE_EXTRA_CA_CERTS).toBe("/etc/ssl/corp-ca.pem");
	});
});

describe("parseEnvText: extended denylist", () => {
	it("drops shell-option, trust-store and client-config keys in every letter case", () => {
		const text = [
			"SSL_CERT_DIR=/tmp/certs",
			"ssl_cert_dir=/tmp/certs",
			"CURL_HOME=/tmp/curl",
			"curl_home=/tmp/curl",
			"SSLKEYLOGFILE=/tmp/keys.log",
			"sslkeylogfile=/tmp/keys.log",
			"NODE_TLS_REJECT_UNAUTHORIZED=0",
			"NODE_PATH=/tmp/mods",
			"TMPDIR=/tmp/x",
			"SHELLOPTS=xtrace",
			"BASHOPTS=extdebug",
			"PS4=$(id)",
			"PROMPT_COMMAND=id",
			"CDPATH=/tmp",
			"GLOBIGNORE=*",
			"BUN_INSTALL=/tmp/bun",
			"BUN_CONFIG_REGISTRY=http://r.example",
			"XDG_CONFIG_HOME=/tmp/xdg",
			"KNOWLEDGE_CONFIG_HOME=/tmp/kc",
			"KEEP_ME=yes",
		].join("\n");
		expect(Object.keys(parseEnvText(text))).toEqual(["KEEP_ME"]);
	});
});

describe("loadEnvFile", () => {
	it("assigns parsed values and keeps variables already set in the environment", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-envfile-test-"));
		try {
			const path = join(dir, ".env");
			writeFileSync(path, FIXTURE);
			const target: NodeJS.ProcessEnv = {
				KNOWLEDGE_ADMIN_TOKEN: "from-launchd",
			};
			const result = loadEnvFile(path, target);
			expect(result.error).toBeUndefined();
			expect(target.KNOWLEDGE_ADMIN_TOKEN).toBe("from-launchd");
			expect(target.LLM_EXTRACTION_MODEL).toBe("anthropic/claude-sonnet-4-6");
			expect(target.PATH).toBeUndefined();
			expect(result.loaded).not.toContain("KNOWLEDGE_ADMIN_TOKEN");
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("reports each dropped key by name and keeps the allowed proxy values", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-envfile-test-"));
		try {
			const path = join(dir, ".env");
			writeFileSync(
				path,
				[
					"NODE_OPTIONS=--require /tmp/SENTINEL-secret.js",
					"PATH=/SENTINEL/bin",
					"HTTPS_PROXY=http://proxy.example:3128",
					"LD_PRELOAD=/SENTINEL.so",
					"",
				].join("\n"),
			);
			const target: NodeJS.ProcessEnv = {};
			const result = loadEnvFile(path, target);
			expect(result.ignored).toEqual(["NODE_OPTIONS", "PATH", "LD_PRELOAD"]);
			expect(target.HTTPS_PROXY).toBe("http://proxy.example:3128");
			expect(target.NODE_OPTIONS).toBeUndefined();
			expect(result.loaded).toEqual(["HTTPS_PROXY"]);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("reports a read error instead of throwing", () => {
		const result = loadEnvFile(join(tmpdir(), "ks-missing", ".env"), {});
		expect(result.error).toBeString();
		expect(result.loaded).toEqual([]);
	});
});

describe("env file search order", () => {
	it("prefers KNOWLEDGE_CONFIG_HOME, then XDG, then the legacy location", () => {
		const home = "/home/tester";
		expect(
			envFileCandidates(
				{ KNOWLEDGE_CONFIG_HOME: "/custom" } as NodeJS.ProcessEnv,
				home,
			),
		).toEqual([
			"/custom/.env",
			"/home/tester/.config/knowledge-server/.env",
			"/home/tester/.local/share/knowledge-server/.env",
		]);
	});

	it("returns null when no candidate exists", () => {
		const dir = mkdtempSync(join(tmpdir(), "ks-envfind-test-"));
		try {
			expect(findEnvFile({} as NodeJS.ProcessEnv, dir)).toBeNull();
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe("launchers do not parse .env", () => {
	const root = resolve(import.meta.dir, "..");
	for (const rel of ["scripts/install.sh", "scripts/start.sh"]) {
		it(`${rel} execs the binary without a key=value loop`, () => {
			const text = readFileSync(join(root, rel), "utf8");
			expect(text).not.toContain("printf -v");
			expect(text).not.toContain('export "$_key"');
			expect(text).not.toMatch(/_val="\\?\$\{_line/);
		});
	}
});

describe("env.ts startup: ignored-key warning", () => {
	const envModule = resolve(import.meta.dir, "../src/env.ts");

	it("warns once per file with key names only and loads allowed proxy keys", () => {
		const home = mkdtempSync(join(tmpdir(), "ks-envstartup-test-"));
		try {
			const configDir = join(home, "knowledge-server");
			mkdirSync(configDir, { recursive: true });
			const envFile = join(configDir, ".env");
			writeFileSync(
				envFile,
				[
					"NODE_OPTIONS=--require /tmp/SENTINEL-secret.js",
					"PATH=/SENTINEL/bin",
					"HTTPS_PROXY=http://proxy.example:3128",
					"LD_PRELOAD=/SENTINEL.so",
					"",
				].join("\n"),
			);
			const childEnv: Record<string, string> = {};
			for (const [key, value] of Object.entries(process.env)) {
				if (
					value !== undefined &&
					!key.startsWith("KNOWLEDGE_") &&
					key !== "NODE_OPTIONS"
				) {
					childEnv[key] = value;
				}
			}
			Object.assign(childEnv, {
				HOME: home,
				XDG_CONFIG_HOME: home,
				KNOWLEDGE_LOAD_ENV: "1",
			});
			const child = Bun.spawnSync(
				[
					process.execPath,
					"-e",
					`import ${JSON.stringify(envModule)}; process.stdout.write(process.env.HTTPS_PROXY ?? "");`,
				],
				{ env: childEnv, stdout: "pipe", stderr: "pipe" },
			);
			const stdout = child.stdout.toString();
			const stderr = child.stderr.toString();
			expect(stdout).toBe("http://proxy.example:3128");
			const warnings = stderr.split("\n").filter((l) => l.includes("[env]"));
			expect(warnings).toHaveLength(1);
			expect(warnings[0]).toContain(`Ignored 3 key(s) in ${envFile}`);
			expect(warnings[0]).toContain("NODE_OPTIONS, PATH, LD_PRELOAD");
			expect(stderr).not.toContain("SENTINEL");
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});
