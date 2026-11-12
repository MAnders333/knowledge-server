import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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

describe("parseEnvText: extended denylist", () => {
	it("drops proxy, CA and shell-option keys in every letter case", () => {
		const text = [
			"HTTP_PROXY=http://proxy.example:3128",
			"https_proxy=http://proxy.example:3128",
			"No_Proxy=*",
			"NODE_EXTRA_CA_CERTS=/tmp/ca.pem",
			"NODE_TLS_REJECT_UNAUTHORIZED=0",
			"SSL_CERT_FILE=/tmp/ca.pem",
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
