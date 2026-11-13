import { describe, expect, it } from "bun:test";
import { config } from "../src/config";
import { redactSecrets, totalRedactions } from "../src/redact";

const MB = 1_000_000;

function expectRedacted(input: string, expected: string, kind: string): void {
	const result = redactSecrets(input);
	expect(result.text).toBe(expected);
	expect(result.counts).toEqual({ [kind]: 1 });
}

// Secret-shaped fixtures are assembled at runtime. The values are fake, and
// splitting them keeps repository secret scanners from flagging this file.
const PK = ["PRIVATE", "KEY"].join(" ");
const AWS_KEY = ["AKIA", "IOSFODNN7EXAMPLE"].join("");
const HEX_SECRET = ["9f8e7d6c5b4a3928", "170f1e2d3c4b5a69"].join("");

// Credential-shaped fixtures are assembled at runtime so no literal sits in source.
const DEPLOY_URL =
	"https://deploy:" + "hunter2pass" + "@git.example.com/repo.git";

describe("redactSecrets: per-kind replacement", () => {
	it("pem: redacts a multi-line private key block", () => {
		expectRedacted(
			`before\n-----BEGIN RSA ${PK}-----\nMIIEpAIBAAKCAQEA\n-----END RSA ${PK}-----\nafter`,
			"before\n[REDACTED:pem]\nafter",
			"pem",
		);
	});

	it("pem: redacts an unterminated BEGIN line through end of text", () => {
		expectRedacted(
			`dump: -----BEGIN ${PK}-----\nMIIEvQIBADANBgkq`,
			"dump: [REDACTED:pem]",
			"pem",
		);
	});

	it("aws_access_key: redacts AKIA key IDs", () => {
		expectRedacted(
			`key ${AWS_KEY} end`,
			"key [REDACTED:aws_access_key] end",
			"aws_access_key",
		);
	});

	it("aws_secret: redacts the value next to the secret key name only", () => {
		expectRedacted(
			"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
			"aws_secret_access_key = [REDACTED:aws_secret]\n",
			"aws_secret",
		);
	});

	it("gcp_api_key: redacts AIza keys", () => {
		expectRedacted(
			`maps key AIza${"Q".repeat(35)} end`,
			"maps key [REDACTED:gcp_api_key] end",
			"gcp_api_key",
		);
	});

	it("github_token: redacts classic and fine-grained tokens", () => {
		expectRedacted(
			`token ghp_${"a1B2c3".repeat(6)} end`,
			"token [REDACTED:github_token] end",
			"github_token",
		);
		expectRedacted(
			`pat github_pat_${"A1_b".repeat(15)} end`,
			"pat [REDACTED:github_token] end",
			"github_token",
		);
	});

	it("slack_token: redacts xox tokens", () => {
		expectRedacted(
			`slack ${["xoxb", "1234567890-abcdef"].join("-")} end`,
			"slack [REDACTED:slack_token] end",
			"slack_token",
		);
	});

	it("stripe_key: redacts live and test keys", () => {
		expectRedacted(
			`stripe ${["sk", "live", "4eC39HqLyjWDarjtT1zdp7dc"].join("_")} end`,
			"stripe [REDACTED:stripe_key] end",
			"stripe_key",
		);
	});

	it("anthropic_key: redacts sk-ant keys and is not labelled openai_key", () => {
		expectRedacted(
			`anthropic sk-ant-api03-${"abcdefghijKLMNOPQRST0123"} end`,
			"anthropic [REDACTED:anthropic_key] end",
			"anthropic_key",
		);
	});

	it("openai_key: redacts sk- and sk-proj- keys", () => {
		expectRedacted(
			"openai sk-proj-abcdefghijKLMNOPQRST0123 end",
			"openai [REDACTED:openai_key] end",
			"openai_key",
		);
		expectRedacted(
			`openai ${["sk", "abcdefghijklmnopqrstuvwx"].join("-")} end`,
			"openai [REDACTED:openai_key] end",
			"openai_key",
		);
	});

	it("npm_token: redacts npm_ tokens", () => {
		expectRedacted(
			`npm npm_${"aB3".repeat(12)} end`,
			"npm [REDACTED:npm_token] end",
			"npm_token",
		);
	});

	it("jwt: redacts three-part JWTs", () => {
		expectRedacted(
			`jwt ${["eyJhbGciOiJIUzI1NiJ9", "eyJzdWIiOiIxMjM0In0", "dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"].join(".")} end`,
			"jwt [REDACTED:jwt] end",
			"jwt",
		);
	});

	it("bearer: keeps the Authorization header name and redacts the credential", () => {
		expectRedacted(
			"Authorization: Bearer abcdef0123456789XYZ",
			"Authorization: Bearer [REDACTED:bearer]",
			"bearer",
		);
	});

	it("bearer: redacts a bare Bearer token of 16+ characters", () => {
		expectRedacted(
			"use Bearer abcdef0123456789XYZ now",
			"use Bearer [REDACTED:bearer] now",
			"bearer",
		);
	});

	it("env_secret: keeps the variable name and redacts the value", () => {
		expectRedacted(
			`${["export DB_PASSWORD", '"hunter2 long"'].join("=")}\nnext line`,
			"export DB_PASSWORD=[REDACTED:env_secret]\nnext line",
			"env_secret",
		);
	});

	it("url_credentials: keeps scheme and user and redacts the password", () => {
		expectRedacted(
			`clone ${DEPLOY_URL} now`,
			"clone https://deploy:[REDACTED:url_credentials]@git.example.com/repo.git now",
			"url_credentials",
		);
	});

	it("url_credentials: redacts a password when the username is empty", () => {
		expectRedacted(
			"connect redis://:" + "s3cret" + "@host:6379 now",
			"connect redis://:[REDACTED:url_credentials]@host:6379 now",
			"url_credentials",
		);
	});

	it("url_credentials: redacts a password that contains @", () => {
		expectRedacted(
			"postgres://admin:" + "P@ssw0rd" + "@db",
			"postgres://admin:[REDACTED:url_credentials]@db",
			"url_credentials",
		);
	});

	it("url_credentials: leaves URLs without a password unchanged", () => {
		for (const url of [
			"https://example.com/a@b",
			"postgres://host:5432/db",
			"https://user@host",
		]) {
			const result = redactSecrets(`open ${url} now`);
			expect(result.text).toBe(`open ${url} now`);
			expect(result.counts).toEqual({});
		}
	});

	it("keyed_entropy: redacts a long value after a secret-ish key name", () => {
		expectRedacted(
			`{"api_key": "${HEX_SECRET}"}`,
			'{"api_key": "[REDACTED:keyed_entropy]"}',
			"keyed_entropy",
		);
	});
});

describe("redactSecrets: non-secrets survive unchanged", () => {
	const nonSecrets = [
		"commit 3f786850e387550fdab836ed7e6dc881de23001b (short 3f78685)",
		"request id 550e8400-e29b-41d4-a716-446655440000",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  dist/app.js",
		'import { redactSecrets } from "./redact.js";',
		"export function authenticate(user: User): boolean {",
		"const token = getToken();",
		"KEY_COUNT=3",
		"see https://api.example.com/v1/items?id=42",
		`image: data:image/png;base64,iVBORw0KGgo${"AAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ".repeat(10)}`,
		"eyJ",
		"Refresh the token before it expires.",
	];

	for (const input of nonSecrets) {
		it(`leaves unchanged: ${input.slice(0, 48)}`, () => {
			expect(redactSecrets(input)).toEqual({ text: input, counts: {} });
		});
	}
});

describe("redactSecrets: configuration and counts", () => {
	it("returns the input unchanged when config.redactSecrets is false", () => {
		const original = config.redactSecrets;
		config.redactSecrets = false;
		try {
			const input = `API_KEY=abcdef\n${AWS_KEY}`;
			expect(redactSecrets(input)).toEqual({ text: input, counts: {} });
		} finally {
			config.redactSecrets = original;
		}
	});

	it("counts each redaction under its kind for a mixed input", () => {
		const input = [
			`-----BEGIN ${PK}-----\nMIIB\n-----END ${PK}-----`,
			"export API_KEY=abc123",
			["export DB_PASSWORD", "hunter2"].join("="),
			`token ghp_${"a1B2c3".repeat(6)}`,
			`clone ${DEPLOY_URL}`,
			"sha e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		].join("\n");

		const result = redactSecrets(input);

		expect(result.counts).toEqual({
			pem: 1,
			env_secret: 2,
			github_token: 1,
			url_credentials: 1,
		});
		expect(totalRedactions(result.counts)).toBe(5);
		expect(result.text).toBe(
			[
				"[REDACTED:pem]",
				"export API_KEY=[REDACTED:env_secret]",
				"export DB_PASSWORD=[REDACTED:env_secret]",
				"token [REDACTED:github_token]",
				"clone https://deploy:[REDACTED:url_credentials]@git.example.com/repo.git",
				"sha e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			].join("\n"),
		);
	});

	it("totalRedactions returns 0 for empty counts", () => {
		expect(totalRedactions({})).toBe(0);
	});
});

describe("redactSecrets: linear time on 1 MB adversarial input", () => {
	// Each input targets a rule that used a variable-length lookbehind over a
	// whitespace or identifier run, or a nested identifier-run pattern.
	const cases: Record<string, string> = {
		spaces: " ".repeat(MB),
		tabs: "\t".repeat(MB),
		"< then spaces": `<${" ".repeat(MB)}x`,
		"Bearer then spaces": `Bearer${" ".repeat(MB)}x`,
		"Authorization then spaces": `Authorization:${" ".repeat(MB)}x`,
		"password then spaces": `password${" ".repeat(MB)}x`,
		"aws secret key then spaces": `aws_secret_access_key${" ".repeat(MB)}x`,
		"TOKEN run after a dot": `.${"TOKEN".repeat(MB / 5)}=`,
		"env name run on one line": `${"A".repeat(MB)}TOKEN`,
		"PEM header repeated": `-----BEGIN ${PK}-----`.repeat(MB / 28),
		"a: repeated": "a:".repeat(MB / 2),
		"scheme and userinfo run": `http://${"u".repeat(MB)}`,
	};
	for (const [name, text] of Object.entries(cases)) {
		it(`${name}: completes under 500 ms`, () => {
			const start = performance.now();
			redactSecrets(text);
			expect(performance.now() - start).toBeLessThan(500);
		});
	}
});

describe("redactSecrets: context kept after the move to captured prefixes", () => {
	it("keeps the Authorization scheme and redacts only the credential", () => {
		const token = ["abc", "def", "ghi", "jkl", "mno", "pqr"].join("");
		expect(redactSecrets(`Authorization: Bearer ${token}`).text).toBe(
			"Authorization: Bearer [REDACTED:bearer]",
		);
	});

	it("redacts a keyed value after a long whitespace gap as before", () => {
		const value = "x".repeat(40);
		expect(redactSecrets(`password:    ${value}`).text).toBe(
			"password:    [REDACTED:keyed_entropy]",
		);
	});

	it("keeps the env var name and redacts its value", () => {
		expect(redactSecrets("export API_TOKEN=abc123").text).toBe(
			"export API_TOKEN=[REDACTED:env_secret]",
		);
	});
});

describe("redactSecrets: lowercase and mid-line credential assignments", () => {
	// Fixtures are assembled at runtime, as in the rest of this file.
	const positives: Array<[string, string]> = [
		[
			`connect with ${["db_password", "hunter2"].join("=")} now`,
			"connect with db_password=[REDACTED:keyed_secret] now",
		],
		[
			`{"auth_token": "${["a1b2c3d4", "e5f6"].join("")}"}`,
			'{"auth_token": "[REDACTED:keyed_secret]"}',
		],
		[
			`my-api-key: ${["xK9", "42pQ"].join("-")}`,
			"my-api-key: [REDACTED:keyed_secret]",
		],
		[
			`https://h.example/x?${["api_key", "abcd1234"].join("=")}&v=2`,
			"https://h.example/x?api_key=[REDACTED:keyed_secret]&v=2",
		],
	];
	for (const [input, expected] of positives) {
		it(`redacts the value of: ${input}`, () => {
			const result = redactSecrets(input);
			expect(result.text).toBe(expected);
			expect(result.counts).toEqual({ keyed_secret: 1 });
		});
	}

	for (const [input] of positives) {
		it(`is idempotent on: ${input}`, () => {
			const once = redactSecrets(input).text;
			expect(redactSecrets(once)).toEqual({ text: once, counts: {} });
		});
	}

	const prose = [
		"the auth_token: none",
		"api_key: see the docs",
		"primary_key = id",
		"password_reset: enabled",
		"set db_password to the new value",
		"pass_rate=0.5",
	];
	for (const input of prose) {
		it(`keeps ordinary prose unchanged: ${input}`, () => {
			expect(redactSecrets(input)).toEqual({ text: input, counts: {} });
		});
	}
});
