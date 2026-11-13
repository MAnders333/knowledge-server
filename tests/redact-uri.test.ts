import { describe, expect, it } from "bun:test";
import { redactUriCredentials } from "../src/redact-uri";

// Connection strings are assembled at runtime so no literal credential sits in source.
const PW = ["hunter", "2"].join("");

describe("redactUriCredentials", () => {
	it("masks the password and keeps scheme, user, host and database", () => {
		expect(
			redactUriCredentials(`postgres://app:${PW}@db.internal:5432/knowledge`),
		).toBe("postgres://app:***@db.internal:5432/knowledge");
	});

	it("masks a password that contains a raw @", () => {
		expect(redactUriCredentials(`postgres://u:p@ss${PW}@host/db`)).toBe(
			"postgres://u:***@host/db",
		);
	});

	it("masks a password that contains a raw /", () => {
		expect(redactUriCredentials("postgres://app:pa/ss9@db/k")).toBe(
			"postgres://app:***@db/k",
		);
	});

	it("masks a percent-encoded password", () => {
		expect(redactUriCredentials("postgresql://u:p%40ss@host/db")).toBe(
			"postgresql://u:***@host/db",
		);
	});

	it("masks a password query parameter", () => {
		expect(
			redactUriCredentials(
				`postgres://host/db?sslmode=require&password=${PW}&application_name=ks`,
			),
		).toBe(
			"postgres://host/db?sslmode=require&password=***&application_name=ks",
		);
	});

	it("masks a password query parameter when there is no path", () => {
		expect(redactUriCredentials(`postgres://host?password=${PW}`)).toBe(
			"postgres://host?password=***",
		);
	});

	it("finds the userinfo end before a query that contains @", () => {
		expect(redactUriCredentials("postgres://u:pw@h?x=a@b")).toBe(
			"postgres://u:***@h?x=a@b",
		);
	});

	it("keeps a URI without a password unchanged", () => {
		for (const uri of [
			"postgres://user@host/db",
			"postgres://host:5432/db",
			"postgres://host/db?sslmode=require",
			"postgres://u@host:5432/db?x=a@b",
			"postgres://host/db?x=a@b",
		]) {
			expect(redactUriCredentials(uri)).toBe(uri);
		}
	});

	it("leaves input without a scheme unchanged", () => {
		expect(redactUriCredentials("/var/data/knowledge.db")).toBe(
			"/var/data/knowledge.db",
		);
	});

	it("is idempotent", () => {
		const once = redactUriCredentials(
			`postgres://u:p@ss${PW}@host/db?password=${PW}`,
		);
		expect(redactUriCredentials(once)).toBe(once);
	});
});
