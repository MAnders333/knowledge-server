import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	isNonLocalPostgresUri,
	resolvePostgresTls,
} from "../src/db/postgres/tls";

const REMOTE = "postgres://user:" + "secret" + "@db.example.com:5432/knowledge";
const LOCAL = "postgres://user:" + "secret" + "@localhost:5432/knowledge";

describe("resolvePostgresTls", () => {
	let dir: string;

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), "pg-tls-"));
	});

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true });
	});

	it("passes a URI without sslmode through unchanged", () => {
		for (const uri of [LOCAL, REMOTE]) {
			const r = resolvePostgresTls(uri);
			expect(r.ssl).toBeUndefined();
			expect(r.uri).toBe(uri);
		}
	});

	it("leaves sslmode=require to the library (no verification flag)", () => {
		const uri = `${REMOTE}?sslmode=require`;
		const r = resolvePostgresTls(uri);
		expect(r.ssl).toBeUndefined();
		expect(r.uri).toBe(uri);
		expect(r.warnNoTls).toBe(false);
	});

	it("sets rejectUnauthorized for sslmode=verify-full without a CA file", () => {
		const r = resolvePostgresTls(`${REMOTE}?sslmode=verify-full`);
		expect(r.ssl).toEqual({ rejectUnauthorized: true });
		expect(r.ssl?.ca).toBeUndefined();
	});

	it("loads the sslrootcert file as the CA for sslmode=verify-ca", () => {
		const pem =
			"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n";
		const caPath = join(dir, "root.crt");
		writeFileSync(caPath, pem);

		const r = resolvePostgresTls(
			`${REMOTE}?sslmode=verify-ca&sslrootcert=${encodeURIComponent(caPath)}&application_name=kb`,
		);
		expect(r.ssl?.rejectUnauthorized).toBe(true);
		expect(r.ssl?.ca).toEqual(Buffer.from(pem));
		// sslrootcert is removed (postgres.js would send it as a startup parameter);
		// sslmode and other parameters are kept.
		expect(r.uri).toBe(`${REMOTE}?sslmode=verify-ca&application_name=kb`);
	});

	it("throws a clear error for verify-ca when sslrootcert is unreadable", () => {
		const missing = join(dir, "does-not-exist.crt");
		expect(() =>
			resolvePostgresTls(
				`${REMOTE}?sslmode=verify-ca&sslrootcert=${encodeURIComponent(missing)}`,
			),
		).toThrow(/sslrootcert/);
	});

	it("throws for verify-full when sslrootcert is unreadable", () => {
		const missing = join(dir, "does-not-exist.crt");
		expect(() =>
			resolvePostgresTls(
				`${REMOTE}?sslmode=verify-full&sslrootcert=${encodeURIComponent(missing)}`,
			),
		).toThrow(/sslrootcert/);
	});

	it("maps sslrootcert=system to verification with the default trust store", () => {
		const r = resolvePostgresTls(`${REMOTE}?sslrootcert=system`);
		expect(r.ssl).toEqual({ rejectUnauthorized: true });
		expect(r.uri).toBe(REMOTE);
	});

	it("does not change the URI for a local host with sslmode=disable", () => {
		const uri = `${LOCAL}?sslmode=disable`;
		const r = resolvePostgresTls(uri);
		expect(r.ssl).toBeUndefined();
		expect(r.uri).toBe(uri);
	});

	it("reports warnNoTls only for non-local hosts without TLS", () => {
		// Remote, TLS not required -> warn.
		expect(resolvePostgresTls(REMOTE).warnNoTls).toBe(true);
		expect(resolvePostgresTls(`${REMOTE}?sslmode=disable`).warnNoTls).toBe(
			true,
		);
		expect(resolvePostgresTls(`${REMOTE}?sslmode=prefer`).warnNoTls).toBe(true);
		expect(resolvePostgresTls(`${REMOTE}?sslmode=allow`).warnNoTls).toBe(true);
		// Local host without TLS -> no warning.
		expect(resolvePostgresTls(LOCAL).warnNoTls).toBe(false);
		// Remote with verification -> no warning.
		expect(resolvePostgresTls(`${REMOTE}?sslmode=verify-full`).warnNoTls).toBe(
			false,
		);
		expect(resolvePostgresTls(`${REMOTE}?sslmode=require`).warnNoTls).toBe(
			false,
		);
	});
});

describe("isNonLocalPostgresUri", () => {
	it("treats localhost, loopback, ::1 and unix sockets as local", () => {
		expect(isNonLocalPostgresUri("postgres://u@localhost/db")).toBe(false);
		expect(isNonLocalPostgresUri("postgres://u@127.0.0.2:5432/db")).toBe(false);
		expect(isNonLocalPostgresUri("postgres://u@[::1]:5432/db")).toBe(false);
		expect(isNonLocalPostgresUri("postgres://u@%2Fvar%2Frun%2Fpg/db")).toBe(
			false,
		);
		expect(isNonLocalPostgresUri("postgres://u@mac.local/db")).toBe(false);
	});

	it("treats any remote host in the list as non-local", () => {
		expect(isNonLocalPostgresUri("postgres://u@db.example.com/db")).toBe(true);
		expect(
			isNonLocalPostgresUri("postgres://u@localhost,db.example.com/db"),
		).toBe(true);
		expect(isNonLocalPostgresUri("postgres://u@[2001:db8::1]:5432/db")).toBe(
			true,
		);
	});

	it("reads the host after a password that contains '/'", () => {
		expect(isNonLocalPostgresUri("postgres://u:pa/ss@db.example.com/db")).toBe(
			true,
		);
		expect(isNonLocalPostgresUri("postgres://u:pa/ss@localhost/db")).toBe(
			false,
		);
	});
});
