/**
 * Verified download helpers for GitHub release assets.
 *
 * Every asset that `knowledge-server update`, the daemon self-heal at server
 * startup and `setup-tool` fetch from a release must be listed in that platform's
 * SHA256SUMS-<platform> file. A missing entry, a malformed manifest or a hash
 * mismatch is always fatal: nothing is installed.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { chmod, unlink } from "node:fs/promises";
import { join } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";

export const REPO = "MAnders333/knowledge-server";
export const GITHUB_RELEASES = `https://github.com/${REPO}/releases/download`;

/** Upper bound on a single decompressed asset. Release binaries are ~100 MB. */
export const MAX_ASSET_BYTES = 1024 * 1024 * 1024;

/** Filenames are bare: no path separators, no leading dot. */
const SAFE_ASSET_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
const SHA256_HEX = /^[0-9a-f]{64}$/;

/**
 * Detect the platform suffix used in release asset names.
 * Matches the suffixes produced by the CI build matrix.
 */
export function detectPlatform(): string {
	const os = process.platform;
	const arch = process.arch;

	if (os === "linux" && arch === "x64") return "linux-x64";
	if (os === "darwin" && arch === "arm64") return "darwin-arm64";

	throw new Error(
		`Unsupported platform: ${os}/${arch}. Supported: linux/x64, darwin/arm64. To update manually, download from https://github.com/${REPO}/releases`,
	);
}

/**
 * Parse a sha256sum-format manifest ("<64 hex>  <name>" per line, optional
 * binary-mode "*" before the name). Any malformed non-empty line is an error:
 * a manifest we cannot fully understand must not be partially trusted.
 */
export function parseChecksums(text: string): Map<string, string> {
	const map = new Map<string, string>();
	for (const raw of text.split("\n")) {
		const line = raw.trim();
		if (!line) continue;
		const match = /^([0-9a-fA-F]{64}) [ *]?(\S+)$/.exec(line);
		if (!match) {
			throw new Error(`Malformed SHA256SUMS line: ${JSON.stringify(line)}`);
		}
		const hash = match[1].toLowerCase();
		const name = match[2];
		const existing = map.get(name);
		if (existing !== undefined && existing !== hash) {
			throw new Error(`Conflicting SHA256SUMS entries for ${name}`);
		}
		map.set(name, hash);
	}
	return map;
}

/** Return the expected hash for `name`, or throw if the manifest lacks it. */
export function requireExpectedSha(
	checksums: Map<string, string>,
	name: string,
): string {
	if (!SAFE_ASSET_NAME.test(name)) {
		throw new Error(`Invalid asset name: ${name}`);
	}
	const hash = checksums.get(name);
	if (hash === undefined) {
		throw new Error(
			`SHA256SUMS has no entry for ${name} — refusing to install an unverified file.`,
		);
	}
	if (!SHA256_HEX.test(hash)) {
		throw new Error(`Invalid SHA-256 for ${name} in SHA256SUMS.`);
	}
	return hash;
}

/** Fetch and parse SHA256SUMS-<platform> for a release. Throws on any failure. */
export async function fetchChecksums(
	version: string,
	platform: string,
	fetchFn: typeof fetch = fetch,
): Promise<Map<string, string>> {
	const url = `${GITHUB_RELEASES}/${version}/SHA256SUMS-${platform}`;
	const res = await fetchFn(url, {
		headers: { "User-Agent": "knowledge-server-updater" },
		redirect: "follow",
	});
	if (!res.ok) {
		throw new Error(
			`Failed to fetch checksums: ${res.status} ${res.statusText} (${url})`,
		);
	}
	return parseChecksums(await res.text());
}

export function releaseAssetUrl(version: string, name: string): string {
	return `${GITHUB_RELEASES}/${version}/${name}`;
}

export interface VerifiedDownload {
	/** Release asset URL. */
	url: string;
	/** Directory the staged file is written to. Must be the final filesystem. */
	stageDir: string;
	/** Asset name as listed in SHA256SUMS (also the name the file is installed under). */
	name: string;
	/** Expected SHA-256 of the file after gunzip (if `gzip`). */
	expectedSha: string;
	/** Whether the release asset is gzip-compressed. */
	gzip: boolean;
	/** Permission bits of the staged file. Defaults to 0644. */
	mode?: number;
	fetchFn?: typeof fetch;
}

/**
 * Stream a release asset to a fresh staging file in `stageDir`, hashing the
 * final (decompressed) bytes as they are written. The hash is compared before
 * the function returns, so the caller never re-reads the file from disk. The
 * staged file gets `mode` and is deleted on any failure.
 *
 * Returns the staged path; the caller renames it into place.
 */
export async function downloadVerified(
	opts: VerifiedDownload,
): Promise<string> {
	if (!SAFE_ASSET_NAME.test(opts.name)) {
		throw new Error(`Invalid asset name: ${opts.name}`);
	}
	const fetchFn = opts.fetchFn ?? fetch;
	const res = await fetchFn(opts.url, {
		headers: { "User-Agent": "knowledge-server-updater" },
		redirect: "follow",
	});
	if (!res.ok) {
		throw new Error(
			`Download failed: ${res.status} ${res.statusText} (${opts.url})`,
		);
	}
	if (!res.body) {
		throw new Error(`No response body from ${opts.url}`);
	}

	const stagedPath = join(
		opts.stageDir,
		`.${opts.name}.${randomUUID()}.partial`,
	);
	const hash = createHash("sha256");
	let bytes = 0;
	const hashing = new Transform({
		transform(chunk: Buffer, _enc, callback) {
			bytes += chunk.length;
			if (bytes > MAX_ASSET_BYTES) {
				callback(new Error(`${opts.name} exceeds ${MAX_ASSET_BYTES} bytes`));
				return;
			}
			hash.update(chunk);
			callback(null, chunk);
		},
	});
	const mode = opts.mode ?? 0o644;
	const out = createWriteStream(stagedPath, { flags: "wx", mode });
	// Web ReadableStream → Node Readable. The double cast is needed because the
	// DOM and @types/node stream types do not structurally overlap.
	const source = Readable.fromWeb(
		res.body as unknown as Parameters<typeof Readable.fromWeb>[0],
	);

	try {
		if (opts.gzip) {
			await pipeline(source, createGunzip(), hashing, out);
		} else {
			await pipeline(source, hashing, out);
		}
	} catch (err) {
		await unlink(stagedPath).catch(() => {});
		throw err;
	}

	const actual = hash.digest("hex");
	if (actual !== opts.expectedSha) {
		await unlink(stagedPath).catch(() => {});
		throw new Error(
			`Checksum mismatch for ${opts.name}:\n  expected: ${opts.expectedSha}\n  actual:   ${actual}\n  Aborting — the download may be corrupt or tampered.`,
		);
	}
	// createWriteStream's mode is subject to the umask; set the exact bits.
	await chmod(stagedPath, mode);
	return stagedPath;
}

/**
 * Synchronous curl fetch for callers that cannot await (setup-tool).
 * Strict TLS: https only, TLS 1.2 minimum, bounded time.
 */
export function fetchBufferSync(url: string): Buffer {
	const result = spawnSync(
		"curl",
		[
			"--fail",
			"--silent",
			"--show-error",
			"--location",
			"--proto",
			"=https",
			"--proto-redir",
			"=https",
			"--tlsv1.2",
			"--max-time",
			"60",
			url,
		],
		{ maxBuffer: MAX_ASSET_BYTES, timeout: 65_000 },
	);
	if (result.error) {
		throw new Error(`Failed to run curl: ${result.error.message}`);
	}
	if (result.status !== 0) {
		throw new Error(
			`curl failed for ${url}: ${result.stderr?.toString().trim() || "unknown error"}`,
		);
	}
	return result.stdout;
}

export function sha256Hex(data: Buffer): string {
	return createHash("sha256").update(data).digest("hex");
}
