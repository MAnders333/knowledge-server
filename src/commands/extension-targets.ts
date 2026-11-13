/**
 * Registry of the copies setup-tool makes of release assets (plugin and
 * extension files) for binary installs.
 *
 * setup-tool copies `<install-dir>/<asset>` into a tool's own config directory
 * (for example ~/.pi/agent/extensions/knowledge.ts). `knowledge-server update`
 * refreshes `<install-dir>/<asset>`, so without this registry the copies would
 * stay on the old version. setup-tool records each copy here, and update
 * re-copies every registered target after the verified install. Copies made
 * by releases before this registry are adopted by update when their bytes
 * match a known release (see findLegacyExtensionCopies).
 *
 * The registry lives beside the assets in <install-dir>/extension-targets.json.
 * Source installs use symlinks and are never recorded.
 */
import { createHash } from "node:crypto";
import {
	copyFileSync,
	existsSync,
	lstatSync,
	readFileSync,
	renameSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { isAbsolute, join } from "node:path";

export const EXTENSION_TARGETS_FILE = "extension-targets.json";

/** Assets setup-tool may copy. update only ever downloads names from this list. */
export const EXTENSION_ASSETS = [
	"knowledge.ts",
	"pi-knowledge.ts",
	"omp-knowledge.ts",
	"consolidate.md",
	"knowledge-review.md",
] as const;

/**
 * sha256 of each release asset that setup-tool copied before the registry
 * existed (v3.x releases up to v3.10.3). A pre-registry copy is adopted only
 * when its bytes match one of these digests, so an edited copy is never
 * overwritten. The list is frozen: those releases will not change, and every
 * later setup-tool run records its copy in the registry.
 */
const LEGACY_ASSET_SHA256: Readonly<Record<string, readonly string[]>> = {
	"knowledge.ts": [
		"f2d5711ab5ab4ea17fe31f9430b75134398ae139f069a3b5dc46b5ab681c3867",
		"c9d0d2363c06cff193a4daa66fd3d88d78d5a8ad468c00edca1c24df47434563",
		"f521fd3e1beeacabaa0190a0cf5c4d9609038a42a7805fac45915af393416bfb",
		"38023face26805ad50477e8d1e1254d579fef1e1f12da02c950d3a7f47f4590f",
		"b2c49ebef507a5f0f6480e2fb1dab5bf6c0afa0e76bca30f8bfdeea5d7199216",
	],
	"pi-knowledge.ts": [
		"4e83aebcc094ce61ee1613c1d0246f31263cdf37fd074733932b26f74e968508",
	],
	"consolidate.md": [
		"8480b358488be8c4c0676c50bb4b3b6987345bbc93a6341ffc18421f27b26fd3",
	],
	"knowledge-review.md": [
		"79da754a150c794b4f6a3a68d09e3174bcfa31f82680ba8e9e560eb5cdbbaf35",
	],
};

/** Where setup-tool placed each asset before the registry, relative to $HOME. */
const LEGACY_COPY_LOCATIONS: ReadonlyArray<{
	asset: string;
	path: readonly string[];
}> = [
	{
		asset: "knowledge.ts",
		path: [".config", "opencode", "plugins", "knowledge.ts"],
	},
	{
		asset: "consolidate.md",
		path: [".config", "opencode", "command", "consolidate.md"],
	},
	{
		asset: "knowledge-review.md",
		path: [".config", "opencode", "command", "knowledge-review.md"],
	},
	{
		asset: "pi-knowledge.ts",
		path: [".pi", "agent", "extensions", "knowledge.ts"],
	},
	{
		asset: "pi-knowledge.ts",
		path: [".pi", "shared", "extensions", "knowledge.ts"],
	},
];

interface LegacyCopy {
	target: ExtensionTarget;
	/** True when the bytes match a known release, so update may take it over. */
	adoptable: boolean;
}

/**
 * Copies that setup-tool placed before the registry existed, found at the
 * locations in LEGACY_COPY_LOCATIONS. Symlinks, directories and missing paths
 * are not copies setup-tool made, so they are not returned. `digests` is
 * injectable for tests.
 */
export function findLegacyExtensionCopies(
	home: string,
	digests: Readonly<Record<string, readonly string[]>> = LEGACY_ASSET_SHA256,
): LegacyCopy[] {
	const copies: LegacyCopy[] = [];
	for (const { asset, path } of LEGACY_COPY_LOCATIONS) {
		const dst = join(home, ...path);
		try {
			if (!lstatSync(dst).isFile()) continue;
			const sha256 = createHash("sha256")
				.update(readFileSync(dst))
				.digest("hex");
			copies.push({
				target: { asset, dst },
				adoptable: digests[asset]?.includes(sha256) === true,
			});
		} catch {
			/* missing or unreadable: nothing to adopt */
		}
	}
	return copies;
}

export interface ExtensionTarget {
	/** Release asset name, one of EXTENSION_ASSETS. */
	asset: string;
	/** Absolute path of the copy in the tool's config directory. */
	dst: string;
}

function isTarget(value: unknown): value is ExtensionTarget {
	if (typeof value !== "object" || value === null) return false;
	const { asset, dst } = value as Record<string, unknown>;
	return (
		typeof asset === "string" &&
		(EXTENSION_ASSETS as readonly string[]).includes(asset) &&
		typeof dst === "string" &&
		isAbsolute(dst)
	);
}

/** Registered targets. A missing or unreadable registry yields none. */
export function readExtensionTargets(installDir: string): ExtensionTarget[] {
	const path = join(installDir, EXTENSION_TARGETS_FILE);
	if (!existsSync(path)) return [];
	try {
		const parsed = JSON.parse(readFileSync(path, "utf8")) as {
			targets?: unknown;
		};
		if (!Array.isArray(parsed.targets)) return [];
		return parsed.targets.filter(isTarget);
	} catch {
		return [];
	}
}

/**
 * Record that `target.asset` was copied to `target.dst`. Idempotent. A later
 * record for the same destination replaces the earlier one.
 */
export function recordExtensionTarget(
	installDir: string,
	target: ExtensionTarget,
): void {
	const existing = readExtensionTargets(installDir);
	if (existing.some((t) => t.dst === target.dst && t.asset === target.asset)) {
		return;
	}
	const next = [...existing.filter((t) => t.dst !== target.dst), target];
	const path = join(installDir, EXTENSION_TARGETS_FILE);
	const tmp = `${path}.tmp`;
	try {
		writeFileSync(tmp, `${JSON.stringify({ targets: next }, null, 2)}\n`);
		renameSync(tmp, path);
	} catch (err) {
		try {
			unlinkSync(tmp);
		} catch {
			/* tmp may not exist */
		}
		throw err;
	}
}

interface RefreshOutcome {
	target: ExtensionTarget;
	status: "refreshed" | "skipped";
	reason?: string;
}

/**
 * Copy each registered asset from `<installDir>` over its target. Only copies
 * that still exist are refreshed: a target the user deleted is not re-created.
 * Symlinks are left alone (source installs own them). Each copy goes through a
 * temp file and a rename, so a failure never leaves a half-written extension.
 */
export function refreshExtensionTargets(
	installDir: string,
	targets: ExtensionTarget[],
): RefreshOutcome[] {
	return targets.map((target) => {
		const src = join(installDir, target.asset);
		if (!existsSync(src)) {
			return { target, status: "skipped", reason: "asset not on disk" };
		}
		if (!existsSync(target.dst)) {
			return { target, status: "skipped", reason: "target removed" };
		}
		if (lstatSync(target.dst).isSymbolicLink()) {
			return { target, status: "skipped", reason: "target is a symlink" };
		}
		const tmp = `${target.dst}.tmp`;
		try {
			copyFileSync(src, tmp);
			renameSync(tmp, target.dst);
			return { target, status: "refreshed" };
		} catch (err) {
			try {
				unlinkSync(tmp);
			} catch {
				/* tmp may not exist */
			}
			return {
				target,
				status: "skipped",
				reason: err instanceof Error ? err.message : String(err),
			};
		}
	});
}
