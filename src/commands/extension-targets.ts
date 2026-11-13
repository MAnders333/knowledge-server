/**
 * Registry of the copies setup-tool makes of release assets (plugin and
 * extension files) for binary installs.
 *
 * setup-tool copies `<install-dir>/<asset>` into a tool's own config directory
 * (for example ~/.pi/agent/extensions/knowledge.ts). `knowledge-server update`
 * refreshes `<install-dir>/<asset>`, so without this registry the copies would
 * stay on the old version. setup-tool records each copy here, and update
 * re-copies every registered target after the verified install.
 *
 * The registry lives beside the assets in <install-dir>/extension-targets.json.
 * Source installs use symlinks and are never recorded.
 */
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

export interface RefreshOutcome {
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
