import { createHash } from "node:crypto";
import { type Dirent, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { config } from "../../config.js";
import { logger } from "../../logger.js";
import type { Episode, IEpisodeReader, ProcessedRange } from "../../types.js";
import { MAX_TOKENS_PER_EPISODE, approxTokens } from "./shared.js";

/**
 * Reads Markdown files from a local directory as knowledge episodes.
 *
 * Design:
 * - Each `.md` file is one episode (no message-level chunking).
 * - `sessionId`      = absolute file path — stable identity across runs.
 * - `startMessageId` = `endMessageId` = SHA-256 content hash — idempotency key.
 *   When a file's content changes, the hash changes and the file is reprocessed.
 *   Old episode ranges (prior hashes) remain in consolidated_episode but are
 *   never matched again, so the file runs through extraction fresh and
 *   reconsolidation handles deduplication against previously-extracted entries.
 * - `maxMessageTime` = file mtime (ms) — used for cursor advance.
 *
 * Cursor semantics:
 * - The source cursor `lastMessageTimeCreated` tracks the mtime high-water mark.
 * - Files with mtime ≤ cursor AND a processed episode matching the current hash
 *   are skipped cheaply.
 * - Files with mtime > cursor are always scanned (may already be processed if
 *   the file was touched without content change — hash check catches this).
 *
 * Files that are too large (> MAX_TOKENS_PER_EPISODE) are processed anyway —
 * the consolidate.ts chunk-level token guard will split the chunk if needed.
 *
 * The directory is not created automatically. If it does not exist, the reader
 * returns empty results silently — this is the opt-in UX (create the dir to enable).
 */
export class LocalFilesEpisodeReader implements IEpisodeReader {
	readonly source = "local-files";

	private readonly dir: string;

	constructor(dir?: string) {
		this.dir = dir ?? config.localFilesDir;
	}

	// ── IEpisodeReader implementation ─────────────────────────────────────────

	/**
	 * Return all Markdown files whose mtime is after the cursor.
	 * Used by the consolidation engine to decide whether to start a run and to
	 * drive the session loop.
	 *
	 * For local files, each "session" is one file. The `maxMessageTime` is the
	 * file's mtime — used to advance the source cursor after processing.
	 */
	getCandidateSessions(
		afterMessageTimeCreated: number,
		_limit?: number,
	): Array<{ id: string; maxMessageTime: number }> {
		const files = this.scanMarkdownFiles();
		return files
			.filter((f) => f.mtimeMs > afterMessageTimeCreated)
			.sort((a, b) => a.mtimeMs - b.mtimeMs)
			.map((f) => ({ id: f.path, maxMessageTime: f.mtimeMs }));
	}

	/**
	 * Count Markdown files with mtime after the cursor.
	 * Cheap check used at startup to decide whether consolidation is needed.
	 */
	countNewSessions(afterMessageTimeCreated: number): number {
		return this.scanMarkdownFiles().filter(
			(f) => f.mtimeMs > afterMessageTimeCreated,
		).length;
	}

	/**
	 * Build episodes for the given file paths, skipping those whose content hash
	 * is already recorded in processedRanges (i.e. the exact same file content
	 * was already consolidated in a previous run).
	 *
	 * The processedRanges map uses `sessionId` (= file path) as the key, and
	 * contains `{ startMessageId, endMessageId }` pairs where both IDs are the
	 * content hash at the time of processing. We check whether the current file's
	 * hash appears as any startMessageId in the processed set for that path.
	 */
	getNewEpisodes(
		candidateSessionIds: string[],
		processedRanges: Map<string, ProcessedRange[]>,
	): Episode[] {
		if (candidateSessionIds.length === 0) return [];

		const episodes: Episode[] = [];

		for (const filePath of candidateSessionIds) {
			let content: string;
			let mtimeMs: number;

			try {
				content = readFileSync(filePath, "utf8");
				mtimeMs = statSync(filePath).mtimeMs;
			} catch {
				// File disappeared between scan and read — skip silently.
				continue;
			}

			const hash = contentHash(content);

			// Check whether this exact version (hash) was already processed.
			const prior = processedRanges.get(filePath) ?? [];
			if (prior.some((r) => r.startMessageId === hash)) {
				continue; // already consolidated — content unchanged
			}

			const title = deriveTitle(content, filePath);
			const tokens = approxTokens(content);

			// Warn if over the per-episode soft limit — consolidate.ts chunk guard
			// will handle it, but surface it in logs so large files are visible.
			if (tokens > MAX_TOKENS_PER_EPISODE) {
				logger.warn(
					`[local-files] "${title}" is ~${tokens} tokens — exceeds soft limit (${MAX_TOKENS_PER_EPISODE}), will be processed in a single chunk.`,
				);
			}

			episodes.push({
				sessionId: filePath,
				startMessageId: hash,
				endMessageId: hash,
				sessionTitle: title,
				projectName: "local-files",
				directory: this.dir,
				timeCreated: mtimeMs,
				maxMessageTime: mtimeMs,
				content,
				contentType: "document",
				approxTokens: tokens,
			});
		}

		return episodes;
	}

	close(): void {
		// Stateless — nothing to release.
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	/**
	 * Enumerate all `.md` files directly under the configured directory
	 * (non-recursive for now — keeps the feature simple and predictable).
	 * Returns an empty array if the directory does not exist.
	 */
	private scanMarkdownFiles(): Array<{ path: string; mtimeMs: number }> {
		let entries: Dirent<string>[];
		try {
			entries = readdirSync(this.dir, { withFileTypes: true, encoding: "utf8" });
		} catch {
			// Directory does not exist — opt-in feature, silently return empty.
			return [];
		}

		const results: Array<{ path: string; mtimeMs: number }> = [];
		for (const entry of entries) {
			if (!entry.isFile()) continue;
			if (extname(entry.name).toLowerCase() !== ".md") continue;

			const fullPath = join(this.dir, entry.name);
			try {
				const stat = statSync(fullPath);
				results.push({ path: fullPath, mtimeMs: stat.mtimeMs });
			} catch {
				// File disappeared between readdir and stat — skip.
			}
		}
		return results;
	}
}

/**
 * Derive a human-readable title for a Markdown file.
 *
 * Resolution order:
 * 1. First `# Heading` in the file content.
 * 2. Filename without extension (fallback).
 */
function deriveTitle(content: string, filePath: string): string {
	const headingMatch = content.match(/^#\s+(.+)$/m);
	if (headingMatch) return headingMatch[1].trim();

	// Fallback: filename without extension, with hyphens/underscores replaced by spaces.
	const base = basename(filePath);
	return base.replace(/\.md$/i, "").replace(/[-_]/g, " ");
}

/**
 * SHA-256 content hash truncated to 16 hex chars — sufficient for idempotency
 * within a single source (collision probability negligible for personal file counts).
 */
function contentHash(content: string): string {
	return createHash("sha256").update(content, "utf8").digest("hex").slice(0, 16);
}
