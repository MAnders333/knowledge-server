import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { ActivationEngine } from "../activation/activate.js";
import { resolveUserId } from "../config-file.js";
import { ConsolidationEngine } from "../consolidation/consolidate.js";
import { PendingEpisodesReader } from "../consolidation/readers/pending.js";
import { LocalFilesEpisodeReader } from "../daemon/readers/local-files.js";
import { EpisodeUploader } from "../daemon/uploader.js";
import { DaemonDB } from "../db/daemon/index.js";
import { StoreRegistry } from "../db/store-registry.js";
import { logger } from "../logger.js";
import {
	drainConsolidation,
	runEmbeddingCheck,
	runSynthesisPass,
} from "./consolidation-drain.js";

/**
 * `knowledge-server process-inbox [dir]`
 *
 * One-shot command that runs the full pipeline end-to-end:
 *
 *   1. **Upload** — reads Markdown files from the inbox directory (like the
 *      daemon's local-files reader) and inserts them as pending episodes.
 *   2. **Consolidate** — drains all pending episodes through the full
 *      consolidation pipeline (extraction → reconsolidation → contradiction
 *      scan → decay → embeddings).
 *   3. **KB synthesis** — runs a synthesis pass over the knowledge graph to
 *      discover higher-order principles from accumulated entries.
 *
 * Designed for batch/cron use (e.g. Cloud Run Jobs): start, process, exit.
 * No long-running server or daemon — just the pipeline.
 *
 * @param args  CLI arguments. First positional argument is the inbox directory
 *              path. Falls back to LOCAL_FILES_DIR / ~/knowledge if omitted.
 */
export async function runProcessInbox(args: string[]): Promise<void> {
	const { config } = await import("../config.js");
	logger.init(config.logPath);

	// ── Resolve inbox directory ──────────────────────────────────────────────
	const inboxDir = args[0] ? resolve(args[0]) : config.localFilesDir;

	if (!existsSync(inboxDir)) {
		console.error(
			`Inbox directory not found: ${inboxDir}\nPass a directory path as the first argument, or set LOCAL_FILES_DIR.`,
		);
		process.exit(1);
	}

	console.log(`Inbox directory: ${inboxDir}`);

	// ── Phase 1: Upload (inbox → pending_episodes) ──────────────────────────
	console.log("\n── Phase 1: Upload ──");

	const registry = await StoreRegistry.create();
	const db = registry.writableStore();
	const { serverStateDb } = registry;

	// Daemon-local DB for cursor tracking (always local SQLite).
	const daemonDb = new DaemonDB();
	const userId = resolveUserId();

	// Only the local-files reader, pointed at the inbox directory.
	const reader = new LocalFilesEpisodeReader(inboxDir);
	const uploader = new EpisodeUploader(
		[reader],
		serverStateDb,
		daemonDb,
		userId,
	);

	let uploadResult: Awaited<ReturnType<typeof uploader.upload>>;
	try {
		uploadResult = await uploader.upload();
	} finally {
		reader.close();
		daemonDb.close();
	}

	if (uploadResult.episodesUploaded > 0) {
		console.log(
			`  Uploaded ${uploadResult.episodesUploaded} episodes from ${uploadResult.sessionsProcessed} sessions.`,
		);
	} else {
		console.log("  No new files to process.");
	}

	// ── Phase 2: Consolidation (pending_episodes → knowledge) ───────────────
	console.log("\n── Phase 2: Consolidate ──");

	const activation = new ActivationEngine(
		db,
		registry.readStores(),
		registry.writableStores(),
	);
	const consolidation = new ConsolidationEngine(
		db,
		serverStateDb,
		activation,
		[new PendingEpisodesReader(serverStateDb)],
		registry.domainRouter,
	);

	try {
		// Check embedding model consistency before consolidating.
		await runEmbeddingCheck(activation);

		const { pendingSessions } = await consolidation.checkPending();

		if (pendingSessions > 0) {
			console.log(
				`  ${pendingSessions} sessions pending — starting consolidation...`,
			);
		} else {
			console.log("  No pending sessions. Running KB synthesis pass only...");
		}

		const { totalSessions, totalCreated, totalUpdated } =
			await drainConsolidation(consolidation);

		// ── Phase 3: KB Synthesis ────────────────────────────────────────────
		console.log("\n── Phase 3: KB Synthesis ──");
		await runSynthesisPass(consolidation);

		// ── Summary ──────────────────────────────────────────────────────────
		console.log("");
		console.log("Pipeline complete.");
		console.log(`  Files uploaded:     ${uploadResult.episodesUploaded}`);
		console.log(`  Sessions processed: ${totalSessions}`);
		console.log(`  Entries created:    ${totalCreated}`);
		console.log(`  Entries updated:    ${totalUpdated}`);
	} finally {
		consolidation.close();
		await registry.close();
	}
}
