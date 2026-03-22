import * as readline from "node:readline";
import { EmbeddingClient } from "../activation/embeddings.js";
import { REVIEW_STALE_STRENGTH_THRESHOLD } from "../config.js";
import { createKnowledgeDB } from "../db/index.js";
import { KnowledgeService } from "../knowledge-service.js";
import type { KnowledgeEntry } from "../types.js";

const CONTENT_DISPLAY_MAX = 200;

/**
 * `knowledge-server review [--filter <conflicted|stale|all>]`
 *
 * Interactive CLI to page through knowledge entries that need attention:
 *   - conflicted: entries flagged as contradicting each other
 *   - stale: active entries whose strength has decayed below threshold
 *
 * Per entry, the user can:
 *   k  keep    — leave as-is, move to next
 *   d  delete  — hard-delete the entry
 *   a  archive — change status to archived
 *   e  edit    — edit content inline (re-embeds automatically)
 *   q  quit    — stop reviewing, keep remaining entries untouched
 */
export async function runReview(args: string[]): Promise<void> {
	const filterIdx = args.indexOf("--filter");

	// Guard: --filter with no value is a user error, not a silent fallback.
	if (filterIdx !== -1 && args[filterIdx + 1] === undefined) {
		console.error(
			"--filter requires a value. Valid values: conflicted, stale, all",
		);
		process.exit(1);
	}

	const filter: string = filterIdx !== -1 ? args[filterIdx + 1] : "all";

	const validFilters = ["conflicted", "stale", "all"];
	if (!validFilters.includes(filter)) {
		console.error(
			`Unknown filter: "${filter}". Valid values: ${validFilters.join(", ")}`,
		);
		process.exit(1);
	}

	const db = await createKnowledgeDB();
	// Share one EmbeddingClient instance for the lifetime of the command so
	// re-embeds on edit reuse the same config/connection as the rest of the run.
	const embedder = new EmbeddingClient();
	const service = new KnowledgeService(db, embedder);

	try {
		// Gather entries to review
		const toReview: Array<{ entry: KnowledgeEntry; reason: string }> = [];

		if (filter === "conflicted" || filter === "all") {
			const conflicted = await db.getEntriesByStatus("conflicted");
			for (const e of conflicted) {
				toReview.push({ entry: e, reason: "conflicted" });
			}
		}

		if (filter === "stale" || filter === "all") {
			const active = await db.getActiveEntries();
			const stale = active
				.filter((e) => e.strength < REVIEW_STALE_STRENGTH_THRESHOLD)
				.sort((a, b) => a.strength - b.strength);
			for (const e of stale) {
				toReview.push({
					entry: e,
					reason: `stale (strength ${e.strength.toFixed(3)})`,
				});
			}
		}

		// Early return is inside try so db.close() in finally always runs.
		if (toReview.length === 0) {
			console.log("Nothing to review.");
			return;
		}

		console.log(`\nReviewing ${toReview.length} entries (filter: ${filter})`);
		console.log("───────────────────────────────────────");
		console.log(
			"  k  keep     d  delete     a  archive     e  edit     q  quit\n",
		);

		const rl = readline.createInterface({
			input: process.stdin,
			output: process.stdout,
		});

		const prompt = (question: string): Promise<string> =>
			new Promise((resolve) => rl.question(question, resolve));

		let kept = 0;
		let deleted = 0;
		let archived = 0;
		let edited = 0;

		for (let i = 0; i < toReview.length; i++) {
			const { entry, reason } = toReview[i];

			printEntry(entry, reason, i + 1, toReview.length);

			let handled = false;
			while (!handled) {
				const answer = (await prompt("  > ")).trim().toLowerCase();

				switch (answer) {
					case "k":
					case "keep":
					case "":
						kept++;
						handled = true;
						break;

					case "d":
					case "delete": {
						const confirm = await prompt("  Delete this entry? (y/N) ");
						if (confirm.trim().toLowerCase() === "y") {
							await db.deleteEntry(entry.id);
							console.log("  Deleted.\n");
							deleted++;
						} else {
							console.log("  Cancelled.\n");
							kept++;
						}
						handled = true;
						break;
					}

					case "a":
					case "archive":
						// status is a non-semantic field — db.updateEntry is correct here,
						// no re-embedding needed.
						await db.updateEntry(entry.id, { status: "archived" });
						console.log("  Archived.\n");
						archived++;
						handled = true;
						break;

					case "e":
					case "edit": {
						console.log(
							`  Current content:\n    ${truncate(entry.content, CONTENT_DISPLAY_MAX)}`,
						);
						const newContent = await prompt(
							"  New content (blank to cancel): ",
						);
						if (newContent.trim()) {
							// service.updateEntry auto-re-embeds when content changes.
							await service.updateEntry(entry.id, {
								content: newContent.trim(),
							});
							console.log("  Updated.\n");
							edited++;
						} else {
							console.log("  Cancelled.\n");
							kept++;
						}
						handled = true;
						break;
					}

					case "q":
					case "quit":
						rl.close();
						// i is zero-based index of the entry being reviewed when quit was
						// pressed — add 1 so "reviewed" reflects entries seen, not loop index.
						printSummary(
							i + 1,
							toReview.length,
							kept,
							deleted,
							archived,
							edited,
						);
						return;

					default:
						console.log(
							"  Unknown action. Use: k keep  d delete  a archive  e edit  q quit",
						);
				}
			}
		}

		rl.close();
		printSummary(
			toReview.length,
			toReview.length,
			kept,
			deleted,
			archived,
			edited,
		);
	} finally {
		await db.close();
	}
}

function truncate(s: string, max: number): string {
	return s.length > max ? `${s.slice(0, max)}…` : s;
}

function printEntry(
	entry: KnowledgeEntry,
	reason: string,
	index: number,
	total: number,
): void {
	console.log(`[${index}/${total}]  ${reason.toUpperCase()}`);
	console.log(
		`  Type:       ${entry.type}  |  Scope: ${entry.scope}  |  Confidence: ${entry.confidence.toFixed(2)}  |  Strength: ${entry.strength.toFixed(3)}`,
	);
	console.log(`  Topics:     ${entry.topics.join(", ")}`);
	console.log(`  Content:    ${truncate(entry.content, CONTENT_DISPLAY_MAX)}`);
	console.log(`  Source:     ${entry.source}`);
	console.log(
		`  Updated:    ${new Date(entry.updatedAt).toLocaleDateString()}`,
	);
	console.log("");
}

function printSummary(
	reviewed: number,
	total: number,
	kept: number,
	deleted: number,
	archived: number,
	edited: number,
): void {
	const skipped = total - reviewed;
	console.log("\n───────────────────────────────────────");
	console.log(`Review complete. ${reviewed}/${total} entries reviewed.`);
	if (kept > 0) console.log(`  Kept:     ${kept}`);
	if (deleted > 0) console.log(`  Deleted:  ${deleted}`);
	if (archived > 0) console.log(`  Archived: ${archived}`);
	if (edited > 0) console.log(`  Edited:   ${edited}`);
	if (skipped > 0) console.log(`  Skipped:  ${skipped}`);
}
