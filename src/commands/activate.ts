import { ActivationEngine } from "../activation/activate.js";
import { createRerankerFromConfig } from "../activation/rerank.js";
import { StoreRegistry } from "../db/store-registry.js";

/**
 * `knowledge-server activate <query>`
 *
 * Tests knowledge activation for a query — shows which entries would be
 * injected into a conversation for that query, with similarity scores.
 */
export async function runActivate(query: string): Promise<void> {
	if (!query) {
		console.error("Usage: knowledge-server activate <query>");
		process.exit(1);
	}

	const registry = await StoreRegistry.create();
	const activation = new ActivationEngine(
		registry.writableStore(),
		registry.readStores(),
		registry.writableStores(),
		createRerankerFromConfig(),
	);

	try {
		const result = await activation.activate(query);

		console.log(`Activation results for: "${query}"`);
		console.log("───────────────────────────────────────");
		console.log(
			`  ${result.entries.length} of ${result.totalActive} active entries matched\n`,
		);

		if (result.entries.length === 0) {
			console.log("  No entries matched the similarity threshold.");
			return;
		}

		for (const r of result.entries) {
			console.log(`  [${r.entry.type}] ${r.entry.content}`);
			console.log(
				`  topics: ${r.entry.topics.join(", ")}  |  ` +
					`similarity: ${r.rawSimilarity.toFixed(3)}  |  ` +
					`score: ${r.similarity.toFixed(3)}`,
			);
			console.log("");
		}
	} finally {
		await registry.close();
	}
}
