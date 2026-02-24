import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { KnowledgeDB } from "../db/database.js";
import { ActivationEngine } from "../activation/activate.js";
// @ts-ignore — Bun supports JSON imports natively
import pkg from "../../package.json" with { type: "json" };

/**
 * MCP server interface for the knowledge system.
 *
 * Exposes a single tool: `activate`
 *
 * This is the same mechanism used by the passive plugin,
 * but available for agents to use deliberately when they
 * want to "actively recall" knowledge by providing cues.
 *
 * Usage: Agent sends cues (keywords, topics, questions)
 * and receives associated knowledge entries ranked by relevance.
 */
async function main() {
  const db = new KnowledgeDB();
  const activation = new ActivationEngine(db);

  const server = new McpServer({
    name: "knowledge-server",
    version: pkg.version,
  });

  server.tool(
    "activate",
    "Activate associated knowledge by providing cues. Returns knowledge entries that are semantically related to the provided cues. Use this when you need to recall what has been learned from prior sessions about a specific topic. Provide descriptive cues — topics, questions, or keywords — and receive relevant knowledge entries ranked by association strength.",
    {
      cues: z
        .string()
        .describe(
          "One or more cues to activate associated knowledge. Can be a question, topic description, or comma-separated keywords. Example: 'churn analysis, segment X, onboarding'"
        ),
    },
    async ({ cues }) => {
      try {
        const result = await activation.activate(cues);

        if (result.entries.length === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: "No relevant knowledge found for these cues.",
              },
            ],
          };
        }

        const formatted = result.entries
          .map(
            (r, i) =>
              `${i + 1}. [${r.entry.type}] ${r.entry.content}\n   Topics: ${r.entry.topics.join(", ")}\n   Confidence: ${r.entry.confidence} | Scope: ${r.entry.scope} | Relevance: ${r.similarity.toFixed(3)}`
          )
          .join("\n\n");

        return {
          content: [
            {
              type: "text" as const,
              text: `## Activated Knowledge (${result.entries.length} entries, ${result.totalActive} total active)\n\n${formatted}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error activating knowledge: ${e}`,
            },
          ],
          isError: true,
        };
      }
    }
  );

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch(console.error);
