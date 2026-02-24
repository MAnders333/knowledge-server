import type { Plugin } from "@opencode-ai/plugin";

/**
 * Knowledge Injection Plugin for OpenCode.
 *
 * Implements PASSIVE knowledge activation using the `chat.message` hook:
 * - Fires once per user message, BEFORE the LLM processes it
 * - Queries the knowledge server for semantically relevant entries
 * - Injects matching knowledge as additional message parts
 *
 * This is cue-dependent retrieval: the user's query is the cue,
 * and only relevant knowledge activates. The LLM sees it as context.
 *
 * For multi-turn tool loops, the injected context persists from the first turn.
 * For mid-loop knowledge needs, agents can use the MCP `activate` tool.
 *
 * Installation:
 *   Symlink this file to ~/.config/opencode/plugins/knowledge.ts
 *
 * Configuration:
 *   Set KNOWLEDGE_SERVER_URL environment variable (default: http://127.0.0.1:3179)
 *
 * Design principle: NEVER throw. All errors are caught and silently swallowed.
 * A broken knowledge plugin must never affect OpenCode's core functionality.
 */

const KNOWLEDGE_SERVER_URL =
  process.env.KNOWLEDGE_SERVER_URL || "http://127.0.0.1:3179";

const safeLog = async (
  client: Parameters<Plugin>[0]["client"],
  level: "debug" | "info" | "warn" | "error",
  message: string
) => {
  try {
    await client.app.log({
      body: { service: "knowledge-plugin", level, message },
    });
  } catch {
    // Logging itself must never throw
  }
};

export const KnowledgePlugin: Plugin = async (ctx) => {
  // Verify server is reachable on plugin load — but never throw
  try {
    const health = await fetch(`${KNOWLEDGE_SERVER_URL}/status`, {
      signal: AbortSignal.timeout(2000),
    });
    if (health.ok) {
      const data = (await health.json()) as {
        knowledge?: { active?: number };
      };
      await safeLog(
        ctx.client,
        "info",
        `Connected to knowledge server. ${data.knowledge?.active || 0} active entries.`
      );
    }
  } catch {
    await safeLog(
      ctx.client,
      "warn",
      `Knowledge server not reachable at ${KNOWLEDGE_SERVER_URL}. Will retry on first message.`
    );
  }

  return {
    "chat.message": async (input, output) => {
      try {
        // Extract text from the user message parts
        const textParts = output.parts
          .filter(
            (p): p is { type: "text"; text: string } =>
              "type" in p && p.type === "text" && "text" in p && !!p.text
          )
          .map((p) => p.text);

        if (textParts.length === 0) {
          await safeLog(ctx.client, "debug", "chat.message fired — no text parts, skipping");
          return;
        }

        const queryText = textParts.join("\n");

        await safeLog(ctx.client, "debug", `chat.message fired — session: ${input.sessionID}, query length: ${queryText.length} chars`);

        // Skip very short messages (greetings, confirmations, "yes", "continue")
        if (queryText.length < 20) {
          await safeLog(ctx.client, "debug", "chat.message skipped — query too short");
          return;
        }

        // Truncate to 500 chars for embedding — enough signal, avoids bloated URLs
        const queryForEmbedding = queryText.slice(0, 500);

        const response = await fetch(
          `${KNOWLEDGE_SERVER_URL}/activate?q=${encodeURIComponent(queryForEmbedding)}`,
          { signal: AbortSignal.timeout(5000) }
        );

        if (!response.ok) {
          await safeLog(ctx.client, "warn", `chat.message — activate request failed: ${response.status}`);
          return;
        }

        const result = (await response.json()) as {
          entries: Array<{
            entry: {
              type: string;
              content: string;
              topics: string[];
              confidence: number;
              scope: string;
            };
            similarity: number;
            staleness: {
              ageDays: number;
              strength: number;
              mayBeStale: boolean;
            };
          }>;
        };

        if (!result.entries || result.entries.length === 0) {
          await safeLog(ctx.client, "debug", "chat.message — no relevant knowledge found");
          return;
        }

        // Format activated knowledge as an injected context part
        const knowledgeLines = result.entries
          .map((r) => {
            const staleTag = r.staleness.mayBeStale ? " [may be outdated]" : "";
            return `- [${r.entry.type}] ${r.entry.content}${staleTag}`;
          })
          .join("\n");

        const contextText = [
          "## Recalled Knowledge (from prior sessions)",
          "Use what is relevant. Verify entries marked [may be outdated] before relying on them.",
          "",
          knowledgeLines,
        ].join("\n");

        // Inject as an additional text part in the user message.
        // TextPart requires id, sessionID, messageID — populate from output.message.
        // synthetic: true = injected by system, not user-typed.
        //   - included in LLM context on turn 1 (toModelMessages does NOT filter synthetic)
        //   - skipped by step-reminder mutation on turn 2+ (not re-wrapped as user message)
        output.parts.push({
          id: `prt_knowledge_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
          sessionID: output.message.sessionID,
          messageID: output.message.id,
          type: "text" as const,
          text: contextText,
          synthetic: true,
        } as Parameters<typeof output.parts.push>[0]);
        await safeLog(ctx.client, "info", `chat.message — injected ${result.entries.length} knowledge entries`);
      } catch (err) {
        await safeLog(ctx.client, "error", `chat.message — unexpected error: ${err}`);
      }
    },

    // Inject knowledge system awareness during compaction
    "experimental.session.compacting": async (_input, output) => {
      try {
        const response = await fetch(`${KNOWLEDGE_SERVER_URL}/status`, {
          signal: AbortSignal.timeout(2000),
        });
        if (!response.ok) return;

        const status = (await response.json()) as {
          knowledge?: { active?: number };
        };

        if (status.knowledge?.active && status.knowledge.active > 0) {
          output.context.push(
            `## Knowledge System\nA knowledge server is running with ${status.knowledge.active} active knowledge entries. These are automatically injected based on user queries -- no manual retrieval needed.`
          );
        }
      } catch {
        // Silent fail
      }
    },
  };
};
