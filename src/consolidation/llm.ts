import { generateText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createGoogleGenerativeAI } from "@ai-sdk/google";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { config } from "../config.js";

/**
 * LLM interface for consolidation.
 *
 * Uses the Vercel AI SDK to abstract across providers.
 * The model string (e.g., "google/gemini-2.5-flash") determines:
 * - Which provider SDK to use (Anthropic, Google, OpenAI-compatible)
 * - Which base URL suffix on the unified endpoint
 *
 * All providers share the same API key (unified endpoint handles routing).
 */

/**
 * Provider routing based on model string prefix.
 *
 * Model format: "provider/model-name"
 * - "anthropic/..." -> Anthropic SDK, .../anthropic/v1
 * - "google/..."    -> Google SDK, .../gemini/v1beta
 * - "openai/..."    -> OpenAI-compatible SDK, .../openai/v1
 * - anything else   -> OpenAI-compatible (fallback)
 */
function createModel(modelString: string) {
  const [providerName, ...modelParts] = modelString.split("/");
  const modelId = modelParts.join("/");
  const baseEndpoint = config.llm.baseEndpoint;
  const apiKey = config.llm.apiKey;

  switch (providerName) {
    case "anthropic": {
      const provider = createAnthropic({
        baseURL: `${baseEndpoint}/anthropic/v1`,
        apiKey,
      });
      return provider(modelId);
    }
    case "google": {
      const provider = createGoogleGenerativeAI({
        baseURL: `${baseEndpoint}/gemini/v1beta`,
        apiKey,
      });
      return provider(modelId);
    }
    default: {
      const provider = createOpenAICompatible({
        name: providerName,
        baseURL: `${baseEndpoint}/openai/v1`,
        apiKey,
      });
      return provider.chatModel(modelId);
    }
  }
}

export class ConsolidationLLM {
  /**
   * Send a prompt to the configured consolidation model.
   * Returns the text response.
   */
  async complete(systemPrompt: string, userPrompt: string): Promise<string> {
    const { text } = await generateText({
      model: createModel(config.llm.model),
      system: systemPrompt,
      prompt: userPrompt,
      temperature: 0.2,
      maxOutputTokens: 8192,
    });

    return text;
  }

  /**
   * Extract knowledge entries from a batch of episodes.
   * Returns structured JSON that we parse into knowledge entries.
   */
  async extractKnowledge(
    episodeSummaries: string,
    existingKnowledge: string
  ): Promise<ExtractedKnowledge[]> {
    const systemPrompt = `You are a knowledge consolidation engine. Your job is to distill raw conversation episodes into structured, durable knowledge entries.

You operate like the human brain during sleep consolidation — most experiences fade, only genuinely useful things are encoded into long-term memory.

THE BAR IS HIGH. Most episodes should produce NO entries (return []). Only encode something if a future version of yourself would genuinely benefit from remembering it across sessions. Ask yourself: "Would I be glad this was in my memory six months from now?" If not, skip it.

Knowledge types:
- "fact": A specific, stable piece of information (e.g., "The MI Jira Team field ID is customfield_11000 = '370a2d4c...'")
- "principle": A general rule derived from experience (e.g., "Always pre-aggregate before joining large tables")
- "pattern": A recurring tendency worth anticipating (e.g., "Stakeholders consistently prefer visual outputs over raw exports")
- "decision": An architectural or design choice with rationale (e.g., "Chose BigQuery over Snowflake because of existing GCP infra")
- "procedure": A non-obvious multi-step workflow (e.g., "To deploy: run X, wait for Y, then trigger Z")

Scope:
- "personal": Only relevant to this individual's workflow
- "team": Relevant to any team member (schemas, business rules, processes)

ENCODE if:
- It's a concrete, reusable fact that would otherwise require looking up (API field IDs, custom statuses, naming conventions, config values)
- It's a decision with rationale that would be hard to reconstruct later
- It's a non-obvious procedure or workflow that took effort to figure out
- It's a principle or pattern confirmed across multiple observations

DO NOT ENCODE if:
- The episode is just Q&A, debugging, exploration, or trial-and-error with no lasting conclusion
- The information is obvious, easily googleable, or derivable from first principles
- It's only relevant to that specific moment (e.g., "fixed a typo in X")
- It duplicates or closely restates something already in EXISTING KNOWLEDGE
- It's a version number, model name, or configuration value likely to change soon
- The session was mostly back-and-forth clarification with no concrete outcome

KNOWLEDGE EVOLUTION — when existing knowledge should be upgraded:
- If a new episode reinforces an existing "fact" into a recurring pattern, extract the generalized version and set conflicts_with to the existing fact's content.
- Example: fact "User X preferred a dashboard" + new episode → pattern "Stakeholders consistently prefer visual formats over raw exports"

CONFLICT HANDLING:
- If a new entry contradicts an existing one, set conflicts_with to the EXACT content string of the existing entry.
- The new entry replaces the old understanding. Be precise — the old entry will be superseded.

FORMAT:
- Each entry: 1-3 sentences, self-contained, no assumed context.
- Confidence: 0.9+ for explicitly stated facts, 0.7–0.9 for strong inferences, 0.5–0.7 for tentative patterns.

Respond ONLY with a JSON array. No markdown, no explanation. Return [] if nothing meets the bar.`;

    const userPrompt = `## EXISTING KNOWLEDGE
${existingKnowledge || "(No existing knowledge yet -- this is a fresh start)"}

## RECENT EPISODES
${episodeSummaries}

Extract knowledge entries as a JSON array:
[
  {
    "type": "fact|principle|pattern|decision|procedure",
    "content": "The knowledge itself (1-3 sentences)",
    "topics": ["topic1", "topic2"],
    "confidence": 0.5-1.0,
    "scope": "personal|team",
    "source": "Brief provenance (e.g., 'session: Churn Analysis, Feb 2026')",
    "conflicts_with": null or "exact content string of the existing entry this supersedes"
  }
]

If there is nothing new worth extracting, return an empty array: []`;

    const response = await this.complete(systemPrompt, userPrompt);

    try {
      // Extract JSON from response (handle potential markdown wrapping)
      const jsonMatch = response.match(/\[[\s\S]*\]/);
      if (!jsonMatch) return [];

      const parsed = JSON.parse(jsonMatch[0]) as ExtractedKnowledge[];
      return parsed.filter(
        (entry) =>
          entry.content &&
          entry.type &&
          ["fact", "principle", "pattern", "decision", "procedure"].includes(
            entry.type
          )
      );
    } catch (e) {
      console.error("Failed to parse LLM extraction response:", e);
      console.error("Raw response:", response.slice(0, 500));
      return [];
    }
  }

  /**
   * Focused reconsolidation decision.
   *
   * Given one existing knowledge entry and one newly extracted observation,
   * decide what to do. Models how recalled memories become labile (editable)
   * when new related information is encountered.
   *
   * Returns one of:
   * - "keep"    — existing entry is correct and complete, discard the new observation
   * - "update"  — existing entry should be enriched/expanded with new detail
   * - "replace" — new observation supersedes the existing entry entirely
   * - "insert"  — they are genuinely distinct, insert the new one as a separate entry
   */
  async decideMerge(
    existing: { content: string; type: string; topics: string[]; confidence: number },
    extracted: { content: string; type: string; topics: string[]; confidence: number }
  ): Promise<MergeDecision> {
    const systemPrompt = `You are a knowledge memory manager. You will be shown an EXISTING memory entry and a NEW observation that is semantically similar to it.

Your job is to decide what to do with the new observation:

- "keep"    — The existing entry already captures this fully. The new observation adds nothing. Discard it.
- "update"  — The existing entry is partially correct but the new observation adds important detail, nuance, or a correction. Merge them into an improved version of the existing entry.
- "replace" — The new observation is a clear upgrade (more general, more accurate, or supersedes) that should entirely replace the existing entry.
- "insert"  — Despite surface similarity, they capture genuinely distinct knowledge. Keep both.

Rules:
- Prefer "keep" when the new observation is just a restatement or minor rephrasing.
- Prefer "update" when the new observation adds a specific detail, exception, or expanded context.
- Prefer "replace" when the new observation generalizes the existing fact into a pattern/principle, or corrects it.
- Prefer "insert" only when they are genuinely about different things despite similar wording.

Respond ONLY with a JSON object. No markdown, no explanation.

If the action is "update" or "replace", include the full improved content (incorporating both the existing entry and the new observation), the best type, topics array, and confidence.`;

    const userPrompt = `EXISTING ENTRY:
type: ${existing.type}
topics: ${existing.topics.join(", ")}
confidence: ${existing.confidence}
content: ${existing.content}

NEW OBSERVATION:
type: ${extracted.type}
topics: ${extracted.topics.join(", ")}
confidence: ${extracted.confidence}
content: ${extracted.content}

Respond with one of:
{"action": "keep"}
{"action": "update", "content": "...", "type": "...", "topics": [...], "confidence": 0.0}
{"action": "replace", "content": "...", "type": "...", "topics": [...], "confidence": 0.0}
{"action": "insert"}`;

    const response = await this.complete(systemPrompt, userPrompt);

    try {
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) return { action: "insert" };
      const parsed = JSON.parse(jsonMatch[0]) as MergeDecision;
      if (!["keep", "update", "replace", "insert"].includes(parsed.action)) {
        return { action: "insert" };
      }
      return parsed;
    } catch {
      // On parse failure, default to insert (safe — no data loss)
      return { action: "insert" };
    }
  }
}

export interface ExtractedKnowledge {
  type: "fact" | "principle" | "pattern" | "decision" | "procedure";
  content: string;
  topics: string[];
  confidence: number;
  scope: "personal" | "team";
  source: string;
  conflicts_with: string | null;
}

export type MergeDecision =
  | { action: "keep" }
  | { action: "update"; content: string; type: string; topics: string[]; confidence: number }
  | { action: "replace"; content: string; type: string; topics: string[]; confidence: number }
  | { action: "insert" };
