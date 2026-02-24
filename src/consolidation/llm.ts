import { generateText } from "ai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createGoogleGenerativeAI } from "@ai-sdk/google";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { config } from "../config.js";

/**
 * LLM interface for consolidation.
 *
 * Uses the Vercel AI SDK to abstract across providers.
 * The model string (e.g., "anthropic/claude-haiku-4-5") determines:
 * - Which provider SDK to use (Anthropic, Google, OpenAI-compatible)
 * - Which base URL suffix on the unified endpoint
 *
 * Three independent model slots (all configurable via env vars):
 * - extractionModel   — episode → knowledge extraction   (LLM_EXTRACTION_MODEL)
 * - mergeModel        — near-duplicate merge decision     (LLM_MERGE_MODEL)
 * - contradictionModel — contradiction detect + resolve   (LLM_CONTRADICTION_MODEL)
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

/**
 * Send a prompt to a specific model. All LLM calls go through here.
 */
async function complete(
  modelString: string,
  systemPrompt: string,
  userPrompt: string,
  maxTokens = 8192
): Promise<string> {
  const { text } = await generateText({
    model: createModel(modelString),
    system: systemPrompt,
    prompt: userPrompt,
    temperature: 0.2,
    maxOutputTokens: maxTokens,
  });
  return text;
}

/**
 * Parse JSON from an LLM response. Handles markdown code fences and stray
 * text before/after the JSON block.
 *
 * Tries multiple strategies in order:
 * 1. Direct parse (model returned clean JSON)
 * 2. Extract from ```json ... ``` fence
 * 3. Greedy bracket match (last resort)
 */
function parseJSON<T>(response: string, arrayMode: boolean): T | null {
  const strategies = [
    // Strategy 1: direct parse
    () => response.trim(),
    // Strategy 2: extract from code fence
    () => {
      const fence = response.match(/```(?:json)?\s*([\s\S]*?)```/);
      return fence ? fence[1].trim() : null;
    },
    // Strategy 3: greedy bracket match
    () => {
      const bracket = arrayMode
        ? response.match(/\[[\s\S]*\]/)
        : response.match(/\{[\s\S]*\}/);
      return bracket ? bracket[0] : null;
    },
  ];

  for (const strategy of strategies) {
    const candidate = strategy();
    if (!candidate) continue;
    try {
      return JSON.parse(candidate) as T;
    } catch {
      // try next strategy
    }
  }
  return null;
}

export class ConsolidationLLM {
  /**
   * Extract knowledge entries from a batch of episodes.
   * Uses the extraction model (highest quality — complex reasoning task).
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
- If a new episode reinforces an existing "fact" into a recurring pattern, extract the generalized version.
- Example: fact "User X preferred a dashboard" + new episode → pattern "Stakeholders consistently prefer visual formats over raw exports"
- Near-duplicate or contradictory entries are handled by the reconsolidation step after extraction — you don't need to signal conflicts here.

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
    "source": "Brief provenance (e.g., 'session: Churn Analysis, Feb 2026')"
  }
]

If there is nothing new worth extracting, return an empty array: []`;

    const response = await complete(config.llm.extractionModel, systemPrompt, userPrompt);

    const parsed = parseJSON<ExtractedKnowledge[]>(response, true);
    if (!parsed) {
      console.error("[llm] Failed to parse extraction response:", response.slice(0, 500));
      return [];
    }

    return parsed.filter(
      (entry) =>
        entry.content &&
        entry.type &&
        ["fact", "principle", "pattern", "decision", "procedure"].includes(entry.type)
    );
  }

  /**
   * Focused reconsolidation decision for near-duplicate entries (sim ≥ 0.82).
   * Uses the merge model (cheaper — this is essentially a classification task).
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

    const response = await complete(config.llm.mergeModel, systemPrompt, userPrompt, 1024);

    const parsed = parseJSON<MergeDecision>(response, false);
    if (!parsed || !["keep", "update", "replace", "insert"].includes(parsed.action)) {
      console.warn("[llm] decideMerge parse failure — defaulting to insert:", response.slice(0, 200));
      return { action: "insert" }; // safe default — no data loss
    }
    return parsed;
  }

  /**
   * Post-extraction contradiction scan.
   *
   * For a newly inserted/updated entry, checks a set of topic-overlapping
   * candidates (in the 0.4–0.82 similarity band — too dissimilar for decideMerge,
   * but related enough to potentially contradict) and attempts resolution.
   *
   * Uses the contradiction model (Sonnet — nuanced semantic reasoning).
   *
   * Returns only genuine contradictions — `no_conflict` results are filtered
   * before returning. Possible resolutions in the returned array:
   * - "supersede_old" — new entry is more correct; existing entry marked superseded
   * - "supersede_new" — existing entry is more correct; new entry marked superseded
   * - "merge"         — contradiction resolves into a unified entry
   * - "irresolvable"  — genuine tie; both entries flagged for human review
   */
  async detectAndResolveContradiction(
    newEntry: { id: string; content: string; type: string; topics: string[]; confidence: number; createdAt: number },
    candidates: Array<{ id: string; content: string; type: string; topics: string[]; confidence: number; createdAt: number }>
  ): Promise<ContradictionResult[]> {
    if (candidates.length === 0) return [];

    const candidateList = candidates
      .map((c, i) =>
        `[${i + 1}] id: ${c.id}
type: ${c.type} | confidence: ${c.confidence} | created: ${new Date(c.createdAt).toISOString().split("T")[0]}
topics: ${c.topics.join(", ")}
content: ${c.content}`
      )
      .join("\n\n");

    const systemPrompt = `You are a knowledge integrity checker. You will be shown a NEWLY ADDED knowledge entry and a list of EXISTING entries that share related topics.

Your job is to check each existing entry for genuine contradiction with the new entry, and if a contradiction exists, determine how to resolve it.

CONTRADICTION means the two entries make mutually exclusive claims about the same subject. Examples:
- "The server runs on port 8080" vs "The server port was changed to 9090" → contradiction
- "Always pre-aggregate before joining" vs "Pre-aggregation caused wrong results in campaign analysis" → contradiction
- "User prefers dark mode" vs "User prefers light mode" → contradiction

NOT a contradiction:
- Two entries about different aspects of the same topic
- One entry being more specific than the other (that's refinement, not contradiction)
- Temporal statements that don't overlap ("In Q1 we used X" vs "In Q3 we switched to Y")

For each existing entry, respond with one of these resolutions:
- "no_conflict"   — no genuine contradiction
- "supersede_old" — new entry is more correct/recent; existing entry should be superseded
- "supersede_new" — existing entry is more correct; new entry should be downgraded
- "merge"         — apparent contradiction resolves into a unified truth; provide merged content
- "irresolvable"  — genuine tie (equal evidence, equal recency); needs human review

Respond ONLY with a JSON array — one object per candidate, in the same order.`;

    const userPrompt = `NEW ENTRY (just added):
id: ${newEntry.id}
type: ${newEntry.type} | confidence: ${newEntry.confidence} | created: ${new Date(newEntry.createdAt).toISOString().split("T")[0]}
topics: ${newEntry.topics.join(", ")}
content: ${newEntry.content}

EXISTING CANDIDATES to check:
${candidateList}

Respond with a JSON array (one result per candidate, same order):
[
  {
    "candidateId": "...",
    "resolution": "no_conflict|supersede_old|supersede_new|merge|irresolvable",
    "reason": "one sentence explaining why",
    "mergedContent": "only if resolution is merge — the unified entry content",
    "mergedType": "only if resolution is merge",
    "mergedTopics": [],
    "mergedConfidence": 0.0
  }
]`;

    const response = await complete(config.llm.contradictionModel, systemPrompt, userPrompt, 2048);

    const parsed = parseJSON<ContradictionResult[]>(response, true);
    if (!parsed) {
      console.error("[llm] Failed to parse contradiction response:", response.slice(0, 500));
      return [];
    }

    // Filter to only genuine contradictions (skip no_conflict)
    return parsed.filter(
      (r) =>
        r.candidateId &&
        ["supersede_old", "supersede_new", "merge", "irresolvable"].includes(r.resolution)
    );
  }
}

export interface ExtractedKnowledge {
  type: "fact" | "principle" | "pattern" | "decision" | "procedure";
  content: string;
  topics: string[];
  confidence: number;
  scope: "personal" | "team";
  source: string;
}

export type MergeDecision =
  | { action: "keep" }
  | { action: "update"; content: string; type: string; topics: string[]; confidence: number }
  | { action: "replace"; content: string; type: string; topics: string[]; confidence: number }
  | { action: "insert" };

export interface ContradictionResult {
  candidateId: string;
  resolution: "supersede_old" | "supersede_new" | "merge" | "irresolvable";
  reason: string;
  mergedContent?: string;
  mergedType?: string;
  mergedTopics?: string[];
  mergedConfidence?: number;
}
