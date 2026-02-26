# knowledge-server

Persistent semantic memory for [OpenCode](https://opencode.ai) agents — fully local, no external service required.

Reads your OpenCode session history, extracts what's worth keeping into a local SQLite knowledge store, and injects relevant entries into new conversations automatically.

## Design

The basic problem with agent memory is that naive approaches store everything and retrieve too much — the context window fills with loosely-related facts, and the store grows without bound.

This system takes a different approach, with three properties that distinguish it from a simple write-everything/retrieve-top-k store:

**High extraction bar.** The LLM reads session transcripts and asks whether each thing learned would still be useful months from now. Most sessions produce nothing. The store only grows when something genuinely new was established — a confirmed fact, a decision made, a procedure that worked.

**Reconsolidation instead of accumulation.** Before a new entry is inserted, it's embedded and compared to the nearest existing entry. If similarity ≥ 0.82, a focused LLM call decides whether to keep the existing entry, update it, replace it, or insert both as distinct entries. Entries in the 0.4–0.82 band (related but not near-duplicate) get a contradiction scan: if two entries make mutually exclusive claims, the system resolves it — newer supersedes older, they merge, or the conflict is flagged for human review. The store updates rather than appends.

**Cue-dependent activation.** Nothing is retrieved proactively. When a new message arrives, its text is embedded and matched against all stored entries. Only semantically similar entries activate. The query is the retrieval cue — entries that have no bearing on the current conversation stay silent.

Entries have a strength score that decays with time and inactivity, and increases with repeated access. Entries that fall below the archive threshold are eventually removed. There is no manual pruning.

The similarity thresholds (0.82 for reconsolidation, 0.4 for the contradiction scan band) were calibrated against `text-embedding-3-large` and may need adjustment for other embedding models. Extraction quality depends on the LLM — cheaper models tend to over-extract or miss nuance.

## How it works

1. **Episodes** — OpenCode session logs are the raw input. Each conversation is an episode.
2. **Consolidation** — On startup, an LLM reads any new sessions and extracts entries worth keeping. Most sessions produce nothing.
3. **Reconsolidation** — Each candidate entry is embedded and compared to the nearest existing entry. If similarity ≥ 0.82, a second LLM call decides whether to keep the existing, update it, replace it, or insert both. The store updates rather than appends.
4. **Contradiction scan** — Entries in the 0.4–0.82 similarity band are checked for genuine contradictions. Resolution options: `supersede_old`, `supersede_new`, `merge`, or `irresolvable` (flagged for human review).
5. **Activation** — On each new user message, the query text is embedded and matched against all entries. Entries above the similarity threshold (default: 0.30) are injected into the conversation context.
6. **Decay** — Entry strength decays with age and inactivity, increases with access. Strength below 0.15 → archived. Archived for 180+ days → tombstoned.

## Architecture

```
OpenCode session DB (read-only)
        │
        ▼
  EpisodeReader          reads new sessions since cursor
        │
        ▼
  ConsolidationLLM       extracts knowledge entries (high bar: most sessions → [])
  [extractionModel]
        │
        ▼
  Reconsolidation        embed → nearest-neighbor → LLM merge decision
  [mergeModel]           (sim ≥ 0.82 → keep/update/replace/insert)
        │
        ▼
  Contradiction scan     topic overlap → mid-band similarity filter → LLM resolution
  [contradictionModel]   (0.4 ≤ sim < 0.82 → supersede/merge/flag)
        │
        ▼
  KnowledgeDB (SQLite)   persistent graph with embeddings, strength, decay, relations
        │
        ▼
  ActivationEngine       cosine similarity search over embeddings
        │
   ┌────┴────┐
   ▼         ▼
HTTP API    MCP server
   │
   ▼
OpenCode plugin (passive injection on every user message)
```

## Components

### HTTP API (`src/index.ts`, `src/api/server.ts`)

Hono-based HTTP server. Starts on `127.0.0.1:3179` by default.

| Endpoint | Method | Description |
|---|---|---|
| `/activate?q=...` | GET | Activate knowledge entries by query |
| `/consolidate` | POST | Run a consolidation batch |
| `/reinitialize?confirm=yes` | POST | Wipe all entries and reset cursor |
| `/status` | GET | Health check and stats |
| `/entries` | GET | List entries (filter by `status`, `type`, `scope`) |
| `/entries/:id` | GET | Get a specific entry with relations |
| `/review` | GET | Surface conflicted, stale, and team-relevant entries |

### MCP server (`src/mcp/index.ts`)

Exposes a single tool: `activate`. Agents use this for deliberate recall — when they want to pull knowledge about a specific topic mid-task. Same underlying mechanism as the passive plugin.

```json
{
  "mcp": {
    "knowledge": {
      "type": "local",
      "command": ["bun", "run", "/absolute/path/to/knowledge-server/src/mcp/index.ts"],
      "enabled": true,
      "environment": {
        "LLM_API_KEY": "<your key>",
        "LLM_BASE_ENDPOINT": "<your endpoint>"
      }
    }
  }
}
```

`bun run setup` prints a ready-to-paste version of this block with the correct absolute path and your `.env` values already interpolated.

### OpenCode plugin (`plugin/knowledge.ts`)

Passive injection. Fires on every user message via the `chat.message` hook, before the LLM sees it. Queries `/activate` and injects matching knowledge as a synthetic message part. The LLM sees it as additional context on turn 1.

Design principle: **never throws**. All errors are caught and silently swallowed. A broken plugin must never affect OpenCode's core functionality.

Install by symlinking to `~/.config/opencode/plugins/knowledge.ts`.

### Consolidation engine (`src/consolidation/`)

- `episodes.ts` — reads OpenCode's SQLite session DB, segments long sessions, respects compaction summaries
- `llm.ts` — three LLM calls across two model slots: `extractKnowledge` (extraction model), `decideMerge` (merge model — cheaper), `detectAndResolveContradiction` (contradiction model)
- `consolidate.ts` — orchestrates the full cycle: read → extract → reconsolidate → contradiction scan → decay → embed → advance cursor
- `decay.ts` — forgetting curve with type-specific half-lives (facts decay faster than procedures)

## Installation

**Prerequisites:** [Bun](https://bun.sh), OpenCode with an active session database.

```bash
git clone <repo>
cd knowledge-server
cp .env.template .env
# Edit .env and set LLM_API_KEY
bun run setup
bun run start
```

`setup` installs dependencies, creates the data directory, and symlinks the plugin and commands into your OpenCode config.

## Configuration

All config is via environment variables in `.env`. Defaults are sensible for local use.

| Variable | Default | Description |
|---|---|---|
| `LLM_API_KEY` | — | **Required.** API key for the LLM endpoint |
| `LLM_BASE_ENDPOINT` | — | **Required.** Base URL for LLM API. Provider-specific paths are appended automatically (`/anthropic/v1`, `/openai/v1`, etc.) |
| `LLM_EXTRACTION_MODEL` | `anthropic/claude-sonnet-4-6` | Model for episode → knowledge extraction. Prefix routes the provider: `anthropic/`, `google/`, `openai/` |
| `LLM_MERGE_MODEL` | `anthropic/claude-haiku-4-5` | Model for near-duplicate merge decisions (cheaper — essentially a classification call) |
| `LLM_CONTRADICTION_MODEL` | `anthropic/claude-sonnet-4-6` | Model for contradiction detection and resolution (nuanced — fires rarely) |
| `EMBEDDING_MODEL` | `text-embedding-3-large` | Embedding model (OpenAI-compatible API) |
| `EMBEDDING_DIMENSIONS` | `3072` | Embedding dimensions |
| `KNOWLEDGE_PORT` | `3179` | HTTP port |
| `KNOWLEDGE_HOST` | `127.0.0.1` | HTTP host |
| `KNOWLEDGE_DB_PATH` | `~/.local/share/knowledge-server/knowledge.db` | Knowledge database path |
| `KNOWLEDGE_ADMIN_TOKEN` | *(random)* | Fixed admin token for scripted use. If unset, a random token is generated per process lifetime and printed at startup |
| `OPENCODE_DB_PATH` | `~/.local/share/opencode/opencode.db` | OpenCode session database (read-only) |
| `CONSOLIDATION_MAX_SESSIONS` | `50` | Sessions per consolidation batch |
| `CONSOLIDATION_CHUNK_SIZE` | `10` | Episodes per LLM extraction call |
| `CONTRADICTION_MIN_SIMILARITY` | `0.4` | Lower bound of the contradiction scan similarity band (upper bound is always 0.82) |
| `ACTIVATION_MAX_RESULTS` | `10` | Max entries returned by activation |
| `ACTIVATION_SIMILARITY_THRESHOLD` | `0.3` | Minimum cosine similarity to activate |

## Usage

### Start the server

```bash
bun run start
```

On startup, the server counts pending sessions and runs background consolidation if any are found. The HTTP API is available immediately while consolidation runs behind it.

### Trigger consolidation manually

`POST /consolidate` and `POST /reinitialize` require the admin token printed at startup:

```bash
# Via HTTP (token is printed to the console when the server starts)
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:3179/consolidate
curl -X POST -H "Authorization: Bearer <token>" 'http://127.0.0.1:3179/reinitialize?confirm=yes'

# Via CLI (no token needed — calls the consolidation engine directly)
bun run consolidate
```

### Check status

```bash
curl http://127.0.0.1:3179/status
```

### Query knowledge directly

```bash
curl "http://127.0.0.1:3179/activate?q=how+do+we+handle+auth"
```

### Review knowledge health

```bash
curl http://127.0.0.1:3179/review
```

Returns:
- **conflicted** — entries flagged as `irresolvable` by the contradiction scan, needing human resolution
- **stale** — active entries with low strength (haven't been accessed recently)
- **team-relevant** — high-confidence `team`-scoped entries that may warrant external documentation

## Knowledge entry types

| Type | Description | Example half-life |
|---|---|---|
| `fact` | A confirmed factual statement | ~30 days |
| `pattern` | A recurring pattern or observation | ~90 days |
| `decision` | A decision made and its rationale | ~120 days |
| `principle` | A guiding principle or preference | ~180 days |
| `procedure` | A step-by-step process or workflow | ~365 days |

Entries decay based on age and access frequency. Strength drops below 0.15 → archived. Archived for 180+ days → tombstoned.

## Security

### Admin token

`POST /consolidate` and `POST /reinitialize` require an admin token. The token is generated randomly at startup and printed to the console:

```
Admin token: a3f9c2e1b4d7...
Usage: curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:3179/consolidate
```

By default the token is not persisted — it changes every time the server restarts. Set `KNOWLEDGE_ADMIN_TOKEN` in `.env` to use a stable token instead (useful for scripted/automated use). This guards against browser-based CSRF attacks on `POST /consolidate` and `POST /reinitialize`: a malicious web page has no way to learn the token (it is never in a cookie, never auto-sent, and changes on every restart), so it cannot forge a valid `Authorization` header. Without the token, any page open in your browser could trigger these operations against your local server.

The `/activate`, `/status`, `/entries`, and `/review` endpoints are intentionally unauthenticated. Adding auth to read endpoints would require either a per-startup token (unusable for manual `curl` inspection) or a static token in `.env` — but any local process that can read `.env` can also read the SQLite database directly. Auth on reads would be security theater against same-user processes, which are already trusted by the OS. For browser-based reads, a cross-origin page can still *send* requests to these endpoints, but the browser's same-origin policy prevents the page's JavaScript from reading the response body — so the knowledge graph content cannot be exfiltrated that way. Non-browser clients (`curl`, scripts) running as the same user are treated as trusted by design.

On a shared multi-user machine, run the server behind a reverse proxy with authentication.

### Localhost only

The server binds to `127.0.0.1` by default and will exit at startup if `KNOWLEDGE_HOST` is set to a non-loopback address. There is no TLS — this server is not designed to be exposed on a network.

### Prompt injection

The consolidation pipeline sends raw session content to an LLM. The more realistic risk is not a dedicated attacker — it's adversarial text that ended up in your own sessions: code you pasted, web content you discussed, or documentation that contained prompt-like instructions. Such content could in principle influence what gets consolidated into the knowledge graph. The extraction prompt is hardened against this, and any injected entry would still need to pass the similarity threshold and reconsolidation check — but no instruction-following model is fully immune. Be aware of this if you regularly paste large amounts of external content into your coding sessions.

### Rate limiting

The `/activate` endpoint makes a paid embedding API call per request. There is no rate limiting — this is intentional for a personal local tool where the call volume is naturally bounded by typing speed. If you expose the server to other processes, consider adding a rate limit.

## Development

```bash
bun run dev          # Watch mode
bun test             # Run tests (50 tests)
bun run lint         # Biome lint
bun run format       # Biome format
```

Data directory: `~/.local/share/knowledge-server/`
