# knowledge-server

Persistent semantic memory for [OpenCode](https://opencode.ai) agents — fully local, no external service required.

Your agents forget everything between sessions. This fixes that. It reads your OpenCode session history, distills what's worth keeping into a local knowledge graph, and automatically surfaces relevant entries whenever you start a new conversation.

## The approach

Most "memory" systems for AI agents work like a notebook: everything gets written in, everything gets retrieved. The result is noise — every session dumps its raw content into the store, retrieval floods the context window with marginally-related facts, and the store grows without bound.

This system is modeled on how human memory actually works. The key insight from cognitive science is that episodic memory (what happened) and semantic memory (what you know) are distinct systems — and the brain doesn't store episodes, it consolidates them. During sleep it replays the day's experiences and extracts only what's worth encoding into long-term memory: facts confirmed, patterns recognized, decisions made, procedures learned.

Three properties make this different from a naive memory store:

**High extraction bar.** The LLM reads session transcripts and asks: would this still be useful six months from now? Most sessions produce nothing. The store only grows when something genuinely new was learned.

**Reconsolidation instead of accumulation.** Before any new entry is inserted, it's embedded and compared to the nearest existing entry. If they're similar enough (cosine similarity ≥ 0.82), a focused LLM call decides whether to keep the existing, update it, replace it, or insert both as distinct. Memory updates rather than accumulates — the same way a person's understanding of a topic changes rather than appends.

**Cue-dependent activation.** Nothing is retrieved proactively. When a new message arrives, the query is embedded and matched against the knowledge graph. Only semantically relevant entries activate. The user's message is the retrieval cue — just like human recall requires a prompt to surface a memory.

The result is a compact, high-signal knowledge graph that gets better over time. Entries that keep proving relevant strengthen; entries that become stale decay and eventually disappear.

## How it works

1. **Episodes** — OpenCode session logs are the raw material. Each conversation is an episode.
2. **Consolidation** — An LLM reads new sessions and extracts what's genuinely worth remembering. The bar is high — most sessions produce nothing. Runs automatically on server startup.
3. **Reconsolidation** — Each new entry is embedded and compared to the nearest existing entry. If similarity ≥ 0.82, a focused LLM call decides whether to keep, update, replace, or insert both. The store updates rather than accumulates.
4. **Activation** — When a query arrives, it's embedded and matched against all entries. Only semantically relevant entries activate. This is cue-dependent retrieval: the query is the cue.
5. **Decay** — Entries that aren't accessed decay over time. Entries that are repeatedly relevant strengthen. Eventually unused entries are archived, then tombstoned.

## Architecture

```
OpenCode session DB (read-only)
        │
        ▼
  EpisodeReader          reads new sessions since cursor
        │
        ▼
  ConsolidationLLM       extracts knowledge entries (high bar: most sessions → [])
        │
        ▼
  Reconsolidation        embed → nearest-neighbor → LLM merge decision
        │
        ▼
  KnowledgeDB (SQLite)   persistent graph with embeddings, strength, decay
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
  "knowledge": {
    "type": "local",
    "command": ["bun", "run", "/path/to/src/mcp/index.ts"],
    "enabled": true
  }
}
```

### OpenCode plugin (`plugin/knowledge.ts`)

Passive injection. Fires on every user message via the `chat.message` hook, before the LLM sees it. Queries `/activate` and injects matching knowledge as a synthetic message part. The LLM sees it as additional context on turn 1.

Design principle: **never throws**. All errors are caught and silently swallowed. A broken plugin must never affect OpenCode's core functionality.

Install by symlinking to `~/.config/opencode/plugins/knowledge.ts`.

### Consolidation engine (`src/consolidation/`)

- `episodes.ts` — reads OpenCode's SQLite session DB, segments long sessions, respects compaction summaries
- `llm.ts` — two LLM calls: `extractKnowledge` (batch extraction) and `decideMerge` (focused reconsolidation)
- `consolidate.ts` — orchestrates the full cycle: read → extract → reconsolidate → decay → embed → advance cursor
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
| `LLM_MODEL` | `anthropic/claude-sonnet-4-6` | Model for consolidation. Prefix routes the provider: `anthropic/`, `google/`, `openai/` |
| `EMBEDDING_MODEL` | `text-embedding-3-large` | Embedding model (OpenAI-compatible API) |
| `EMBEDDING_DIMENSIONS` | `3072` | Embedding dimensions |
| `KNOWLEDGE_PORT` | `3179` | HTTP port |
| `KNOWLEDGE_HOST` | `127.0.0.1` | HTTP host |
| `KNOWLEDGE_DB_PATH` | `~/.local/share/knowledge-server/knowledge.db` | Knowledge database path |
| `OPENCODE_DB_PATH` | `~/.local/share/opencode/opencode.db` | OpenCode session database (read-only) |
| `CONSOLIDATION_MAX_SESSIONS` | `50` | Sessions per consolidation batch |
| `CONSOLIDATION_CHUNK_SIZE` | `10` | Episodes per LLM extraction call |
| `ACTIVATION_MAX_RESULTS` | `10` | Max entries returned by activation |
| `ACTIVATION_SIMILARITY_THRESHOLD` | `0.3` | Minimum cosine similarity to activate |

## Usage

### Start the server

```bash
bun run start
```

On startup, the server counts pending sessions and runs background consolidation if any are found. The HTTP API is available immediately while consolidation runs behind it.

### Trigger consolidation manually

`POST /consolidate` requires the admin token printed at startup:

```bash
# Via HTTP (token is printed to the console when the server starts)
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:3179/consolidate

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

Returns conflicted entries, stale entries (low strength), and team-relevant entries that may warrant external documentation.

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

The token is not persisted — it changes every time the server restarts. This guards against browser-based CSRF attacks on `POST /consolidate` and `POST /reinitialize`: a malicious web page has no way to learn the token (it is never in a cookie, never auto-sent, and changes on every restart), so it cannot forge a valid `Authorization` header. Without the token, any page open in your browser could trigger these operations against your local server.

The `/activate`, `/status`, `/entries`, and `/review` endpoints are intentionally unauthenticated. Adding auth to read endpoints would require either a per-startup token (unusable for manual `curl` inspection) or a static token in `.env` — but any local process that can read `.env` can also read the SQLite database directly. Auth on reads would be security theater against same-user processes, which are already trusted by the OS. For browser-based reads, the same-origin policy provides protection: browsers block cross-origin *responses* from being read by the page, regardless of whether the endpoint is authenticated. Non-browser clients (`curl`, scripts) running as the same user are treated as trusted by design.

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
bun test             # Run tests (29 tests)
bun run lint         # Biome lint
bun run format       # Biome format
```

Data directory: `~/.local/share/knowledge-server/`
