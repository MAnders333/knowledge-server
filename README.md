# knowledge-server

Persistent semantic memory for [OpenCode](https://opencode.ai), [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview), [Cursor](https://cursor.com), [Codex CLI](https://github.com/openai/codex), and [VSCode](https://code.visualstudio.com) (GitHub Copilot) — fully local, no external service required.

Reads your session history, extracts what's worth keeping into a local SQLite knowledge store, and injects relevant entries into new conversations automatically.

## Install

Supports **Linux x64** and **macOS arm64** (Apple Silicon). No Bun or Node.js required.

```bash
curl -fsSL https://raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh | bash
```

This downloads the server binary into `~/.local/share/knowledge-server/` and generates a `.env` template.

**After running:**

1. Edit `~/.local/share/knowledge-server/.env` — set `LLM_API_KEY` and `LLM_BASE_ENDPOINT`
2. Run the setup command for your tool(s):
   - `knowledge-server setup-tool opencode` — symlinks the plugin and commands, registers the MCP server in `opencode.jsonc`
   - `knowledge-server setup-tool claude-code` — registers the MCP server, `UserPromptSubmit` hook, and slash commands
   - `knowledge-server setup-tool cursor` — registers the MCP server in `~/.cursor/mcp.json`
   - `knowledge-server setup-tool codex` — registers the MCP server in `~/.codex/config.toml`
   - `knowledge-server setup-tool vscode` — registers the MCP server via `code --add-mcp`
3. Start the server: `knowledge-server`

**To update later:**

```bash
knowledge-server update
```

### From source

**Prerequisites:** [Bun](https://bun.sh) and at least one supported tool with an active session history.

```bash
git clone https://github.com/MAnders333/knowledge-server
cd knowledge-server
cp .env.template .env
# Edit .env — set LLM_API_KEY and LLM_BASE_ENDPOINT
bun run setup
bun run start
```

`bun run setup` installs dependencies, creates the data directory, and runs `setup-tool opencode` (symlinks the plugin and commands into `~/.config/opencode/`, registers the MCP server in `opencode.jsonc`).

For other tools, run the corresponding setup step:

```bash
bun run src/index.ts setup-tool claude-code  # MCP server + hook + slash commands
bun run src/index.ts setup-tool cursor       # MCP server in ~/.cursor/mcp.json
bun run src/index.ts setup-tool codex        # MCP server in ~/.codex/config.toml
bun run src/index.ts setup-tool vscode       # MCP server via `code --add-mcp`
```

All setup steps are idempotent — re-running is safe.

## Supported session sources

| Source | What is read | Platform |
|---|---|---|
| **OpenCode** | `~/.local/share/opencode/opencode.db` (SQLite) | macOS, Linux |
| **Claude Code** | `~/.claude/projects/**/*.jsonl` (JSONL conversation logs) | macOS, Linux |
| **Codex CLI** | `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` (JSONL rollout logs) | macOS, Linux |
| **Cursor** | `~/Library/Application Support/Cursor/User/globalStorage/state.vscdb` (SQLite) | macOS |
| **Cursor** | `~/.config/Cursor/User/globalStorage/state.vscdb` (SQLite) | Linux |
| **VSCode** | `~/Library/Application Support/Code/User/workspaceStorage/*/chatSessions/*.json` (JSON) | macOS |
| **VSCode** | `~/.config/Code/User/workspaceStorage/*/chatSessions/*.json` (JSON) | Linux |

All sources are enabled by default and auto-detected on startup. Disable any with `OPENCODE_ENABLED=false`, `CLAUDE_ENABLED=false`, `CODEX_ENABLED=false`, `CURSOR_ENABLED=false`, or `VSCODE_ENABLED=false`. Override a path with `OPENCODE_DB_PATH`, `CLAUDE_DB_PATH`, `CODEX_SESSIONS_DIR`, `CURSOR_DB_PATH`, or `VSCODE_DATA_DIR`.

## How it works

Once running, three things happen automatically:

1. **On startup** — the server reads any sessions you've had since it last ran and extracts knowledge entries worth keeping. Most sessions produce nothing; the LLM applies a high bar.
2. **On each new message** — the query is embedded and matched against stored entries. Semantically relevant entries are injected into the conversation as background context before the LLM sees your message.
3. **Over time** — entries that haven't been accessed decay and are eventually archived. The store updates rather than accumulates.

There is also an MCP `activate` tool for agents that want to deliberately recall knowledge mid-task.

## Design

The basic problem with agent memory is that naive approaches store everything and retrieve too much — the context window fills with loosely-related facts, and the store grows without bound.

The design is loosely inspired by how human memory handles the same problem. Episodic memory (what happened in a session) and semantic memory (what you actually know) are distinct. The brain doesn't store episodes wholesale — it consolidates them, extracting what's worth encoding into long-term memory and discarding the rest. Retrieval is also cue-dependent: memories don't surface proactively, they activate in response to context.

That's the rough model here. Three properties follow from it:

**High extraction bar.** The LLM reads session transcripts and asks whether each thing learned would still be useful months from now. Most sessions produce nothing. The store only grows when something genuinely new was established — a confirmed fact, a decision made, a procedure that worked.

**Reconsolidation instead of accumulation.** Before a new entry is inserted, it's embedded and compared to the nearest existing entry. If similarity ≥ 0.82, a focused LLM call decides whether to keep the existing entry, update it, replace it, or insert both as distinct entries. Entries in the 0.4–0.82 band (related but not near-duplicate) get a contradiction scan: if two entries make mutually exclusive claims, the system resolves it — newer supersedes older, they merge, or the conflict is flagged for human review. The store updates rather than appends.

**Cue-dependent activation.** Nothing is retrieved proactively. When a new message arrives, its text is embedded and matched against all stored entries. Only semantically similar entries activate. The query is the retrieval cue — entries that have no bearing on the current conversation stay silent.

Entries have a strength score that decays with time and inactivity, and increases with repeated access. Entries that fall below the archive threshold are eventually removed. There is no manual pruning.

The similarity thresholds (0.82 for reconsolidation, 0.4 for the contradiction scan band) were calibrated against `text-embedding-3-large` and may need adjustment for other embedding models. Extraction quality depends on the LLM — cheaper models tend to over-extract or miss nuance.

## Architecture

```
OpenCode    Claude Code    Cursor    Codex CLI    VSCode
(SQLite)      (JSONL)    (SQLite)    (JSONL)     (JSON)
    │             │          │          │           │
    └─────────────┴──────────┴──────────┴───────────┘
                            ▼
                   EpisodeReader          reads new sessions since cursor (per source)
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
                HTTP API    MCP stdio proxy (`knowledge-server mcp` → GET /activate)
                    │
                  /mcp      MCP streamable-http (built into HTTP server)
                   │
                   ▼
         OpenCode plugin (passive injection on every user message)
         Claude Code hook (passive injection via UserPromptSubmit)
```

## Components

### HTTP API (`src/api/server.ts`)

Hono-based HTTP server. Starts on `127.0.0.1:3179` by default.

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/activate?q=...` | GET | — | Activate knowledge entries by query |
| `/consolidate` | POST | admin | Run a consolidation batch |
| `/reinitialize?confirm=yes` | POST | admin | Wipe all entries and reset cursor |
| `/status` | GET | — (config block requires admin) | Health check and stats |
| `/entries` | GET | — | List entries (filter by `status`, `type`, `scope`) |
| `/entries/:id` | GET | — | Get a specific entry with relations |
| `/entries/:id` | PATCH | admin | Update content, topics, confidence, status, scope |
| `/entries/:id/resolve` | POST | admin | Resolve a conflicted entry pair |
| `/entries/:id` | DELETE | admin | Hard-delete an entry |
| `/review` | GET | — | Surface conflicted, stale, and team-relevant entries |
| `/hooks/claude-code/user-prompt` | POST | — | Claude Code `UserPromptSubmit` hook endpoint |
| `/mcp` | ALL | — (admin token if `KNOWLEDGE_ADMIN_TOKEN` is set) | MCP streamable-http endpoint — connect MCP clients directly via HTTP |

### MCP server

Exposes a single tool: `activate`. Agents use this for deliberate recall — when they want to pull knowledge about a specific topic mid-task.

There are two ways to connect an MCP client:

**stdio (local)** — `knowledge-server mcp` starts a lightweight stdio proxy that forwards `activate` calls to the already-running HTTP server via `GET /activate`. It does not open the database or call any LLM directly. Only `KNOWLEDGE_HOST` and `KNOWLEDGE_PORT` are required; no LLM credentials are needed. The `setup-tool` commands register this automatically.

**streamable-http (local or hosted)** — the main HTTP server also exposes `ALL /mcp` as a stateless MCP endpoint. MCP clients can connect directly at `http://127.0.0.1:3179/mcp` without a separate subprocess. When `KNOWLEDGE_ADMIN_TOKEN` is set, the `/mcp` endpoint requires a `Authorization: Bearer <token>` header — suitable for hosted deployments. When unset (random per-process token), `/mcp` is unauthenticated since the server already binds to `127.0.0.1`.

**OpenCode** — registered automatically by `knowledge-server setup-tool opencode` (or `bun run setup` from source). Writes the `mcp.knowledge` entry directly into `~/.config/opencode/opencode.jsonc`.

**Claude Code** — registered automatically by `knowledge-server setup-tool claude-code` (or `bun run src/index.ts setup-tool claude-code` from source). Uses `claude mcp add-json` to write to `~/.claude.json`.

**Cursor** — registered automatically by `knowledge-server setup-tool cursor`. Writes the `knowledge` entry into `~/.cursor/mcp.json`.

**Codex CLI** — registered automatically by `knowledge-server setup-tool codex`. Registers `[mcp_servers.knowledge]` in `~/.codex/config.toml`.

**VSCode** — registered automatically by `knowledge-server setup-tool vscode`. Uses `code --add-mcp` to register the `knowledge` server in the active profile's `mcp.json`.

### OpenCode plugin (`plugin/knowledge.ts`)

Passive injection. Fires on every user message via the `chat.message` hook, before the LLM sees it. Queries `/activate` and injects matching knowledge as a synthetic message part. The LLM sees it as additional context on turn 1.

Design principle: **never throws**. All errors are caught and silently swallowed. A broken plugin must never affect OpenCode's core functionality.

Install by symlinking to `~/.config/opencode/plugins/knowledge.ts` (the installer does this automatically).

### Claude Code hook (`~/.claude/settings.json`)

Passive injection for Claude Code. A `UserPromptSubmit` HTTP hook calls `POST /hooks/claude-code/user-prompt` before each prompt is processed. The server queries `/activate` and prepends matching knowledge as a system reminder injected into the hook response.

Registered automatically by `setup-tool claude-code`.

### Consolidation engine (`src/consolidation/`)

- `readers/opencode.ts` — reads OpenCode's SQLite session DB, segments long sessions, respects compaction summaries
- `readers/claude-code.ts` — reads Claude Code JSONL session files, handles compacted sessions, correlates tool call results
- `readers/cursor.ts` — reads Cursor's SQLite state DB (`state.vscdb`), handles both inline (Format A) and bubble-per-KV (Format B) conversation layouts
- `readers/codex.ts` — reads Codex CLI JSONL rollout files, two-pass parse for stable session IDs, skips injected context blocks
- `readers/vscode.ts` — reads VSCode / GitHub Copilot Chat session JSON files from per-workspace `chatSessions/` directories, extracts user messages and assistant text responses
- `consolidate.ts` — orchestrates the full cycle: read → extract → reconsolidate → contradiction scan → decay → embed → advance cursor (per source)
- `llm.ts` — three LLM calls across three model slots: `extractKnowledge` (extraction model), `decideMerge` (merge model — cheaper), `detectAndResolveContradiction` (contradiction model)
- `decay.ts` — forgetting curve with type-specific half-lives (facts decay faster than procedures)

## Setup

### OpenCode

```bash
knowledge-server setup-tool opencode    # binary install
bun run src/index.ts setup-tool opencode  # source install
```

This symlinks the plugin and commands into `~/.config/opencode/` and registers the MCP server directly in `~/.config/opencode/opencode.jsonc`. All steps are idempotent — re-running is safe.

### Claude Code

```bash
knowledge-server setup-tool claude-code    # binary install
bun run src/index.ts setup-tool claude-code  # source install
```

This:
1. Registers the `knowledge` MCP server in `~/.claude.json` via `claude mcp add-json` (user scope)
2. Adds a `UserPromptSubmit` hook to `~/.claude/settings.json` pointing at the knowledge server
3. Symlinks slash commands (`consolidate.md`, `knowledge-review.md`) into `~/.claude/commands/`

All three steps are idempotent — re-running is safe.

### Cursor

```bash
knowledge-server setup-tool cursor    # binary install
bun run src/index.ts setup-tool cursor  # source install
```

Registers the `knowledge` MCP server in `~/.cursor/mcp.json`. Idempotent — re-running is safe. Cursor does not support user-defined slash commands, so no command symlinks are created.

### Codex CLI

```bash
knowledge-server setup-tool codex    # binary install
bun run src/index.ts setup-tool codex  # source install
```

Registers the `[mcp_servers.knowledge]` block in `~/.codex/config.toml`. Idempotent — re-running is safe. Codex CLI has no user-defined slash command directory, so no command symlinks are created.

### VSCode

```bash
knowledge-server setup-tool vscode    # binary install
bun run src/index.ts setup-tool vscode  # source install
```

Registers the `knowledge` MCP server via `code --add-mcp`, which writes to the active VSCode profile's `mcp.json`. Requires the `code` CLI to be on PATH (install via VSCode: Cmd+Shift+P → "Shell Command: Install 'code' command in PATH").

VSCode reads GitHub Copilot Chat session files from per-workspace `chatSessions/` directories. No additional hooks or plugins are needed — VSCode natively supports MCP tools through its chat interface.

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
| `EMBEDDING_DIMENSIONS` | *(not set — uses model default)* | Embedding dimensions. Only needed to reduce dimensions (e.g. for storage savings); omit to use the model's native output size |
| `KNOWLEDGE_PORT` | `3179` | HTTP port |
| `KNOWLEDGE_HOST` | `127.0.0.1` | HTTP host |
| `KNOWLEDGE_DB_PATH` | `~/.local/share/knowledge-server/knowledge.db` | Knowledge database path |
| `KNOWLEDGE_ADMIN_TOKEN` | *(random)* | Fixed admin token (≥16 chars) for scripted use. If unset, a random token is generated per process lifetime and printed at startup |
| `OPENCODE_DB_PATH` | `~/.local/share/opencode/opencode.db` | OpenCode session database (read-only) |
| `OPENCODE_ENABLED` | `true` | Set to `false` to disable OpenCode session reading |
| `CLAUDE_DB_PATH` | `~/.claude` | Root directory for Claude Code JSONL session files |
| `CLAUDE_ENABLED` | `true` | Set to `false` to disable Claude Code session reading |
| `CODEX_SESSIONS_DIR` | *(auto-detected)* | Root directory for Codex CLI JSONL rollout files. Auto-detected at `~/.codex/sessions` (or `$CODEX_HOME/sessions` if `CODEX_HOME` is set) |
| `CODEX_ENABLED` | `true` | Set to `false` to disable Codex CLI session reading |
| `CURSOR_DB_PATH` | *(auto-detected)* | Path to Cursor's `state.vscdb`. Auto-detected per platform (macOS: `~/Library/Application Support/Cursor/…`, Linux: `~/.config/Cursor/…`) |
| `CURSOR_ENABLED` | `true` | Set to `false` to disable Cursor session reading |
| `VSCODE_DATA_DIR` | *(auto-detected)* | Path to VSCode's data directory. Auto-detected per platform (macOS: `~/Library/Application Support/Code`, Linux: `~/.config/Code`) |
| `VSCODE_ENABLED` | `true` | Set to `false` to disable VSCode session reading |
| `CONSOLIDATION_MAX_SESSIONS` | `50` | Sessions per consolidation batch |
| `CONSOLIDATION_CHUNK_SIZE` | `10` | Episodes per LLM extraction call |
| `CONTRADICTION_MIN_SIMILARITY` | `0.4` | Lower bound of the contradiction scan similarity band (upper bound is always 0.82) |
| `CONSOLIDATION_POLL_INTERVAL_MS` | `0` (disabled) | Auto-consolidation polling interval in ms while server runs. `0` = disabled; e.g. `1800000` = every 30 min |
| `ACTIVATION_MAX_RESULTS` | `10` | Max entries returned by activation |
| `ACTIVATION_SIMILARITY_THRESHOLD` | `0.3` | Minimum cosine similarity to activate |

## Usage

### Start the server

```bash
knowledge-server        # binary install
bun run start           # source install
```

On startup, the server counts pending sessions across all sources and runs background consolidation if any are found. The HTTP API is available immediately while consolidation runs behind it.

### Trigger consolidation manually

`POST /consolidate` and `POST /reinitialize` require the admin token:

```bash
# Token is printed to the console when the server starts
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:3179/consolidate
curl -X POST -H "Authorization: Bearer <token>" 'http://127.0.0.1:3179/reinitialize?confirm=yes'
```

### Check status

```bash
curl http://127.0.0.1:3179/status
```

### Query knowledge directly

```bash
curl "http://127.0.0.1:3179/activate?q=how+do+we+handle+auth"
```

### Review and resolve knowledge entries

```bash
curl http://127.0.0.1:3179/review
```

Returns:
- **conflicted** — entries flagged as `irresolvable` by the contradiction scan, needing human resolution
- **stale** — active entries with low strength (haven't been accessed recently)
- **team-relevant** — high-confidence `team`-scoped entries that may warrant external documentation

OpenCode and Claude Code users also have a `/knowledge-review` slash command (installed by `setup-tool opencode` and `setup-tool claude-code` respectively) for an interactive review workflow inside the TUI. Cursor, Codex CLI, and VSCode do not support user-defined slash commands.

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

Mutation endpoints (`POST /consolidate`, `POST /reinitialize`, `PATCH /entries/:id`, `POST /entries/:id/resolve`, `DELETE /entries/:id`) require an admin token. A random token is generated at startup and printed to the console:

```
Admin token: a3f9c2e1b4d7...
```

By default the token is not persisted — it changes every time the server restarts. Set `KNOWLEDGE_ADMIN_TOKEN` in `.env` (minimum 16 characters) to use a stable token instead (useful for scripted/automated use). This guards against browser-based CSRF attacks: a malicious web page has no way to learn the token, so it cannot forge a valid `Authorization` header.

The `GET /status` config block (model names, port) is also gated behind the admin token. Unauthenticated callers receive health stats but not configuration details.

### Localhost only

The server binds to `127.0.0.1` by default and will exit at startup if `KNOWLEDGE_HOST` is set to a non-loopback address. There is no TLS — this server is not designed to be exposed on a network.

### Prompt injection

The system has two prompt injection surfaces:

**Consolidation pipeline:** Raw session content is sent to an LLM for knowledge extraction. The more realistic risk is not a dedicated attacker — it's adversarial text that ended up in your own sessions: code you pasted, web content you discussed, or documentation that contained prompt-like instructions. Such content could in principle influence what gets consolidated into the knowledge graph. The extraction prompt is hardened against this, and any injected entry would still need to pass the similarity threshold and reconsolidation check — but no instruction-following model is fully immune. Be aware of this if you regularly paste large amounts of external content into your coding sessions.

**Plugin injection:** Activated knowledge entries are injected verbatim into the LLM context of new sessions. An entry whose content contains instruction-like text could subtly influence agent behaviour. The plugin labels injected entries as "background context, not instructions", which helps but does not fully prevent a well-crafted entry from being followed. Entries that appear suspicious can be reviewed and deleted via `GET /review` and `DELETE /entries/:id`. The extraction bar (most sessions produce no entries) and the reconsolidation deduplication step significantly limit what can reach the store, but no LLM-based system is fully immune to injection.

### Rate limiting

The `/activate` endpoint makes a paid embedding API call per request. There is no rate limiting — this is intentional for a personal local tool where the call volume is naturally bounded by typing speed. If you expose the server to other processes, consider adding a rate limit.

### Binary integrity

Release binaries are distributed as gzip-compressed assets (`.gz`) and verified with SHA-256 checksums on the uncompressed binary. The installer decompresses the binary on the fly (streaming — never buffered in full), then verifies the SHA-256 against `SHA256SUMS-<platform>` from the release before moving it into place. `knowledge-server update` performs the same streaming decompress + checksum check before replacing the running binary.

## Development

```bash
bun run dev          # Watch mode
bun test             # Run tests
bun run lint         # Biome lint
bun run format       # Biome format
```

Data directory: `~/.local/share/knowledge-server/`
