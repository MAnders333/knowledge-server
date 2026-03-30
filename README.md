# knowledge-server

AI coding agents are stateless by default — every session starts from zero. knowledge-server gives them memory: it runs in the background, learns from your coding conversations, and quietly surfaces what's relevant when you start a new session.

No prompting required. No manual notes. Just context that was missing before.

**How it shows up in practice:**
- You spend an afternoon figuring out why your deployment pipeline fails on certain file patterns. The next week, a teammate starts asking a related question — the agent already knows the conclusion you reached.
- You establish a naming convention during a refactor. Future sessions in that codebase start with that context already loaded.
- A decision about which library to use gets captured. It stops being re-litigated every time someone asks.

Everything happens in the background. The tool reads your sessions, extracts what's worth keeping, and injects relevant entries before the LLM sees your next message. You don't interact with it directly.

Works with [OpenCode](https://opencode.ai), [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview), [Cursor](https://cursor.com), [Codex CLI](https://github.com/openai/codex), and [VSCode](https://code.visualstudio.com) (GitHub Copilot). Single machine or shared across a team — knowledge accumulates in a shared database that everyone's agent draws from.

## Install

Supports **Linux x64** and **macOS arm64** (Apple Silicon). No Bun or Node.js required.

```bash
curl -fsSL https://raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh | bash
```

**After running:**

1. Edit `~/.config/knowledge-server/.env` — set your LLM API key (e.g. `ANTHROPIC_API_KEY=sk-ant-...`)
2. Register with your tool(s):
   ```bash
   knowledge-server setup-tool opencode     # OpenCode
   knowledge-server setup-tool claude-code  # Claude Code
   knowledge-server setup-tool cursor       # Cursor
   knowledge-server setup-tool codex        # Codex CLI
   knowledge-server setup-tool vscode       # VSCode / GitHub Copilot
   ```
3. Start the server: `knowledge-server`

That's it. The server auto-starts the episode collector alongside itself — nothing else to configure for single-machine use.

**To update:**

```bash
knowledge-server update
```

### From source

**Prerequisites:** [Bun](https://bun.sh) ≥ 1.2

```bash
git clone https://github.com/MAnders333/knowledge-server
cd knowledge-server
cp .env.template .env
# Edit .env — set ANTHROPIC_API_KEY or OPENAI_API_KEY
bun run setup   # installs deps, registers OpenCode MCP server
bun run start   # starts both server and daemon
```

For other tools, run the corresponding setup command after `bun run start`:

```bash
bun run start setup-tool claude-code  # Claude Code
bun run start setup-tool cursor       # Cursor
bun run start setup-tool codex        # Codex CLI
bun run start setup-tool vscode       # VSCode
```

All setup steps are idempotent — re-running is safe.

## How it works

Once running, three things happen automatically and passively:

1. **Episode collection** — `knowledge-daemon` (started alongside the server) reads your AI tool session files in the background and uploads new conversations to a staging table.
2. **Consolidation** — every 8 hours, the server reads staged conversations and asks an LLM: *what from this session is worth remembering long-term?* Most sessions produce nothing. High bar.
3. **Activation** — on every new message, the query is embedded and matched against stored entries. Only semantically relevant entries activate, injected silently as context before the LLM sees your message.

There is no manual step. Knowledge accumulates in the background.

## Design

The core problem with agent memory is that naive approaches store everything and retrieve too much. Context windows fill with loosely-related facts, and the store grows without bound.

The design is loosely inspired by how human memory consolidates experience. Three properties follow from this:

**High extraction bar.** The LLM reads session transcripts and asks whether each thing learned would still be useful months from now. Most sessions produce nothing. The store only grows when something genuinely new was established — a confirmed fact, a decision made, a procedure that worked.

**Reconsolidation instead of accumulation.** Before a new entry is inserted, it's embedded and compared to the nearest existing entry. If similarity ≥ 0.82, a focused LLM call decides whether to keep, update, replace, or insert both. Entries in the 0.4–0.82 band (related but not near-duplicate) get a contradiction scan: if two entries make mutually exclusive claims, the system resolves it. The store updates rather than appends.

**Cue-dependent activation.** Nothing is retrieved proactively. When a new message arrives, its text is embedded and matched against all stored entries. Only semantically similar entries activate. The query is the retrieval cue — entries that have no bearing on the current conversation stay silent.

Entries have a strength score that decays with time and inactivity, and increases with repeated access. Entries that fall below the archive threshold are eventually removed. There is no manual pruning.

## Architecture

```
  ┌─────── knowledge-daemon (local, lightweight) ───────────────────────────┐
  │                                                                          │
  │  OpenCode  Claude Code  Cursor  Codex CLI  VSCode  Local Files          │
  │  (SQLite)    (JSONL)  (SQLite)   (JSONL)   (JSON)  (Markdown)           │
  │      └──────────┴─────────┴──────────┴──────────┴──────────┘           │
  │                           EpisodeReaders                                 │
  │                           (reads session files, tracks cursor)           │
  │                                  │                                       │
  │                           EpisodeUploader                                │
  └──────────────────────────────────┼───────────────────────────────────────┘
                                     │ writes pending_episodes
                    ┌────────────────▼────────────────────────────┐
                    │          KnowledgeDB (SQLite / Postgres)     │
                    │             pending_episodes table           │
                    └────────────────┬────────────────────────────┘
                                     │ drains every 8h
  ┌─────── knowledge-server ─────────▼───────────────────────────────────────┐
  │                                                                           │
  │   PendingEpisodesReader → ConsolidationLLM → Reconsolidation             │
  │                           (extractionModel)   (mergeModel)               │
  │                                │                                         │
  │                           ContradictionScanner → KnowledgeDB             │
  │                           (contradictionModel)   (entries, embeddings)   │
  │                                │                                         │
  │                           Synthesis pass → KnowledgeDB                  │
  │                           (synthesisModel)   (higher-order principles)   │
  │                                │                                         │
  │                           ActivationEngine                               │
  │                                │                                         │
  │                    ┌───────────┴───────────┐                            │
  │                  HTTP API             MCP stdio proxy                    │
  │               (/activate, /mcp)     (knowledge-server mcp)              │
  └──────────────────────────────────────────────────────────────────────────┘
```

The daemon and server share the same database. In single-machine setups this is a local SQLite file. In team setups, both point at a shared Postgres instance.

## Supported session sources

| Source | What is read | Platform |
|---|---|---|
| **OpenCode** | `~/.local/share/opencode/opencode.db` (SQLite) | macOS, Linux |
| **Claude Code** | `~/.claude/projects/**/*.jsonl` (JSONL conversation logs) | macOS, Linux |
| **Codex CLI** | `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` (JSONL rollout logs) | macOS, Linux |
| **Cursor** | `~/Library/Application Support/Cursor/User/globalStorage/state.vscdb` (SQLite) | macOS |
| **Cursor** | `~/.config/Cursor/User/globalStorage/state.vscdb` (SQLite) | Linux |
| **VSCode** | `~/Library/Application Support/Code/User/workspaceStorage/*/chatSessions/*.json` | macOS |
| **VSCode** | `~/.config/Code/User/workspaceStorage/*/chatSessions/*.json` | Linux |
| **Local files** | `~/knowledge/*.md` (Markdown) | macOS, Linux |

All sources are auto-detected. Disable any with `OPENCODE_ENABLED=false`, etc. Override a path with `OPENCODE_DB_PATH`, etc.

The local-files source is opt-in by directory presence: if `~/knowledge/` does not exist it is silently skipped.

## Components

### knowledge-daemon (`src/daemon/`)

A lightweight background process (~250 KB binary) that reads local session files and uploads new episodes to the `pending_episodes` staging table. Keeps its own upload cursor in local SQLite (`daemon_cursor`), independent from consolidation progress.

**Started automatically** alongside the server for single-machine setups (set `DAEMON_AUTO_SPAWN=false` to opt out). For multi-machine or remote setups, run it separately:

```bash
knowledge-daemon --interval=300     # poll every 5 minutes
knowledge-server setup-tool daemon  # or register as a system service (launchd / systemd)
```

### knowledge-server (`src/index.ts`)

The main HTTP server. Drains `pending_episodes` via the consolidation engine, exposes knowledge via `/activate` and MCP, and serves the HTTP API.

### HTTP API (`src/api/server.ts`)

Hono-based HTTP server. Binds to `127.0.0.1:3179` by default.

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/activate?q=...` | GET | — | Activate knowledge entries by query |
| `/consolidate` | POST | admin | Run a consolidation batch |
| `/reinitialize?confirm=yes` | POST | admin | Wipe all entries and reset consolidation state |
| `/status` | GET | — (config requires admin) | Health check and stats |
| `/entries` | GET | — | List entries (filter by `status`, `type`, `scope`) |
| `/entries/:id` | GET | — | Get a specific entry with relations |
| `/entries/:id` | PATCH | admin | Update content, topics, confidence, status, scope |
| `/entries/:id/resolve` | POST | admin | Resolve a conflicted entry pair |
| `/entries/:id` | DELETE | admin | Hard-delete an entry |
| `/review` | GET | — | Surface conflicted, stale, and team-relevant entries |
| `/hooks/claude-code/user-prompt` | POST | — | Claude Code `UserPromptSubmit` hook endpoint |
| `/mcp` | ALL | — / admin | MCP streamable-http endpoint |

### MCP server

Exposes a single tool: `activate`. Agents use this for deliberate recall mid-task.

Two connection modes:

**stdio** — `knowledge-server mcp` starts a lightweight stdio proxy. Registered automatically by `setup-tool`. Only `KNOWLEDGE_HOST`/`KNOWLEDGE_PORT` needed; no LLM credentials required.

**streamable-http** — `ALL /mcp` on the main HTTP server. MCP clients connect directly at `http://127.0.0.1:3179/mcp`. When `KNOWLEDGE_ADMIN_TOKEN` is set, requires `Authorization: Bearer <token>` — suitable for hosted setups.

### Consolidation engine (`src/consolidation/`)

- `readers/pending.ts` — drains `pending_episodes` uploaded by the daemon
- `consolidate.ts` — full cycle: read → extract → reconsolidate → contradiction scan → decay → embed → synthesis → delete processed rows
- `llm.ts` — four LLM calls: `extractKnowledge` (extraction model), `decideMerge` (merge model), `detectAndResolveContradiction` (contradiction model), `synthesizePrinciple` (synthesis model, fires rarely on ripe clusters)
- `decay.ts` — forgetting curve with type-specific half-lives

## Configuration

Configuration is split across two files:

**`~/.config/knowledge-server/.env`** — credentials and tuning parameters (API keys, model names, thresholds). Loaded by the launcher wrapper at startup.

**`~/.config/knowledge-server/config.jsonc`** — store topology (which databases to use and where). Created automatically on first run (default: local SQLite). Edit this for Postgres or multi-store setups.

### Editor validation (`$schema`)

Add a `$schema` line to `config.jsonc` for IDE autocomplete and validation:

```jsonc
{
  "$schema": "https://raw.githubusercontent.com/MAnders333/knowledge-server/main/config.schema.json",
  "stores": [ ... ]
}
```

VS Code, JetBrains, and any editor with JSON Language Server support will validate your config and show hover documentation.

### Store configuration (`config.jsonc`)

The default `config.jsonc` uses a single local SQLite store — no editing needed for single-machine use.

For Postgres, a non-default port, or to disable daemon auto-spawn, edit `~/.config/knowledge-server/config.jsonc`:

```jsonc
{
  "$schema": "https://raw.githubusercontent.com/MAnders333/knowledge-server/main/config.schema.json",
  // Store config (required)
  "stores": [
    {
      "id": "main",
      "kind": "postgres",
      "uri": "postgres://user:pass@host:5432/knowledge",
      "writable": true
    }
  ],

  // Deployment settings (optional — defaults shown)
  "port": 3179,
  "host": "127.0.0.1",
  "daemonAutoSpawn": true,   // set false if you manage the daemon separately

  // Multi-user identity (optional)
  // "userId": "your-name"
}
```

Secrets (Postgres URIs, API keys) can also be kept out of the config file using env vars:
```bash
# Instead of "uri" in config.jsonc:
export STORE_MAIN_URI="postgres://user:pass@host:5432/knowledge"
```
Env vars take precedence over the config file for all fields.

**Migrating from v2:** If you had `POSTGRES_CONNECTION_URI`, `KNOWLEDGE_DB_PATH`, `KNOWLEDGE_PORT`, or `KNOWLEDGE_HOST` set, run once to generate `config.jsonc` from them:
```bash
knowledge-server migrate-config
```

### Domains and projects (multi-store routing)

By default, all knowledge is written to the single writable store. If you have multiple writable stores and want to route knowledge to the right one automatically, configure `domains` and `projects`.

**Domains** define logical buckets for knowledge. Each domain names a writable store that receives its entries:

```jsonc
"domains": [
  {
    "id": "work",          // unique identifier — lowercase alphanumeric, hyphens, underscores
    "description": "Work-related knowledge: professional projects, team decisions, technical choices.",
    "store": "work"        // must match a writable store id
  },
  {
    "id": "personal",
    "description": "Personal projects, private workflows, and anything outside of professional work.",
    "store": "personal"    // each domain can point to a different store
  }
]
```

The `description` is injected into the LLM extraction prompt. Write it as a brief characterisation of what belongs here — the LLM uses it to classify individual entries within a session, and may override the default domain for entries that clearly fit elsewhere (e.g. a personal preference found while working in a work project).

**Projects** map directory path prefixes to a default domain. When a session was recorded in that directory, entries from that session are routed to the corresponding domain by default:

```jsonc
"projects": [
  { "path": "~/Documents/priv",        "default_domain": "personal" },
  { "path": "~/Documents/work-repo",   "default_domain": "work" }
]
```

- `path` — absolute path prefix; `~` is expanded to the home directory at parse time.
- `default_domain` — domain id to use for sessions whose working directory matches this prefix. The **longest matching prefix wins** when paths overlap.
- Sessions that match no project entry fall back to `domains[0]` (the first domain in the list).

**Full two-store example:**

```jsonc
{
  "$schema": "https://raw.githubusercontent.com/MAnders333/knowledge-server/main/config.schema.json",
  "stores": [
    { "id": "work",     "kind": "postgres", "uri": "postgres://user:pass@host:5432/knowledge",  "writable": true },
    { "id": "personal", "kind": "sqlite",   "path": "~/.local/share/knowledge-server/personal.db", "writable": true }
  ],
  "domains": [
    { "id": "work",     "description": "Professional projects and team knowledge.", "store": "work" },
    { "id": "personal", "description": "Personal projects and private notes.",      "store": "personal" }
  ],
  "projects": [
    { "path": "~/Documents/work",    "default_domain": "work" },
    { "path": "~/Documents/private", "default_domain": "personal" }
  ]
}
```

Activation reads fan out across **all** configured stores regardless of domain, so a query always searches the full knowledge base.

### LLM credentials (`.env`)

**Option A — Direct API key (most common):**

```bash
ANTHROPIC_API_KEY=sk-ant-...   # Anthropic models
OPENAI_API_KEY=sk-...          # OpenAI models + embeddings
GOOGLE_API_KEY=...             # Google models
```

**Option B — Unified proxy (OpenRouter, LiteLLM, etc.):**

```bash
LLM_API_KEY=your-key
LLM_BASE_ENDPOINT=https://your-proxy.example.com
```

Provider-specific credentials take precedence over the unified endpoint.

### LLM models

| Variable | Default | Description |
|---|---|---|
| `LLM_EXTRACTION_MODEL` | `anthropic/claude-sonnet-4-6` | Episode → knowledge extraction |
| `LLM_MERGE_MODEL` | `anthropic/claude-haiku-4-5` | Near-duplicate merge decisions (cheaper) |
| `LLM_CONTRADICTION_MODEL` | `anthropic/claude-sonnet-4-6` | Contradiction detection and resolution |
| `LLM_SYNTHESIS_MODEL` | *(inherits `LLM_EXTRACTION_MODEL`)* | Cross-session principle synthesis |
| `LLM_TIMEOUT_MS` | `300000` | Per-call LLM timeout in ms |
| `LLM_MAX_RETRIES` | `2` | Retry attempts on timeout/error |

### Embeddings

| Variable | Default | Description |
|---|---|---|
| `EMBEDDING_MODEL` | `text-embedding-3-large` | Embedding model. **Changing this re-embeds all entries on next startup.** |
| `EMBEDDING_DIMENSIONS` | *(model default)* | Truncated output dimensions (only valid for `text-embedding-3-*`) |
| `EMBEDDING_BASE_URL` | *(see below)* | Dedicated embedding endpoint. Priority: `EMBEDDING_BASE_URL` → `OPENAI_BASE_URL` → `LLM_BASE_ENDPOINT/openai/v1` |
| `EMBEDDING_API_KEY` | *(see below)* | Falls back to `OPENAI_API_KEY` then `LLM_API_KEY` |

For local embeddings via Ollama:
```bash
EMBEDDING_BASE_URL=http://localhost:11434/v1
EMBEDDING_MODEL=nomic-embed-text
```

### Server

`port`, `host`, and `daemonAutoSpawn` are deployment settings — set them in `config.jsonc` (see above) or override at runtime with an env var.

| Setting | config.jsonc key | Env var override | Default | Description |
|---|---|---|---|---|
| HTTP port | `port` | `KNOWLEDGE_PORT` | `3179` | |
| Bind address | `host` | `KNOWLEDGE_HOST` | `127.0.0.1` | Loopback only |
| Daemon auto-spawn | `daemonAutoSpawn` | `DAEMON_AUTO_SPAWN=false` | `true` | Set false to manage daemon yourself |

Credentials and paths stay in `.env`:

| Variable | Default | Description |
|---|---|---|
| `KNOWLEDGE_ADMIN_TOKEN` | *(random per process)* | Fixed admin token for scripted use (≥16 chars) |
| `KNOWLEDGE_LOG_PATH` | `~/.local/share/knowledge-server/server.log` | Log file. Set to `""` to disable. |

### Session sources

All sources are auto-detected. Override paths or disable sources via environment variables:

| Variable | Default | Description |
|---|---|---|
| `OPENCODE_DB_PATH` | `~/.local/share/opencode/opencode.db` | OpenCode SQLite DB |
| `OPENCODE_ENABLED` | `true` | Set to `false` to disable |
| `CLAUDE_DB_PATH` | `~/.claude` | Claude Code JSONL root dir |
| `CLAUDE_ENABLED` | `true` | Set to `false` to disable |
| `CODEX_SESSIONS_DIR` | *(auto-detected)* | Codex CLI sessions root |
| `CODEX_ENABLED` | `true` | Set to `false` to disable |
| `CURSOR_DB_PATH` | *(auto-detected)* | Cursor `state.vscdb` path |
| `CURSOR_ENABLED` | `true` | Set to `false` to disable |
| `VSCODE_DATA_DIR` | *(auto-detected)* | VSCode data directory |
| `VSCODE_ENABLED` | `true` | Set to `false` to disable |
| `LOCAL_FILES_DIR` | `~/knowledge` | Markdown files to ingest |
| `LOCAL_FILES_ENABLED` | `true` | Set to `false` to disable |

### Consolidation

| Variable | Default | Description |
|---|---|---|
| `CONSOLIDATION_POLL_INTERVAL_MS` | `28800000` (8h) | Auto-consolidation interval. Set to `0` to disable. |
| `CONSOLIDATION_MAX_SESSIONS` | `50` | Sessions per consolidation batch |
| `CONSOLIDATION_CHUNK_SIZE` | `10` | Episodes per LLM extraction call |
| `CONSOLIDATION_MIN_MESSAGES` | `4` | Minimum messages for a session to be eligible |
| `RECONSOLIDATION_SIMILARITY_THRESHOLD` | `0.82` | Near-duplicate merge cutoff |
| `CONTRADICTION_MIN_SIMILARITY` | `0.4` | Lower bound of the contradiction scan band |

### Decay

| Variable | Default | Description |
|---|---|---|
| `DECAY_ARCHIVE_THRESHOLD` | `0.15` | Strength below which an entry is archived |
| `DECAY_TOMBSTONE_DAYS` | `180` | Days after archiving before permanent deletion |

### Activation

| Variable | Default | Description |
|---|---|---|
| `ACTIVATION_MAX_RESULTS` | `10` | Max entries returned per activation |
| `ACTIVATION_SIMILARITY_THRESHOLD` | `0.3` | Minimum cosine similarity to activate |

## Usage

### Start the server

```bash
knowledge-server        # binary install
bun run start           # source install
```

The daemon starts automatically. The server runs background consolidation on startup if pending sessions exist, then polls every 8 hours.

### Stop the server

```bash
knowledge-server stop
```

### Check status

```bash
knowledge-server status
```

Shows whether the server is running, entry counts, last consolidation time, and pending sessions.

### Trigger consolidation manually

```bash
knowledge-server consolidate                         # CLI (no server needed)
curl -X POST -H "Authorization: Bearer <token>" \
  http://127.0.0.1:3179/consolidate                 # HTTP API
```

### Process inbox (batch / cron)

```bash
knowledge-server process-inbox [dir]
```

One-shot command that runs the full pipeline end-to-end: uploads Markdown files from the inbox directory, consolidates all pending episodes, and runs KB synthesis. Designed for batch or cron use (e.g. Cloud Run Jobs) — no long-running server needed.

The directory defaults to `LOCAL_FILES_DIR` (or `~/knowledge`) if omitted.

### Test knowledge activation

```bash
knowledge-server activate "how do we handle authentication"
```

Shows which entries would be injected for a given query, with similarity scores.

### Calibrate similarity thresholds

```bash
knowledge-server calibrate
```

Derives recommended threshold values for your embedding model. Run this when switching models.

### Review knowledge entries

```bash
curl http://127.0.0.1:3179/review
```

Returns conflicted, stale, and team-relevant entries. OpenCode and Claude Code users also have a `/knowledge-review` slash command (installed by `setup-tool`).

### Reinitialize / reset state

The `reinitialize` command resets varying amounts of state depending on which flags you pass. Flags are additive — each level includes everything below it.

```bash
# Default — reset daemon cursor only.
# The daemon re-uploads all historical episodes on its next tick.
# Safe for shared stores (only local state is touched).
# Use when: connecting to a new or existing store for the first time.
knowledge-server reinitialize --confirm

# --reset-state — also wipe consolidated_episode and reset consolidation state.
# Episodes re-upload AND re-consolidate under the current domain config.
# Safe for shared stores (session IDs are per-machine by nature).
# Use when: retroactively rerouting knowledge after adding a new domain.
knowledge-server reinitialize --reset-state --confirm

# --reset-store — also wipe all knowledge entries from the store(s).
# Full fresh start. Not safe for shared stores with other active users.
# Use --store=<id> to scope to a single store.
knowledge-server reinitialize --reset-store --confirm
knowledge-server reinitialize --reset-store --store=personal --confirm

# --dry-run — preview what would happen without making changes.
knowledge-server reinitialize --reset-store --dry-run
```

**Note:** The server must be stopped before running any reinitialize variant (`knowledge-server stop`). The command checks the PID file and exits with an error if the server is live.

`--dry-run` and `--confirm` are mutually exclusive — `--dry-run` previews the action without requiring `--confirm`.

## Multi-machine / team setup

There are two distinct scenarios where you'd run knowledge-daemon separately from knowledge-server:

### Team shared knowledge base

Multiple developers share a single knowledge store. Each developer runs `knowledge-daemon` locally to upload their sessions. A single `knowledge-server` instance (on a dedicated server, CI machine, or any always-on host) does the consolidation.

**Each machine:**
1. Install `knowledge-server`
2. Point `config.jsonc` at the shared Postgres:
   ```jsonc
   { "stores": [{ "id": "main", "kind": "postgres", "uri": "postgres://...", "writable": true }] }
   ```
3. Start: `knowledge-server`

Each machine runs its own server instance (with auto-spawned daemon). The daemon scans local session files and stages episodes locally, the co-located server consolidates them and writes knowledge entries to the shared Postgres. All machines draw from the same knowledge base for activation.

**Want a dedicated consolidation server?** You can run `knowledge-server` on a remote host (cloud or team server) and have developer machines run daemon-only. Set `stateDb` to Postgres in `config.jsonc` on both the daemon machines and the consolidation server — the daemon will write `pending_episodes` to shared Postgres, and the remote server will consolidate from there:

```jsonc
{
  "stores": [{ "id": "main", "kind": "postgres", "uri": "postgres://...", "writable": true }],
  "stateDb": { "kind": "postgres", "uri": "postgres://..." }
}
```

On developer machines, set `DAEMON_AUTO_SPAWN=false` (or install only `knowledge-daemon`) and run just the daemon. The remote server does the consolidation.

### Personal multi-machine setup

You work across multiple machines and want a single knowledge base that spans all of them. Run `knowledge-server` on each machine pointing at a shared Postgres — same as the team setup above, just a single user.

## Knowledge entry types

| Type | Description | Approximate half-life |
|---|---|---|
| `fact` | A confirmed factual statement | ~30 days |
| `pattern` | A recurring pattern or observation | ~90 days |
| `decision` | A decision made and its rationale | ~120 days |
| `principle` | A guiding principle or preference | ~180 days |
| `procedure` | A step-by-step process or workflow | ~365 days |

## Security

**Admin token** — Mutation endpoints require an admin token. A random token is generated at startup and printed to stdout. Set `KNOWLEDGE_ADMIN_TOKEN` in `.env` (≥16 chars) for a stable token across restarts.

**Localhost only** — The server binds to `127.0.0.1` by default and refuses to start if `KNOWLEDGE_HOST` is set to a non-loopback address. No TLS — not designed for network exposure.

**Prompt injection** — Session content is sent to an LLM for extraction. Adversarial text in your sessions (code you pasted, web content you discussed) could influence what gets consolidated. The extraction prompt is hardened, and any injected entry must still pass similarity checks. The extraction bar (most sessions → nothing) significantly limits attack surface.

**Binary integrity** — Binaries are SHA-256 verified before install. The installer decompresses and verifies before moving files into place. `knowledge-server update` does the same.

## Development

```bash
bun run dev     # Watch mode
bun test        # Run tests
bun run lint    # Biome lint
bun run format  # Biome format
```

Data directory: `~/.local/share/knowledge-server/`
