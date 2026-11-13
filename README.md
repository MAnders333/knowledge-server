# knowledge-server

AI coding agents are stateless by default — every session starts from zero. knowledge-server gives them memory: it runs in the background, learns from your coding conversations, and quietly surfaces what's relevant when you start a new session.

No prompting required. No manual notes. Just context that was missing before.

**How it shows up in practice:**
- You spend an afternoon figuring out why your deployment pipeline fails on certain file patterns. The next week, a teammate starts asking a related question — the agent already knows the conclusion you reached.
- You establish a naming convention during a refactor. Future sessions in that codebase start with that context already loaded.
- A decision about which library to use gets captured. It stops being re-litigated every time someone asks.

Everything happens in the background. The tool reads your sessions, extracts what's worth keeping, and injects relevant entries before the LLM sees your next message. You don't interact with it directly.

Captures knowledge from [OpenCode](https://opencode.ai), [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview), [Codex CLI](https://github.com/openai/codex), [pi](https://github.com/earendil-works/pi-mono), [oh-my-pi](https://www.npmjs.com/package/@oh-my-pi/pi-coding-agent), [Cursor](https://cursor.com) and [VSCode](https://code.visualstudio.com) (GitHub Copilot) sessions. Knowledge is injected before each prompt into OpenCode, Claude Code, Codex CLI, pi and oh-my-pi; Cursor and VSCode get MCP tools only. See [Injected into](#injected-into-tool-support). Single machine or shared across a team — knowledge accumulates in a shared database that everyone's agent draws from.

## Install

Supports **Linux x64** and **macOS arm64** (Apple Silicon). No Bun or Node.js required.

```bash
curl -fsSL https://github.com/MAnders333/knowledge-server/releases/latest/download/install.sh | bash
```

The installer is attached to the latest release, so it always matches that release's binaries and checksums.

To check the installer before running it, download it with its checksum manifest from the same release and verify both (Linux x64; on macOS arm64 use `SHA256SUMS-darwin-arm64` and `shasum -a 256 --check --ignore-missing`):

```bash
release=https://github.com/MAnders333/knowledge-server/releases/latest/download
curl -fsSLO "$release/install.sh" -fsSLO "$release/SHA256SUMS-linux-x64"
sha256sum --check --ignore-missing SHA256SUMS-linux-x64
bash install.sh
```

Or verify the installer's GitHub attestation: `gh attestation verify install.sh --repo MAnders333/knowledge-server --signer-workflow MAnders333/knowledge-server/.github/workflows/release-please.yml`.

**After running:**

1. Edit `~/.config/knowledge-server/.env` — set your LLM API key (e.g. `ANTHROPIC_API_KEY=sk-ant-...`)
2. Register with your tool(s):
   ```bash
   knowledge-server setup-tool opencode     # OpenCode
   knowledge-server setup-tool claude-code  # Claude Code
   knowledge-server setup-tool cursor       # Cursor
   knowledge-server setup-tool codex        # Codex CLI
   knowledge-server setup-tool vscode       # VSCode / GitHub Copilot
   knowledge-server setup-tool pi           # pi
   knowledge-server setup-tool oh-my-pi     # oh-my-pi
   ```
3. Start the server: `knowledge-server`

That's it. The server auto-starts the episode collector alongside itself — nothing else to configure for single-machine use.

**To update:**

```bash
knowledge-server update                             # latest release
knowledge-server update --version vX.Y.Z --force    # an older release (downgrades need --force)
```

Every downloaded file is checked against the release's `SHA256SUMS` before anything is replaced. The new binaries must also pass a smoke test. The previous binaries are kept as `*.prev`, and a failed post-install check restores them. `update` refreshes the copies that `setup-tool` placed in tool directories (the OpenCode plugin and commands, the pi and oh-my-pi extensions), so they stay on the installed version. Copies made by v3.10.3 or earlier are picked up only when they are byte-identical to a known release; an edited copy is left unchanged and `update` says so. The first `update` from v3.10.3 runs the old updater, so after upgrading re-run `knowledge-server setup-tool opencode`, `setup-tool pi` or `setup-tool oh-my-pi` once to register the copies you use.

Releases published before plugin and command files were checksummed (their `SHA256SUMS` has no `knowledge.ts` entry) cannot be installed with `update --version` or `install.sh --version`; both stop with a message. To roll back an update, use the `*.prev` binaries it kept.

### Upgrade notes

- **Upgrade the server and the daemon together.** The state DB schema is now v6, and older builds refuse a v6 state DB. Upgrade the server and the daemon on every machine, and every machine that shares a Postgres state DB, at the same time. Rolling back with the `*.prev` binaries works only until a v6 state DB is written; after that, roll forward.
- **Stricter settings.** A value that used to be clamped or ignored now stops startup and names the variable, for example `LOG_MAX_BYTES=-1`, `LLM_MAX_RETRIES=-1`, `LLM_TIMEOUT_*_MS=0`, `RERANK_TIMEOUT_MS=50`, `POSTGRES_POOL_MAX=0` or `DAEMON_UPLOAD_INTERVAL_SECONDS=0`. Empty values count as unset. `ACTIVATION_PRECISION_GATE`, `ACTIVATION_STRENGTH_DAMPENING` and `CONTRADICTION_CONFLICTS_ONLY` accept true/false, on/off, yes/no or 1/0; any other spelling stops startup. Before this change, any value other than `off` enabled the precision gate.
- **Codex hooks need trust again.** After `knowledge-server setup-tool codex` is re-run, open Codex, run `/hooks` and trust the knowledge-server hook again.
- **`/mcp` and read protection.** `/mcp` requires the stable `KNOWLEDGE_ADMIN_TOKEN` whenever one is set, as on earlier releases. Other reads (`/activate`, `/entries`, `/status`, the hooks) are open to local clients unless you set `KNOWLEDGE_PROTECT_READS=true`, which requires the token on every route except `/health`. With it set, plugins, hooks and any other client need the token in their own environment, not only in `.env`.

### From source

**Prerequisites:** [Bun](https://bun.sh) 1.4.4, the version in `.bun-version` that the release binaries are built with. Other Bun versions are not tested.

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
bun run start setup-tool pi           # pi
bun run start setup-tool oh-my-pi     # oh-my-pi
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

**Reconsolidation instead of accumulation.** Before a new entry is inserted, it's embedded and compared to the nearest existing entry. If similarity ≥ 0.78, a focused LLM call decides whether to keep, update, replace, or insert both. Entries in the 0.4–0.78 band (related but not near-duplicate) get a contradiction scan: if two entries make mutually exclusive claims, the system resolves it. The store updates rather than appends.

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

## Captured from (session sources)

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
| **pi** | `~/.pi/*/sessions/<projectDir>/*.jsonl` (JSONL session logs, v3) | macOS, Linux |
| **oh-my-pi (omp)** | `~/.omp/agent/sessions/**` and `~/.omp/profiles/*/agent/sessions/**` (JSONL, v3) | macOS, Linux |

All sources are auto-detected. Disable any with `OPENCODE_ENABLED=false`, etc. Override a path with `OPENCODE_DB_PATH`, etc.

The local-files source is opt-in by directory presence: if `~/knowledge/` does not exist it is silently skipped.

## Injected into (tool support)

Each tool can be a source (its sessions are captured), a target (knowledge is injected before each prompt), or both.

| Tool | Captured from | Injected into | How injection works |
|---|---|---|---|
| OpenCode | yes | yes | Plugin `chat.message` hook (`setup-tool opencode`) + MCP |
| Claude Code | yes | yes | `UserPromptSubmit` HTTP hook (`setup-tool claude-code`) + MCP |
| Codex CLI | yes | yes | `UserPromptSubmit` command hook in `~/.codex/hooks.json` (`setup-tool codex`) + MCP |
| pi | yes | yes | `before_agent_start` extension (`setup-tool pi`) |
| oh-my-pi | yes | yes | `before_agent_start` extension in each `~/.omp` agent layer (`setup-tool oh-my-pi`) |
| Cursor | yes | no | MCP tool only |
| VSCode (GitHub Copilot) | yes | no | MCP tool only |

Cursor and VSCode are capture-only in practice. Cursor's session-start hook is not used because it runs before any prompt exists, so there is nothing to match entries against. VSCode is wired to MCP only. An MCP tool runs only when the agent chooses to call it, and agents almost never do so unprompted. Knowledge captured from these tools reaches the other tools, not the ones they run.

Codex: after `setup-tool codex`, open Codex, run `/hooks`, and trust the knowledge-server hook. Codex does not run a new or changed hook until it is trusted, so trust it again after a port change. Codex limits each hook's output to about 2,500 tokens and saves the rest to a file. The injected block is capped at 6,000 characters, which is usually under that limit.

Each injected entry starts with a short id, for example `(id 1a2b3c4d)`. When an entry is wrong or outdated, the agent can call the `knowledge_flag` MCP tool with that id. The entry is queued for review (status `conflicted`), is no longer injected into sessions, and is not deleted. The flag reason is neither stored on the entry nor written to the log (only its length is). Flagging is read-level: it needs no admin token of its own in any mode, so the MCP process never needs `KNOWLEDGE_ADMIN_TOKEN` for it. With `KNOWLEDGE_PROTECT_READS` it takes the same Bearer token as every other read.

`knowledge-server status` shows the server's health (`ok`, `degraded` or `down`, with reasons) when the server answers, and, per source, the episodes captured in the last 7 days next to the activations that injected entries. It warns when a source is captured but nothing was injected.

## Components

### knowledge-daemon (`src/daemon/`)

A lightweight background process that reads local session files and uploads new episodes to the `pending_episodes` staging table. Keeps its own upload cursor in local SQLite (`daemon_cursor`) and a per-session upload watermark (`daemon_session_cursor`), independent from consolidation progress.

**Started automatically** alongside the server for single-machine setups (set `DAEMON_AUTO_SPAWN=false` to opt out). For multi-machine or remote setups, run it separately:

```bash
knowledge-daemon --interval=300     # poll every 5 minutes
knowledge-server setup-tool daemon  # or register as a system service (launchd / systemd)
```

### knowledge-server (`src/index.ts`)

The main HTTP server. Drains `pending_episodes` via the consolidation engine, exposes knowledge via `/activate` and MCP, and serves the HTTP API.

### HTTP API (`src/api/server.ts`)

Hono-based HTTP server. Binds to `127.0.0.1:3179` by default.

The Auth column: `—` is open under the Host and Origin checks, unless `KNOWLEDGE_PROTECT_READS=true` with a stable `KNOWLEDGE_ADMIN_TOKEN` is set, which then requires the Bearer token on every route except `/health`. `stable token` (`/mcp`) needs it whenever a stable `KNOWLEDGE_ADMIN_TOKEN` is set, with or without `KNOWLEDGE_PROTECT_READS`. `admin` needs the admin token in every mode (the random per-start token, or the stable one). `read` is read-level: open like `—`, and with `KNOWLEDGE_PROTECT_READS` it takes the same Bearer token. `/health` is always open. Plugins and the Claude Code and Codex hooks read the token only from their own process environment, so with `KNOWLEDGE_PROTECT_READS` it must be exported there, not only written to `.env`.

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/health` | GET | — | Liveness probe with no store access. Always open, even with a stable `KNOWLEDGE_ADMIN_TOKEN` |
| `/activate?q=...` | GET | — | Activate knowledge entries by query. Optional `client` query param or `x-knowledge-client` header (`opencode`, `claude-code`, `pi`, `omp`, `codex`, `mcp`, `cli`); missing or unknown values are recorded as `unknown`. |
| `/consolidate` | POST | admin | Run a consolidation batch |
| `/reinitialize?confirm=yes` | POST | admin | Wipe all entries and reset consolidation state |
| `/status` | GET | — (config requires admin) | Liveness and stats. `status` is `"ok"` whenever the server answers. `health` is the verdict: `ok`, `degraded` (with `reasons`: a store or provider failing, consolidation errors or dead-lettered episodes, more than 500 pending sessions, or the daemon not running when one is expected) or `down` (state DB unreachable, or every knowledge store unreachable). Config and diagnostics are admin-only. |
| `/entries` | GET | — | List entries (filter by `status`, `type`, `scope`) |
| `/entries/:id` | GET | — | Get a specific entry with relations |
| `/entries/:id` | PATCH | admin | Update content, topics, confidence, status, scope |
| `/entries/:id/resolve` | POST | admin | Resolve a conflicted entry pair |
| `/entries/:id` | DELETE | admin | Hard-delete an entry |
| `/review` | GET | — | Surface conflicted, stale, and team-relevant entries |
| `/hooks/claude-code/user-prompt` | POST | — | `UserPromptSubmit` hook endpoint for Claude Code; Codex CLI uses it too, with `?client=codex` |
| `/entries/:id/flag` | POST | read | Queue an active entry for review (status `conflicted`); accepts the short id shown in injected lines. Reversible, so it needs no admin token of its own |
| `/mcp` | ALL | stable token | MCP streamable-http endpoint |

### MCP server

Exposes two tools. `activate` recalls knowledge mid-task. `knowledge_flag` reports an entry (by its id) as `wrong`, `outdated` or `irrelevant`; the entry is queued for review, stops being injected, and is kept, not deleted.

Two connection modes:

**stdio** — `knowledge-server mcp` starts a lightweight stdio proxy. Registered automatically by `setup-tool`. Only `KNOWLEDGE_HOST`/`KNOWLEDGE_PORT` needed; no LLM credentials required.

**streamable-http** — `ALL /mcp` on the main HTTP server. MCP clients connect directly at `http://127.0.0.1:3179/mcp`. Whenever a stable `KNOWLEDGE_ADMIN_TOKEN` is set, `/mcp` requires `Authorization: Bearer <token>`, with or without `KNOWLEDGE_PROTECT_READS`. The stdio proxy loads `.env` itself and sends the token on its calls; a client that connects directly needs the token in its own environment.

### Consolidation engine (`src/consolidation/`)

- `readers/pending.ts` — drains `pending_episodes` uploaded by the daemon
- `consolidate.ts` — full cycle: read → extract → reconsolidate → contradiction scan → embed → synthesis → delete processed rows. Decay runs on its own timer (`DECAY_INTERVAL_MS`).
- `llm.ts` — four LLM calls: `extractKnowledge` (extraction model), `decideMerge` (merge model), `detectAndResolveContradiction` (contradiction model), `synthesizePrinciple` (synthesis model, fires rarely on ripe clusters)
- `src/domain/strength.ts` — forgetting curve with type-specific half-lives

## Configuration

Configuration is split across two files:

**`~/.config/knowledge-server/.env`** — credentials and tuning parameters (API keys, model names, thresholds). Read by the server binary at startup from `$KNOWLEDGE_CONFIG_HOME/.env` or `$XDG_CONFIG_HOME/knowledge-server/.env` (default `~/.config/knowledge-server/.env`; the legacy `~/.local/share/knowledge-server/.env` is used when neither exists). Compiled binaries never read a `.env` from the current directory.

**Keys a `.env` cannot set.** Process-control keys are ignored: `PATH`, `HOME`, `TMPDIR`, `IFS`, `SHELL`, `SHELLOPTS`, `BASHOPTS`, `BASH_ENV`, `ENV`, `PS4`, `PROMPT_COMMAND`, `CDPATH`, `GLOBIGNORE`, `NODE_OPTIONS`, `NODE_PATH`, `NODE_TLS_REJECT_UNAUTHORIZED`, `BUN_OPTIONS`, `SSL_CERT_DIR`, `CURL_HOME`, `SSLKEYLOGFILE`, `KNOWLEDGE_CONFIG_HOME`, and every `LD_*`, `DYLD_*`, `BUN_INSTALL*`, `BUN_CONFIG_*` and `XDG_*` key. Each ignored key is named in a startup warning (`[env] Ignored N key(s) in <file>`); its value is never printed. Set these in the service environment instead (the launchd plist `EnvironmentVariables`, the systemd unit `Environment=`, or the shell that starts the server).

**Proxy and CA variables are read from `.env`.** `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY` (and their lowercase forms), `NODE_EXTRA_CA_CERTS`, `CURL_CA_BUNDLE` and `SSL_CERT_FILE` are loaded from this file, so a machine behind a corporate proxy or CA keeps them across binary upgrades.

**`~/.config/knowledge-server/config.jsonc`** — store topology (which databases to use and where). Created automatically on first run (default: local SQLite). Edit this for Postgres or multi-store setups.

**Invalid values fail at startup.** The server refuses to start when a value is not a number, is outside its range, or is not a recognised flag spelling; the message names the variable. An empty value means the default. The daemon logs these as warnings and keeps running; the MCP proxy does not check them. Flags take `true`/`false`, `on`/`off`, `yes`/`no` or `1`/`0` (case-insensitive): `ACTIVATION_PRECISION_GATE`, `ACTIVATION_STRENGTH_DAMPENING`, `CONTRADICTION_CONFLICTS_ONLY`, `RERANK_ENABLED`, the `*_ENABLED` source switches, `DAEMON_AUTO_SPAWN` and `REDACT_SECRETS`.

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
| `LLM_CONTRADICTION_MODEL` | `anthropic/claude-haiku-4-5` | Contradiction detection and resolution |
| `LLM_SYNTHESIS_MODEL` | *(inherits `LLM_EXTRACTION_MODEL`)* | Cross-session principle synthesis |
| `LLM_TIMEOUT_CONTRADICTION_MS` | `60000` | Per-attempt timeout for contradiction detection. Raise for slow local models |
| `LLM_TIMEOUT_MERGE_MS` | `90000` | Per-attempt timeout for merge decisions |
| `LLM_TIMEOUT_EXTRACTION_MS` | `300000` | Per-attempt timeout for knowledge extraction |
| `LLM_TIMEOUT_SYNTHESIS_MS` | `300000` | Per-attempt timeout for principle synthesis |
| `LLM_TIMEOUT_MS` | *(per-type defaults)* | Fallback per-attempt timeout in ms for any call type without its own `LLM_TIMEOUT_<TYPE>_MS` |
| `LLM_MAX_RETRIES` | `2` | Retry attempts on transient errors (a timed-out call is not retried) |
| `OPENAI_MAX_TOKENS_PARAM` | `max_tokens` | Token-limit parameter sent on OpenAI-compatible calls. Set to `max_completion_tokens` for newer OpenAI models (gpt-5.x, o-series) that reject `max_tokens` |
| `OPENAI_DROP_PARAMS` | *(none)* | Comma-separated request parameters to omit from OpenAI-compatible calls (e.g. `temperature` for gpt-5.x/o-series models that only accept the default value) |

### Embeddings

| Variable | Default | Description |
|---|---|---|
| `EMBEDDING_MODEL` | `text-embedding-3-large` | Embedding model. **Changing this re-embeds all entries on next startup.** |
| `EMBEDDING_DIMENSIONS` | *(model default)* | Truncated output dimensions (only valid for `text-embedding-3-*`) |
| `EMBEDDING_BASE_URL` | *(see below)* | Dedicated embedding endpoint. Priority: `EMBEDDING_BASE_URL` → `OPENAI_BASE_URL` → `LLM_BASE_ENDPOINT/openai/v1` |
| `EMBEDDING_API_KEY` | *(see below)* | Key for a dedicated `EMBEDDING_BASE_URL`. `LLM_API_KEY` is used there only when that URL shares `LLM_BASE_ENDPOINT`'s origin. Otherwise no key is sent. |
| `EMBEDDING_TIMEOUT_MS` | `20000` | Per-request embedding timeout in ms. Background re-embed and backfill use at least `60000` |
| `EMBEDDING_MAX_RETRIES` | `2` | Retries per 100-text chunk on transient errors (timeouts, network errors, HTTP 408/429/5xx, truncated or malformed responses) during background re-embed and backfill. Interactive activation does not retry |

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
| Daemon auto-spawn | `daemonAutoSpawn` | `DAEMON_AUTO_SPAWN=false` | `true` | Set `false` (or `0`, `no`, `off`) to manage the daemon yourself |

Credentials and paths stay in `.env`:

| Variable | Default | Description |
|---|---|---|
| `KNOWLEDGE_ADMIN_TOKEN` | *(random per process)* | Stable admin token, for service or scripted use (`openssl rand -hex 24`). At least 16 characters: startup refuses a shorter value, and so does the template placeholder `your-stable-token-here`. Under 32 characters, or with fewer than 12 distinct characters, logs a warning; the value is never printed. It guards every mutation route and `/mcp` |
| `KNOWLEDGE_PROTECT_READS` | `false` | With a stable `KNOWLEDGE_ADMIN_TOKEN`, require the Bearer token on every route except `/health`, reads included. Needs the stable token (startup error otherwise). Plugins and the Claude Code and Codex hooks read the token only from their own process environment, so export it there too; a token only in `.env` does not reach them |
| `KNOWLEDGE_ALLOWED_HOSTS` | *(empty)* | Extra `Host` names the API accepts, comma-separated (`name` or `name:port`). `localhost`, `127.0.0.1`, `[::1]` and the bind host are always accepted. Needed when a reverse proxy forwards a different hostname. |
| `POSTGRES_POOL_MAX` | `10` | Connections per Postgres pool: each Postgres knowledge store and the state DB |
| `REDACT_SECRETS` | `true` | Scrub credentials (private keys, cloud/SaaS/LLM API tokens, JWTs, `Authorization` headers, `*_SECRET`/`*_TOKEN`/`*_PASSWORD`/`*_API_KEY` values, URL passwords, long keyed secrets) from transcript text before it is staged, sent to an LLM or embedding API, or stored. Set `false` (or `0`, `no`, `off`) to disable. Counts are logged per run; values are never logged. |
| `KNOWLEDGE_LOG_PATH` | `server.log` in the data directory: the directory of `stateDb.path` (default `~/.local/share/knowledge-server`) | Log file. Set to `""` to disable. When stdout or stderr already points at this file (the launchd plist does this), each line is written once. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. `info` logs lengths, ids, counts and similarity scores; entry and episode text is logged only at `debug`. `warn` suppresses info lines. |
| `LOG_MAX_BYTES` | `20971520` (20 MB) | Size at which the active log rotates. Rotation keeps `server.log.1` to `server.log.3` and drops the oldest, so the files total about four times this value. `0` disables rotation. Rotated copies can hold text written by earlier versions, including an old admin token; delete them once you no longer need them. |

**Upgrading with a custom `stateDb.path`.** The PID file and server log now sit in the directory of `stateDb.path`, not in `~/.local/share/knowledge-server`. `knowledge-server stop` and `status` find a PID file left in the old location, so stop a server that was started before the upgrade with `knowledge-server stop`.

### Session sources

All sources are auto-detected. Override paths or disable sources via environment variables:

| Variable | Default | Description |
|---|---|---|
| `OPENCODE_DB_PATH` | `~/.local/share/opencode/opencode.db` | OpenCode SQLite DB |
| `OPENCODE_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `CLAUDE_DB_PATH` | `~/.claude` | Claude Code JSONL root dir |
| `CLAUDE_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `CODEX_SESSIONS_DIR` | *(auto-detected)* | Codex CLI sessions root |
| `CODEX_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `CURSOR_DB_PATH` | *(auto-detected)* | Cursor `state.vscdb` path |
| `CURSOR_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `VSCODE_DATA_DIR` | *(auto-detected)* | VSCode data directory |
| `VSCODE_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `LOCAL_FILES_DIR` | `~/knowledge` | Markdown files to ingest |
| `LOCAL_FILES_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `PI_SESSIONS_ROOT` | `~/.pi` | pi sessions root (scans `<root>/*/sessions`) |
| `PI_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |
| `OMP_SESSIONS_ROOT` | `~/.omp` | oh-my-pi sessions root (scans shared + profile layers) |
| `OMP_ENABLED` | `true` | Set to `false` (or `0`, `no`, `off`) to disable |

#### Daemon upload behaviour

| Variable | Default | Description |
|---|---|---|
| `INITIAL_LOOKBACK_DAYS` | `30` | First run of each source the daemon reads only ingests sessions (or local Markdown files, by modification time) with activity in the last N days. Set `0` to ingest all history on the first run. A session in the window is ingested in full. The `process-inbox` command is not affected. After `reinitialize` (cursor reset), the next run re-reads all history regardless of this setting. |

- **First run preview.** The daemon logs the episode count and a rough token estimate for each batch of the first run. No LLM cost is estimated, because no model price is configured. Review the count before a large history is consolidated.
- **Only new content is uploaded.** A session that grows is uploaded from its last uploaded message onward. Earlier chunks are not re-sent.
- **Small tails wait.** A new tail shorter than `CONSOLIDATION_MIN_MESSAGES` is held until the session grows or has been idle for 30 minutes, then uploaded.
- **Skips are counted, not silent.** An unreadable session, a malformed JSONL line or a null record is skipped and logged once with its reason. The per-cycle summary counts skips. Other sessions and sources still upload. NUL bytes are stripped from text before staging, since Postgres rejects them.

### Consolidation

| Variable | Default | Description |
|---|---|---|
| `CONSOLIDATION_POLL_INTERVAL_MS` | `28800000` (8h) | Auto-consolidation interval. Set to `0` to disable. |
| `CONSOLIDATION_MAX_SESSIONS` | `50` | Sessions per consolidation batch |
| `CONSOLIDATION_CHUNK_SIZE` | `10` | Episodes per LLM extraction call |
| `CONSOLIDATION_MIN_MESSAGES` | `4` | Minimum messages for a session to be eligible |
| `RECONSOLIDATION_SIMILARITY_THRESHOLD` | `0.78` | Near-duplicate merge cutoff |
| `CONTRADICTION_MIN_SIMILARITY` | `0.4` | Lower bound of the contradiction scan band |
| `CONTRADICTION_MAX_CANDIDATES` | unset (no cap) | Most candidates checked per entry and scan path, highest similarity first; checked in batches of 10 |
| `CONTRADICTION_MIN_TOP_SIMILARITY` | unset | Skip an entry whose best candidate is below this similarity (unset = no floor) |
| `CONTRADICTION_CONFLICTS_ONLY` | `false` | Opt-in: reply lists conflicts only (about 65% fewer output tokens per call). Recall is not validated; enable only after an online spot-check |

### Decay

| Variable | Default | Description |
|---|---|---|
| `DECAY_ARCHIVE_THRESHOLD` | `0.15` | Strength below which an entry is archived |
| `DECAY_TOMBSTONE_DAYS` | `180` | Days after archiving before permanent deletion |
| `DECAY_INTERVAL_MS` | `86400000` (24h) | How often decay runs, independent of pending sessions. Set to `0` to disable. |

### Activation

| Variable | Default | Description |
|---|---|---|
| `ACTIVATION_MAX_RESULTS` | `10` | Max entries returned per activation |
| `ACTIVATION_SIMILARITY_THRESHOLD` | `0.42` | Minimum cosine similarity to activate |
| `ACTIVATION_PRECISION_GATE` | `on` | `off`, `false`, `0` or `no` disables the floors and the token budget below (dedupe is separate). The render cap still applies, so the block never exceeds the formatter's 6,000-character limit |
| `RERANK_FLOOR` | unset | Absolute rerank-score floor for the configured scorer. Unset: relative rule only; no absolute floor is enabled by default. The benchmark's suggested value for `jev-1.14.0` is `0.2`, with a precision trade-off (see the note under Reranking). Applies to the top entry too |
| `RERANK_REL_FRAC` | `0.35` | With rerank, a candidate must reach this fraction of the best rerank score (floor = max of this and `RERANK_FLOOR`) |
| `ACTIVATION_DENSE_MARGIN` | `0` | Used when rerank did not run (disabled, failed, skipped, or out of time): raw cosine must clear the similarity threshold by this margin. Above `0`, relation-expanded support sources are gated too, at 0.9× their parent's score. At `0` without rerank the dense fallback adds no filtering beyond the threshold, and sources are not gated |
| `ACTIVATION_TOKEN_BUDGET` | `1500` | Estimated tokens (chars / 4) for the injected entries. The top entry is always kept. Values above `1500` are clamped to it: the formatter caps the block at 6,000 characters, so an entry counted here is always an entry rendered |
| `ACTIVATION_SESSION_TTL_MS` | `1800000` | Per-session dedupe: an entry injected in a session with a client session id is suppressed for this long after its injection. `0` disables it. Without a session id there is no dedupe |
| `ACTIVATION_STRENGTH_DAMPENING` | `false` | Rank-time freshness dampening: `rerank × (0.5 + 0.5·strength)` instead of `rerank × strength` |

The gate defaults (`RERANK_REL_FRAC`, `ACTIVATION_DENSE_MARGIN`, `ACTIVATION_TOKEN_BUDGET`, `ACTIVATION_SESSION_TTL_MS`) are conservative placeholders, not calibrated values. They were chosen before any replay measurement on real prompts; use `bun run bench/replay-activation.ts` on your own prompts before relying on them. With `ACTIVATION_DENSE_MARGIN=0` the dense fallback adds no filtering when rerank fails.

Injected entries are counted in `injection_count`, and `last_accessed_at` moves at most once per entry per 24 h. `access_count` is no longer incremented by injection and keeps its existing values. Injections are recorded in the store that owns each entry; read-only stores are skipped. Requests the client abandoned are not recorded, and neither are entries the server remembers for session dedupe: both are committed only after the response is delivered. Dedupe state lives in server memory: a restart or a compaction in the client can repeat one injection (the TTL is the only reset). An entry is marked `[may be outdated]` only when it is older than its type's half-life, was not used within that half-life, and has fewer than 3 uses (retrievals plus injections).

### Reranking

Dense retrieval overfetches `RERANK_CANDIDATES` entries and a second stage re-scores them. If reranking fails or times out, activation uses dense ordering. Two providers are available:

- `local` (default): an in-process cross-encoder. It runs offline after a one-time model download (~23MB). The default model is pinned to a Hugging Face revision and checksum. Nothing leaves the machine.
- `remote` (opt-in): one call per activation to a decision model through the System One API (`POST` to `RERANK_REMOTE_URL`). Each candidate gets one Noul question ("is this document relevant to the query?", hev/reranker's `generic-1` wording, Apache-2.0), and the scores are the model's probabilities in [0, 1]. A response with a missing, non-noul, or out-of-range answer fails open to dense ordering. It needs no local model memory and does not block the event loop. After 3 consecutive failures remote reranking pauses for 60 s, and dense ordering is used meanwhile. The default model is the versioned `jev-1.14.0`: aliases such as `jev-latest` move, and a changed model changes the scores.

> **Privacy: `RERANK_PROVIDER=remote` sends content off this machine.** On every activation the remote provider sends (1) the query tail, the last `RERANK_MAX_QUERY_TOKENS` tokens of the prompt, and (2) the text of each candidate entry, its first `RERANK_MAX_DOC_TOKENS` tokens with its type and topics. Only the candidates dense retrieval returned for that prompt are sent, never the whole store. Both are first scrubbed with the `REDACT_SECRETS` rules (keys, tokens, URL passwords), unless `REDACT_SECRETS=false`; that catches known credential shapes, not arbitrary sensitive text. The endpoint forwards the request to the hosting provider of the model, so the retention and processing terms are those of that host, which this project does not control. Keep the default `local` provider unless you accept that.

Hosts of the models seen on the maintainer's gateway. Check the routing of your own gateway, since the gateway decides where a model runs:

| Model | Hosted by (as seen on the maintainer's gateway) | Notes |
|---|---|---|
| `jev-1.14.0`, `jev-latest` | TypeSafe (Jev) | Also reachable directly at `https://api.typesafe.ai/v1/systemone` with a TypeSafe key (`RERANK_REMOTE_URL` and `RERANK_REMOTE_API_KEY`). No `-eu` variant on the gateway |
| `clef` | Cloudflare | `clef-eu` is not a twin: on NFCorpus it scored 0.016 nDCG lower than `clef` (CI +0.002 to +0.031). Confirm its hosting with your gateway administrator |
| `decision-1` | Microsoft | `decision-1-eu` scored the same as `decision-1` on both corpora. Confirm its hosting with your gateway administrator |
| `pplx-decider-v1-27b` | Perplexity | |

The default name `jev-1.14.0` exists on the maintainer's gateway and on TypeSafe's direct API. Any other endpoint needs the model name it offers. The gateway decides where a model runs.

| Variable | Default | Description |
|---|---|---|
| `RERANK_ENABLED` | `true` | `false` (or `0`, `no`, `off`) restores dense-only ordering |
| `RERANK_PROVIDER` | `local` | `local` or `remote`. Any other value fails validation. `remote` sends content off this machine (see the privacy note) |
| `RERANK_MODEL` | `Xenova/ms-marco-MiniLM-L-6-v2` | Local Hugging Face ONNX cross-encoder. **The default is English-only.** For other languages use a multilingual ONNX export with a WordPiece `tokenizer.json`. Other models are fetched from `main` without a checksum |
| `RERANK_MODELS_DIR` | `models` in the data directory (see `KNOWLEDGE_LOG_PATH`) | Local model cache |
| `RERANK_CANDIDATES` | `30` when the remote reranker is usable (`RERANK_PROVIDER=remote` with an endpoint and key); otherwise `16` | Dense candidates scored per activation. Remote scores at most 40 in one call. 30 was the benchmark's choice: 16 to 40 gave +0.030 nDCG on NFCorpus (CI +0.014 to +0.047) and +0.011 on SciFact (not significant) |
| `RERANK_TIMEOUT_MS` | `3000` | Budget for one local rerank call. On expiry the whole batch falls back to dense ordering (no partial scores) |
| `RERANK_MAX_QUERY_TOKENS` | `128` | Last N tokens of the prompt used as the rerank query (remote: N × 4 characters) |
| `RERANK_MAX_DOC_TOKENS` | `256` | First N tokens of each candidate that is scored (remote: N × 4 characters). Shorter caps lowered quality on long documents |
| `RERANK_REMOTE_URL` | `${LLM_BASE_ENDPOINT}/decisions/v1/systemone` | Full System One URL. Direct TypeSafe: `https://api.typesafe.ai/v1/systemone`. A URL on another origin than `LLM_BASE_ENDPOINT` needs `RERANK_REMOTE_API_KEY`; without it remote reranking stays off and the local reranker runs. Plain http to a non-loopback host logs a startup warning, since the key is then sent unencrypted; requests are not refused |
| `RERANK_REMOTE_API_KEY` | `LLM_API_KEY`, only on the `LLM_BASE_ENDPOINT` origin | Key for `RERANK_REMOTE_URL`. The gateway key is never sent to another origin |
| `RERANK_REMOTE_MODEL` | `jev-1.14.0` | Decision model for `RERANK_PROVIDER=remote`, named as your endpoint names it. Use a versioned name. The gateway does not resolve an alias to a version (`jev-latest` comes back as `typesafe/jev-latest`), and calibrated floors are keyed by the configured name. `activation_event.rerank_model` stores `remote/<configured model>`, the floor key; the name the endpoint reports resolving to is logged as `resolved_model` |
| `RERANK_REMOTE_TIMEOUT_MS` | `2500` | Budget for one call, including one retry after a 429 whose `Retry-After` fits in what is left. The call gets no more than the request's remaining budget less 500 ms reserved for later work (the request budget is 4.5 s per activation, 3.5 s for the Codex hook). Rerank is skipped when under 300 ms is left, and dense ordering is used. The request clock starts at request entry, so the query embedding and store reads count against it. On expiry dense ordering is used |

Remote mode needs an endpoint (`RERANK_REMOTE_URL`, or `LLM_BASE_ENDPOINT`) and a key (`RERANK_REMOTE_API_KEY`, or `LLM_API_KEY` for the gateway's own origin). If either is missing (or still a template placeholder), the server logs a warning at startup and uses the local reranker. When remote reranking is active, startup also rejects `RERANK_CANDIDATES` above 40, and any configuration whose request could exceed about 100,000 characters: `RERANK_CANDIDATES × (RERANK_MAX_DOC_TOKENS × 4 + 512) + RERANK_MAX_QUERY_TOKENS × 4`. The defaults give about 25,000.

The default model is the pinned `jev-1.14.0`: its quality is within the confidence interval of `jev-1.13.0`, `clef` and `pplx-decider-v1-27b`, it costs about a sixth of `clef`, and it had the lowest measured p95 with no errors or 429s.

**Measured (benchmark, 2026-11-13).** SciFact and NFCorpus, 150 queries each, dense top-40 candidates with 256-token documents, one call per query. The host was a shared 10-core Mac under load 5 to 16, so latency is an upper bound.

- Cost per 1,000 activations (input tokens; output tokens are free): `jev-1.14.0` $0.25 at 16 candidates and $0.61 at 40 (30 was not measured); `clef` $1.50 at 16 and $3.72 at 40; `pplx-decider-v1-27b` about $0.22 at 16. `decision-1` returns no `usage.cost` on the gateway, so it shows as $0 here; its billing is unknown.
- Latency, `jev-1.14.0`: p50 456 ms and p95 587 ms at 16 candidates, concurrency 1 (357 and 405 ms at concurrency 4); p95 561 ms at 40 candidates, concurrency 1 (654 ms at concurrency 4). `clef`: p95 1,113 ms at 16 candidates and 2,558 ms at 40. No timeouts, errors or 429s in any cell.
- Scores are ranking signals, not probabilities. On the benchmark's labels only 60 to 70% of candidates scoring above 0.9 were relevant, so any score cut-off is a choice to verify, not a calibrated probability.
- Repeated identical requests are not bit-exact on `jev-1.14.0`: over 20 repeated queries, 2 top-1 choices flipped (`clef` was exact on the same 20). Dense order is kept as the input order; order changed nDCG by at most ±0.003 in the benchmark.

**Absolute floor (not enabled).** The benchmark suggests `RERANK_FLOOR=0.2` for `jev-1.14.0`. On its labels that keeps 15% (SciFact) and 46% (NFCorpus) of candidates, with recall 0.83 and 0.86, but precision of only 0.36 and 0.39. The labels count any judged link as relevant, which is looser than memory relevance. The relative rule alone keeps 39% and 63% of candidates when the top score is below 0.3; at 0.2 the floor keeps 1.4% and 4.7% of those. Verify on your own memory entries before setting it. The per-model floor table in `gate.ts` is empty.

**EU variants.** `decision-1-eu` matches `decision-1` on both corpora. `clef-eu` does not match `clef` exactly (on NFCorpus `clef` scored 0.016 nDCG higher), so it is not a drop-in twin. `jev` has no `-eu` variant on the gateway.

Pairs are scored one at a time and the event loop is yielded between them, so one long candidate no longer stalls other requests. Latency grows with the token caps. Measured on a 10-core Apple Silicon machine at a load average of about 12, 16 candidates at the defaults took a median of 2.5 s and 20 candidates took 3.2 s. With long prompts and long candidates on every pair, 16 candidates took 3.9 s, so those calls fall back to dense ordering. On slower CPUs lower `RERANK_CANDIDATES` or `RERANK_MAX_QUERY_TOKENS`, or raise `RERANK_TIMEOUT_MS`. Keep it well under 5 s, because the plugin gives up after that.

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

Shows whether the server is running, entry counts, last consolidation time, and pending sessions. For the last 7 days it also shows, per source, episodes captured versus activations injected, and warns when a source is captured but never injected.

Activation telemetry is kept in the `activation_event` table of the server state DB. It holds ids, counts, scores, latency and a client tag, never prompt or entry text, and rows older than 90 days are pruned.

To replay prompts from a local JSONL file without changing any store, run `bun run bench/replay-activation.ts prompts.jsonl`. Each line is `{"prompt": "..."}`. Keep the file outside the repo. The script prints cap-hit, zero-hit, hub-concentration and latency figures.

### Trigger consolidation manually

```bash
knowledge-server consolidate                         # CLI (no server needed)
curl -X POST -H "Authorization: Bearer <token>" \
  http://127.0.0.1:3179/consolidate                 # HTTP API
```

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

### Failed and deferred episodes

A chunk that fails for a deterministic reason (malformed model output, or a rejected request such as an over-long prompt) counts one attempt against each of its episodes, at most once per drain. After 5 attempts (`PENDING_EPISODE_MAX_ATTEMPTS`) an episode is dead-lettered: it stays in `pending_episodes`, is no longer consolidated, and `knowledge-server status` shows the count. Requeue them with `knowledge-server consolidate --retry-dead`. Transient failures (rate limits, timeouts, dropped connections, expired credentials, provider outages) never count; those episodes are retried on a later run.

Episodes whose target store is unavailable, or whose store lock is held by another process, are skipped without counting and are retried on a later run. They are excluded before the batch window, so they do not hold up other sessions. **Known gap:** the exclusion only sees the 2000 oldest pending rows per run, so a backlog for an unavailable store larger than that can still delay newer sessions.

### Reinitialize / reset state

The `reinitialize` command resets varying amounts of state depending on which flags you pass. Flags are additive — each level includes everything below it.

```bash
# Default — reset daemon cursor only.
# The daemon re-reads the full history of every source on its next tick. This is
# not limited by INITIAL_LOOKBACK_DAYS. Once that pass completes, the lookback applies again.
# Safe for shared stores (only local state is touched).
# Use when: connecting to a new or existing store for the first time.
knowledge-server reinitialize --confirm

# --reset-state — also wipe consolidated_episode, reset consolidation state, and
# delete THIS user's pending episodes (user_id). Other users' pending episodes on a
# shared state DB are kept. The consolidation log and counters are shared, so they
# are reset for everyone.
# Episodes re-upload AND re-consolidate under the current domain config.
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

**Note:** Stop both the server and the daemon before running any reinitialize variant. The command exits with an error if either is live. The server is checked by its PID file: run `knowledge-server stop`. The daemon is checked by its lock file (`daemon.lock` in the data directory): an upload cycle that is in flight writes a fresh `daemon_cursor` after the reset, which defeats the full re-ingest. Stop the daemon with its service command when it is registered as one: `launchctl bootout gui/$(id -u)/com.knowledge-server.daemon` on macOS, or `systemctl --user stop knowledge-daemon` on Linux. Otherwise, check the PID first: `ps -p <pid> -o command=` must show `knowledge-daemon`, because a lock left by a crashed daemon can name a PID that another process now uses. Then run `kill <pid>`, using the `pid` field of `daemon.lock`.

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

**Host and Origin checks** — Every route, including `/mcp`, rejects a `Host` header that does not name `localhost`, `127.0.0.1`, `[::1]`, the bind host, or an entry in `KNOWLEDGE_ALLOWED_HOSTS` (403). A request carrying an `Origin` header must also name an allowed host over `http` or `https`, so cross-origin browser pages are refused. Clients that send no `Origin` (curl, the hook, MCP stdio proxy) are unaffected.

**Admin token** — Mutation endpoints (`/consolidate`, `/reinitialize`, and `PATCH`, `resolve` and `DELETE` on entries) always require an admin token. A random token is generated at startup. It is printed only when the server's stdout is a terminal; under launchd, systemd or redirected output it is never printed and never written to `server.log`. For service or scripted use, set `KNOWLEDGE_ADMIN_TOKEN` (`openssl rand -hex 24`) for a stable token across restarts. A stable token also guards `/mcp`. Other reads (`/activate`, `/entries`, `/status` and the hooks) stay open to loopback clients that pass the Host and Origin checks, unless `KNOWLEDGE_PROTECT_READS=true`, which requires the stable token on every route except `/health`. Failed attempts against a stable token are throttled per client address (10 a minute, then 429 with `Retry-After`). The throttle only bounds noise and CPU; it does not make guessing hard. The protection is the token's entropy. A token shorter than 16 characters is refused at startup; one under 32 characters, or with fewer than 12 distinct characters, logs a warning without printing the value. A correct token is never refused. `PATCH`, `resolve` and `DELETE` act only on writable stores; an entry that lives only in a read-only store returns 403.

**Input limits** — Request bodies are capped at 1 MB. `/activate`, the MCP `activate` tool and the Claude Code hook keep at most 12 cues: duplicates are removed first, then the first 6 and the last 6 are kept, and the last is the full message. Each query is truncated to head and tail (20k characters for the full query, 2k per segment), and `limit` is clamped to 1–50. Read endpoints still count each returned entry as an injection (`injection_count`); gating that is tracked separately.

**Postgres TLS** — `sslmode=require` encrypts the connection but does not verify the server certificate. For remote databases use `sslmode=verify-full` (system CAs), or `sslmode=verify-ca&sslrootcert=/path/to/ca.pem`. A non-local Postgres host connected without `sslmode` logs a one-time warning. Existing URIs without `sslmode` keep their behaviour.

**Localhost only** — The server binds to `127.0.0.1` by default and refuses to start if `KNOWLEDGE_HOST` is set to a non-loopback address. No TLS on the HTTP API — not designed for network exposure.

**Prompt injection** — Session content is sent to an LLM for extraction. Adversarial text in your sessions (code you pasted, web content you discussed) could influence what gets consolidated. The extraction prompt is hardened, and any injected entry must still pass similarity checks. The extraction bar (most sessions → nothing) significantly limits attack surface.

**Binary integrity** — The installer and `knowledge-server update` download the binaries, plugin, and slash-command files, then compare each one with the release's `SHA256SUMS-<platform>`. They abort, without installing anything, if a file has no manifest entry, if a hash does not match, or if neither `sha256sum` nor `shasum` is available. Set `KNOWLEDGE_REQUIRE_ATTESTATION=1` to make the installer also require a GitHub artifact attestation (this needs the `gh` CLI).

**Verifying a release** — Release binaries and `SHA256SUMS` files carry GitHub build-provenance attestations, created by this repository's release workflow. To check a binary, decompress the `.gz` first: attestations cover the uncompressed files.

```bash
gh attestation verify ~/.local/share/knowledge-server/libexec/knowledge-server --repo MAnders333/knowledge-server --signer-workflow MAnders333/knowledge-server/.github/workflows/release-please.yml
gh attestation verify SHA256SUMS-linux-x64 --repo MAnders333/knowledge-server --signer-workflow MAnders333/knowledge-server/.github/workflows/release-please.yml
```

An attestation shows that the file was built from this repository by its release workflow. It does not protect against a compromised maintainer account.

## Development

```bash
bun run dev     # Watch mode
bun test        # Run tests
bun run lint    # Biome lint
bun run format  # Biome format
```

Data directory: `~/.local/share/knowledge-server/`
