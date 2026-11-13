#!/usr/bin/env bash
# knowledge-server installer
# Downloads release binaries and sets up OpenCode integration.
# No Bun or Node.js required.
#
# Install (latest release):
#   curl -fsSL https://raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh | bash
#
# Install (specific version):
#   curl -fsSL https://raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh | bash -s -- --version v1.2.0
#
# Update an existing installation:
#   knowledge-server update
#
# Verification: every downloaded file (binaries, plugin, slash commands) must
# match its entry in the release's SHA256SUMS-<platform>. A missing entry, a
# mismatch, or a missing sha256sum/shasum aborts before anything is installed.
# Set KNOWLEDGE_REQUIRE_ATTESTATION=1 to also require a GitHub artifact
# attestation (needs the gh CLI). See the README "Verifying releases" section.
#
# The whole script lives in main(), called on the last line. A truncated
# download therefore fails to parse and executes nothing.
set -euo pipefail

REPO="MAnders333/knowledge-server"
INSTALL_DIR="${KNOWLEDGE_SERVER_DIR:-$HOME/.local/share/knowledge-server}"
# CONFIG_DIR: where .env lives. KNOWLEDGE_CONFIG_HOME overrides entirely;
# otherwise XDG_CONFIG_HOME/knowledge-server (defaults to ~/.config/knowledge-server).
CONFIG_DIR="${KNOWLEDGE_CONFIG_HOME:-${XDG_CONFIG_HOME:-$HOME/.config}/knowledge-server}"
# Script-level constants so both are always defined, regardless of execution path.
ENV_FILE="$CONFIG_DIR/.env"
LEGACY_ENV_FILE="$INSTALL_DIR/.env"
BIN_DIR="$HOME/.local/bin"
PLUGIN_DIR="${OPENCODE_PLUGIN_DIR:-$HOME/.config/opencode/plugins}"
COMMAND_DIR="${OPENCODE_COMMAND_DIR:-$HOME/.config/opencode/command}"
VERSION=""
# Set by main(); read by the EXIT trap.
STAGE_DIR=""
LAUNCHER_TMP=""

die() {
  echo "ERROR: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$STAGE_DIR" ]]; then rm -rf "$STAGE_DIR"; fi
  if [[ -n "$LAUNCHER_TMP" ]]; then rm -f "$LAUNCHER_TMP"; fi
}
trap cleanup EXIT

# Print the SHA-256 of a file (hex only). Dies when no hashing tool exists.
hash_file() {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum > /dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "no sha256sum or shasum found — cannot verify downloads. Install coreutils (or perl's shasum) and re-run."
  fi
}

# Print the manifest hash for an asset name. Dies unless exactly one valid entry exists.
lookup_sha256() {
  local name="$1" matches count
  matches="$(awk -v n="$name" '{ f = $2; sub(/^\*/, "", f); if (f == n) print $1 }' "$STAGE_DIR/SHA256SUMS")"
  count="$(printf '%s' "$matches" | grep -c . || true)"
  [[ "$count" -eq 1 ]] || die "SHA256SUMS has $count entries for $name (expected exactly 1). Refusing to install."
  [[ "$matches" =~ ^[0-9a-f]{64}$ ]] || die "SHA256SUMS entry for $name is not a SHA-256 hash."
  printf '%s' "$matches"
}

# Verify one staged file against its SHA256SUMS entry. Dies on any failure.
verify_asset() {
  local path="$1" name="$2" expected actual
  expected="$(lookup_sha256 "$name")"
  actual="$(hash_file "$path")"
  if [[ "$actual" != "$expected" ]]; then
    die "checksum mismatch for $name — aborting install (expected $expected, got $actual)."
  fi
  echo "  ✓ verified $name"
}

# Require a GitHub artifact attestation for a staged file. Only used when
# KNOWLEDGE_REQUIRE_ATTESTATION=1.
verify_attestation() {
  command -v gh > /dev/null 2>&1 \
    || die "KNOWLEDGE_REQUIRE_ATTESTATION=1 but the gh CLI is not installed (https://cli.github.com)."
  gh attestation verify "$1" --repo "$REPO" > /dev/null \
    || die "attestation verification failed for $(basename "$1")."
  echo "  ✓ attestation $(basename "$1")"
}

# Download one release file over https only, failing on HTTP errors.
fetch_asset() {
  curl --fail --location --silent --show-error \
    --proto '=https' --proto-redir '=https' --tlsv1.2 --retry 3 \
    --output "$2" "$1"
}

# Replace $1 with $2 unless $1 is already a symlink. A regular file at the
# link path is moved aside, never clobbered.
link_file() {
  local target="$1" link="$2"
  if [[ -e "$link" && ! -L "$link" ]]; then
    local backup
    backup="$link.bak-$(date +%Y%m%d%H%M%S)"
    mv "$link" "$backup"
    echo "  ⚠ existing file moved aside: $backup"
  fi
  ln -sfn "$target" "$link"
}

main() {
  # ── Guard INSTALL_DIR ───────────────────────────────────────────────────────
  # INSTALL_DIR is baked verbatim into the generated launcher script.
  # Reject paths with characters that could break the generated script or be
  # exploited if KNOWLEDGE_SERVER_DIR is set to a crafted value.
  # PLUGIN_DIR and COMMAND_DIR are only used in quoted `ln` / `mv` calls, so a
  # crafted value can't inject code, just cause a broken symlink.
  # Leading hyphen is safe because all uses of $INSTALL_DIR are double-quoted.
  if [[ ! "$INSTALL_DIR" =~ ^[a-zA-Z0-9_./\ -]+$ ]]; then
    die "INSTALL_DIR contains unsafe characters: $INSTALL_DIR
       Unset KNOWLEDGE_SERVER_DIR or use a path with only alphanumeric, _, ., /, space, or - characters."
  fi

  # ── Parse arguments ───────────────────────────────────────────────────────────
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        [[ -n "${2:-}" ]] || die "--version requires a value"
        VERSION="$2"
        shift 2
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done

  # Validate explicit version format to prevent user input interpolation into URLs
  if [[ -n "$VERSION" ]] && [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    die "Invalid version format: $VERSION (expected e.g. v1.2.0)"
  fi

  # ── Detect platform ─────────────────────────────────────────────────────────
  local os arch platform
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      case "$arch" in
        x86_64) platform="linux-x64" ;;
        *) die "Unsupported architecture: $arch on Linux. Only x86_64 is supported." ;;
      esac
      ;;
    Darwin)
      case "$arch" in
        arm64) platform="darwin-arm64" ;;
        x86_64)
          echo "Intel Mac (x86_64) is not supported by the binary installer — no pre-built binary is available."
          die "Install from source instead: https://github.com/$REPO#option-b--from-source"
          ;;
        *) die "Unsupported architecture: $arch on macOS." ;;
      esac
      ;;
    *)
      die "Unsupported OS: $os. Supported: Linux x64, macOS arm64."
      ;;
  esac

  echo "┌──────────────────────────────────────────┐"
  echo "│  knowledge-server installer              │"
  echo "└──────────────────────────────────────────┘"
  echo ""
  echo "Platform:    $platform"

  # Refuse before any download if nothing can verify it.
  if ! command -v sha256sum > /dev/null 2>&1 && ! command -v shasum > /dev/null 2>&1; then
    die "no sha256sum or shasum found — cannot verify downloads. Install coreutils (or perl's shasum) and re-run."
  fi

  # ── Resolve version ─────────────────────────────────────────────────────────
  if [ -z "$VERSION" ]; then
    echo "Fetching latest release..."
    local raw_version
    # `|| true`: a failed API call (rate limit, offline) must reach the friendly error below.
    raw_version="$(curl --fail --location --silent --show-error \
      --proto '=https' --proto-redir '=https' --tlsv1.2 \
      "https://api.github.com/repos/$REPO/releases/latest" \
      | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')" || true
    # Re-validate the auto-resolved version against the same format as --version input
    if [[ ! "$raw_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo "ERROR: Could not determine latest version (got: '$raw_version')."
      echo "       Check your internet connection or specify --version v1.2.0"
      exit 1
    fi
    VERSION="$raw_version"
  fi

  echo "Version:     $VERSION"
  echo "Install dir: $INSTALL_DIR"
  echo ""

  local base_url="https://github.com/$REPO/releases/download/$VERSION"

  # ── Create directories ──────────────────────────────────────────────────────
  mkdir -p "$INSTALL_DIR/libexec"
  mkdir -p "$CONFIG_DIR"
  chmod 700 "$CONFIG_DIR"
  mkdir -p "$BIN_DIR"
  mkdir -p "$PLUGIN_DIR"
  mkdir -p "$COMMAND_DIR"

  # Stage inside libexec/ so every later mv is a same-filesystem rename.
  STAGE_DIR="$(mktemp -d "$INSTALL_DIR/libexec/.stage.XXXXXX")"

  # ── Download and verify ─────────────────────────────────────────────────────
  local server_asset="knowledge-server-$platform"
  local daemon_asset="knowledge-daemon-$platform"
  local plugin_assets=(knowledge.ts pi-knowledge.ts consolidate.md knowledge-review.md)

  echo "Downloading checksums..."
  fetch_asset "$base_url/SHA256SUMS-$platform" "$STAGE_DIR/SHA256SUMS"

  echo "Downloading binaries..."
  local asset
  for asset in "$server_asset" "$daemon_asset"; do
    fetch_asset "$base_url/$asset.gz" "$STAGE_DIR/$asset.gz"
    # Streaming would hide a truncated gzip behind a partial file; gunzip -c
    # fails on truncation and set -e stops the install here.
    gunzip -c "$STAGE_DIR/$asset.gz" > "$STAGE_DIR/$asset"
    rm -f "$STAGE_DIR/$asset.gz"
  done

  echo "Downloading plugin and slash commands..."
  for asset in "${plugin_assets[@]}"; do
    fetch_asset "$base_url/$asset" "$STAGE_DIR/$asset"
  done

  echo "Verifying checksums..."
  for asset in "$server_asset" "$daemon_asset" "${plugin_assets[@]}"; do
    verify_asset "$STAGE_DIR/$asset" "$asset"
  done
  # The manifest itself is trusted through the release origin, unless
  # KNOWLEDGE_REQUIRE_ATTESTATION=1 (then it is attested too).

  if [[ "${KNOWLEDGE_REQUIRE_ATTESTATION:-}" == "1" ]]; then
    echo "Verifying attestations (KNOWLEDGE_REQUIRE_ATTESTATION=1)..."
    verify_attestation "$STAGE_DIR/$server_asset"
    verify_attestation "$STAGE_DIR/$daemon_asset"
    verify_attestation "$STAGE_DIR/SHA256SUMS"
  fi

  # Smoke-test the staged binaries before anything replaces the installed ones.
  # `--help` and `--version` exit before any database or network work.
  chmod 755 "$STAGE_DIR/$server_asset" "$STAGE_DIR/$daemon_asset"
  "$STAGE_DIR/$server_asset" --help > /dev/null \
    || die "staged knowledge-server failed its smoke test — nothing was installed."
  "$STAGE_DIR/$daemon_asset" --version > /dev/null \
    || die "staged knowledge-daemon failed its smoke test — nothing was installed."

  # ── Install verified files ──────────────────────────────────────────────────
  # Each mv is an atomic same-directory rename. Both binaries are verified and
  # smoke-tested first, so a failure here is a disk-level error, not a bad release.
  mv -f "$STAGE_DIR/$server_asset" "$INSTALL_DIR/libexec/knowledge-server"
  echo "  ✓ knowledge-server"
  mv -f "$STAGE_DIR/$daemon_asset" "$INSTALL_DIR/libexec/knowledge-daemon"
  echo "  ✓ knowledge-daemon"
  for asset in "${plugin_assets[@]}"; do
    mv -f "$STAGE_DIR/$asset" "$INSTALL_DIR/$asset"
  done
  echo "  ✓ plugin and commands"

  # Record installed version — written after the install succeeds so a failed
  # update doesn't leave the version file pointing at a partially-updated install
  echo "$VERSION" > "$INSTALL_DIR/version"

  # ── Create .env on fresh install only ───────────────────────────────────────
  # New location: $CONFIG_DIR/.env  (~/.config/knowledge-server/.env by default,
  # respecting $KNOWLEDGE_CONFIG_HOME and $XDG_CONFIG_HOME).
  # Legacy location: $INSTALL_DIR/.env (~/.local/share/knowledge-server/.env).
  # If only the legacy file exists, print a migration hint but don't move it.
  # ENV_FILE and LEGACY_ENV_FILE are defined at the top of the script.

  if [ -f "$ENV_FILE" ]; then
    chmod 600 "$ENV_FILE"
    echo "  ✓ .env already exists at $ENV_FILE — not overwritten"
  elif [ -f "$LEGACY_ENV_FILE" ]; then
    echo "  ✓ .env found at legacy location $LEGACY_ENV_FILE — not overwritten"
    echo "  ℹ  Consider moving it to the new location:"
    echo "       mv \"$LEGACY_ENV_FILE\" \"$ENV_FILE\""
  else
    echo ""
    echo "Creating .env template..."
    # Quoted EOF delimiter ('EOF') prevents $HOME from expanding at write time.
    # The resulting file contains the literal string "$HOME" which is correct —
    # users can uncomment these lines and the shell will expand $HOME at runtime.
    # Use $HOME rather than ~ — tilde is not expanded in sourced variable assignments.
    # Create owner-only (0600) before writing: the file holds API keys.
    install -m 600 /dev/null "$ENV_FILE"
    cat > "$ENV_FILE" << 'EOF'
# knowledge-server credentials
# Set ONE of the following options, then start with: knowledge-server

# ── Option A: Direct API key (most common) ────────────────────────────────────
# Uncomment the key for the provider you use.
#
# ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
# GOOGLE_API_KEY=...

# ── Option B: Unified proxy endpoint ─────────────────────────────────────────
# If you use a proxy that fronts multiple providers (e.g. OpenRouter, LiteLLM).
#
# LLM_API_KEY=your-api-key-here
# LLM_BASE_ENDPOINT=https://your-llm-endpoint.example.com

# ── Optional overrides ────────────────────────────────────────────────────────
# Model selection (defaults shown):
# LLM_EXTRACTION_MODEL=anthropic/claude-sonnet-4-6
# LLM_MERGE_MODEL=anthropic/claude-haiku-4-5

# Embeddings (defaults to OpenAI text-embedding-3-large):
# EMBEDDING_MODEL=text-embedding-3-large
# EMBEDDING_DIMENSIONS=3072
# Local embeddings via Ollama:
# EMBEDDING_BASE_URL=http://localhost:11434/v1
# EMBEDDING_MODEL=nomic-embed-text

# Server:
# KNOWLEDGE_PORT=3179
# KNOWLEDGE_ADMIN_TOKEN=your-stable-token-here   # fixes the token across restarts

# Source paths (auto-detected by default — only set if your tool is in a non-standard location):
# OPENCODE_DB_PATH=$HOME/.local/share/opencode/opencode.db

# Store config (SQLite by default).
# For Postgres or multi-store setups, edit ~/.config/knowledge-server/config.jsonc instead.
EOF
    echo "  ✓ Created $ENV_FILE"
    echo "  ⚠ Edit it and set your API key (ANTHROPIC_API_KEY, OPENAI_API_KEY, or LLM_BASE_ENDPOINT+LLM_API_KEY)"
  fi

  # ── Generate launcher wrapper for the HTTP server ───────────────────────────
  # Exec-only. The binary loads .env itself (src/env-file.ts, the single parser),
  # so every entry point reads values the same way and this script never exports
  # .env values into the environment.

  LAUNCHER_TMP="$(mktemp "$BIN_DIR/.knowledge-server.XXXXXX")"
  cat > "$LAUNCHER_TMP" << WRAPPER
#!/usr/bin/env bash
# knowledge-server launcher — auto-generated by install.sh $VERSION
# Execs the server binary; the binary reads the .env file itself.
# To update: knowledge-server update
# To reinstall: curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install.sh | bash

exec "$INSTALL_DIR/libexec/knowledge-server" "\$@"
WRAPPER
  chmod 755 "$LAUNCHER_TMP"
  mv -f "$LAUNCHER_TMP" "$BIN_DIR/knowledge-server"
  LAUNCHER_TMP=""
  echo "  ✓ launcher: $BIN_DIR/knowledge-server"

  # ── Symlink plugin and commands into OpenCode ───────────────────────────────

  echo ""
  echo "Installing OpenCode integration..."

  link_file "$INSTALL_DIR/knowledge.ts" "$PLUGIN_DIR/knowledge.ts"
  echo "  ✓ plugin: $PLUGIN_DIR/knowledge.ts"

  link_file "$INSTALL_DIR/consolidate.md"      "$COMMAND_DIR/consolidate.md"
  link_file "$INSTALL_DIR/knowledge-review.md" "$COMMAND_DIR/knowledge-review.md"
  echo "  ✓ commands: consolidate.md, knowledge-review.md"

  # ── Symlink commands into Claude Code (if available) ────────────────────────

  local claude_command_dir="$HOME/.claude/commands"
  if command -v claude >/dev/null 2>&1 || [ -d "$HOME/.claude" ]; then
    mkdir -p "$claude_command_dir"
    link_file "$INSTALL_DIR/consolidate.md"      "$claude_command_dir/consolidate.md"
    link_file "$INSTALL_DIR/knowledge-review.md" "$claude_command_dir/knowledge-review.md"
    echo "  ✓ Claude Code commands: consolidate.md, knowledge-review.md"
  fi

  # ── Check whether $BIN_DIR is in PATH ───────────────────────────────────────

  local path_ok=false
  case ":$PATH:" in
    *":$BIN_DIR:"*) path_ok=true ;;
  esac

  # Detect shell RC file for PATH hint
  local shell_rc=""
  case "${SHELL:-}" in
    */bash) shell_rc="$HOME/.bashrc" ;;
    */zsh)  shell_rc="$HOME/.zshrc" ;;
    */fish) shell_rc="$HOME/.config/fish/config.fish" ;;
  esac

  # ── Check OpenCode DB ───────────────────────────────────────────────────────

  # Resolve which .env file is active (same order as the launcher wrapper).
  local active_env_file=""
  if [ -n "${KNOWLEDGE_CONFIG_HOME:-}" ] && [ -f "$KNOWLEDGE_CONFIG_HOME/.env" ]; then
    active_env_file="$KNOWLEDGE_CONFIG_HOME/.env"
  elif [ -f "$CONFIG_DIR/.env" ]; then
    active_env_file="$CONFIG_DIR/.env"
  elif [ -f "$LEGACY_ENV_FILE" ]; then
    active_env_file="$LEGACY_ENV_FILE"
  fi
  # For display purposes — prefer new location even if file doesn't exist yet
  local display_env_file="${active_env_file:-$ENV_FILE}"

  local opencode_db="$HOME/.local/share/opencode/opencode.db"
  if [ -f "$opencode_db" ]; then
    local session_count
    if command -v sqlite3 >/dev/null 2>&1; then
      session_count="$(sqlite3 "$opencode_db" "SELECT COUNT(*) FROM session WHERE parent_id IS NULL" 2>/dev/null || echo "unknown")"
    else
      session_count="(sqlite3 not available)"
    fi
    echo "  ✓ OpenCode DB: $session_count sessions available for consolidation"
  else
    echo "  ⚠ OpenCode DB not found at $opencode_db"
    echo "    Set OPENCODE_DB_PATH in $display_env_file if it's elsewhere"
  fi

  # ── Print summary ───────────────────────────────────────────────────────────

  # Detect whether the user has set an actual API key.
  # Checks for an uncommented key line — the template has these as comments,
  # so an unedited file registers as unconfigured.
  local env_configured=true
  if ! grep -qE '^(ANTHROPIC|OPENAI|GOOGLE)_API_KEY=.+|^LLM_API_KEY=.+' "$display_env_file" 2>/dev/null; then
    env_configured=false
  fi

  echo ""
  echo "Installation complete!"
  echo ""
  echo "Next steps:"
  echo ""

  local step=1

  # For v3.0.0+, store configuration has moved from POSTGRES_CONNECTION_URI to
  # config.jsonc. Offer to run migrate-config if the old env var is still set.
  if [ -n "${POSTGRES_CONNECTION_URI:-}" ] || [ -n "${KNOWLEDGE_DB_PATH:-}" ]; then
    echo "  $step. Migrate your config (v2 → v3 breaking change):"
    echo "     You have legacy env vars set (POSTGRES_CONNECTION_URI / KNOWLEDGE_DB_PATH)."
    echo "     Run this once to generate config.jsonc from them:"
    echo "     knowledge-server migrate-config"
    echo ""
    step=$((step + 1))
  fi

  if [ "$env_configured" = false ]; then
    echo "  $step. Edit $display_env_file"
    echo "     Uncomment and set your API key in that file (for example ANTHROPIC_API_KEY)."
    echo ""
    step=$((step + 1))
  fi

  # The MCP subcommand is built into the main binary — no separate process needed.
  # LLM credentials stay in the knowledge server's .env, not the MCP process.
  echo "  $step. Run the setup command for your tool(s):"
  echo "     knowledge-server setup-tool opencode     # OpenCode"
  echo "     knowledge-server setup-tool claude-code  # Claude Code (MCP + hook + commands)"
  echo "     knowledge-server setup-tool cursor       # Cursor"
  echo "     knowledge-server setup-tool codex        # Codex CLI"
  echo "     knowledge-server setup-tool vscode       # VSCode / GitHub Copilot"
  echo ""
  step=$((step + 1))

  if [ "$path_ok" = true ]; then
    echo "  $step. Start the server:"
    echo "     knowledge-server"
  else
    echo "  $step. Start the server:"
    echo "     $BIN_DIR/knowledge-server"
    echo ""
    if [ -n "$shell_rc" ]; then
      echo "     To add to PATH permanently:"
      echo "       echo 'export PATH=\"$BIN_DIR:\$PATH\"' >> $shell_rc"
      echo "       . $shell_rc"
    fi
  fi
  echo ""
  echo "     The episode collector (knowledge-daemon) starts automatically alongside"
  echo "     the server — no separate setup needed for single-machine use."
  echo ""
  step=$((step + 1))

  echo "  $step. Verify it's running:"
  echo "     curl http://127.0.0.1:3179/status"
  echo ""
  echo "The OpenCode plugin is already active — no restart needed."
  echo "The MCP 'activate' tool is available in any session once you add the config block above."
  echo ""
}

main "$@"
