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
# SHA-256 checksums are downloaded and verified before binaries are installed.
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

# Guard: INSTALL_DIR is baked verbatim into the generated launcher script.
# Reject paths with characters that could break the generated script or be
# exploited if KNOWLEDGE_SERVER_DIR is set to a crafted value.
# PLUGIN_DIR and COMMAND_DIR are not guarded — they are only used in `ln -sf`
# (quoted), so a crafted value can't inject code, just cause a broken symlink.
# Leading hyphen is safe because all uses of $INSTALL_DIR are double-quoted.
if [[ ! "$INSTALL_DIR" =~ ^[a-zA-Z0-9_./\ -]+$ ]]; then
  echo "ERROR: INSTALL_DIR contains unsafe characters: $INSTALL_DIR"
  echo "       Unset KNOWLEDGE_SERVER_DIR or use a path with only alphanumeric, _, ., /, space, or - characters."
  exit 1
fi

# ── Parse arguments ───────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      [[ -n "${2:-}" ]] || { echo "ERROR: --version requires a value"; exit 1; }
      VERSION="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Validate explicit version format to prevent user input interpolation into URLs
if [[ -n "$VERSION" ]] && [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: Invalid version format: $VERSION (expected e.g. v1.2.0)"
  exit 1
fi

# ── Detect platform ───────────────────────────────────────────────────────────

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64) PLATFORM="linux-x64" ;;
      *) echo "Unsupported architecture: $ARCH on Linux. Only x86_64 is supported."; exit 1 ;;
    esac
    ;;
  Darwin)
    case "$ARCH" in
      arm64)  PLATFORM="darwin-arm64" ;;
      x86_64)
        echo "Intel Mac (x86_64) is not supported by the binary installer — no pre-built binary is available."
        echo "Install from source instead: https://github.com/$REPO#option-b--from-source"
        exit 1
        ;;
      *) echo "Unsupported architecture: $ARCH on macOS."; exit 1 ;;
    esac
    ;;
  *)
    echo "Unsupported OS: $OS. Supported: Linux x64, macOS arm64."
    exit 1
    ;;
esac

echo "┌──────────────────────────────────────────┐"
echo "│  knowledge-server installer              │"
echo "└──────────────────────────────────────────┘"
echo ""
echo "Platform:    $PLATFORM"

# ── Resolve version ───────────────────────────────────────────────────────────

if [ -z "$VERSION" ]; then
  echo "Fetching latest release..."
  RAW_VERSION="$(curl --fail --location --silent --show-error "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
  # Re-validate the auto-resolved version against the same format as --version input
  if [[ ! "$RAW_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: Could not determine latest version (got: '$RAW_VERSION')."
    echo "       Check your internet connection or specify --version v1.2.0"
    exit 1
  fi
  VERSION="$RAW_VERSION"
fi

echo "Version:     $VERSION"
echo "Install dir: $INSTALL_DIR"
echo ""

BASE_URL="https://github.com/$REPO/releases/download/$VERSION"

# ── Create directories ────────────────────────────────────────────────────────

mkdir -p "$INSTALL_DIR/libexec"
mkdir -p "$CONFIG_DIR"
mkdir -p "$BIN_DIR"
mkdir -p "$PLUGIN_DIR"
mkdir -p "$COMMAND_DIR"

# ── Download binaries and verify checksums ────────────────────────────────────

echo "Downloading binaries..."

TMPDIR_DL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_DL"' EXIT

# Download the checksum file first (contains hashes of both uncompressed binaries)
curl --fail --location --silent --show-error \
  "$BASE_URL/SHA256SUMS-$PLATFORM" \
  -o "$TMPDIR_DL/SHA256SUMS"

# Download gzip-compressed binaries and decompress on the fly.
# Streaming decompress avoids storing both compressed and uncompressed copies.
curl --fail --location --show-error --progress-bar \
  "$BASE_URL/knowledge-server-$PLATFORM.gz" \
  | gunzip > "$TMPDIR_DL/knowledge-server"

curl --fail --location --show-error --progress-bar \
  "$BASE_URL/knowledge-daemon-$PLATFORM.gz" \
  | gunzip > "$TMPDIR_DL/knowledge-daemon"

echo "Verifying checksums..."
# The SHA256SUMS file contains the hashes of the uncompressed binaries.
# Normalise names so sha*sum --check can find the local files.
_verify_checksums() {
  sed \
    -e "s|knowledge-server-$PLATFORM|knowledge-server|" \
    -e "s|knowledge-daemon-$PLATFORM|knowledge-daemon|" \
    SHA256SUMS | "$@"
}
if [[ "$OS" == "Darwin" ]] && command -v shasum > /dev/null 2>&1; then
  (cd "$TMPDIR_DL" && _verify_checksums shasum -a 256 --check --status) || {
    echo "Checksum verification FAILED — aborting install."
    exit 1
  }
elif command -v sha256sum > /dev/null 2>&1; then
  (cd "$TMPDIR_DL" && _verify_checksums sha256sum --check --status) || {
    echo "Checksum verification FAILED — aborting install."
    exit 1
  }
elif command -v shasum > /dev/null 2>&1; then
  (cd "$TMPDIR_DL" && _verify_checksums shasum -a 256 --check --status) || {
    echo "Checksum verification FAILED — aborting install."
    exit 1
  }
else
  echo "  ⚠ No sha256sum or shasum found — skipping checksum verification"
fi
echo "  ✓ Checksums verified"

# Move verified binaries into place
mv "$TMPDIR_DL/knowledge-server" "$INSTALL_DIR/libexec/knowledge-server"
chmod +x "$INSTALL_DIR/libexec/knowledge-server"
echo "  ✓ knowledge-server"

mv "$TMPDIR_DL/knowledge-daemon" "$INSTALL_DIR/libexec/knowledge-daemon"
chmod +x "$INSTALL_DIR/libexec/knowledge-daemon"
echo "  ✓ knowledge-daemon"

# Record installed version — written after download succeeds so a failed
# update doesn't leave the version file pointing at a partially-updated install
echo "$VERSION" > "$INSTALL_DIR/version"

# ── Create .env on fresh install only ────────────────────────────────────────
# New location: $CONFIG_DIR/.env  (~/.config/knowledge-server/.env by default,
# respecting $KNOWLEDGE_CONFIG_HOME and $XDG_CONFIG_HOME).
# Legacy location: $INSTALL_DIR/.env (~/.local/share/knowledge-server/.env).
# If only the legacy file exists, print a migration hint but don't move it.
# ENV_FILE and LEGACY_ENV_FILE are defined at the top of the script.

if [ -f "$ENV_FILE" ]; then
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

# ── Generate launcher wrapper for the HTTP server ─────────────────────────────
# Searches for .env in priority order, loads it, then execs the binary.
# Search order (same as resolveEnvFilePath() in src/config.ts):
#   1. $KNOWLEDGE_CONFIG_HOME/.env
#   2. $XDG_CONFIG_HOME/knowledge-server/.env  (or ~/.config/knowledge-server/.env)
#   3. ~/.local/share/knowledge-server/.env    (legacy — backwards compat)
#
# INSTALL_DIR is baked in at generation time (unquoted WRAPPER delimiter).
# Runtime variables inside the script use \$ to defer expansion.

cat > "$BIN_DIR/knowledge-server" << WRAPPER
#!/usr/bin/env bash
# knowledge-server launcher — auto-generated by install.sh $VERSION
# Searches for .env in priority order and loads it before exec.
# To update: knowledge-server update
# To reinstall: curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install.sh | bash

# Resolve .env path at runtime using the same priority order as the server's
# resolveEnvFilePath() in src/config.ts.
_xdg_cfg="\${XDG_CONFIG_HOME:-\$HOME/.config}"
_env_file=""
if [ -n "\${KNOWLEDGE_CONFIG_HOME:-}" ] && [ -f "\$KNOWLEDGE_CONFIG_HOME/.env" ]; then
  _env_file="\$KNOWLEDGE_CONFIG_HOME/.env"
elif [ -f "\$_xdg_cfg/knowledge-server/.env" ]; then
  _env_file="\$_xdg_cfg/knowledge-server/.env"
elif [ -f "\$HOME/.local/share/knowledge-server/.env" ]; then
  _env_file="\$HOME/.local/share/knowledge-server/.env"
fi

# Safe KEY=value parser — does NOT execute arbitrary shell code.
# Key and value are split manually; printf -v assigns the literal value string
# to the variable without any word-splitting, glob expansion, or command
# substitution — so a crafted value like \$(evil-command) or \`evil\` is stored
# verbatim rather than executed.
# Only keys with a known knowledge-server prefix are loaded; this prevents a
# crafted .env from overwriting sensitive process variables such as PATH,
# LD_PRELOAD, or IFS — variables that a valid identifier check alone would allow.
if [ -n "\$_env_file" ]; then
  while IFS= read -r _line || [ -n "\$_line" ]; do # || handles no-trailing-newline files
    # Skip blank lines and comments
    case "\$_line" in
      ''|'#'*) continue ;;
    esac
    # Split on first '=' to get key and value separately
    _key="\${_line%%=*}"
    _val="\${_line#*=}"
    # Allowlist: only load keys with a known knowledge-server prefix.
    # This rejects PATH, LD_PRELOAD, IFS, BASH_ENV, etc.
    if [[ "\$_key" =~ ^(KNOWLEDGE_|LLM_|EMBEDDING_|ANTHROPIC_|OPENAI_|GOOGLE_|OPENCODE_|CONSOLIDATION_|ACTIVATION_|CONTRADICTION_|DECAY_|CURSOR_|CLAUDE_|CODEX_|VSCODE_)[A-Za-z0-9_]*\$ ]]; then
      # Strip matching surrounding quotes from the value so KNOWLEDGE_PORT="3179"
      # and KNOWLEDGE_PORT='3179' both work correctly. Asymmetric stripping (e.g.
      # removing a leading " and trailing ' independently) is intentionally avoided
      # to prevent silently corrupting values with mismatched or missing quotes.
      case "\$_val" in
        '\"'*'\"') _val="\${_val:1:\${#_val}-2}" ;;
        "'"*"'")   _val="\${_val:1:\${#_val}-2}" ;;
      esac
      printf -v "\$_key" '%s' "\$_val"  # assign literal value — no shell evaluation
      export "\$_key"
    fi
  done < "\$_env_file"
fi

exec "$INSTALL_DIR/libexec/knowledge-server" "\$@"
WRAPPER
chmod +x "$BIN_DIR/knowledge-server"
echo "  ✓ launcher: $BIN_DIR/knowledge-server"

# ── Download plugin and commands ──────────────────────────────────────────────

echo ""
echo "Downloading plugin and slash commands..."

curl --fail --location --silent --show-error "$BASE_URL/knowledge.ts"         -o "$INSTALL_DIR/knowledge.ts"
curl --fail --location --silent --show-error "$BASE_URL/consolidate.md"       -o "$INSTALL_DIR/consolidate.md"
curl --fail --location --silent --show-error "$BASE_URL/knowledge-review.md"  -o "$INSTALL_DIR/knowledge-review.md"
echo "  ✓ plugin and commands"

# ── Symlink plugin and commands into OpenCode ─────────────────────────────────

echo ""
echo "Installing OpenCode integration..."

ln -sf "$INSTALL_DIR/knowledge.ts"        "$PLUGIN_DIR/knowledge.ts"
echo "  ✓ plugin: $PLUGIN_DIR/knowledge.ts"

ln -sf "$INSTALL_DIR/consolidate.md"      "$COMMAND_DIR/consolidate.md"
ln -sf "$INSTALL_DIR/knowledge-review.md" "$COMMAND_DIR/knowledge-review.md"
echo "  ✓ commands: consolidate.md, knowledge-review.md"

# ── Symlink commands into Claude Code (if available) ─────────────────────────

CLAUDE_COMMAND_DIR="$HOME/.claude/commands"
if command -v claude >/dev/null 2>&1 || [ -d "$HOME/.claude" ]; then
  mkdir -p "$CLAUDE_COMMAND_DIR"
  ln -sf "$INSTALL_DIR/consolidate.md"      "$CLAUDE_COMMAND_DIR/consolidate.md"
  ln -sf "$INSTALL_DIR/knowledge-review.md" "$CLAUDE_COMMAND_DIR/knowledge-review.md"
  echo "  ✓ Claude Code commands: consolidate.md, knowledge-review.md"
fi

# ── Check whether $BIN_DIR is in PATH ────────────────────────────────────────

PATH_OK=false
case ":$PATH:" in
  *":$BIN_DIR:"*) PATH_OK=true ;;
esac

# Detect shell RC file for PATH hint
SHELL_RC=""
case "${SHELL:-}" in
  */bash) SHELL_RC="$HOME/.bashrc" ;;
  */zsh)  SHELL_RC="$HOME/.zshrc" ;;
  */fish) SHELL_RC="$HOME/.config/fish/config.fish" ;;
esac

# ── Check OpenCode DB ─────────────────────────────────────────────────────────

# Resolve which .env file is active (same order as the launcher wrapper).
_xdg_cfg="${XDG_CONFIG_HOME:-$HOME/.config}"
ACTIVE_ENV_FILE=""
if [ -n "${KNOWLEDGE_CONFIG_HOME:-}" ] && [ -f "$KNOWLEDGE_CONFIG_HOME/.env" ]; then
  ACTIVE_ENV_FILE="$KNOWLEDGE_CONFIG_HOME/.env"
elif [ -f "$CONFIG_DIR/.env" ]; then
  ACTIVE_ENV_FILE="$CONFIG_DIR/.env"
elif [ -f "$LEGACY_ENV_FILE" ]; then
  ACTIVE_ENV_FILE="$LEGACY_ENV_FILE"
fi
# For display purposes — prefer new location even if file doesn't exist yet
DISPLAY_ENV_FILE="${ACTIVE_ENV_FILE:-$ENV_FILE}"

OPENCODE_DB="$HOME/.local/share/opencode/opencode.db"
if [ -f "$OPENCODE_DB" ]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    SESSION_COUNT="$(sqlite3 "$OPENCODE_DB" "SELECT COUNT(*) FROM session WHERE parent_id IS NULL" 2>/dev/null || echo "unknown")"
  else
    SESSION_COUNT="(sqlite3 not available)"
  fi
  echo "  ✓ OpenCode DB: $SESSION_COUNT sessions available for consolidation"
else
  echo "  ⚠ OpenCode DB not found at $OPENCODE_DB"
  echo "    Set OPENCODE_DB_PATH in $DISPLAY_ENV_FILE if it's elsewhere"
fi

# ── Print summary ─────────────────────────────────────────────────────────────

# Detect whether the user has set an actual API key.
# Checks for an uncommented key line — the template has these as comments,
# so an unedited file registers as unconfigured.
ENV_CONFIGURED=true
if ! grep -qE '^(ANTHROPIC|OPENAI|GOOGLE)_API_KEY=.+|^LLM_API_KEY=.+' "$DISPLAY_ENV_FILE" 2>/dev/null; then
  ENV_CONFIGURED=false
fi

# Read the configured host/port to interpolate into the printed MCP block.
# Fall back to defaults if not set in .env.
KNOWLEDGE_HOST_VAL="$(grep '^KNOWLEDGE_HOST=' "$DISPLAY_ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '\r' || echo '127.0.0.1')"
KNOWLEDGE_PORT_VAL="$(grep '^KNOWLEDGE_PORT=' "$DISPLAY_ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '\r' || echo '3179')"
KNOWLEDGE_HOST_VAL="${KNOWLEDGE_HOST_VAL:-127.0.0.1}"
KNOWLEDGE_PORT_VAL="${KNOWLEDGE_PORT_VAL:-3179}"

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo ""

STEP=1

# For v3.0.0+, store configuration has moved from POSTGRES_CONNECTION_URI to
# config.jsonc. Offer to run migrate-config if the old env var is still set.
if [ -n "${POSTGRES_CONNECTION_URI:-}" ] || [ -n "${KNOWLEDGE_DB_PATH:-}" ]; then
  echo "  $STEP. Migrate your config (v2 → v3 breaking change):"
  echo "     You have legacy env vars set (POSTGRES_CONNECTION_URI / KNOWLEDGE_DB_PATH)."
  echo "     Run this once to generate config.jsonc from them:"
  echo "     knowledge-server migrate-config"
  echo ""
  STEP=$((STEP + 1))
fi

if [ "$ENV_CONFIGURED" = false ]; then
  echo "  $STEP. Edit $DISPLAY_ENV_FILE"
  echo "     Uncomment and set your API key, e.g.: ANTHROPIC_API_KEY=sk-ant-..."
  echo ""
  STEP=$((STEP + 1))
fi

# The MCP subcommand is built into the main binary — no separate process needed.
# LLM credentials stay in the knowledge server's .env, not the MCP process.
echo "  $STEP. Run the setup command for your tool(s):"
echo "     knowledge-server setup-tool opencode     # OpenCode"
echo "     knowledge-server setup-tool claude-code  # Claude Code (MCP + hook + commands)"
echo "     knowledge-server setup-tool cursor       # Cursor"
echo "     knowledge-server setup-tool codex        # Codex CLI"
echo "     knowledge-server setup-tool vscode       # VSCode / GitHub Copilot"
echo ""
STEP=$((STEP + 1))

if [ "$PATH_OK" = true ]; then
  echo "  $STEP. Start the server:"
  echo "     knowledge-server"
else
  echo "  $STEP. Start the server:"
  echo "     $BIN_DIR/knowledge-server"
  echo ""
  if [ -n "$SHELL_RC" ]; then
    echo "     To add to PATH permanently:"
    echo "       echo 'export PATH=\"$BIN_DIR:\$PATH\"' >> $SHELL_RC"
    echo "       . $SHELL_RC"
  fi
fi
echo ""
echo "     The episode collector (knowledge-daemon) starts automatically alongside"
echo "     the server — no separate setup needed for single-machine use."
echo ""
STEP=$((STEP + 1))

echo "  $STEP. Verify it's running:"
echo "     curl http://127.0.0.1:3179/status"
echo ""
echo "The OpenCode plugin is already active — no restart needed."
echo "The MCP 'activate' tool is available in any session once you add the config block above."
echo ""
