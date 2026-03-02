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
mkdir -p "$BIN_DIR"
mkdir -p "$PLUGIN_DIR"
mkdir -p "$COMMAND_DIR"

# ── Download binaries and verify checksums ────────────────────────────────────

echo "Downloading binaries..."

TMPDIR_DL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_DL"' EXIT

# Download the checksum file first
curl --fail --location --silent --show-error \
  "$BASE_URL/SHA256SUMS-$PLATFORM" \
  -o "$TMPDIR_DL/SHA256SUMS"

# Download both binaries to temp dir, then verify before installing
curl --fail --location --show-error --progress-bar \
  "$BASE_URL/knowledge-server-$PLATFORM" \
  -o "$TMPDIR_DL/knowledge-server"

curl --fail --location --show-error --progress-bar \
  "$BASE_URL/knowledge-server-mcp-$PLATFORM" \
  -o "$TMPDIR_DL/knowledge-server-mcp"

echo "Verifying checksums..."
# The SHA256SUMS file uses the release asset filenames; verify using just the basenames
if command -v sha256sum > /dev/null 2>&1; then
  # MCP substitution must come before server substitution to avoid partial match:
  # "knowledge-server-$PLATFORM" is a prefix of "knowledge-server-mcp-$PLATFORM"
  (cd "$TMPDIR_DL" && sed "s/knowledge-server-mcp-$PLATFORM/knowledge-server-mcp/; s/knowledge-server-$PLATFORM/knowledge-server/" SHA256SUMS | sha256sum --check --status) || {
    echo "Checksum verification FAILED — aborting install."
    exit 1
  }
elif command -v shasum > /dev/null 2>&1; then
  (cd "$TMPDIR_DL" && sed "s/knowledge-server-mcp-$PLATFORM/knowledge-server-mcp/; s/knowledge-server-$PLATFORM/knowledge-server/" SHA256SUMS | shasum -a 256 --check --status) || {
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

mv "$TMPDIR_DL/knowledge-server-mcp" "$INSTALL_DIR/libexec/knowledge-server-mcp"
chmod +x "$INSTALL_DIR/libexec/knowledge-server-mcp"
echo "  ✓ knowledge-server-mcp"

# Record installed version — written after both downloads succeed so a failed
# update doesn't leave the version file pointing at a partially-updated install
echo "$VERSION" > "$INSTALL_DIR/version"

# ── Create .env on fresh install only ────────────────────────────────────────

ENV_FILE="$INSTALL_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo ""
  echo "Creating .env template..."
  # Quoted EOF delimiter ('EOF') prevents $HOME from expanding at write time.
  # The resulting file contains the literal string "$HOME" which is correct —
  # users can uncomment these lines and the shell will expand $HOME at runtime.
  # Use $HOME rather than ~ — tilde is not expanded in sourced variable assignments.
  cat > "$ENV_FILE" << 'EOF'
# knowledge-server configuration
# Fill in LLM_API_KEY and LLM_BASE_ENDPOINT — all other settings have sensible defaults.

# Required
LLM_API_KEY=your-api-key-here
LLM_BASE_ENDPOINT=https://your-llm-endpoint.example.com

# Optional — uncomment to override defaults
# LLM_EXTRACTION_MODEL=anthropic/claude-sonnet-4-6
# LLM_MERGE_MODEL=anthropic/claude-haiku-4-5
# LLM_CONTRADICTION_MODEL=anthropic/claude-sonnet-4-6
# EMBEDDING_MODEL=text-embedding-3-large
# EMBEDDING_DIMENSIONS=3072
# KNOWLEDGE_PORT=3179
# KNOWLEDGE_HOST=127.0.0.1
# Use $HOME rather than ~ — tilde is not expanded in sourced variable assignments
# KNOWLEDGE_DB_PATH=$HOME/.local/share/knowledge-server/knowledge.db
# OPENCODE_DB_PATH=$HOME/.local/share/opencode/opencode.db
# KNOWLEDGE_ADMIN_TOKEN=your-stable-token-here
EOF
  echo "  ✓ Created $ENV_FILE"
  echo "  ⚠ Edit it before starting the server: set LLM_API_KEY and LLM_BASE_ENDPOINT"
else
  echo "  ✓ .env already exists — not overwritten"
fi

# ── Generate launcher wrapper for the HTTP server ─────────────────────────────
# Sources .env before exec so the binary receives config regardless of invocation CWD.
# Paths are expanded at generation time (unquoted WRAPPER delimiter) and quoted
# inside the script to handle spaces in INSTALL_DIR/ENV_FILE correctly.

cat > "$BIN_DIR/knowledge-server" << WRAPPER
#!/usr/bin/env bash
# knowledge-server launcher — auto-generated by install.sh $VERSION
# Loads .env so config is available regardless of invocation directory.
# To update: knowledge-server update
# To reinstall: curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install.sh | bash
set -a
# shellcheck source=/dev/null
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
set +a
exec "$INSTALL_DIR/libexec/knowledge-server" "\$@"
WRAPPER
chmod +x "$BIN_DIR/knowledge-server"
echo "  ✓ launcher: $BIN_DIR/knowledge-server"

# ── Download plugin and commands ──────────────────────────────────────────────

echo ""
echo "Downloading OpenCode integration files..."

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
  echo "    Set OPENCODE_DB_PATH in $ENV_FILE if it's elsewhere"
fi

# ── Print summary ─────────────────────────────────────────────────────────────

ENV_CONFIGURED=true
if grep -qE "your-api-key-here|your-llm-endpoint" "$ENV_FILE" 2>/dev/null; then
  ENV_CONFIGURED=false
fi

MCP_ENDPOINT="$(grep '^LLM_BASE_ENDPOINT=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '\r' || echo '<your-endpoint>')"

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo ""

STEP=1

if [ "$ENV_CONFIGURED" = false ]; then
  echo "  $STEP. Edit $ENV_FILE"
  echo "     Fill in LLM_API_KEY and LLM_BASE_ENDPOINT"
  echo ""
  STEP=$((STEP + 1))
fi

echo "  $STEP. Add to ~/.config/opencode/opencode.jsonc:"
echo ""
echo '     "mcp": {'
echo '       "knowledge": {'
echo '         "type": "local",'
echo "         \"command\": [\"$INSTALL_DIR/libexec/knowledge-server-mcp\"],"
echo '         "enabled": true,'
echo '         "environment": {'
echo "           \"LLM_API_KEY\": \"<copy from $ENV_FILE>\","
echo "           \"LLM_BASE_ENDPOINT\": \"$MCP_ENDPOINT\""
echo '         }'
echo '       }'
echo '     }'
echo ""
STEP=$((STEP + 1))

if [ "$PATH_OK" = true ]; then
  echo "  $STEP. Start the HTTP server:"
  echo "     knowledge-server"
else
  echo "  $STEP. Start the HTTP server:"
  echo "     $BIN_DIR/knowledge-server"
  echo ""
  if [ -n "$SHELL_RC" ]; then
    echo "     To add to PATH permanently:"
    echo "       echo 'export PATH=\"$BIN_DIR:\$PATH\"' >> $SHELL_RC"
    echo "       . $SHELL_RC"
  fi
fi
echo ""
STEP=$((STEP + 1))

echo "  $STEP. Verify it's running:"
echo "     curl http://127.0.0.1:3179/status"
echo ""
echo "The plugin is already active in OpenCode — no restart needed."
echo "The MCP 'activate' tool is available in any session once you add the config block above."
echo ""
