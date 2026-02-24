#!/usr/bin/env bash
set -euo pipefail

# Knowledge Server Setup Script
# Installs dependencies, creates data directories, and configures OpenCode integration.

export PATH="$HOME/.bun/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OPENCODE_CONFIG_DIR="${HOME}/.config/opencode"
OPENCODE_DATA_DIR="${HOME}/.local/share/knowledge-server"

echo "┌─────────────────────────────────────┐"
echo "│  Knowledge Server Setup             │"
echo "└─────────────────────────────────────┘"
echo ""

# 1. Check prerequisites
echo "Checking prerequisites..."

if ! command -v bun &>/dev/null; then
  echo "  ✗ Bun is not installed. Install it: https://bun.sh"
  exit 1
fi
echo "  ✓ Bun $(bun --version)"

if ! command -v sqlite3 &>/dev/null; then
  echo "  ⚠ sqlite3 CLI not found (optional, used for debugging)"
else
  echo "  ✓ sqlite3 available"
fi

# 2. Install dependencies
echo ""
echo "Installing dependencies..."
cd "$PROJECT_DIR"
bun install
echo "  ✓ Dependencies installed"

# 3. Create data directory
echo ""
echo "Setting up data directory..."
mkdir -p "$OPENCODE_DATA_DIR"
echo "  ✓ Created $OPENCODE_DATA_DIR"

# 4. Set up .env if not present
if [ ! -f "$PROJECT_DIR/.env" ]; then
  echo ""
  echo "Creating .env from template..."
  cp "$PROJECT_DIR/.env.template" "$PROJECT_DIR/.env"
  echo "  ⚠ Created .env — you need to set LLM_API_KEY and LLM_BASE_ENDPOINT"
  echo "    Edit: $PROJECT_DIR/.env"
else
  echo "  ✓ .env already exists"
fi

# 5. Symlink plugin to OpenCode
echo ""
echo "Setting up OpenCode integration..."

PLUGIN_DIR="$OPENCODE_CONFIG_DIR/plugins"
mkdir -p "$PLUGIN_DIR"

PLUGIN_SOURCE="$PROJECT_DIR/plugin/knowledge.ts"
PLUGIN_TARGET="$PLUGIN_DIR/knowledge.ts"

# Always force-recreate symlink to keep it in sync
ln -sf "$PLUGIN_SOURCE" "$PLUGIN_TARGET"
echo "  ✓ Symlinked plugin: $PLUGIN_TARGET → $PLUGIN_SOURCE"

# 6. Symlink commands to OpenCode
COMMAND_DIR="$OPENCODE_CONFIG_DIR/command"
mkdir -p "$COMMAND_DIR"

for cmd_file in "$PROJECT_DIR/opencode/command/"*.md; do
  cmd_name=$(basename "$cmd_file")
  cmd_target="$COMMAND_DIR/$cmd_name"
  # Always force-recreate symlink to keep it in sync (matches plugin behaviour)
  ln -sf "$cmd_file" "$cmd_target"
  echo "  ✓ Symlinked command: $cmd_name"
done

# 7. Add MCP config hint
echo ""
echo "To enable the MCP 'activate' tool for agents, add to opencode.jsonc:"
echo ""
echo '  "knowledge": {'
echo '    "type": "local",'
echo "    \"command\": [\"bun\", \"run\", \"$PROJECT_DIR/src/mcp/index.ts\"],"
echo '    "enabled": true,'
echo '    "environment": {'
echo '      "LLM_API_KEY": "your-key-here",'
echo '      "LLM_BASE_ENDPOINT": "https://your-llm-endpoint.example.com"'
echo '    }'
echo '  }'
echo ""

# 8. Check OpenCode DB
OPENCODE_DB="$HOME/.local/share/opencode/opencode.db"
if [ -f "$OPENCODE_DB" ]; then
  SESSION_COUNT=$(sqlite3 "$OPENCODE_DB" "SELECT COUNT(*) FROM session WHERE parent_id IS NULL" 2>/dev/null || echo "?")
  echo "  ✓ OpenCode DB found: $SESSION_COUNT top-level sessions available for consolidation"
else
  echo "  ⚠ OpenCode DB not found at $OPENCODE_DB"
  echo "    Set OPENCODE_DB_PATH in .env if it's elsewhere"
fi

echo ""
echo "Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Set LLM_API_KEY in $PROJECT_DIR/.env"
echo "  2. Start the server:  cd $PROJECT_DIR && bun run start"
echo "  3. Run initial consolidation:  curl -X POST -H \"Authorization: Bearer <token>\" http://127.0.0.1:3179/consolidate"
echo "  4. Check status:  curl http://127.0.0.1:3179/status"
echo ""
