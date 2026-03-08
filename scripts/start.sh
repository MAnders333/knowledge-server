#!/usr/bin/env bash
set -euo pipefail

# Start the knowledge server (source install).
# Searches for .env in priority order, loads it, then runs the server via Bun.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Ensure bun is in PATH
export PATH="$HOME/.bun/bin:$PATH"

cd "$PROJECT_DIR"

# Check that setup has been run (dependencies installed)
if [ ! -d "$PROJECT_DIR/node_modules" ]; then
  echo "Error: dependencies not installed. Run setup first:"
  echo "  bun run setup"
  exit 1
fi

# Resolve .env path — same priority order as resolveEnvFilePath() in src/config.ts:
#   1. $KNOWLEDGE_CONFIG_HOME/.env
#   2. $XDG_CONFIG_HOME/knowledge-server/.env  (or ~/.config/knowledge-server/.env)
#   3. ~/.local/share/knowledge-server/.env    (legacy)
#   4. .env in the project root                (source-install default)
_xdg_config="${XDG_CONFIG_HOME:-$HOME/.config}"
_env_file=""
if [ -n "${KNOWLEDGE_CONFIG_HOME:-}" ] && [ -f "$KNOWLEDGE_CONFIG_HOME/.env" ]; then
  _env_file="$KNOWLEDGE_CONFIG_HOME/.env"
elif [ -f "$_xdg_config/knowledge-server/.env" ]; then
  _env_file="$_xdg_config/knowledge-server/.env"
elif [ -f "$HOME/.local/share/knowledge-server/.env" ]; then
  _env_file="$HOME/.local/share/knowledge-server/.env"
  echo "Note: config loaded from legacy location $_env_file"
  echo "      Consider moving it to $_xdg_config/knowledge-server/.env"
elif [ -f "$PROJECT_DIR/.env" ]; then
  _env_file="$PROJECT_DIR/.env"
fi

# Safe KEY=value parser — does NOT execute arbitrary shell code.
# Key and value are split manually; printf -v assigns the literal value string
# to the variable without any word-splitting, glob expansion, or command
# substitution — so a crafted value like $(evil-command) or `evil` is stored
# verbatim rather than executed.
# Only keys with a known knowledge-server prefix are loaded; this prevents a
# crafted .env from overwriting sensitive process variables such as PATH,
# LD_PRELOAD, or IFS — variables that a valid identifier check alone would allow.
if [ -n "$_env_file" ]; then
  while IFS= read -r _line || [ -n "$_line" ]; do # || handles no-trailing-newline files
    # Skip blank lines and comments
    case "$_line" in
      ''|'#'*) continue ;;
    esac
    # Split on first '=' to get key and value separately
    _key="${_line%%=*}"
    _val="${_line#*=}"
    # Allowlist: only load keys with a known knowledge-server prefix.
    # This rejects PATH, LD_PRELOAD, IFS, BASH_ENV, etc.
    if [[ "$_key" =~ ^(KNOWLEDGE_|LLM_|EMBEDDING_|ANTHROPIC_|OPENAI_|GOOGLE_|OPENCODE_|CONSOLIDATION_|ACTIVATION_|CONTRADICTION_|DECAY_|CURSOR_|CLAUDE_|CODEX_|VSCODE_)[A-Za-z0-9_]*$ ]]; then
      # Strip matching surrounding quotes from the value so KNOWLEDGE_PORT="3179"
      # and KNOWLEDGE_PORT='3179' both work correctly.
      case "$_val" in
        '"'*'"') _val="${_val:1:${#_val}-2}" ;;
        "'"*"'")   _val="${_val:1:${#_val}-2}" ;;
      esac
      printf -v "$_key" '%s' "$_val"  # assign literal value — no shell evaluation
      export "$_key"
    fi
  done < "$_env_file"
fi

exec bun run "$PROJECT_DIR/src/index.ts"
