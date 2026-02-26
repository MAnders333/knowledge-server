#!/usr/bin/env bash
set -euo pipefail

# Start the knowledge server.
# Loads .env if present, then runs the server.

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

# Load .env if it exists
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env 2>/dev/null || true
  set +a
fi

# Warn (but don't exit) if required vars are still at placeholder values.
# The server can still serve reads; consolidation will fail on first attempt.
if [ -z "${LLM_API_KEY:-}" ] || [ "${LLM_API_KEY}" = "your-unified-endpoint-api-key" ] || \
   [ -z "${LLM_BASE_ENDPOINT:-}" ] || [ "${LLM_BASE_ENDPOINT}" = "https://your-llm-endpoint.example.com" ]; then
  echo "Warning: LLM_API_KEY and/or LLM_BASE_ENDPOINT are not configured in .env."
  echo "  The server will start but consolidation will fail until these are set."
  echo "  Edit: $PROJECT_DIR/.env"
  echo ""
fi

exec bun run src/index.ts
