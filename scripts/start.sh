#!/usr/bin/env bash
set -euo pipefail

# Start the knowledge server.
# Loads .env if present, then runs the server.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Ensure bun is in PATH
export PATH="$HOME/.bun/bin:$PATH"

cd "$PROJECT_DIR"

# Load .env if it exists
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

exec bun run src/index.ts
