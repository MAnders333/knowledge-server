#!/usr/bin/env bash
set -euo pipefail

# Start the knowledge server (source install).
# The server reads its .env itself (src/env-file.ts). KNOWLEDGE_LOAD_ENV=1 asks
# it to do so under `bun run`, where it would otherwise rely on Bun's project-
# root .env loading only. This script never parses or exports .env values.

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

export KNOWLEDGE_LOAD_ENV=1
exec bun run "$PROJECT_DIR/src/index.ts"
