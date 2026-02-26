#!/usr/bin/env bash
set -euo pipefail

# Stop the knowledge server.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PORT="${KNOWLEDGE_PORT:-3179}"
ENTRY_POINT="src/index.ts"  # must match the path passed to bun in start.sh

# Find PIDs listening on the port.
if command -v lsof &>/dev/null; then
  # lsof -ti can return multiple lines (one per file descriptor); deduplicate.
  PORT_PIDS=$(lsof -ti :"$PORT" 2>/dev/null | sort -u || true)
elif command -v fuser &>/dev/null; then
  # fuser output format varies; extract only numeric tokens to be safe.
  PORT_PIDS=$(fuser "$PORT/tcp" 2>/dev/null | grep -oE '[0-9]+' | grep -v "^${PORT}$" || true)
else
  echo "Error: neither lsof nor fuser found — cannot detect running server."
  exit 1
fi

if [ -z "$PORT_PIDS" ]; then
  echo "Knowledge server is not running on port $PORT"
  exit 0
fi

# Filter to only PIDs whose command line matches this project's entry point.
# This prevents accidentally killing an unrelated process that happens to be
# on the same port (e.g. another bun server or OpenCode itself).
PIDS=""
while read -r pid; do
  [ -z "$pid" ] && continue
  cmdline=$(ps -p "$pid" -o args= 2>/dev/null || true)
  if echo "$cmdline" | grep -qF "$PROJECT_DIR/$ENTRY_POINT"; then
    PIDS="${PIDS}${pid}"$'\n'
  fi
done <<< "$PORT_PIDS"

if [ -z "$PIDS" ]; then
  echo "No knowledge-server process found on port $PORT."
  echo "  Port $PORT is in use but by a different process — not touching it."
  exit 0
fi

# Use a herestring instead of a pipe so the loop runs in the current shell,
# keeping set -e and exit code propagation intact.
while read -r pid; do
  [ -z "$pid" ] && continue
  if kill "$pid" 2>/dev/null; then
    echo "Knowledge server stopped (PID $pid)"
  else
    echo "Warning: could not kill PID $pid (already gone or permission denied)" >&2
  fi
done <<< "$PIDS"
