#!/usr/bin/env bash
set -euo pipefail

# Stop the knowledge server.

PORT="${KNOWLEDGE_PORT:-3179}"

if command -v lsof &>/dev/null; then
  # lsof -ti can return multiple lines (one per file descriptor); deduplicate.
  PIDS=$(lsof -ti :"$PORT" 2>/dev/null | sort -u || true)
elif command -v fuser &>/dev/null; then
  # fuser output format varies; extract only numeric tokens to be safe.
  PIDS=$(fuser "$PORT/tcp" 2>/dev/null | grep -oE '[0-9]+' || true)
else
  echo "Error: neither lsof nor fuser found — cannot detect running server."
  exit 1
fi

if [ -z "$PIDS" ]; then
  echo "Knowledge server is not running on port $PORT"
  exit 0
fi

# Use a herestring instead of a pipe so the loop runs in the current shell,
# keeping set -e and exit code propagation intact.
while read -r pid; do
  [ -z "$pid" ] && continue
  if kill "$pid" 2>/dev/null; then
    echo "Knowledge server stopped (PID $pid)"
  else
    echo "Warning: could not kill PID $pid (already gone?)" >&2
  fi
done <<< "$PIDS"
