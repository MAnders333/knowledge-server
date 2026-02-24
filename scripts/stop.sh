#!/usr/bin/env bash
# Stop the knowledge server.

PID=$(lsof -ti :${KNOWLEDGE_PORT:-3179} 2>/dev/null || true)

if [ -n "$PID" ]; then
  kill "$PID" 2>/dev/null
  echo "Knowledge server stopped (PID $PID)"
else
  echo "Knowledge server is not running"
fi
