---
description: Run knowledge consolidation — process recent sessions (OpenCode, Claude Code, Cursor, Codex, VSCode) into knowledge entries
---

Run a knowledge consolidation cycle by calling the local knowledge server.

The admin token is printed to the server console at startup only when the server runs in a terminal. Under a service manager or redirected output it is not printed; set `KNOWLEDGE_ADMIN_TOKEN` in `.env` to use a stable token. This admin call needs the token. Flagging (`knowledge_flag`) needs the same auth as reads: with the default random per-start token it needs none; with a stable token the MCP process must have `KNOWLEDGE_ADMIN_TOKEN` set.

```bash
curl -s -X POST -H @- http://127.0.0.1:3179/consolidate <<EOF | python3 -m json.tool
Authorization: Bearer $KNOWLEDGE_ADMIN_TOKEN
EOF
```

This processes recent session logs from all configured sources (OpenCode, Claude Code,
Cursor, Codex CLI, and VSCode) and extracts/updates knowledge entries (semantic knowledge).

After consolidation, show a brief summary of:
- Sessions processed (per source)
- Entries created/archived
- Any conflicts detected
