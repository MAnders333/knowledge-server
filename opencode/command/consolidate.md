---
description: Run knowledge consolidation — process recent OpenCode sessions into knowledge entries
---

Run a knowledge consolidation cycle by calling the local knowledge server.

```bash
curl -s -X POST http://127.0.0.1:3179/consolidate | python3 -m json.tool
```

This processes recent OpenCode session logs (episodic memory) and extracts/updates knowledge entries (semantic knowledge).

After consolidation, show a brief summary of:
- Sessions processed
- Entries created/archived
- Any conflicts detected
