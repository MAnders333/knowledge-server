---
description: Review knowledge entries that need attention — conflicts, stale entries, team-relevant items
---

Review the knowledge graph for entries needing human attention.

```bash
curl -s http://127.0.0.1:3179/review | python3 -m json.tool
```

Present the results clearly:

1. **Conflicts**: Entries that contradict each other. For each, explain the conflict and ask how to resolve it.
2. **Stale**: Entries with low strength (haven't been accessed recently). Ask whether to keep or archive them.
3. **Team-relevant**: High-confidence entries marked as team-relevant that might be worth documenting externally.
