---
description: Review and resolve knowledge entries needing attention — conflicts, stale entries, team-relevant items
---

Review the knowledge graph and resolve entries that need human attention.

## Step 1 — Load the admin token and fetch review data

Load the token before any call. Take it from the environment, else from `.env`. It stays in a shell variable and is never printed. Every call sends it in a header read from stdin (`-H @-`), so it stays out of the process arguments. With the random per-start token an empty bearer counts as no token, so the header is harmless when the variable is unset:

```bash
TOKEN="${KNOWLEDGE_ADMIN_TOKEN:-}"
# Not in the environment: read it from .env (the grep output goes only into TOKEN)
if [ -z "$TOKEN" ]; then
  TOKEN="$(grep '^KNOWLEDGE_ADMIN_TOKEN=' ~/.config/knowledge-server/.env 2>/dev/null | cut -d= -f2-)"
fi
# Legacy location fallback for older installs
if [ -z "$TOKEN" ]; then
  TOKEN="$(grep '^KNOWLEDGE_ADMIN_TOKEN=' ~/.local/share/knowledge-server/.env 2>/dev/null | cut -d= -f2-)"
fi
curl -s -H @- http://127.0.0.1:3179/review <<EOF | python3 -m json.tool
Authorization: Bearer $TOKEN
EOF
```

If the shell was reset, run the load block again before the calls below. Flagging an entry (the `knowledge_flag` MCP tool) needs the same auth as reads: with the default random per-start token it needs no token; with a stable `KNOWLEDGE_ADMIN_TOKEN` the server requires it on every route, so the MCP process must have that variable set. Only the resolve, PATCH and DELETE calls below change the knowledge base.

## Step 2 — Work through each section

### Conflicts (highest priority)

For each conflicted entry pair, present both sides clearly:

- Show entry A and entry B with their content, type, confidence, and source
- Explain what the conflict is (what claims are mutually exclusive)
- Ask the user to choose one of:
  1. **Keep A** (supersede B) — entry A is correct, B is wrong or outdated
  2. **Keep B** (supersede A) — entry B is correct, A is wrong or outdated
  3. **Merge** — both contain useful information; ask the user to provide merged content
  4. **Delete A** — entry A is noise or junk and should be removed entirely
  5. **Delete B** — entry B is noise or junk and should be removed entirely
  6. **Skip** — leave as-is for now

To resolve, call the appropriate endpoint using `$TOKEN`. For the entry whose ID is `:id`:

```bash
# Keep the entry at :id, supersede its counterpart
curl -s -X POST -H @- -H "Content-Type: application/json" \
  -d '{"resolution":"supersede_other"}' \
  http://127.0.0.1:3179/entries/:id/resolve <<EOF
Authorization: Bearer $TOKEN
EOF

# Supersede the entry at :id, keep its counterpart
curl -s -X POST -H @- -H "Content-Type: application/json" \
  -d '{"resolution":"supersede_this"}' \
  http://127.0.0.1:3179/entries/:id/resolve <<EOF
Authorization: Bearer $TOKEN
EOF

# Merge — provide the merged content
curl -s -X POST -H @- -H "Content-Type: application/json" \
  -d '{"resolution":"merge","mergedContent":"<merged text>"}' \
  http://127.0.0.1:3179/entries/:id/resolve <<EOF
Authorization: Bearer $TOKEN
EOF

# Hard-delete the entry at :id (use for noise/junk)
curl -s -X DELETE -H @- http://127.0.0.1:3179/entries/:id <<EOF
Authorization: Bearer $TOKEN
EOF
```

### Stale entries

Entries with low strength haven't been accessed recently. For each:

- Show the content, type, and current strength
- Ask: **keep** (no action), **archive** (set status to archived), or **delete**

To archive (soft removal):
```bash
curl -s -X PATCH -H @- -H "Content-Type: application/json" \
  -d '{"status":"archived"}' \
  http://127.0.0.1:3179/entries/:id <<EOF
Authorization: Bearer $TOKEN
EOF
```

To delete:
```bash
curl -s -X DELETE -H @- http://127.0.0.1:3179/entries/:id <<EOF
Authorization: Bearer $TOKEN
EOF
```

### Team-relevant entries

High-confidence entries scoped to `team`. These may be worth copying into shared documentation (Confluence, README, etc.). Present each and ask:

- Is this accurate and still current?
- Does it belong in external docs?

If the content needs correction, use PATCH (content/topic edits automatically re-compute the embedding):
```bash
curl -s -X PATCH -H @- -H "Content-Type: application/json" \
  -d '{"content":"corrected content here"}' \
  http://127.0.0.1:3179/entries/:id <<EOF
Authorization: Bearer $TOKEN
EOF
```

## Step 3 — Confirm and summarise

After working through all sections, print a summary of actions taken:
- N conflicts resolved (how each was resolved)
- N stale entries archived/deleted
- N team-relevant entries reviewed

If any were skipped, note them.
