---
description: Review and resolve knowledge entries needing attention — conflicts, stale entries, team-relevant items
---

Review the knowledge graph and resolve entries that need human attention.

## Step 1 — Fetch review data

```bash
curl -s http://127.0.0.1:3179/review | python3 -m json.tool
```

Also load the admin token into a shell variable — this keeps it out of shell history while
allowing the curl commands below to reference it:

```bash
# Load token from .env (preferred — token never appears in history)
TOKEN="$(grep '^KNOWLEDGE_ADMIN_TOKEN=' ~/.local/share/knowledge-server/.env 2>/dev/null | cut -d= -f2-)"
# If not set in .env, the token was printed at server startup — paste it here:
# TOKEN="paste-token-here"
echo "Token loaded: ${#TOKEN} chars"
```

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
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"resolution":"supersede_other"}' \
  http://127.0.0.1:3179/entries/:id/resolve

# Supersede the entry at :id, keep its counterpart
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"resolution":"supersede_this"}' \
  http://127.0.0.1:3179/entries/:id/resolve

# Merge — provide the merged content
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"resolution":"merge","mergedContent":"<merged text>"}' \
  http://127.0.0.1:3179/entries/:id/resolve

# Hard-delete the entry at :id (use for noise/junk)
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:3179/entries/:id
```

### Stale entries

Entries with low strength haven't been accessed recently. For each:

- Show the content, type, and current strength
- Ask: **keep** (no action), **archive** (set status to superseded), or **delete**

To archive (soft removal):
```bash
curl -s -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"status":"superseded"}' \
  http://127.0.0.1:3179/entries/:id
```

To delete:
```bash
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:3179/entries/:id
```

### Team-relevant entries

High-confidence entries scoped to `team`. These may be worth copying into shared documentation (Confluence, README, etc.). Present each and ask:

- Is this accurate and still current?
- Does it belong in external docs?

If the content needs correction, use PATCH:
```bash
curl -s -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"corrected content here"}' \
  http://127.0.0.1:3179/entries/:id
```

## Step 3 — Confirm and summarise

After working through all sections, print a summary of actions taken:
- N conflicts resolved (how each was resolved)
- N stale entries archived/deleted
- N team-relevant entries reviewed

If any were skipped, note them.
