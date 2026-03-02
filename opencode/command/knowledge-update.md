---
description: Update knowledge-server to the latest release
---

Update knowledge-server binaries, plugin, and commands to the latest release.

```bash
knowledge-server --update
```

If `knowledge-server` is not in PATH, use the full path:

```bash
~/.local/bin/knowledge-server --update
```

After updating, restart the HTTP server to pick up the new binary. The plugin and MCP server update automatically on next OpenCode session start.

To check the currently installed version:

```bash
cat ~/.local/share/knowledge-server/version
```

To update to a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/MAnders333/knowledge-server/main/scripts/install.sh | bash -s -- --version v1.2.0
```
