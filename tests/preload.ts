/**
 * Preloaded before every test file (bunfig.toml `[test] preload`).
 *
 * setupCursor / setupCodex / setupClaudeCode / … end by calling
 * maybeSetupDaemon(), which would register a launchd or systemd service that
 * runs from this checkout. The opt-out keeps test runs from doing that.
 *
 * Bun fixes os.homedir() when the process starts, so HOME cannot be redirected
 * per test process. Tests therefore point tool directories (CURSOR_HOME,
 * CODEX_HOME, CLAUDE_CONFIG_DIR, …) and database paths at temp dirs explicitly.
 */
process.env.KNOWLEDGE_SKIP_DAEMON_REGISTRATION = "1";
