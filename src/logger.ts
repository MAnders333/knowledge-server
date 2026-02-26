import { appendFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

/**
 * Minimal logger that tees all output to stdout AND an append-only log file.
 *
 * Design decisions:
 * - Synchronous file writes (appendFileSync) — the server is I/O-bound on LLM
 *   calls, not on logging. Sync writes keep log ordering exact with no buffering.
 * - ISO timestamp + [LEVEL] prefix on every persisted line — makes grep/tail
 *   immediately useful. raw() writes a timestamp to the file but not to stdout,
 *   so the banner looks clean in the terminal while still being parseable on disk.
 * - Same .log/.warn/.error API as console.* so call sites are a mechanical replace.
 * - cli.ts is intentionally excluded — CLI output is user-facing, not operational.
 * - mcp/index.ts is intentionally excluded — it uses stdio transport; writing to
 *   stdout would corrupt the MCP framing.
 * - Disabled when logPath is empty string ("") — stdout-only mode (used in tests).
 *
 * Usage:
 *   import { logger } from "./logger.js";
 *   logger.log("[consolidation] Starting...");
 *   logger.warn("[db] Schema mismatch...");
 *   logger.error("[llm] Parse failure:", err);
 */

type LogLevel = "INFO" | "WARN" | "ERROR";

/** Safely serialize an unknown value, handling circular references. */
function serialize(a: unknown): string {
  if (typeof a === "string") return a;
  if (a instanceof Error) {
    // a.stack already includes "Error: <message>" in most runtimes — don't double it.
    return a.stack ?? a.message;
  }
  try {
    return JSON.stringify(a, null, 2);
  } catch {
    // Circular reference or other serialization failure — fall back to String().
    return String(a);
  }
}

class Logger {
  private logPath: string;

  constructor(logPath: string) {
    this.logPath = logPath;
    if (logPath) {
      // Ensure the log directory exists before the first write.
      // Wrapped in try/catch so a temporarily unavailable FS (e.g. NFS home dir)
      // degrades to stdout-only rather than crashing before any output reaches the user.
      try {
        mkdirSync(dirname(logPath), { recursive: true });
      } catch (err) {
        process.stderr.write(`[logger] Could not create log directory for ${logPath}: ${serialize(err)}\n`);
        this.logPath = "";
      }
    }
  }

  private write(level: LogLevel, args: unknown[]): void {
    const ts = new Date().toISOString();
    const message = args.map(serialize).join(" ");
    const line = `${ts} [${level}] ${message}`;

    // Tee to stdout/stderr (always)
    if (level === "ERROR") {
      process.stderr.write(`${line}\n`);
    } else {
      process.stdout.write(`${line}\n`);
    }

    // Write to file (when configured)
    if (this.logPath) {
      try {
        appendFileSync(this.logPath, `${line}\n`);
      } catch {
        // If the file write fails, don't crash the server — stdout is still intact.
        // Avoid recursively calling logger here; use process.stderr directly.
        process.stderr.write(`[logger] Failed to write to ${this.logPath}\n`);
      }
    }
  }

  log(...args: unknown[]): void {
    this.write("INFO", args);
  }

  warn(...args: unknown[]): void {
    this.write("WARN", args);
  }

  error(...args: unknown[]): void {
    this.write("ERROR", args);
  }

  /**
   * Log a raw line without the timestamp/level prefix on stdout.
   * Used for the startup banner where terminal formatting matters.
   *
   * The file still gets a timestamped line so the log file remains
   * fully parseable with grep/awk without mixed-format lines.
   *
   * Lines containing sensitive values (e.g. the admin token) should
   * use rawStdoutOnly() instead so they are never persisted to disk.
   */
  raw(...args: unknown[]): void {
    // stdout: use String() for display fidelity (banner lines are always strings in practice)
    const displayMessage = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
    process.stdout.write(`${displayMessage}\n`);
    if (this.logPath) {
      // file: use serialize() so Errors and circular objects are represented faithfully
      const fileMessage = args.map(serialize).join(" ");
      const ts = new Date().toISOString();
      try {
        appendFileSync(this.logPath, `${ts} [INFO] ${fileMessage}\n`);
      } catch {
        process.stderr.write(`[logger] Failed to write to ${this.logPath}\n`);
      }
    }
  }

  /**
   * Write a raw line to stdout only — never persisted to the log file.
   * Use for sensitive values (admin token) and interactive-only output.
   */
  rawStdoutOnly(...args: unknown[]): void {
    const message = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
    process.stdout.write(`${message}\n`);
  }
}

// Singleton — initialized lazily on first import of logger.ts.
// index.ts calls logger.init(config.logPath) as the very first action in main(),
// before KnowledgeDB and other modules are instantiated, so all subsequent
// operational messages (including DB schema warnings) reach the log file.
// Until init() is called, logPath is "" (stdout-only — safe for tests).
let _logger = new Logger("");

export const logger = {
  init(logPath: string): void {
    _logger = new Logger(logPath);
  },
  log: (...args: unknown[]) => _logger.log(...args),
  warn: (...args: unknown[]) => _logger.warn(...args),
  error: (...args: unknown[]) => _logger.error(...args),
  raw: (...args: unknown[]) => _logger.raw(...args),
  rawStdoutOnly: (...args: unknown[]) => _logger.rawStdoutOnly(...args),
};
