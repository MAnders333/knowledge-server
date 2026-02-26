import { appendFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

/**
 * Minimal logger that tees all output to stdout AND a rotating log file.
 *
 * Design decisions:
 * - Synchronous file writes (appendFileSync) — the server is I/O-bound on LLM
 *   calls, not on logging. Sync writes keep log ordering exact with no buffering.
 * - ISO timestamp prefix on every line — makes grep/tail immediately useful.
 * - Same .log/.warn/.error API as console.* so call sites are a mechanical replace.
 * - cli.ts is explicitly excluded — CLI output is user-facing, not operational.
 * - Disabled when logPath is empty string ("") — stdout-only mode for tests.
 *
 * Usage:
 *   import { logger } from "./logger.js";
 *   logger.log("[consolidation] Starting...");
 *   logger.warn("[db] Schema mismatch...");
 *   logger.error("[llm] Parse failure:", err);
 */

type LogLevel = "INFO" | "WARN" | "ERROR";

class Logger {
  private logPath: string;

  constructor(logPath: string) {
    this.logPath = logPath;
    if (logPath) {
      // Ensure the log directory exists before the first write.
      mkdirSync(dirname(logPath), { recursive: true });
    }
  }

  private write(level: LogLevel, args: unknown[]): void {
    const ts = new Date().toISOString();
    // Format each argument the same way console.* does:
    // strings pass through as-is; everything else is JSON-stringified.
    const message = args
      .map((a) =>
        typeof a === "string"
          ? a
          : a instanceof Error
            ? `${a.message}\n${a.stack ?? ""}`
            : JSON.stringify(a, null, 2)
      )
      .join(" ");

    const line = `${ts} [${level}] ${message}`;

    // Tee to stdout (always)
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
   * Log a raw line without the timestamp/level prefix.
   * Used for the startup banner box where formatting matters.
   */
  raw(...args: unknown[]): void {
    const message = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
    process.stdout.write(`${message}\n`);
    if (this.logPath) {
      try {
        appendFileSync(this.logPath, `${message}\n`);
      } catch {
        process.stderr.write(`[logger] Failed to write to ${this.logPath}\n`);
      }
    }
  }
}

// Singleton — initialized lazily on first import of logger.ts.
// index.ts calls logger.init(config.logPath) before any other module
// uses the logger; until then, logPath is "" (stdout-only).
let _logger = new Logger("");

export const logger = {
  init(logPath: string): void {
    _logger = new Logger(logPath);
  },
  log: (...args: unknown[]) => _logger.log(...args),
  warn: (...args: unknown[]) => _logger.warn(...args),
  error: (...args: unknown[]) => _logger.error(...args),
  raw: (...args: unknown[]) => _logger.raw(...args),
};
