import {
	appendFileSync,
	closeSync,
	fstatSync,
	mkdirSync,
	openSync,
	statSync,
	writeSync,
} from "node:fs";
import { dirname } from "node:path";
import { ensurePrivateFile } from "./utils.js";

/**
 * Minimal logger that tees all output to stdout AND an append-only log file.
 *
 * Design decisions:
 * - One file descriptor, opened once and reused for every line (no open/close per
 *   line). Writes are synchronous so ordering is exact.
 * - Never double-write: launchd points stdout and stderr at the same server.log.
 *   When a stream is already the log file (same device + inode), the line goes to
 *   that stream only. Writing it again to the file was the source of the duplicated
 *   lines in the log.
 * - ISO timestamp + [LEVEL] prefix on every persisted line — makes grep/tail
 *   immediately useful. raw() writes a timestamped line to the file (and to stdout
 *   when stdout is the file) but a plain line to a terminal, so the banner looks clean.
 * - LOG_LEVEL (debug | info | warn | error, default info). Prompt and entry text is
 *   only logged at debug; info lines carry lengths, ids and scores, not content.
 * - Same .log/.warn/.error API as console.* so call sites are a mechanical replace.
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

export type LogLevelName = "debug" | "info" | "warn" | "error";
type LineLevel = "DEBUG" | "INFO" | "WARN" | "ERROR";

const RANK: Record<LogLevelName, number> = {
	debug: 0,
	info: 1,
	warn: 2,
	error: 3,
};
const LABEL: Record<LineLevel, LogLevelName> = {
	DEBUG: "debug",
	INFO: "info",
	WARN: "warn",
	ERROR: "error",
};

/** Parse LOG_LEVEL. Unset or unknown values fall back to "info". */
export function parseLogLevel(raw: string | undefined): LogLevelName {
	const v = raw?.trim().toLowerCase();
	return v === "debug" || v === "info" || v === "warn" || v === "error"
		? v
		: "info";
}

/** Safely serialize an unknown value, handling circular references. */
function serialize(a: unknown): string {
	if (typeof a === "string") return a;
	if (a instanceof Error) {
		// a.stack already includes "Error: <message>" in most runtimes — don't double it.
		// Fetch-thrown DOMExceptions (e.g. AbortSignal.timeout's TimeoutError) have
		// stack === "" in Bun, which previously rendered as nothing.
		return a.stack || `${a.name}: ${a.message}`;
	}
	try {
		return JSON.stringify(a, null, 2);
	} catch {
		// Circular reference or other serialization failure — fall back to String().
		return String(a);
	}
}

/** True when open file descriptor `fd` refers to the same file as `path`. */
function isSameFile(fd: number, path: string): boolean {
	try {
		const a = fstatSync(fd);
		const b = statSync(path);
		return a.dev === b.dev && a.ino === b.ino;
	} catch {
		return false;
	}
}

export interface LoggerOptions {
	/** Mirror to stdout/stderr. Tests turn this off to keep output quiet. */
	mirror?: boolean;
}

export class Logger {
	private readonly logPath: string;
	private readonly level: LogLevelName;
	private readonly mirror: boolean;
	private fd: number | null = null;
	private readonly stdoutIsLog: boolean;
	private readonly stderrIsLog: boolean;
	/** Number of times the log file was opened. Stays at 1 for a process lifetime. */
	fileOpens = 0;

	constructor(
		logPath: string,
		level: LogLevelName = "info",
		options?: LoggerOptions,
	) {
		this.level = level;
		this.mirror = options?.mirror ?? true;
		this.logPath = logPath;
		if (logPath) {
			// Ensure the log directory exists before the first write.
			// Wrapped in try/catch so a temporarily unavailable FS (e.g. NFS home dir)
			// degrades to stdout-only rather than crashing before any output reaches the user.
			try {
				mkdirSync(dirname(logPath), { recursive: true });
				// Logs can carry prompts and tokens — create the file owner-only.
				ensurePrivateFile(logPath);
			} catch (err) {
				process.stderr.write(
					`[logger] Could not create log directory for ${logPath}: ${serialize(err)}\n`,
				);
				this.logPath = "";
			}
		}
		this.stdoutIsLog = this.logPath !== "" && isSameFile(1, this.logPath);
		this.stderrIsLog = this.logPath !== "" && isSameFile(2, this.logPath);
	}

	isEnabled(level: LogLevelName): boolean {
		return RANK[level] >= RANK[this.level];
	}

	/** Append one line to the log file through the reused descriptor. */
	private appendLine(line: string): void {
		try {
			if (this.fd === null) {
				this.fd = openSync(this.logPath, "a", 0o600);
				this.fileOpens++;
			}
			writeSync(this.fd, `${line}\n`);
		} catch {
			// If the file write fails, don't crash the server — stdout is still intact.
			// Avoid recursively calling logger here; use process.stderr directly.
			process.stderr.write(`[logger] Failed to write to ${this.logPath}\n`);
		}
	}

	/**
	 * Send one persisted line to the stream it belongs on. The file gets it only
	 * when that stream is not already the log file (avoids the double write).
	 */
	private emit(toStderr: boolean, line: string): void {
		const stream = toStderr ? process.stderr : process.stdout;
		if (this.mirror) stream.write(`${line}\n`);
		if (this.logPath && !(toStderr ? this.stderrIsLog : this.stdoutIsLog)) {
			this.appendLine(line);
		}
	}

	private write(level: LineLevel, args: unknown[]): void {
		if (!this.isEnabled(LABEL[level])) return;
		const ts = new Date().toISOString();
		const message = args.map(serialize).join(" ");
		this.emit(level === "ERROR", `${ts} [${level}] ${message}`);
	}

	log(...args: unknown[]): void {
		this.write("INFO", args);
	}

	debug(...args: unknown[]): void {
		this.write("DEBUG", args);
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
	private toDisplay(args: unknown[]): string {
		return args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
	}

	raw(...args: unknown[]): void {
		if (!this.isEnabled("info")) return;
		if (this.stdoutIsLog) {
			// stdout is the log file: the timestamped form is the only copy written.
			this.emit(
				false,
				`${new Date().toISOString()} [INFO] ${args.map(serialize).join(" ")}`,
			);
			return;
		}
		if (this.mirror) process.stdout.write(`${this.toDisplay(args)}\n`);
		if (this.logPath) {
			// file: use serialize() so Errors and circular objects are represented faithfully
			this.appendLine(
				`${new Date().toISOString()} [INFO] ${args.map(serialize).join(" ")}`,
			);
		}
	}

	/**
	 * Write a raw line to stdout only — never persisted to the log file.
	 * Use for sensitive values (admin token) and interactive-only output.
	 */
	rawStdoutOnly(...args: unknown[]): void {
		if (this.mirror) process.stdout.write(`${this.toDisplay(args)}\n`);
	}

	/**
	 * Write a pre-formatted line as-is to stdout and the log file.
	 * Used for forwarding output from child processes (e.g. knowledge-daemon)
	 * that already include their own timestamp and level prefix, to avoid
	 * double-stamping in the log file.
	 */
	passthrough(line: string): void {
		if (this.mirror) process.stdout.write(`${line}\n`);
		if (this.logPath && !this.stdoutIsLog) this.appendLine(line);
	}

	/** Close the file descriptor. Only for shutdown and tests. */
	close(): void {
		if (this.fd !== null) {
			closeSync(this.fd);
			this.fd = null;
		}
	}
}

// Singleton — initialized lazily on first import of logger.ts.
// index.ts calls logger.init(config.logPath, config.logLevel) as the very first
// action in main(), before KnowledgeDB and other modules are instantiated, so all
// subsequent operational messages (including DB schema warnings) reach the log file.
// Until init() is called, logPath is "" (stdout-only — safe for tests).
let _logger = new Logger("");

export const logger = {
	init(logPath: string, level: LogLevelName = "info"): void {
		_logger = new Logger(logPath, level);
	},
	/** True when the given level would be written. Use to skip building costly debug text. */
	isEnabled: (level: LogLevelName) => _logger.isEnabled(level),
	log: (...args: unknown[]) => _logger.log(...args),
	debug: (...args: unknown[]) => _logger.debug(...args),
	warn: (...args: unknown[]) => _logger.warn(...args),
	error: (...args: unknown[]) => _logger.error(...args),
	raw: (...args: unknown[]) => _logger.raw(...args),
	rawStdoutOnly: (...args: unknown[]) => _logger.rawStdoutOnly(...args),
	passthrough: (line: string) => _logger.passthrough(line),
};
