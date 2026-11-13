import {
	chmodSync,
	mkdirSync,
	mkdtempSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";

export interface SetupIsolation {
	/** Temp HOME that setup-tool resolves its service and config paths under. */
	home: string;
	/** Written by the launchctl/systemctl stubs: one line per invocation. */
	callsLog: string;
	restore(): void;
}

/**
 * Points HOME at a temp dir and prepends a PATH dir whose `launchctl` and
 * `systemctl` stubs only record their argv. Setup runs that try to register a
 * background service then leave a call record instead of touching launchd or
 * systemd. Works from any cwd: it needs no bunfig.toml or preload.
 */
export function isolateSetupTool(): SetupIsolation {
	const root = mkdtempSync(join(tmpdir(), "setup-isolation-"));
	const home = join(root, "home");
	const bin = join(root, "bin");
	const callsLog = join(root, "service-calls.log");
	mkdirSync(home);
	mkdirSync(bin);
	for (const name of ["launchctl", "systemctl"]) {
		const stub = join(bin, name);
		writeFileSync(stub, `#!/bin/sh\necho "${name} $*" >> "${callsLog}"\n`);
		chmodSync(stub, 0o755);
	}

	const savedHome = process.env.HOME;
	const savedPath = process.env.PATH;
	process.env.HOME = home;
	process.env.PATH = `${bin}${delimiter}${savedPath ?? ""}`;

	return {
		home,
		callsLog,
		restore() {
			if (savedHome === undefined) Reflect.deleteProperty(process.env, "HOME");
			else process.env.HOME = savedHome;
			process.env.PATH = savedPath;
			rmSync(root, { recursive: true, force: true });
		},
	};
}
