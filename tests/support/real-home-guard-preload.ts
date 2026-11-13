/**
 * Preload (see bunfig.toml). Before any test file loads, it takes the real-home
 * baseline and points HOME and the config variables at a temp home, so the run
 * writes there. After the whole run it checks the real home once and removes the
 * temp home. A test file cannot take the baseline itself, because Bun loads and
 * runs files one by one, and a file's own tests cannot see writes made by files
 * that run after it.
 */
import { afterAll } from "bun:test";
import { rmSync } from "node:fs";
import {
	REAL_HOME,
	diffSnapshots,
	redirectHomeEnv,
	snapshotHome,
} from "./real-home-guard.js";

const baseline = snapshotHome(REAL_HOME);
(globalThis as { __realHomeBaseline?: typeof baseline }).__realHomeBaseline =
	baseline;
const sandbox = redirectHomeEnv();

afterAll(() => {
	const changed = diffSnapshots(baseline, snapshotHome(REAL_HOME));
	rmSync(sandbox, { recursive: true, force: true });
	if (changed.length > 0) {
		throw new Error(
			`tests changed watched paths under the real home: ${changed.join(", ")}`,
		);
	}
});
