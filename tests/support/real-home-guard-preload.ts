/**
 * Preload (see bunfig.toml): takes the real-home baseline before any test file
 * loads, and checks it once after the whole run. A test file cannot take that
 * baseline itself, because Bun loads and runs files one by one, and a file's
 * own tests cannot see writes made by files that run after it.
 */
import { afterAll } from "bun:test";
import { REAL_HOME, diffSnapshots, snapshotHome } from "./real-home-guard.js";

const baseline = snapshotHome(REAL_HOME);
(globalThis as { __realHomeBaseline?: typeof baseline }).__realHomeBaseline =
	baseline;

afterAll(() => {
	const changed = diffSnapshots(baseline, snapshotHome(REAL_HOME));
	if (changed.length > 0) {
		throw new Error(
			`tests changed watched paths under the real home: ${changed.join(", ")}`,
		);
	}
});
