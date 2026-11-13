/**
 * Shared rules for versioned schema migrations. Every backend (knowledge.db,
 * Postgres knowledge store, state.db, Postgres state DB) keeps one ordered list
 * of migrations and derives its SCHEMA_VERSION from the last entry, so a version
 * bump cannot land without the migration that goes with it.
 *
 * Stores are never dropped to resolve a mismatch: a store newer than this build
 * is refused, and a store with a missing migration is refused, naming the version.
 */

export interface VersionedMigration {
	readonly version: number;
	readonly label: string;
}

/** Throws unless versions are consecutive ascending integers. */
export function assertContiguousMigrations(
	list: readonly VersionedMigration[],
): void {
	for (let i = 1; i < list.length; i++) {
		if (list[i].version !== list[i - 1].version + 1) {
			throw new Error(
				`migration registry is not contiguous: v${list[i - 1].version} is followed by v${list[i].version}`,
			);
		}
	}
}

/** The schema version this build expects: the last registered migration. */
export function latestVersion(list: readonly VersionedMigration[]): number {
	assertContiguousMigrations(list);
	if (list.length === 0) throw new Error("migration registry is empty");
	return list[list.length - 1].version;
}

/** Refuse a store stamped by a newer build. Nothing is modified. */
export function assertNotNewerThanCode(
	stored: number,
	codeVersion: number,
	what: string,
): void {
	if (stored > codeVersion) {
		throw new Error(
			`${what} is at schema v${stored}, newer than this build (v${codeVersion}). Upgrade knowledge-server. Refusing to open it so its data is not modified.`,
		);
	}
}

/**
 * Migrations needed to bring a store from `stored` up to `target`, in order.
 * A version with no registered migration is a hard error; nothing is dropped.
 */
export function pendingMigrations<T extends VersionedMigration>(
	list: readonly T[],
	stored: number,
	target: number,
	what: string,
): T[] {
	const pending: T[] = [];
	for (let v = stored + 1; v <= target; v++) {
		const migration = list.find((m) => m.version === v);
		if (!migration) {
			throw new Error(
				`${what} is at schema v${stored} and needs v${target}, but no migration for v${v} is registered. Refusing to modify the store; restore a backup or add the missing migration.`,
			);
		}
		pending.push(migration);
	}
	return pending;
}
