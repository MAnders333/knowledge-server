/**
 * Boolean environment flags. Every on/off variable uses these spellings
 * (case-insensitive, surrounding whitespace ignored). Kept free of other
 * imports so config-file.ts and config.ts can both use it.
 */
const TRUE_SPELLINGS = ["true", "1", "on", "yes"];
const FALSE_SPELLINGS = ["false", "0", "off", "no"];

/** The accepted spellings, quoted for error messages. */
export const FLAG_SPELLINGS = [...TRUE_SPELLINGS, ...FALSE_SPELLINGS]
	.map((s) => `"${s}"`)
	.join(", ");

/**
 * undefined when the value is unset or empty; true or false for a known
 * spelling; null for anything else, which callers report as an error.
 */
export function parseFlag(raw: string | undefined): boolean | null | undefined {
	if (raw === undefined || raw.trim() === "") return undefined;
	const word = raw.trim().toLowerCase();
	if (TRUE_SPELLINGS.includes(word)) return true;
	if (FALSE_SPELLINGS.includes(word)) return false;
	return null;
}
