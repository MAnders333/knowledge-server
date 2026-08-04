/**
 * Asset module declarations for binary-embedded files.
 *
 * The reranker imports onnxruntime-web's WASM runtime via `with { type: "file" }`:
 * - In source/dev runs, the import resolves to the real node_modules path.
 * - In `bun build --compile` binaries, the file is embedded into the single-file
 *   binary and exposed under the virtual /$bunfs/ filesystem (readable via node:fs).
 * One import statement thereby covers both distribution modes.
 */
declare module "*.wasm" {
	const path: string;
	export default path;
}

declare module "*.mjs" {
	const path: string;
	export default path;
}
