/**
 * Shared helpers for converting between float32 arrays and the two storage
 * representations used by PostgresKnowledgeDB:
 *   - BYTEA  (`embedding` column)  — packed float32 little-endian bytes
 *   - vector  (`embedding_vec` column) — pgvector literal '[f0,f1,...,fN]'
 *
 * Extracted so they can be shared between index.ts and migrations.ts
 * without creating a circular dependency.
 */

/** Pack a float32 number[] into a Buffer (little-endian IEEE 754). */
export function floatsToBuffer(arr: number[]): Buffer {
	return Buffer.from(new Float32Array(arr).buffer);
}

/**
 * Convert a float32 number[] to a pgvector literal string: '[f0,f1,...,fN]'.
 * postgres.js passes this as a plain string parameter; the column's vector type
 * handles parsing.  Using a string avoids the need for a custom type codec.
 */
export function floatsToVectorLiteral(arr: number[]): string {
	return `[${arr.join(",")}]`;
}

/** Convert a PostgreSQL BYTEA Buffer back to a number[] of float32 values. */
export function bufferToFloats(buf: Buffer | Uint8Array): number[] {
	const uint8 =
		buf instanceof Buffer
			? new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength)
			: buf;
	const float32 = new Float32Array(
		uint8.buffer,
		uint8.byteOffset,
		uint8.byteLength / 4,
	);
	return Array.from(float32);
}
