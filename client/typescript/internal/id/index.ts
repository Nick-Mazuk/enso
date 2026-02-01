/**
 * ID Conversion Utilities
 *
 * The server requires exactly 16-byte IDs for both entity_id and attribute_id.
 * This module provides utilities to:
 * - Generate 16-byte entity IDs
 * - Convert between 16-byte arrays and string representations
 * - Generate deterministic 16-byte attribute IDs from field paths
 *
 * Pre-conditions:
 * - All byte arrays must be Uint8Array
 * - Field paths must be non-empty strings
 *
 * Post-conditions:
 * - All generated IDs are exactly 16 bytes
 * - String conversions are reversible (bytesToHex/hexToBytes)
 *
 * Invariants:
 * - Same field path always produces same attribute ID (deterministic)
 */

import { assert } from "../../../shared/assert.js";

const ID_LENGTH = 16;

/**
 * Generates a random 16-byte entity ID.
 *
 * Pre-conditions: None
 * Post-conditions: Returns exactly 16 random bytes
 */
export function generateEntityId(): Uint8Array {
	const bytes = new Uint8Array(ID_LENGTH);
	crypto.getRandomValues(bytes);
	return bytes;
}

/**
 * Converts a 16-byte array to a hex string.
 *
 * Pre-conditions: bytes.length === 16
 * Post-conditions: Returns a 32-character hex string
 */
export function bytesToHex(bytes: Uint8Array): string {
	assert(
		bytes.length === ID_LENGTH,
		`Expected ${ID_LENGTH} bytes, got ${bytes.length}`,
	);
	return Array.from(bytes)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

/**
 * Converts a hex string to a 16-byte array.
 *
 * Pre-conditions: hex is a 32-character hex string
 * Post-conditions: Returns exactly 16 bytes
 */
export function hexToBytes(hex: string): Uint8Array {
	assert(
		hex.length === ID_LENGTH * 2,
		`Expected ${ID_LENGTH * 2} hex chars, got ${hex.length}`,
	);
	const bytes = new Uint8Array(ID_LENGTH);
	for (let i = 0; i < ID_LENGTH; i++) {
		bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	}
	return bytes;
}

/**
 * Simple hash function (FNV-1a variant) that produces 16 bytes.
 * Used for deterministic attribute ID generation.
 *
 * Pre-conditions: input is a non-empty string
 * Post-conditions: Returns exactly 16 bytes, deterministic for same input
 */
function hashString(input: string): Uint8Array {
	assert(input.length > 0, "Input string must not be empty");

	// Use two 64-bit FNV-1a hashes with different seeds to get 128 bits
	const FNV_PRIME = 0x01000193n;
	const FNV_OFFSET_1 = 0xcbf29ce484222325n;
	const FNV_OFFSET_2 = 0x84222325cbf29ce4n;

	let hash1 = FNV_OFFSET_1;
	let hash2 = FNV_OFFSET_2;

	const encoder = new TextEncoder();
	const data = encoder.encode(input);

	for (const byte of data) {
		hash1 ^= BigInt(byte);
		hash1 = (hash1 * FNV_PRIME) & 0xffffffffffffffffn;
		hash2 ^= BigInt(byte);
		hash2 = (hash2 * FNV_PRIME) & 0xffffffffffffffffn;
	}

	const result = new Uint8Array(ID_LENGTH);
	// First 8 bytes from hash1
	for (let i = 0; i < 8; i++) {
		result[i] = Number((hash1 >> BigInt(i * 8)) & 0xffn);
	}
	// Last 8 bytes from hash2
	for (let i = 0; i < 8; i++) {
		result[8 + i] = Number((hash2 >> BigInt(i * 8)) & 0xffn);
	}

	return result;
}

/**
 * Generates a deterministic 16-byte attribute ID from an entity name and field name.
 *
 * Pre-conditions: entityName and fieldName are non-empty strings
 * Post-conditions: Returns exactly 16 bytes, same inputs always produce same output
 *
 * @param entityName - The entity type (e.g., "users")
 * @param fieldName - The field name (e.g., "name")
 * @returns 16-byte attribute ID
 */
export function fieldToAttributeId(
	entityName: string,
	fieldName: string,
): Uint8Array {
	assert(entityName.length > 0, "Entity name must not be empty");
	assert(fieldName.length > 0, "Field name must not be empty");

	const path = `${entityName}/${fieldName}`;
	return hashString(path);
}

/**
 * Validates that a byte array is exactly 16 bytes.
 *
 * Pre-conditions: None
 * Post-conditions: Returns true if bytes.length === 16
 */
export function isValidId(bytes: Uint8Array): boolean {
	return bytes.length === ID_LENGTH;
}
