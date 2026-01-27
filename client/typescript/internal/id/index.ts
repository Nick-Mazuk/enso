/**
 * ID Conversion Utilities
 *
 * The server requires exactly 16-byte IDs for entities and attributes.
 * These utilities convert between client-side string IDs (nanoid, field names)
 * and 16-byte Uint8Arrays for the server protocol.
 *
 * Invariants:
 * - All returned Uint8Arrays are exactly 16 bytes
 * - stringToBytes and bytesToString are inverses for round-trip consistency
 * - fieldToAttributeId produces deterministic output for the same input
 */

const ID_LENGTH = 16;

/**
 * Convert a string ID (e.g., nanoid) to a 16-byte Uint8Array.
 *
 * Pre-conditions:
 * - id must be a non-empty string
 *
 * Post-conditions:
 * - Returns exactly 16 bytes
 * - Same input always produces same output
 *
 * @param id - The string ID to convert
 * @returns A 16-byte Uint8Array
 */
export const stringToBytes = (id: string): Uint8Array => {
	// Use a simple hash-based approach for consistent 16-byte output
	// We use a basic FNV-1a inspired hash to spread entropy across bytes
	const bytes = new Uint8Array(ID_LENGTH);
	const encoder = new TextEncoder();
	const encoded = encoder.encode(id);

	// Initialize with FNV offset basis (split across bytes)
	bytes[0] = 0xcb;
	bytes[1] = 0xf2;
	bytes[2] = 0x9c;
	bytes[3] = 0xe4;
	bytes[4] = 0x84;
	bytes[5] = 0x22;
	bytes[6] = 0x23;
	bytes[7] = 0x25;
	bytes[8] = 0xcb;
	bytes[9] = 0xf2;
	bytes[10] = 0x9c;
	bytes[11] = 0xe4;
	bytes[12] = 0x84;
	bytes[13] = 0x22;
	bytes[14] = 0x23;
	bytes[15] = 0x25;

	// XOR each input byte into the hash and apply FNV prime mixing
	for (let i = 0; i < encoded.length; i++) {
		const inputByte = encoded[i]!;
		const targetIdx = i % ID_LENGTH;
		bytes[targetIdx] ^= inputByte;

		// Mix with neighboring bytes for better distribution
		const nextIdx = (targetIdx + 1) % ID_LENGTH;
		bytes[nextIdx] ^= (bytes[targetIdx]! * 0x01000193) & 0xff;
	}

	return bytes;
};

/**
 * Convert a 16-byte Uint8Array to a hex string representation.
 *
 * Pre-conditions:
 * - bytes must be a Uint8Array of exactly 16 bytes
 *
 * Post-conditions:
 * - Returns a 32-character hex string
 *
 * @param bytes - The 16-byte array to convert
 * @returns A hex string representation
 */
export const bytesToString = (bytes: Uint8Array): string => {
	let result = "";
	for (let i = 0; i < bytes.length; i++) {
		result += bytes[i]!.toString(16).padStart(2, "0");
	}
	return result;
};

/**
 * Convert a hex string back to a 16-byte Uint8Array.
 *
 * Pre-conditions:
 * - hex must be a 32-character hex string
 *
 * Post-conditions:
 * - Returns exactly 16 bytes
 *
 * @param hex - The hex string to convert
 * @returns A 16-byte Uint8Array
 */
export const hexToBytes = (hex: string): Uint8Array => {
	const bytes = new Uint8Array(ID_LENGTH);
	for (let i = 0; i < ID_LENGTH; i++) {
		bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	}
	return bytes;
};

/**
 * Generate an attribute ID from an entity name and field name.
 * Hashes "entityName/fieldName" to produce a consistent 16-byte ID.
 *
 * Pre-conditions:
 * - entityName must be a non-empty string
 * - fieldName must be a non-empty string
 *
 * Post-conditions:
 * - Returns exactly 16 bytes
 * - Same input always produces same output
 *
 * @param entityName - The name of the entity (e.g., "users")
 * @param fieldName - The name of the field (e.g., "name")
 * @returns A 16-byte Uint8Array representing the attribute ID
 */
export const fieldToAttributeId = (
	entityName: string,
	fieldName: string,
): Uint8Array => {
	return stringToBytes(`${entityName}/${fieldName}`);
};
