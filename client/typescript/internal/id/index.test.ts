import { describe, expect, it } from "bun:test";
import {
	bytesToString,
	fieldToAttributeId,
	hexToBytes,
	stringToBytes,
} from "./index.js";

describe("ID conversion utilities", () => {
	describe("stringToBytes", () => {
		it("returns exactly 16 bytes", () => {
			const result = stringToBytes("abc123");
			expect(result.length).toBe(16);
		});

		it("produces consistent output for same input", () => {
			const result1 = stringToBytes("test-id");
			const result2 = stringToBytes("test-id");
			expect(result1).toEqual(result2);
		});

		it("produces different output for different inputs", () => {
			const result1 = stringToBytes("id-1");
			const result2 = stringToBytes("id-2");
			expect(result1).not.toEqual(result2);
		});

		it("handles nanoid-length strings (21 chars)", () => {
			const nanoid = "V1StGXR8_Z5jdHi6B-myT";
			const result = stringToBytes(nanoid);
			expect(result.length).toBe(16);
		});

		it("handles short strings", () => {
			const result = stringToBytes("a");
			expect(result.length).toBe(16);
		});

		it("handles long strings", () => {
			const longString = "a".repeat(1000);
			const result = stringToBytes(longString);
			expect(result.length).toBe(16);
		});
	});

	describe("bytesToString", () => {
		it("returns a 32-character hex string for 16 bytes", () => {
			const bytes = new Uint8Array(16);
			const result = bytesToString(bytes);
			expect(result.length).toBe(32);
		});

		it("correctly converts bytes to hex", () => {
			const bytes = new Uint8Array([0, 1, 15, 16, 255]);
			const result = bytesToString(bytes);
			expect(result).toBe("00010f10ff");
		});
	});

	describe("hexToBytes", () => {
		it("returns exactly 16 bytes", () => {
			const hex = "00112233445566778899aabbccddeeff";
			const result = hexToBytes(hex);
			expect(result.length).toBe(16);
		});

		it("correctly converts hex to bytes", () => {
			const hex = "00010f10ff000000000000000000000000";
			const result = hexToBytes(hex);
			expect(result[0]).toBe(0);
			expect(result[1]).toBe(1);
			expect(result[2]).toBe(15);
			expect(result[3]).toBe(16);
			expect(result[4]).toBe(255);
		});

		it("round-trips with bytesToString", () => {
			const original = "a1b2c3d4e5f67890a1b2c3d4e5f67890";
			const bytes = hexToBytes(original);
			const result = bytesToString(bytes);
			expect(result).toBe(original);
		});
	});

	describe("fieldToAttributeId", () => {
		it("returns exactly 16 bytes", () => {
			const result = fieldToAttributeId("users", "name");
			expect(result.length).toBe(16);
		});

		it("produces consistent output for same input", () => {
			const result1 = fieldToAttributeId("users", "name");
			const result2 = fieldToAttributeId("users", "name");
			expect(result1).toEqual(result2);
		});

		it("produces different output for different entities", () => {
			const result1 = fieldToAttributeId("users", "name");
			const result2 = fieldToAttributeId("posts", "name");
			expect(result1).not.toEqual(result2);
		});

		it("produces different output for different fields", () => {
			const result1 = fieldToAttributeId("users", "name");
			const result2 = fieldToAttributeId("users", "email");
			expect(result1).not.toEqual(result2);
		});
	});
});
