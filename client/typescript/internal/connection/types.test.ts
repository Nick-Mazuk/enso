import { describe, expect, it } from "bun:test";
import { createApiKey, createServerUrl } from "./types.js";

describe("createServerUrl", () => {
	it("creates valid ServerUrl from ws:// URL", () => {
		const url = createServerUrl("ws://localhost:8080");
		expect(url.href).toBe("ws://localhost:8080/");
		expect(url.protocol).toBe("ws:");
	});

	it("creates valid ServerUrl from wss:// URL", () => {
		const url = createServerUrl("wss://example.com");
		expect(url.href).toBe("wss://example.com/");
		expect(url.protocol).toBe("wss:");
	});

	it("throws on empty string", () => {
		expect(() => createServerUrl("")).toThrow("Server URL must not be empty");
	});

	it("throws on invalid URL format", () => {
		expect(() => createServerUrl("not-a-url")).toThrow("Invalid server URL");
	});

	it("throws on http:// protocol", () => {
		expect(() => createServerUrl("http://localhost:8080")).toThrow(
			"Server URL must use ws:// or wss:// protocol",
		);
	});

	it("throws on https:// protocol", () => {
		expect(() => createServerUrl("https://example.com")).toThrow(
			"Server URL must use ws:// or wss:// protocol",
		);
	});

	it("preserves URL path and query parameters", () => {
		const url = createServerUrl("ws://localhost:8080/api/v1?token=abc");
		expect(url.pathname).toBe("/api/v1");
		expect(url.search).toBe("?token=abc");
	});
});

describe("createApiKey", () => {
	it("creates valid ApiKey from alphanumeric string", () => {
		const key = createApiKey("abc123XYZ");
		expect(key as string).toBe("abc123XYZ");
	});

	it("creates valid ApiKey with hyphens", () => {
		const key = createApiKey("my-api-key");
		expect(key as string).toBe("my-api-key");
	});

	it("creates valid ApiKey with underscores", () => {
		const key = createApiKey("my_api_key");
		expect(key as string).toBe("my_api_key");
	});

	it("creates valid ApiKey with mixed characters", () => {
		const key = createApiKey("My-API_Key-123");
		expect(key as string).toBe("My-API_Key-123");
	});

	it("throws on empty string", () => {
		expect(() => createApiKey("")).toThrow("API key must not be empty");
	});

	it("throws on string with spaces", () => {
		expect(() => createApiKey("my api key")).toThrow(
			"API key must contain only alphanumeric characters, hyphens, and underscores",
		);
	});

	it("throws on string with special characters", () => {
		expect(() => createApiKey("key@123")).toThrow(
			"API key must contain only alphanumeric characters, hyphens, and underscores",
		);
		expect(() => createApiKey("key#test")).toThrow(
			"API key must contain only alphanumeric characters, hyphens, and underscores",
		);
		expect(() => createApiKey("key!")).toThrow(
			"API key must contain only alphanumeric characters, hyphens, and underscores",
		);
	});
});
