import { describe, expect, it } from "bun:test";
import { createApiKey, createSchema, createServerUrl } from "./index.js";

describe("createServerUrl", () => {
	it("accepts valid ws:// URLs", () => {
		const url = createServerUrl("ws://localhost:8080");
		expect(url.protocol).toBe("ws:");
		expect(url.hostname).toBe("localhost");
		expect(url.port).toBe("8080");
	});

	it("accepts valid wss:// URLs", () => {
		const url = createServerUrl("wss://api.example.com");
		expect(url.protocol).toBe("wss:");
		expect(url.hostname).toBe("api.example.com");
	});

	it("rejects http:// URLs", () => {
		expect(() => createServerUrl("http://localhost:8080")).toThrow(
			"Server URL must use ws:// or wss:// protocol",
		);
	});

	it("rejects https:// URLs", () => {
		expect(() => createServerUrl("https://api.example.com")).toThrow(
			"Server URL must use ws:// or wss:// protocol",
		);
	});

	it("rejects invalid URLs", () => {
		expect(() => createServerUrl("not-a-url")).toThrow("Invalid server URL");
	});

	it("rejects empty URLs", () => {
		expect(() => createServerUrl("")).toThrow("Server URL must not be empty");
	});
});

describe("createApiKey", () => {
	it("accepts valid alphanumeric API keys", () => {
		const key = createApiKey("abc123XYZ");
		expect(key as string).toBe("abc123XYZ");
	});

	it("accepts API keys with hyphens", () => {
		const key = createApiKey("api-key-123");
		expect(key as string).toBe("api-key-123");
	});

	it("accepts API keys with underscores", () => {
		const key = createApiKey("api_key_123");
		expect(key as string).toBe("api_key_123");
	});

	it("rejects empty API keys", () => {
		expect(() => createApiKey("")).toThrow("API key must not be empty");
	});

	it("rejects API keys with spaces", () => {
		expect(() => createApiKey("api key")).toThrow(
			"API key must contain only alphanumeric characters",
		);
	});

	it("rejects API keys with special characters", () => {
		expect(() => createApiKey("api@key!")).toThrow(
			"API key must contain only alphanumeric characters",
		);
	});
});

describe("createSchema", () => {
	it("creates a schema with the correct entities", () => {
		const schema = createSchema({
			entities: {
				users: {},
				posts: {},
				tags: {},
			},
		});
		expect(Object.keys(schema.entities)).toEqual(["users", "posts", "tags"]);
	});
});
