/**
 * Connection-related branded types with domain-specific validation.
 *
 * These types ensure that values meet all invariants at creation time,
 * so they can be passed around without re-checking.
 */

import type { Tagged } from "type-fest";

import { assert } from "../../../shared/assert.js";

/**
 * Branded type for validated WebSocket server URLs.
 *
 * Invariants:
 * - URL is valid and parseable
 * - Protocol is ws:// or wss://
 */
export type ServerUrl = Tagged<URL, "ServerUrl">;

/**
 * Creates a validated ServerUrl from a string.
 *
 * Pre-conditions: url is a non-empty string
 * Post-conditions: Returns a ServerUrl with valid ws:// or wss:// protocol
 *
 * @param url - The WebSocket URL string
 * @throws Error if URL is invalid or not a WebSocket URL
 */
export function createServerUrl(url: string): ServerUrl {
	assert(url.length > 0, "Server URL must not be empty");

	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		throw new Error(`Invalid server URL: ${url}`);
	}

	assert(
		parsed.protocol === "ws:" || parsed.protocol === "wss:",
		`Server URL must use ws:// or wss:// protocol, got: ${parsed.protocol}`,
	);

	return parsed as ServerUrl;
}

/**
 * Branded type for validated API keys.
 *
 * Invariants:
 * - Non-empty string
 * - Contains only valid characters (alphanumeric, hyphens, underscores)
 */
export type ApiKey = Tagged<string, "ApiKey">;

/** Regex for valid API key characters */
const API_KEY_PATTERN = /^[a-zA-Z0-9_-]+$/;

/**
 * Creates a validated ApiKey from a string.
 *
 * Pre-conditions: apiKey is a non-empty string with valid characters
 * Post-conditions: Returns a validated ApiKey
 *
 * @param apiKey - The API key string
 * @throws Error if API key is invalid
 */
export function createApiKey(apiKey: string): ApiKey {
	assert(apiKey.length > 0, "API key must not be empty");
	assert(
		API_KEY_PATTERN.test(apiKey),
		"API key must contain only alphanumeric characters, hyphens, and underscores",
	);

	return apiKey as ApiKey;
}
