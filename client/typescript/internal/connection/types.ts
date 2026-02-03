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

/**
 * Branded type for validated JWT tokens.
 *
 * Invariants:
 * - Non-empty string
 * - Contains exactly two dots (header.payload.signature format)
 */
export type Jwt = Tagged<string, "Jwt">;

/** Regex for basic JWT structure validation (three base64url-encoded parts separated by dots) */
const JWT_PATTERN = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;

/**
 * Creates a validated Jwt from a string.
 *
 * Pre-conditions: jwt is a non-empty string in valid JWT format
 * Post-conditions: Returns a validated Jwt
 *
 * @param jwt - The JWT string
 * @throws Error if JWT format is invalid
 */
export function createJwt(jwt: string): Jwt {
	assert(jwt.length > 0, "JWT must not be empty");
	assert(
		JWT_PATTERN.test(jwt),
		"JWT must be in valid format (header.payload.signature)",
	);

	return jwt as Jwt;
}

/**
 * Function type for providing JWT tokens dynamically.
 *
 * This allows tokens to be fetched or refreshed on demand, supporting
 * scenarios like token refresh before expiration.
 *
 * Invariants:
 * - Function returns a valid JWT string or Promise resolving to one
 */
export type JwtProvider = () => string | Promise<string>;

/**
 * Options for JWT-based authentication on connections.
 *
 * Invariants:
 * - If jwt is provided, it must be a valid JWT string
 * - If jwtProvider is provided, it must be a function returning a JWT
 * - At most one of jwt or jwtProvider should be used (jwt takes precedence)
 */
export interface JwtOptions {
	/**
	 * A static JWT token for authentication.
	 * If provided, this token will be used for all requests.
	 */
	jwt?: string;

	/**
	 * A function that provides JWT tokens dynamically.
	 * Called when a token is needed, allowing for token refresh.
	 * If both jwt and jwtProvider are provided, jwt takes precedence.
	 */
	jwtProvider?: JwtProvider;
}

/**
 * Resolves a JWT from JwtOptions, supporting both static and dynamic tokens.
 *
 * Pre-conditions: options contains either jwt, jwtProvider, or neither
 * Post-conditions: Returns a validated Jwt or undefined if no auth configured
 *
 * @param options - The JWT options to resolve
 * @returns A validated Jwt, or undefined if no JWT is configured
 */
export async function resolveJwt(options: JwtOptions): Promise<Jwt | undefined> {
	if (options.jwt !== undefined) {
		return createJwt(options.jwt);
	}

	if (options.jwtProvider !== undefined) {
		const token = await options.jwtProvider();
		return createJwt(token);
	}

	return undefined;
}
