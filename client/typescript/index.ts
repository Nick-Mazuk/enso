import { Connection } from "./internal/connection/index.js";
import { createApiKey, createServerUrl } from "./internal/connection/types.js";
import type { JwtProvider } from "./internal/connection/types.js";
import { createDatabase } from "./internal/database/index.js";
import type { Database } from "./internal/database/types.js";
import type { Field, FieldValue, Schema } from "./internal/schema/types.js";
import { NetworkStore } from "./internal/store/network-store.js";
import type { StoreInterface } from "./internal/store/types.js";

export type { ApiKey, Jwt, JwtProvider, ServerUrl } from "./internal/connection/types.js";
export { createApiKey, createJwt, createServerUrl } from "./internal/connection/types.js";
export { createSchema } from "./internal/schema/create.js";
export { t } from "./internal/schema/t.js";

type ClientOptions<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> = {
	schema: S;
	serverUrl: string;
	apiKey: string;
	/**
	 * A static JWT token for authentication.
	 * If provided, this token will be sent with the ConnectRequest.
	 */
	jwt?: string;
	/**
	 * A function that provides JWT tokens dynamically.
	 * Called when connecting, allowing for token refresh.
	 * If both jwt and jwtProvider are provided, jwt takes precedence.
	 */
	jwtProvider?: JwtProvider;
};

/**
 * Creates a new Enso client connected to the server.
 *
 * Pre-conditions:
 * - serverUrl must be a valid WebSocket URL (ws:// or wss://)
 * - apiKey must be a valid API key
 * - If jwt is provided, it must be a valid JWT string
 * - If jwtProvider is provided, it must return a valid JWT string
 *
 * Post-conditions:
 * - Returns a connected client ready for operations
 * - Client is connected to server via WebSocket
 * - If jwt or jwtProvider was provided, the token was sent in ConnectRequest
 *
 * @param opts - Client configuration options
 * @returns A promise that resolves to the connected client
 * @throws Error if serverUrl or apiKey are invalid, or connection fails
 */
export const createClient = async <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	opts: ClientOptions<S>,
): Promise<Client<S>> => {
	const serverUrl = createServerUrl(opts.serverUrl);
	const apiKey = createApiKey(opts.apiKey);
	const connection = new Connection(serverUrl, apiKey, {
		jwt: opts.jwt,
		jwtProvider: opts.jwtProvider,
	});
	await connection.connect();
	return new Client(opts.schema, connection);
};

/**
 * Enso client for interacting with the database.
 *
 * Invariants:
 * - All operations go through NetworkStore to the server
 * - Connection is always established
 */
class Client<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> {
	public readonly database: Database<S>;
	private readonly store: StoreInterface;
	private readonly connection: Connection;

	constructor(schema: S, connection: Connection) {
		this.connection = connection;
		this.store = new NetworkStore(connection, "default");
		this.database = createDatabase(schema, this.store);
	}

	/**
	 * Closes the connection to the server.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Connection is closed, client can no longer communicate with server
	 */
	close(): void {
		this.connection.close();
	}

	/**
	 * Returns whether the client is connected to the server.
	 */
	isConnected(): boolean {
		return this.connection.isConnected();
	}
}
