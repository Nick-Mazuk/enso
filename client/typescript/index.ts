import { Connection } from "./internal/connection/index.js";
import { createDatabase } from "./internal/database/index.js";
import type { Database } from "./internal/database/types.js";
import type { Field, FieldValue, Schema } from "./internal/schema/types.js";
import { Store } from "./internal/store/index.js";
import { NetworkStore } from "./internal/store/network-store.js";
import type { StoreInterface } from "./internal/store/store-interface.js";

export { createSchema } from "./internal/schema/create.js";
export { t } from "./internal/schema/t.js";

type ClientOptions<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> = {
	schema: S;
	serverUrl?: string;
	apiKey?: string;
};

/**
 * Create a new Enso client.
 *
 * Pre-conditions:
 * - If serverUrl is provided, apiKey must also be provided
 *
 * Post-conditions:
 * - Returns a connected client ready for database operations
 * - If serverUrl is provided, client is connected to server via WebSocket
 * - If serverUrl is not provided, client uses in-memory storage
 *
 * @param opts - Client configuration options
 * @returns A promise that resolves to the client
 */
export const createClient = async <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	opts: ClientOptions<S>,
): Promise<Client<S>> => {
	if (opts.serverUrl) {
		if (!opts.apiKey) {
			throw new Error("apiKey is required when serverUrl is provided");
		}
		return Client.createWithServer(opts.schema, opts.serverUrl, opts.apiKey);
	}
	return Client.createInMemory(opts.schema);
};

/**
 * Enso client for interacting with the database.
 *
 * Invariants:
 * - database is always available after construction
 * - connection is only set when using server mode
 */
class Client<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> {
	public readonly database: Database<S>;
	private readonly store: StoreInterface;
	private readonly connection: Connection | null;

	private constructor(
		schema: S,
		store: StoreInterface,
		connection: Connection | null,
	) {
		this.store = store;
		this.connection = connection;
		this.database = createDatabase(schema, this.store);
	}

	/**
	 * Create a client with in-memory storage.
	 *
	 * @param schema - The database schema
	 * @returns A client using in-memory storage
	 */
	static createInMemory<
		S extends Schema<
			Record<string, Record<string, Field<FieldValue, boolean>>>
		>,
	>(schema: S): Client<S> {
		const store = new Store();
		return new Client(schema, store, null);
	}

	/**
	 * Create a client connected to a server.
	 *
	 * @param schema - The database schema
	 * @param serverUrl - WebSocket URL of the server
	 * @param apiKey - API key for authentication
	 * @returns A promise that resolves to a connected client
	 */
	static async createWithServer<
		S extends Schema<
			Record<string, Record<string, Field<FieldValue, boolean>>>
		>,
	>(schema: S, serverUrl: string, apiKey: string): Promise<Client<S>> {
		const connection = new Connection(serverUrl, apiKey);
		await connection.connect();

		const store = new NetworkStore(connection);
		await store.initialize();

		return new Client(schema, store, connection);
	}

	/**
	 * Close the client connection.
	 * Only has effect when using server mode.
	 */
	close(): void {
		if (this.connection) {
			this.connection.close();
		}
	}
}
