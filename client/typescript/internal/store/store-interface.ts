/**
 * Store Interface
 *
 * Common interface for both in-memory Store and NetworkStore.
 * This allows the Database layer to work with either implementation.
 */

import type {
	Datom,
	Filter,
	Id,
	QueryPattern,
	QueryVariable,
	Triple,
} from "./types.js";

export type Query<Find extends QueryVariable[]> = {
	find: Find;
	where: QueryPattern[];
	optional?: QueryPattern[];
	filters?: Filter[];
	whereNot?: QueryPattern[];
};

/**
 * StoreInterface defines the contract for store implementations.
 *
 * Both the in-memory Store and NetworkStore implement this interface,
 * allowing the Database layer to work with either transparently.
 */
export interface StoreInterface {
	/**
	 * Add triples to the store.
	 *
	 * @param triples - The triples to add
	 * @returns A promise that resolves when the triples are added
	 */
	add(...triples: Triple[]): Promise<void>;

	/**
	 * Execute a query against the store.
	 *
	 * @param query - The query to execute
	 * @returns A promise that resolves with the query results
	 */
	query<Find extends QueryVariable[]>(query: Query<Find>): Promise<Datom[][]>;

	/**
	 * Delete all triples for an entity.
	 *
	 * @param id - The entity ID to delete
	 * @returns A promise that resolves when deletion is complete
	 */
	deleteAllById(id: Id): Promise<void>;
}
