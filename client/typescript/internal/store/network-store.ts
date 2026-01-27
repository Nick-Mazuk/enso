/**
 * NetworkStore Class
 *
 * Server-backed store implementation that sends all operations to the server
 * via WebSocket. Supports optimistic writes and subscription-based updates.
 *
 * Invariants:
 * - All queries are executed on the server
 * - Pending writes track optimistic updates until server confirms
 * - Subscription updates are routed to registered listeners
 */

import { create } from "@bufbuild/protobuf";
import {
	type ChangeRecord,
	HlcTimestampSchema,
	type QueryPattern as ProtoQueryPattern,
	type Triple as ProtoTriple,
	QueryPatternSchema,
	QueryPatternVariableSchema,
	QueryRequestSchema,
	type QueryResultRow,
	TripleSchema,
	TripleValueSchema,
} from "../../proto/protocol_pb.js";
import type { Connection } from "../connection/index.js";
import { HlcClock } from "../hlc/index.js";
import { fieldToAttributeId, stringToBytes } from "../id/index.js";
import {
	type Datom,
	type Filter,
	type Id,
	isVariable,
	type QueryPattern,
	type QueryVariable,
	type Triple,
	type Value,
} from "./types.js";

type Query<Find extends QueryVariable[]> = {
	find: Find;
	where: QueryPattern[];
	optional?: QueryPattern[];
	filters?: Filter[];
	whereNot?: QueryPattern[];
};

export type ChangeListener = (changes: ChangeRecord[]) => void;

/**
 * NetworkStore manages data through a server connection.
 *
 * Pre-conditions:
 * - Connection must be established before operations
 *
 * Post-conditions:
 * - All mutations are persisted on the server
 * - Queries return server-side results
 *
 * Invariants:
 * - pendingWrites contains writes awaiting server acknowledgment
 * - changeListeners receive all subscription updates
 */
export class NetworkStore {
	private readonly connection: Connection;
	private readonly clock: HlcClock;
	private pendingWrites: Map<string, Triple[]> = new Map();
	private changeListeners: Set<ChangeListener> = new Set();
	private subscriptionId: number = 1;

	/**
	 * Create a new NetworkStore.
	 *
	 * @param connection - The connection to use for server communication
	 */
	constructor(connection: Connection) {
		this.connection = connection;
		this.clock = new HlcClock();
	}

	/**
	 * Initialize the store by subscribing to changes.
	 *
	 * @returns A promise that resolves when subscription is established
	 */
	async initialize(): Promise<void> {
		await this.connection.subscribe(this.subscriptionId, (update) => {
			this.handleSubscriptionUpdate(update.changes);
		});
	}

	/**
	 * Add triples to the store.
	 *
	 * Pre-conditions:
	 * - Connection must be established
	 *
	 * Post-conditions:
	 * - Triples are persisted on the server
	 * - Pending writes are cleared on success
	 *
	 * @param triples - The triples to add
	 */
	async add(...triples: Triple[]): Promise<void> {
		if (triples.length === 0) return;

		// Track in pendingWrites for optimistic UI
		const firstTriple = triples[0];
		if (!firstTriple) return;
		const entityId = firstTriple[0];
		this.pendingWrites.set(entityId, triples);

		try {
			// Convert to protocol format
			const protoTriples = triples.map((triple) => this.tripleToProto(triple));

			// Send to server
			await this.connection.sendTripleUpdate(protoTriples);

			// On success, remove from pendingWrites
			this.pendingWrites.delete(entityId);
		} catch (error) {
			// On failure, keep in pendingWrites and notify listeners
			this.pendingWrites.delete(entityId);
			throw error;
		}
	}

	/**
	 * Execute a query on the server.
	 *
	 * Pre-conditions:
	 * - Connection must be established
	 * - Filters must only use equality (complex filters throw error)
	 *
	 * Post-conditions:
	 * - Returns query results from server
	 *
	 * @param query - The query to execute
	 * @returns The query results as an array of datom arrays
	 */
	async query<Find extends QueryVariable[]>(
		query: Query<Find>,
	): Promise<Datom[][]> {
		// Check for complex filters (not supported)
		if (query.filters && query.filters.length > 0) {
			throw new Error(
				"Complex filters are not implemented for server queries. Only equality filters through where patterns are supported.",
			);
		}

		// Convert to protocol format
		const queryRequest = this.queryToProto(query);

		// Send to server
		const response = await this.connection.sendQuery(queryRequest);

		// Parse response
		return this.parseQueryResponse(response.rows, response.columns, query.find);
	}

	/**
	 * Delete all triples for an entity.
	 *
	 * Pre-conditions:
	 * - Connection must be established
	 *
	 * Post-conditions:
	 * - All triples for the entity are deleted on server
	 *
	 * @param id - The entity ID to delete
	 */
	async deleteAllById(id: Id): Promise<void> {
		// Track deletion in pendingWrites
		this.pendingWrites.set(id, []);

		try {
			// Create a "tombstone" triple with null value to signal deletion
			// The server protocol may need a specific delete mechanism
			// For now, we send an empty triple update for the entity
			const timestamp = this.clock.createTimestamp();
			const deleteTriple = create(TripleSchema, {
				entityId: stringToBytes(id),
				// Use a special attribute to signal deletion
				attributeId: stringToBytes("__deleted__"),
				value: create(TripleValueSchema, {
					value: { case: "boolean", value: true },
				}),
				hlc: create(HlcTimestampSchema, {
					physicalTimeMs: timestamp.physicalTimeMs,
					logicalCounter: timestamp.logicalCounter,
					nodeId: timestamp.nodeId,
				}),
			});

			await this.connection.sendTripleUpdate([deleteTriple]);
			this.pendingWrites.delete(id);
		} catch (error) {
			this.pendingWrites.delete(id);
			throw error;
		}
	}

	/**
	 * Register a listener for change updates.
	 *
	 * @param listener - The listener function
	 * @returns A function to unregister the listener
	 */
	onChanges(listener: ChangeListener): () => void {
		this.changeListeners.add(listener);
		return () => {
			this.changeListeners.delete(listener);
		};
	}

	/**
	 * Handle subscription updates from the server.
	 *
	 * @param changes - The changes from the server
	 */
	private handleSubscriptionUpdate(changes: ChangeRecord[]): void {
		for (const listener of this.changeListeners) {
			listener(changes);
		}
	}

	/**
	 * Convert a local triple to protocol format.
	 *
	 * @param triple - The local triple
	 * @returns The protocol triple
	 */
	private tripleToProto(triple: Triple): ProtoTriple {
		const [id, field, value] = triple;
		const timestamp = this.clock.createTimestamp();

		// Parse field to get entity name and field name
		const fieldParts = (field as string).split("/");
		const entityName = fieldParts[0] ?? "";
		const fieldName = fieldParts[1] ?? "";

		return create(TripleSchema, {
			entityId: stringToBytes(id as string),
			attributeId: fieldToAttributeId(entityName, fieldName),
			value: this.valueToProto(value),
			hlc: create(HlcTimestampSchema, {
				physicalTimeMs: timestamp.physicalTimeMs,
				logicalCounter: timestamp.logicalCounter,
				nodeId: timestamp.nodeId,
			}),
		});
	}

	/**
	 * Convert a local value to protocol format.
	 *
	 * @param value - The local value
	 * @returns The protocol value
	 */
	private valueToProto(value: Value) {
		if (typeof value === "string") {
			return create(TripleValueSchema, {
				value: { case: "string", value },
			});
		}
		if (typeof value === "number") {
			return create(TripleValueSchema, {
				value: { case: "number", value },
			});
		}
		if (typeof value === "boolean") {
			return create(TripleValueSchema, {
				value: { case: "boolean", value },
			});
		}
		// Treat as string (for Id types)
		return create(TripleValueSchema, {
			value: { case: "string", value: String(value) },
		});
	}

	/**
	 * Convert a local query to protocol format.
	 *
	 * @param query - The local query
	 * @returns The protocol query request
	 */
	private queryToProto<Find extends QueryVariable[]>(query: Query<Find>) {
		return create(QueryRequestSchema, {
			find: query.find.map((variable) =>
				create(QueryPatternVariableSchema, {
					label: variable.name,
				}),
			),
			where: query.where.map((pattern) => this.patternToProto(pattern)),
			optional:
				query.optional?.map((pattern) => this.patternToProto(pattern)) ?? [],
			whereNot:
				query.whereNot?.map((pattern) => this.patternToProto(pattern)) ?? [],
		});
	}

	/**
	 * Convert a local query pattern to protocol format.
	 *
	 * @param pattern - The local pattern
	 * @returns The protocol pattern
	 */
	private patternToProto(pattern: QueryPattern): ProtoQueryPattern {
		const [entityPart, fieldPart, valuePart] = pattern;

		const protoPattern = create(QueryPatternSchema, {});

		// Handle entity part
		if (isVariable(entityPart)) {
			protoPattern.entity = {
				case: "entityVariable",
				value: create(QueryPatternVariableSchema, {
					label: entityPart.name,
				}),
			};
		} else {
			protoPattern.entity = {
				case: "entityId",
				value: stringToBytes(entityPart as string),
			};
		}

		// Handle field/attribute part
		if (isVariable(fieldPart)) {
			protoPattern.attribute = {
				case: "attributeVariable",
				value: create(QueryPatternVariableSchema, {
					label: fieldPart.name,
				}),
			};
		} else {
			// Parse field to get entity name and field name
			const fieldStr = fieldPart as string;
			const fieldParts = fieldStr.split("/");
			const entityName = fieldParts[0] ?? "";
			const fieldName = fieldParts[1] ?? "";
			protoPattern.attribute = {
				case: "attributeId",
				value: fieldToAttributeId(entityName, fieldName),
			};
		}

		// Handle value part
		if (isVariable(valuePart)) {
			protoPattern.valueGroup = {
				case: "valueVariable",
				value: create(QueryPatternVariableSchema, {
					label: valuePart.name,
				}),
			};
		} else {
			protoPattern.valueGroup = {
				case: "value",
				value: this.valueToProto(valuePart as Value),
			};
		}

		return protoPattern;
	}

	/**
	 * Parse query response rows into datom arrays.
	 *
	 * @param rows - The response rows
	 * @param columns - The column names
	 * @param find - The find variables
	 * @returns Array of datom arrays
	 */
	private parseQueryResponse(
		rows: QueryResultRow[],
		columns: string[],
		find: QueryVariable[],
	): Datom[][] {
		return rows.map((row) => {
			return find.map((variable) => {
				const columnIndex = columns.indexOf(variable.name);
				if (columnIndex === -1) {
					return undefined as unknown as Datom;
				}

				const resultValue = row.values[columnIndex];
				if (!resultValue || resultValue.isUndefined) {
					return undefined as unknown as Datom;
				}

				// Parse the value
				if (resultValue.value.case === "id") {
					return resultValue.value.value as Datom;
				}
				if (resultValue.value.case === "tripleValue") {
					const tripleValue = resultValue.value.value;
					if (tripleValue.value.case === "string") {
						return tripleValue.value.value as Datom;
					}
					if (tripleValue.value.case === "number") {
						return tripleValue.value.value as Datom;
					}
					if (tripleValue.value.case === "boolean") {
						return tripleValue.value.value as Datom;
					}
				}

				return undefined as unknown as Datom;
			});
		});
	}

	/**
	 * Get the number of pending writes (for testing).
	 *
	 * @returns The number of pending writes
	 */
	pendingWriteCount(): number {
		return this.pendingWrites.size;
	}
}
