/**
 * Network-backed Store
 *
 * Replaces the in-memory Store with a server-backed implementation.
 * All queries go to the server. Writes are sent to the server with
 * optimistic tracking for pending operations.
 *
 * Pre-conditions:
 * - Connection must be established before any operations
 *
 * Post-conditions:
 * - All operations communicate with the server
 * - Query results come from the server
 *
 * Invariants:
 * - Pending writes are tracked until server confirms
 */

import { create } from "@bufbuild/protobuf";

import { assert } from "../../../shared/assert.js";
import type {
	HlcTimestamp,
	QueryPattern as ProtoQueryPattern,
	Triple as ProtoTriple,
	QueryRequest,
	ServerResponse,
	TripleValue,
} from "../../proto/protocol_pb.js";
import {
	QueryPatternSchema,
	QueryPatternVariableSchema,
	QueryRequestSchema,
	TripleSchema,
	TripleUpdateRequestSchema,
	TripleValueSchema,
} from "../../proto/protocol_pb.js";
import type { Connection } from "../connection/index.js";
import { HlcClock } from "../hlc/index.js";
import {
	bytesToHex,
	fieldToAttributeId,
	generateEntityId,
	hexToBytes,
} from "../id/index.js";
import {
	type Datom,
	type Id,
	isVariable,
	type Query,
	type QueryPattern,
	type QueryVariable,
	type StoreInterface,
	type Triple,
	type Value,
} from "./types.js";

/** Pending write operation */
interface PendingWrite {
	triples: Triple[];
	hlc: HlcTimestamp;
}

/**
 * Network-backed store that sends all operations to the server.
 *
 * Implements StoreInterface for compatibility with in-memory Store.
 *
 * Invariants:
 * - Connection is valid and connected
 * - HLC clock generates monotonically increasing timestamps
 */
export class NetworkStore implements StoreInterface {
	private readonly connection: Connection;
	private readonly hlcClock: HlcClock;
	private readonly entityName: string;
	private pendingWrites: Map<string, PendingWrite> = new Map();

	/**
	 * Creates a new NetworkStore.
	 *
	 * Pre-conditions: connection is valid and connected
	 * Post-conditions: Store is ready for operations
	 *
	 * @param connection - The server connection
	 * @param entityName - The entity type name (e.g., "users")
	 */
	constructor(connection: Connection, entityName: string) {
		assert(connection.isConnected(), "Connection must be connected");
		this.connection = connection;
		this.entityName = entityName;
		this.hlcClock = new HlcClock();
	}

	/**
	 * Adds triples to the server.
	 *
	 * Pre-conditions: Triples are valid
	 * Post-conditions: Triples are sent to server (async)
	 */
	async add(...triples: Triple[]): Promise<void> {
		if (triples.length === 0) return;

		const hlc = this.hlcClock.now();
		const protoTriples = triples.map((triple) =>
			this.convertTripleToProto(triple, hlc),
		);

		const updateRequest = create(TripleUpdateRequestSchema, {
			triples: protoTriples,
		});

		// Track pending write
		const writeId = crypto.randomUUID();
		this.pendingWrites.set(writeId, { triples, hlc });

		try {
			const response = await this.connection.send({
				case: "tripleUpdateRequest",
				value: updateRequest,
			});

			this.handleResponse(response, "add");
		} finally {
			this.pendingWrites.delete(writeId);
		}
	}

	/**
	 * Queries the server.
	 *
	 * Pre-conditions: Query is valid
	 * Post-conditions: Returns results from server
	 */
	async query<Find extends QueryVariable[]>(
		query: Query<Find>,
	): Promise<Datom[][]> {
		// Check for unsupported filters
		if (query.filters && query.filters.length > 0) {
			throw new Error(
				"Complex filters are not implemented. Only equality filters are supported.",
			);
		}

		const protoQuery = this.convertQueryToProto(query);

		const response = await this.connection.send({
			case: "query",
			value: protoQuery,
		});

		this.handleResponse(response, "query");

		return this.convertQueryResultsFromProto(response, query.find);
	}

	/**
	 * Deletes all triples for an entity.
	 *
	 * Pre-conditions: id is a valid entity ID
	 * Post-conditions: Delete is sent to server
	 */
	async deleteAllById(id: Id): Promise<void> {
		// To delete, we send triples with no value (tombstone)
		// First we need to know what fields exist for this entity
		// For now, we'll query for all triples with this ID and send deletes
		const queryResult = await this.queryEntityTriples(id);

		if (queryResult.length === 0) return;

		const hlc = this.hlcClock.now();
		const protoTriples = queryResult.map(([entityId, field]) => {
			const entityBytes = hexToBytes(entityId as string);
			const attributeBytes = this.parseFieldToAttributeId(field as string);

			return create(TripleSchema, {
				entityId: entityBytes,
				attributeId: attributeBytes,
				// No value = delete
				hlc,
			});
		});

		const updateRequest = create(TripleUpdateRequestSchema, {
			triples: protoTriples,
		});

		const response = await this.connection.send({
			case: "tripleUpdateRequest",
			value: updateRequest,
		});

		this.handleResponse(response, "delete");
	}

	/**
	 * Generates a new entity ID.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Returns a unique 16-byte ID as hex string
	 */
	generateId(): Id {
		const bytes = generateEntityId();
		return bytesToHex(bytes) as unknown as Id;
	}

	/**
	 * Returns the number of pending writes.
	 */
	pendingWriteCount(): number {
		return this.pendingWrites.size;
	}

	private convertTripleToProto(triple: Triple, hlc: HlcTimestamp): ProtoTriple {
		const [id, field, value] = triple;

		// Convert ID to bytes
		const entityBytes = hexToBytes(id as string);

		// Convert field to attribute ID
		const attributeBytes = this.parseFieldToAttributeId(field as string);

		// Convert value
		const protoValue = this.convertValueToProto(value);

		return create(TripleSchema, {
			entityId: entityBytes,
			attributeId: attributeBytes,
			value: protoValue,
			hlc,
		});
	}

	private parseFieldToAttributeId(field: string): Uint8Array {
		// Field is in format "entityName/fieldName"
		const parts = field.split("/");
		if (parts.length === 2 && parts[0] && parts[1]) {
			return fieldToAttributeId(parts[0], parts[1]);
		}
		// If not in expected format, hash the whole field
		return fieldToAttributeId(this.entityName, field);
	}

	private convertValueToProto(value: Value): TripleValue {
		const rawValue = value as string | number | boolean;

		if (typeof rawValue === "string") {
			return create(TripleValueSchema, {
				value: { case: "string", value: rawValue },
			});
		}
		if (typeof rawValue === "number") {
			return create(TripleValueSchema, {
				value: { case: "number", value: rawValue },
			});
		}
		if (typeof rawValue === "boolean") {
			return create(TripleValueSchema, {
				value: { case: "boolean", value: rawValue },
			});
		}

		throw new Error(`Unsupported value type: ${typeof rawValue}`);
	}

	private convertQueryToProto(query: Query<QueryVariable[]>): QueryRequest {
		const find = query.find.map((v) =>
			create(QueryPatternVariableSchema, { label: v.name }),
		);

		const where = query.where.map((p) => this.convertPatternToProto(p));
		const optional = (query.optional ?? []).map((p) =>
			this.convertPatternToProto(p),
		);
		const whereNot = (query.whereNot ?? []).map((p) =>
			this.convertPatternToProto(p),
		);

		return create(QueryRequestSchema, {
			find,
			where,
			optional,
			whereNot,
		});
	}

	private convertPatternToProto(pattern: QueryPattern): ProtoQueryPattern {
		const [entityPart, fieldPart, valuePart] = pattern;

		const protoPattern = create(QueryPatternSchema, {});

		// Entity
		if (isVariable(entityPart)) {
			protoPattern.entity = {
				case: "entityVariable",
				value: create(QueryPatternVariableSchema, { label: entityPart.name }),
			};
		} else {
			protoPattern.entity = {
				case: "entityId",
				value: hexToBytes(entityPart as string),
			};
		}

		// Attribute (field)
		if (isVariable(fieldPart)) {
			protoPattern.attribute = {
				case: "attributeVariable",
				value: create(QueryPatternVariableSchema, { label: fieldPart.name }),
			};
		} else {
			protoPattern.attribute = {
				case: "attributeId",
				value: this.parseFieldToAttributeId(fieldPart as string),
			};
		}

		// Value
		if (isVariable(valuePart)) {
			protoPattern.valueGroup = {
				case: "valueVariable",
				value: create(QueryPatternVariableSchema, { label: valuePart.name }),
			};
		} else {
			protoPattern.valueGroup = {
				case: "value",
				value: this.convertValueToProto(valuePart),
			};
		}

		return protoPattern;
	}

	private convertQueryResultsFromProto(
		response: ServerResponse,
		find: QueryVariable[],
	): Datom[][] {
		const results: Datom[][] = [];

		for (const row of response.rows) {
			const rowData: Datom[] = [];
			for (let i = 0; i < row.values.length && i < find.length; i++) {
				const protoValue = row.values[i];
				if (!protoValue) continue;

				if (protoValue.isUndefined) {
					// For optional patterns, undefined means no value
					rowData.push(undefined as unknown as Datom);
					continue;
				}

				switch (protoValue.value.case) {
					case "id":
						rowData.push(protoValue.value.value as Datom);
						break;
					case "tripleValue": {
						const tv = protoValue.value.value;
						switch (tv.value.case) {
							case "string":
								rowData.push(tv.value.value as Datom);
								break;
							case "number":
								rowData.push(tv.value.value as Datom);
								break;
							case "boolean":
								rowData.push(tv.value.value as Datom);
								break;
							default:
								rowData.push(undefined as unknown as Datom);
						}
						break;
					}
					default:
						rowData.push(undefined as unknown as Datom);
				}
			}
			results.push(rowData);
		}

		return results;
	}

	private async queryEntityTriples(id: Id): Promise<[Datom, Datom][]> {
		// Query for all triples with this entity ID
		const entityVar = {
			name: "entity",
			__brand: Symbol("QueryVariable"),
		} as QueryVariable;
		const fieldVar = {
			name: "field",
			__brand: Symbol("QueryVariable"),
		} as QueryVariable;
		const valueVar = {
			name: "value",
			__brand: Symbol("QueryVariable"),
		} as QueryVariable;

		const query: Query<QueryVariable[]> = {
			find: [entityVar, fieldVar],
			where: [[id, fieldVar, valueVar]],
		};

		const protoQuery = this.convertQueryToProto(query);

		const response = await this.connection.send({
			case: "query",
			value: protoQuery,
		});

		this.handleResponse(response, "queryEntityTriples");

		return this.convertQueryResultsFromProto(response, [
			entityVar,
			fieldVar,
		]) as [Datom, Datom][];
	}

	private handleResponse(response: ServerResponse, operation: string): void {
		if (response.status && response.status.code !== 0) {
			throw new Error(
				`${operation} failed: ${response.status.message || "Unknown error"} (code: ${response.status.code})`,
			);
		}
	}
}
