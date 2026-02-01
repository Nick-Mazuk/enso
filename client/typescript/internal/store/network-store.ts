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
	type StoreResult,
	type Triple,
	type Value,
	Variable,
} from "./types.js";

/** Pattern to validate hex strings */
const HEX_PATTERN = /^[0-9a-fA-F]+$/;

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
	async add(...triples: Triple[]): Promise<StoreResult<void>> {
		if (triples.length === 0) return { success: true, data: undefined };

		const hlc = this.hlcClock.now();
		const protoTriples: ProtoTriple[] = [];
		for (const triple of triples) {
			const result = this.convertTripleToProto(triple, hlc);
			if (!result.success) return result;
			protoTriples.push(result.data);
		}

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

			const responseResult = this.handleResponse(response, "add");
			if (!responseResult.success) return responseResult;
			return { success: true, data: undefined };
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
	): Promise<StoreResult<Datom[][]>> {
		// Check for unsupported filters
		if (query.filters && query.filters.length > 0) {
			return {
				success: false,
				error:
					"Complex filters are not implemented. Only equality filters are supported.",
			};
		}

		const protoQueryResult = this.convertQueryToProto(query);
		if (!protoQueryResult.success) return protoQueryResult;

		const response = await this.connection.send({
			case: "query",
			value: protoQueryResult.data,
		});

		const responseResult = this.handleResponse(response, "query");
		if (!responseResult.success) return responseResult;

		return {
			success: true,
			data: this.convertQueryResultsFromProto(response, query.find),
		};
	}

	/**
	 * Deletes all triples for an entity.
	 *
	 * Pre-conditions: id is a valid entity ID
	 * Post-conditions: Delete is sent to server
	 */
	async deleteAllById(id: Id): Promise<StoreResult<void>> {
		// To delete, we send triples with no value (tombstone)
		// First we need to know what fields exist for this entity
		// For now, we'll query for all triples with this ID and send deletes
		const queryEntityResult = await this.queryEntityTriples(id);
		if (!queryEntityResult.success) return queryEntityResult;

		if (queryEntityResult.data.length === 0) {
			return { success: true, data: undefined };
		}

		const hlc = this.hlcClock.now();
		const protoTriples = queryEntityResult.data.map(([entityId, field]) => {
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

		const responseResult = this.handleResponse(response, "delete");
		if (!responseResult.success) return responseResult;
		return { success: true, data: undefined };
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

	private convertTripleToProto(
		triple: Triple,
		hlc: HlcTimestamp,
	): { success: true; data: ProtoTriple } | { success: false; error: string } {
		const [id, field, value] = triple;

		// Convert ID to bytes
		const entityBytes = hexToBytes(id as string);

		// Convert field to attribute ID
		const attributeBytes = this.parseFieldToAttributeId(field as string);

		// Convert value
		const protoValueResult = this.convertValueToProto(value);
		if (!protoValueResult.success) {
			return protoValueResult;
		}

		return {
			success: true,
			data: create(TripleSchema, {
				entityId: entityBytes,
				attributeId: attributeBytes,
				value: protoValueResult.data,
				hlc,
			}),
		};
	}

	private parseFieldToAttributeId(field: string): Uint8Array {
		// Check if field is already a hex-formatted attribute ID
		// Handle optional "0x" prefix
		const hexString = field.startsWith("0x") ? field.slice(2) : field;
		// 16-byte IDs are 32 hex characters
		if (hexString.length === 32 && HEX_PATTERN.test(hexString)) {
			return hexToBytes(hexString);
		}

		// Field is in format "entityName/fieldName"
		const parts = field.split("/");
		if (parts.length === 2 && parts[0] && parts[1]) {
			return fieldToAttributeId(parts[0], parts[1]);
		}
		// If not in expected format, hash the whole field
		return fieldToAttributeId(this.entityName, field);
	}

	private convertValueToProto(
		value: Value,
	): { success: true; data: TripleValue } | { success: false; error: string } {
		const rawValue = value as string | number | boolean;

		if (typeof rawValue === "string") {
			return {
				success: true,
				data: create(TripleValueSchema, {
					value: { case: "string", value: rawValue },
				}),
			};
		}
		if (typeof rawValue === "number") {
			return {
				success: true,
				data: create(TripleValueSchema, {
					value: { case: "number", value: rawValue },
				}),
			};
		}
		if (typeof rawValue === "boolean") {
			return {
				success: true,
				data: create(TripleValueSchema, {
					value: { case: "boolean", value: rawValue },
				}),
			};
		}

		return {
			success: false,
			error: `Unsupported value type: ${typeof rawValue}`,
		};
	}

	private convertQueryToProto(
		query: Query<QueryVariable[]>,
	): { success: true; data: QueryRequest } | { success: false; error: string } {
		const find = query.find.map((v) =>
			create(QueryPatternVariableSchema, { label: v.name }),
		);

		const where: ProtoQueryPattern[] = [];
		for (const p of query.where) {
			const result = this.convertPatternToProto(p);
			if (!result.success) return result;
			where.push(result.data);
		}

		const optional: ProtoQueryPattern[] = [];
		for (const p of query.optional ?? []) {
			const result = this.convertPatternToProto(p);
			if (!result.success) return result;
			optional.push(result.data);
		}

		const whereNot: ProtoQueryPattern[] = [];
		for (const p of query.whereNot ?? []) {
			const result = this.convertPatternToProto(p);
			if (!result.success) return result;
			whereNot.push(result.data);
		}

		return {
			success: true,
			data: create(QueryRequestSchema, {
				find,
				where,
				optional,
				whereNot,
			}),
		};
	}

	private convertPatternToProto(
		pattern: QueryPattern,
	):
		| { success: true; data: ProtoQueryPattern }
		| { success: false; error: string } {
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
			const valueResult = this.convertValueToProto(valuePart);
			if (!valueResult.success) {
				return valueResult;
			}
			protoPattern.valueGroup = {
				case: "value",
				value: valueResult.data,
			};
		}

		return { success: true, data: protoPattern };
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
					case "id": {
						const idValue = protoValue.value.value;
						// Convert Uint8Array bytes to hex string if needed
						if (
							typeof idValue === "object" &&
							idValue !== null &&
							"buffer" in idValue
						) {
							rowData.push(bytesToHex(idValue as Uint8Array) as Datom);
						} else {
							rowData.push(idValue as Datom);
						}
						break;
					}
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

	private async queryEntityTriples(
		id: Id,
	): Promise<
		| { success: true; data: [Datom, Datom][] }
		| { success: false; error: string }
	> {
		// Query for all triples with this entity ID
		const entityVar = Variable("entity");
		const fieldVar = Variable("field");
		const valueVar = Variable("value");

		const query: Query<QueryVariable[]> = {
			find: [entityVar, fieldVar],
			where: [[id, fieldVar, valueVar]],
		};

		const protoQueryResult = this.convertQueryToProto(query);
		if (!protoQueryResult.success) return protoQueryResult;

		const response = await this.connection.send({
			case: "query",
			value: protoQueryResult.data,
		});

		const responseResult = this.handleResponse(response, "queryEntityTriples");
		if (!responseResult.success) return responseResult;

		return {
			success: true,
			data: this.convertQueryResultsFromProto(response, [
				entityVar,
				fieldVar,
			]) as [Datom, Datom][],
		};
	}

	private handleResponse(
		response: ServerResponse,
		operation: string,
	): { success: true } | { success: false; error: string } {
		if (response.status && response.status.code !== 0) {
			return {
				success: false,
				error: `${operation} failed: ${response.status.message || "Unknown error"} (code: ${response.status.code})`,
			};
		}
		return { success: true };
	}
}
