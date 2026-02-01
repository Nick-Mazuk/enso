import { beforeEach, describe, expect, it } from "bun:test";
import { create } from "@bufbuild/protobuf";
import type { ClientMessage, ServerResponse } from "../../proto/protocol_pb.js";
import {
	QueryResultRowSchema,
	QueryResultValueSchema,
	ServerResponseSchema,
	TripleValueSchema,
} from "../../proto/protocol_pb.js";
import { NetworkStore } from "./network-store.js";
import { Field, Id, type StoreResult, Value, Variable } from "./types.js";

/** Pattern to validate 32-character hex IDs */
const HEX_ID_PATTERN = /^[0-9a-f]{32}$/;

/**
 * Mock Connection for testing NetworkStore.
 *
 * Pre-conditions: None
 * Post-conditions: Simulates connection behavior
 *
 * Invariants:
 * - isConnected() returns value set by setConnected()
 * - send() records payloads and returns configured response
 */
class MockConnection {
	private connected = false;
	private response: ServerResponse = create(ServerResponseSchema, {
		requestId: 1,
		status: { code: 0, message: "" },
		rows: [],
	});
	readonly sentPayloads: ClientMessage["payload"][] = [];

	/**
	 * Sets the connected state.
	 *
	 * Pre-conditions: None
	 * Post-conditions: isConnected() returns the new value
	 */
	setConnected(connected: boolean): void {
		this.connected = connected;
	}

	/**
	 * Configures the response to return from send().
	 *
	 * Pre-conditions: None
	 * Post-conditions: send() will return this response
	 */
	setResponse(response: ServerResponse): void {
		this.response = response;
	}

	/**
	 * Simulates sending a message to the server.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Payload is recorded and response is returned
	 */
	send(payload: ClientMessage["payload"]): Promise<ServerResponse> {
		this.sentPayloads.push(payload);
		return Promise.resolve(this.response);
	}

	/**
	 * Returns the connection state.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Returns current connected state
	 */
	isConnected(): boolean {
		return this.connected;
	}

	// Not used by NetworkStore but required by Connection interface
	connect(): Promise<void> {
		this.connected = true;
		return Promise.resolve();
	}

	close(): void {
		this.connected = false;
	}

	setSubscriptionHandler(): void {
		// Not used in tests
	}
}

/**
 * Mock Connection that returns error responses.
 *
 * Pre-conditions: None
 * Post-conditions: send() returns error status
 */
class MockConnectionWithError extends MockConnection {
	private readonly errorCode: number;
	private readonly errorMessage: string;

	constructor(code = 3, message = "Error from server") {
		super();
		this.setConnected(true);
		this.errorCode = code;
		this.errorMessage = message;
	}

	override send(payload: ClientMessage["payload"]): Promise<ServerResponse> {
		this.sentPayloads.push(payload);
		return Promise.resolve(
			create(ServerResponseSchema, {
				requestId: 1,
				status: { code: this.errorCode, message: this.errorMessage },
				rows: [],
			}),
		);
	}
}

/**
 * Mock Connection with delayed response for testing pending writes.
 *
 * Pre-conditions: None
 * Post-conditions: send() delays before returning
 */
class MockConnectionWithDelay extends MockConnection {
	private readonly delayMs: number;
	private resolvers: Array<() => void> = [];

	constructor(delayMs = 10) {
		super();
		this.setConnected(true);
		this.delayMs = delayMs;
	}

	override async send(
		payload: ClientMessage["payload"],
	): Promise<ServerResponse> {
		this.sentPayloads.push(payload);
		await new Promise<void>((resolve) => {
			this.resolvers.push(resolve);
			setTimeout(resolve, this.delayMs);
		});
		return create(ServerResponseSchema, {
			requestId: 1,
			status: { code: 0, message: "" },
			rows: [],
		});
	}

	/**
	 * Resolves all pending send() calls immediately.
	 */
	resolveAll(): void {
		for (const resolve of this.resolvers) {
			resolve();
		}
		this.resolvers = [];
	}
}

/** Extracts data from a StoreResult, throws if not successful */
const unwrap = <T>(result: StoreResult<T>): T => {
	if (!result.success) throw new Error(result.error);
	return result.data;
};

/** Creates a valid 32-character hex entity ID from a numeric index */
const createTestId = (index: number): string => {
	const hexIndex = index.toString(16).padStart(8, "0");
	return "0".repeat(24) + hexIndex;
};

describe("NetworkStore", () => {
	describe("constructor", () => {
		it("throws assertion if connection is not connected", () => {
			const mockConnection = new MockConnection();
			// Don't set connected

			expect(
				// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
				() => new NetworkStore(mockConnection as any, "users"),
			).toThrow("Connection must be connected");
		});

		it("creates successfully when connection is connected", () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);

			expect(
				// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
				() => new NetworkStore(mockConnection as any, "users"),
			).not.toThrow();
		});
	});

	describe("add()", () => {
		let mockConnection: MockConnection;
		let store: NetworkStore;

		beforeEach(() => {
			mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			store = new NetworkStore(mockConnection as any, "users");
		});

		it("returns success for empty array", async () => {
			const result = await store.add();

			expect(result.success).toBe(true);
			expect(result.success && result.data).toBeUndefined();
			expect(mockConnection.sentPayloads.length).toBe(0);
		});

		it("sends triple update request to connection", async () => {
			const id = createTestId(1);
			const result = await store.add([
				Id(id),
				Field("users/name"),
				Value("John"),
			]);

			expect(result.success).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(1);
			expect(mockConnection.sentPayloads[0]?.case).toBe("tripleUpdateRequest");
		});

		it("sends multiple triples in one request", async () => {
			const id1 = createTestId(2);
			const id2 = createTestId(3);

			const result = await store.add(
				[Id(id1), Field("users/name"), Value("John")],
				[Id(id2), Field("users/name"), Value("Jane")],
			);

			expect(result.success).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(1);

			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				expect(payload.value.triples.length).toBe(2);
			}
		});

		it("converts string values correctly", async () => {
			const id = createTestId(4);
			const result = await store.add([
				Id(id),
				Field("users/name"),
				Value("Hello World"),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				const triple = payload.value.triples[0];
				expect(triple?.value?.value.case).toBe("string");
				expect(triple?.value?.value.value).toBe("Hello World");
			}
		});

		it("converts number values correctly", async () => {
			const id = createTestId(5);
			const result = await store.add([Id(id), Field("users/age"), Value(42)]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				const triple = payload.value.triples[0];
				expect(triple?.value?.value.case).toBe("number");
				expect(triple?.value?.value.value).toBe(42);
			}
		});

		it("converts boolean values correctly", async () => {
			const id = createTestId(6);
			const result = await store.add([
				Id(id),
				Field("users/active"),
				Value(true),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				const triple = payload.value.triples[0];
				expect(triple?.value?.value.case).toBe("boolean");
				expect(triple?.value?.value.value).toBe(true);
			}
		});

		it("returns error for unsupported value type", async () => {
			const id = createTestId(7);
			const result = await store.add([
				Id(id),
				Field("users/data"),
				// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
				{} as any,
			]);

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("Unsupported value type");
			}
		});

		it("handles server error response", async () => {
			const errorConnection = new MockConnectionWithError(3, "Test error");
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const errorStore = new NetworkStore(errorConnection as any, "users");

			const id = createTestId(8);
			const result = await errorStore.add([
				Id(id),
				Field("users/name"),
				Value("John"),
			]);

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("add failed");
				expect(result.error).toContain("Test error");
			}
		});
	});

	describe("pendingWriteCount()", () => {
		it("returns 0 when no pending writes", () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			expect(store.pendingWriteCount()).toBe(0);
		});

		it("tracks pending writes during add operation", async () => {
			const delayConnection = new MockConnectionWithDelay(50);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(delayConnection as any, "users");

			const id = createTestId(9);
			const addPromise = store.add([
				Id(id),
				Field("users/name"),
				Value("John"),
			]);

			// Check pending count while add is in progress
			expect(store.pendingWriteCount()).toBe(1);

			delayConnection.resolveAll();
			await addPromise;

			// After completion, pending count should be 0
			expect(store.pendingWriteCount()).toBe(0);
		});

		it("cleans up pending writes on error", async () => {
			const errorConnection = new MockConnectionWithError();
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(errorConnection as any, "users");

			const id = createTestId(10);
			await store.add([Id(id), Field("users/name"), Value("John")]);

			// Even though add() failed, pending write should be cleaned up
			expect(store.pendingWriteCount()).toBe(0);
		});

		it("tracks multiple pending writes correctly", async () => {
			const delayConnection = new MockConnectionWithDelay(50);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(delayConnection as any, "users");

			const promises = [
				store.add([Id(createTestId(11)), Field("users/name"), Value("A")]),
				store.add([Id(createTestId(12)), Field("users/name"), Value("B")]),
				store.add([Id(createTestId(13)), Field("users/name"), Value("C")]),
			];

			expect(store.pendingWriteCount()).toBe(3);

			delayConnection.resolveAll();
			await Promise.all(promises);

			expect(store.pendingWriteCount()).toBe(0);
		});
	});

	describe("query()", () => {
		let mockConnection: MockConnection;
		let store: NetworkStore;

		beforeEach(() => {
			mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			store = new NetworkStore(mockConnection as any, "users");
		});

		it("rejects queries with filters", async () => {
			const result = await store.query({
				find: [Variable("name")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
				filters: [
					{
						selector: Variable("name"),
						filter: () => true,
					},
				],
			});

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("Complex filters are not implemented");
			}
		});

		it("sends query request to connection", async () => {
			const result = await store.query({
				find: [Variable("name")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
			});

			expect(result.success).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(1);
			expect(mockConnection.sentPayloads[0]?.case).toBe("query");
		});

		it("handles query with variables in entity position", async () => {
			const result = await store.query({
				find: [Variable("id")],
				where: [[Variable("id"), Field("users/name"), Value("John")]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.where[0]?.entity.case).toBe("entityVariable");
			}
		});

		it("handles query with variables in field position", async () => {
			const idHex = createTestId(14);
			const result = await store.query({
				find: [Variable("field")],
				where: [[Id(idHex), Variable("field"), Value("test")]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.where[0]?.attribute.case).toBe(
					"attributeVariable",
				);
			}
		});

		it("handles query with variables in value position", async () => {
			const idHex = createTestId(15);
			const result = await store.query({
				find: [Variable("value")],
				where: [[Id(idHex), Field("users/name"), Variable("value")]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.where[0]?.valueGroup.case).toBe("valueVariable");
			}
		});

		it("handles query with concrete entity ID", async () => {
			const idHex = createTestId(16);
			const result = await store.query({
				find: [Variable("name")],
				where: [[Id(idHex), Field("users/name"), Variable("name")]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.where[0]?.entity.case).toBe("entityId");
			}
		});

		it("handles query with optional patterns", async () => {
			const result = await store.query({
				find: [Variable("id"), Variable("name"), Variable("age")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
				optional: [[Variable("id"), Field("users/age"), Variable("age")]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.optional.length).toBe(1);
			}
		});

		it("handles query with whereNot patterns", async () => {
			const result = await store.query({
				find: [Variable("name")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
				whereNot: [[Variable("id"), Field("users/deleted"), Value(true)]],
			});

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "query") {
				expect(payload.value.whereNot.length).toBe(1);
			}
		});

		it("handles server error response", async () => {
			const errorConnection = new MockConnectionWithError(3, "Query error");
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const errorStore = new NetworkStore(errorConnection as any, "users");

			const result = await errorStore.query({
				find: [Variable("name")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
			});

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("query failed");
				expect(result.error).toContain("Query error");
			}
		});
	});

	describe("query result conversion", () => {
		it("converts string triple values", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [
						create(QueryResultRowSchema, {
							values: [
								create(QueryResultValueSchema, {
									value: {
										case: "tripleValue",
										value: create(TripleValueSchema, {
											value: { case: "string", value: "John Doe" },
										}),
									},
								}),
							],
						}),
					],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.query({
				find: [Variable("name")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
			});

			expect(result.success).toBe(true);
			const data = unwrap(result);
			expect(data[0]?.[0] as unknown).toEqual("John Doe");
		});

		it("converts number triple values", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [
						create(QueryResultRowSchema, {
							values: [
								create(QueryResultValueSchema, {
									value: {
										case: "tripleValue",
										value: create(TripleValueSchema, {
											value: { case: "number", value: 42 },
										}),
									},
								}),
							],
						}),
					],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.query({
				find: [Variable("age")],
				where: [[Variable("id"), Field("users/age"), Variable("age")]],
			});

			expect(result.success).toBe(true);
			const data = unwrap(result);
			expect(data[0]?.[0] as unknown).toEqual(42);
		});

		it("converts boolean triple values", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [
						create(QueryResultRowSchema, {
							values: [
								create(QueryResultValueSchema, {
									value: {
										case: "tripleValue",
										value: create(TripleValueSchema, {
											value: { case: "boolean", value: true },
										}),
									},
								}),
							],
						}),
					],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.query({
				find: [Variable("active")],
				where: [[Variable("id"), Field("users/active"), Variable("active")]],
			});

			expect(result.success).toBe(true);
			const data = unwrap(result);
			expect(data[0]?.[0] as unknown).toEqual(true);
		});

		it("handles undefined values in optional results", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [
						create(QueryResultRowSchema, {
							values: [
								create(QueryResultValueSchema, {
									value: {
										case: "tripleValue",
										value: create(TripleValueSchema, {
											value: { case: "string", value: "John" },
										}),
									},
								}),
								create(QueryResultValueSchema, {
									isUndefined: true,
								}),
							],
						}),
					],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.query({
				find: [Variable("name"), Variable("age")],
				where: [[Variable("id"), Field("users/name"), Variable("name")]],
				optional: [[Variable("id"), Field("users/age"), Variable("age")]],
			});

			expect(result.success).toBe(true);
			const data = unwrap(result);
			expect(data[0]?.[0] as unknown).toEqual("John");
			expect(data[0]?.[1]).toBeUndefined();
		});

		it("converts ID values from bytes to hex", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			const idHex = createTestId(17);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [
						create(QueryResultRowSchema, {
							values: [
								create(QueryResultValueSchema, {
									value: { case: "id", value: idHex },
								}),
							],
						}),
					],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.query({
				find: [Variable("id")],
				where: [[Variable("id"), Field("users/name"), Value("John")]],
			});

			expect(result.success).toBe(true);
			const data = unwrap(result);
			expect(data[0]?.[0] as unknown).toEqual(idHex);
		});
	});

	describe("deleteAllById()", () => {
		it("returns success when entity has no triples", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// Query returns empty rows
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const idHex = createTestId(18);
			const result = await store.deleteAllById(Id(idHex));

			expect(result.success).toBe(true);
			// Should only have query call, no update since no triples found
			expect(mockConnection.sentPayloads.length).toBe(1);
			expect(mockConnection.sentPayloads[0]?.case).toBe("query");
		});

		it("queries for entity triples then sends delete", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			const idHex = createTestId(19);
			const fieldHex = createTestId(20);

			// First response: query returns one triple
			const queryResponse = create(ServerResponseSchema, {
				requestId: 1,
				status: { code: 0, message: "" },
				rows: [
					create(QueryResultRowSchema, {
						values: [
							create(QueryResultValueSchema, {
								value: { case: "id", value: idHex },
							}),
							create(QueryResultValueSchema, {
								value: { case: "id", value: fieldHex },
							}),
						],
					}),
				],
			});

			// Second response: update succeeds
			const updateResponse = create(ServerResponseSchema, {
				requestId: 2,
				status: { code: 0, message: "" },
				rows: [],
			});

			let callCount = 0;
			mockConnection.send = (payload) => {
				mockConnection.sentPayloads.push(payload);
				callCount++;
				return Promise.resolve(
					callCount === 1 ? queryResponse : updateResponse,
				);
			};

			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.deleteAllById(Id(idHex));

			expect(result.success).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(2);
			expect(mockConnection.sentPayloads[0]?.case).toBe("query");
			expect(mockConnection.sentPayloads[1]?.case).toBe("tripleUpdateRequest");
		});

		it("handles query failure during delete", async () => {
			const errorConnection = new MockConnectionWithError(3, "Query failed");
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(errorConnection as any, "users");

			const idHex = createTestId(21);
			const result = await store.deleteAllById(Id(idHex));

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("queryEntityTriples failed");
			}
		});

		it("handles update failure during delete", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			const idHex = createTestId(22);
			const fieldHex = createTestId(23);

			// First response: query returns one triple
			const queryResponse = create(ServerResponseSchema, {
				requestId: 1,
				status: { code: 0, message: "" },
				rows: [
					create(QueryResultRowSchema, {
						values: [
							create(QueryResultValueSchema, {
								value: { case: "id", value: idHex },
							}),
							create(QueryResultValueSchema, {
								value: { case: "id", value: fieldHex },
							}),
						],
					}),
				],
			});

			// Second response: update fails
			const updateResponse = create(ServerResponseSchema, {
				requestId: 2,
				status: { code: 3, message: "Update failed" },
				rows: [],
			});

			let callCount = 0;
			mockConnection.send = (payload) => {
				mockConnection.sentPayloads.push(payload);
				callCount++;
				return Promise.resolve(
					callCount === 1 ? queryResponse : updateResponse,
				);
			};

			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.deleteAllById(Id(idHex));

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("delete failed");
				expect(result.error).toContain("Update failed");
			}
		});
	});

	describe("generateId()", () => {
		it("returns 32-character hex string", () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const id = store.generateId();

			expect(typeof id).toBe("string");
			expect((id as string).length).toBe(32);
			expect(id as string).toMatch(HEX_ID_PATTERN);
		});

		it("generates unique IDs", () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const ids = new Set<string>();
			for (let i = 0; i < 100; i++) {
				ids.add(store.generateId() as string);
			}

			expect(ids.size).toBe(100);
		});
	});

	describe("field parsing", () => {
		let mockConnection: MockConnection;
		let store: NetworkStore;

		beforeEach(() => {
			mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			store = new NetworkStore(mockConnection as any, "users");
		});

		it("parses entityName/fieldName format", async () => {
			const id = createTestId(24);
			const result = await store.add([
				Id(id),
				Field("users/email"),
				Value("test@example.com"),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				// The attribute should be a deterministic hash of "users/email"
				const attributeId = payload.value.triples[0]?.attributeId;
				expect(attributeId).toBeDefined();
				expect(attributeId?.length).toBe(16); // 16 bytes
			}
		});

		it("parses 32-character hex string as attribute ID", async () => {
			const id = createTestId(25);
			const hexAttributeId = "a".repeat(32); // 32 hex chars = 16 bytes
			const result = await store.add([
				Id(id),
				Field(hexAttributeId),
				Value("test"),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				// Should use the hex directly as bytes
				expect(payload.value.triples[0]?.attributeId).toBeDefined();
			}
		});

		it("handles 0x prefix on hex attribute IDs", async () => {
			const id = createTestId(26);
			const hexAttributeId = `0x${"b".repeat(32)}`;
			const result = await store.add([
				Id(id),
				Field(hexAttributeId),
				Value("test"),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				expect(payload.value.triples[0]?.attributeId).toBeDefined();
			}
		});

		it("hashes field without slash using entityName", async () => {
			const id = createTestId(27);
			const result = await store.add([
				Id(id),
				Field("simplename"), // No slash, should use entityName
				Value("test"),
			]);

			expect(result.success).toBe(true);
			const payload = mockConnection.sentPayloads[0];
			if (payload?.case === "tripleUpdateRequest") {
				// The attribute should be a hash of "users/simplename"
				const attributeId = payload.value.triples[0]?.attributeId;
				expect(attributeId).toBeDefined();
				expect(attributeId?.length).toBe(16);
			}
		});
	});

	describe("concurrent operations", () => {
		it("handles multiple concurrent add calls", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const results = await Promise.all([
				store.add([Id(createTestId(28)), Field("users/name"), Value("A")]),
				store.add([Id(createTestId(29)), Field("users/name"), Value("B")]),
				store.add([Id(createTestId(30)), Field("users/name"), Value("C")]),
			]);

			expect(results.every((r) => r.success)).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(3);
		});

		it("handles concurrent query calls", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const results = await Promise.all([
				store.query({
					find: [Variable("name")],
					where: [[Variable("id"), Field("users/name"), Variable("name")]],
				}),
				store.query({
					find: [Variable("age")],
					where: [[Variable("id"), Field("users/age"), Variable("age")]],
				}),
			]);

			expect(results.every((r) => r.success)).toBe(true);
			expect(mockConnection.sentPayloads.length).toBe(2);
		});
	});

	describe("server response handling", () => {
		it("handles success response with code 0", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 0, message: "" },
					rows: [],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.add([
				Id(createTestId(31)),
				Field("users/name"),
				Value("Test"),
			]);

			expect(result.success).toBe(true);
		});

		it("handles error response with non-zero code", async () => {
			const mockConnection = new MockConnection();
			mockConnection.setConnected(true);
			mockConnection.setResponse(
				create(ServerResponseSchema, {
					requestId: 1,
					status: { code: 5, message: "Not found" },
					rows: [],
				}),
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(mockConnection as any, "users");

			const result = await store.add([
				Id(createTestId(32)),
				Field("users/name"),
				Value("Test"),
			]);

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("code: 5");
			}
		});

		it("includes error message in result", async () => {
			const errorConnection = new MockConnectionWithError(
				13,
				"Internal server error",
			);
			// biome-ignore lint/suspicious/noExplicitAny: testing type boundaries
			const store = new NetworkStore(errorConnection as any, "users");

			const result = await store.add([
				Id(createTestId(33)),
				Field("users/name"),
				Value("Test"),
			]);

			expect(result.success).toBe(false);
			if (!result.success) {
				expect(result.error).toContain("Internal server error");
				expect(result.error).toContain("code: 13");
			}
		});
	});
});
