import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import {
	ServerMessageSchema,
	ServerResponseSchema,
	SubscriptionUpdateSchema,
} from "../../proto/protocol_pb.js";
import { Connection } from "./index.js";

// Mock WebSocket
class MockWebSocket {
	static instances: MockWebSocket[] = [];
	binaryType: string = "arraybuffer";
	onopen: (() => void) | null = null;
	onmessage: ((event: MessageEvent) => void) | null = null;
	onerror: ((event: Event) => void) | null = null;
	onclose: (() => void) | null = null;
	readyState: number = 0; // CONNECTING
	sentMessages: Uint8Array[] = [];

	constructor(_url: string) {
		MockWebSocket.instances.push(this);
	}

	send(data: Uint8Array) {
		this.sentMessages.push(data);
	}

	close() {
		this.readyState = 3; // CLOSED
		if (this.onclose) {
			this.onclose();
		}
	}

	// Test helper: simulate connection open
	simulateOpen() {
		this.readyState = 1; // OPEN
		if (this.onopen) {
			this.onopen();
		}
	}

	// Test helper: simulate receiving a message
	simulateMessage(data: Uint8Array) {
		if (this.onmessage) {
			const event = { data: data.buffer } as MessageEvent;
			this.onmessage(event);
		}
	}

	// Test helper: simulate error
	simulateError() {
		if (this.onerror) {
			this.onerror(new Event("error"));
		}
	}
}

describe("Connection", () => {
	let originalWebSocket: typeof WebSocket;

	beforeEach(() => {
		originalWebSocket = globalThis.WebSocket;
		// @ts-expect-error - Mocking WebSocket
		globalThis.WebSocket = MockWebSocket;
		MockWebSocket.instances = [];
	});

	afterEach(() => {
		globalThis.WebSocket = originalWebSocket;
	});

	it("creates a connection with disconnected state", () => {
		const connection = new Connection("ws://localhost:8080", "test-key");
		expect(connection.getState()).toBe("disconnected");
	});

	it("transitions to connecting state on connect", () => {
		const connection = new Connection("ws://localhost:8080", "test-key");
		connection.connect();
		expect(connection.getState()).toBe("connecting");
	});

	it("sends ConnectRequest on WebSocket open", async () => {
		const connection = new Connection("ws://localhost:8080", "test-key");
		const connectPromise = connection.connect();

		// Wait for WebSocket to be created
		await new Promise((resolve) => setTimeout(resolve, 0));

		const ws = MockWebSocket.instances[0];
		expect(ws).toBeDefined();

		// Simulate WebSocket opening
		ws?.simulateOpen();

		// Wait for ConnectRequest to be sent
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(ws?.sentMessages.length).toBe(1);

		// Simulate server response
		const response = create(ServerResponseSchema, {
			requestId: 1,
		});
		const serverMessage = create(ServerMessageSchema, {
			payload: {
				case: "response",
				value: response,
			},
		});
		ws?.simulateMessage(toBinary(ServerMessageSchema, serverMessage));

		await connectPromise;
		expect(connection.getState()).toBe("connected");
	});

	it("rejects pending requests on connection close", async () => {
		const connection = new Connection("ws://localhost:8080", "test-key");
		const connectPromise = connection.connect();

		await new Promise((resolve) => setTimeout(resolve, 0));
		const ws = MockWebSocket.instances[0];
		if (!ws) throw new Error("WebSocket not created");
		ws.simulateOpen();

		await new Promise((resolve) => setTimeout(resolve, 0));

		// Simulate server response for connect
		const response = create(ServerResponseSchema, { requestId: 1 });
		const serverMessage = create(ServerMessageSchema, {
			payload: { case: "response", value: response },
		});
		ws.simulateMessage(toBinary(ServerMessageSchema, serverMessage));

		await connectPromise;

		// Close the connection
		connection.close();
		expect(connection.getState()).toBe("disconnected");
	});

	it("routes subscription updates to handlers", async () => {
		const connection = new Connection("ws://localhost:8080", "test-key");
		const connectPromise = connection.connect();

		await new Promise((resolve) => setTimeout(resolve, 0));
		const ws = MockWebSocket.instances[0];
		if (!ws) throw new Error("WebSocket not created");
		ws.simulateOpen();

		await new Promise((resolve) => setTimeout(resolve, 0));

		// Respond to connect request
		const connectResponse = create(ServerResponseSchema, { requestId: 1 });
		ws.simulateMessage(
			toBinary(
				ServerMessageSchema,
				create(ServerMessageSchema, {
					payload: { case: "response", value: connectResponse },
				}),
			),
		);

		await connectPromise;

		// Set up subscription handler
		let receivedUpdate: unknown = null;
		const subscribePromise = connection.subscribe(42, (subscriptionUpdate) => {
			receivedUpdate = subscriptionUpdate;
		});

		await new Promise((resolve) => setTimeout(resolve, 0));

		// Respond to subscribe request
		const subscribeResponse = create(ServerResponseSchema, { requestId: 2 });
		ws.simulateMessage(
			toBinary(
				ServerMessageSchema,
				create(ServerMessageSchema, {
					payload: { case: "response", value: subscribeResponse },
				}),
			),
		);

		await subscribePromise;

		// Simulate subscription update
		const update = create(SubscriptionUpdateSchema, {
			subscriptionId: 42,
			changes: [],
		});
		ws.simulateMessage(
			toBinary(
				ServerMessageSchema,
				create(ServerMessageSchema, {
					payload: { case: "subscriptionUpdate", value: update },
				}),
			),
		);

		expect(receivedUpdate).not.toBeNull();
	});
});
