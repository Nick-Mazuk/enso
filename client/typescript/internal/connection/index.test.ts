import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import {
	QueryRequestSchema,
	ServerMessageSchema,
	ServerResponseSchema,
	SubscriptionUpdateSchema,
} from "../../proto/protocol_pb.js";
import { Connection } from "./index.js";
import { createApiKey, createServerUrl } from "./types.js";

/**
 * Mock WebSocket for testing.
 *
 * Simulates WebSocket behavior with manual control over events.
 */
class MockWebSocket {
	binaryType: string = "blob";
	onopen: (() => void) | null = null;
	onerror: ((event: Event) => void) | null = null;
	onclose: (() => void) | null = null;
	onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;

	sentMessages: Uint8Array[] = [];
	private closed = false;
	private pendingResponses: ArrayBuffer[] = [];
	private onSendCallback: (() => void) | null = null;

	send(data: ArrayBuffer | Uint8Array): void {
		if (this.closed) {
			throw new Error("WebSocket is closed");
		}
		this.sentMessages.push(new Uint8Array(data));

		// Send any pending responses
		if (this.pendingResponses.length > 0 && this.onmessage) {
			const response = this.pendingResponses.shift();
			if (response) {
				// Use queueMicrotask to simulate async response
				queueMicrotask(() => {
					if (this.onmessage) {
						this.onmessage({ data: response });
					}
				});
			}
		}

		if (this.onSendCallback) {
			this.onSendCallback();
		}
	}

	close(): void {
		this.closed = true;
		if (this.onclose) {
			this.onclose();
		}
	}

	// Test helpers
	simulateOpen(): void {
		if (this.onopen) {
			this.onopen();
		}
	}

	simulateError(): void {
		if (this.onerror) {
			this.onerror(new Event("error"));
		}
	}

	simulateMessage(data: ArrayBuffer): void {
		if (this.onmessage) {
			this.onmessage({ data });
		}
	}

	simulateClose(): void {
		if (this.onclose) {
			this.onclose();
		}
	}

	/** Queue a response to be sent when the next message is received */
	queueResponse(data: ArrayBuffer): void {
		this.pendingResponses.push(data);
	}

	/** Wait for the next send() call */
	waitForSend(): Promise<void> {
		return new Promise((resolve) => {
			this.onSendCallback = () => {
				this.onSendCallback = null;
				resolve();
			};
		});
	}
}

// Store the original WebSocket
const OriginalWebSocket = globalThis.WebSocket;

// Track created mock instances
let mockWebSocketInstance: MockWebSocket | null = null;

function installMockWebSocket(): void {
	// biome-ignore lint/suspicious/noExplicitAny: mocking global
	(globalThis as any).WebSocket = class extends MockWebSocket {
		constructor(_url: string) {
			super();
			mockWebSocketInstance = this;
		}
	};
}

function restoreWebSocket(): void {
	globalThis.WebSocket = OriginalWebSocket;
	mockWebSocketInstance = null;
}

function getMockWebSocket(): MockWebSocket {
	if (!mockWebSocketInstance) {
		throw new Error("MockWebSocket not created yet");
	}
	return mockWebSocketInstance;
}

function createServerResponse(requestId: number, statusCode = 0): ArrayBuffer {
	const response = create(ServerResponseSchema, {
		requestId,
		status: { code: statusCode, message: statusCode === 0 ? "" : "Error" },
	});
	const serverMessage = create(ServerMessageSchema, {
		payload: { case: "response", value: response },
	});
	const bytes = toBinary(ServerMessageSchema, serverMessage);
	return bytes.buffer.slice(
		bytes.byteOffset,
		bytes.byteOffset + bytes.byteLength,
	);
}

function createSubscriptionUpdate(subscriptionId: number): ArrayBuffer {
	const update = create(SubscriptionUpdateSchema, {
		subscriptionId,
		changes: [],
	});
	const serverMessage = create(ServerMessageSchema, {
		payload: { case: "subscriptionUpdate", value: update },
	});
	const bytes = toBinary(ServerMessageSchema, serverMessage);
	return bytes.buffer.slice(
		bytes.byteOffset,
		bytes.byteOffset + bytes.byteLength,
	);
}

function createQueryPayload() {
	return {
		case: "query" as const,
		value: create(QueryRequestSchema, { find: [], where: [] }),
	};
}

describe("Connection", () => {
	const testUrl = createServerUrl("ws://localhost:8080");
	const testApiKey = createApiKey("test-api-key");

	beforeEach(() => {
		installMockWebSocket();
	});

	afterEach(() => {
		restoreWebSocket();
	});

	describe("isConnected()", () => {
		it("returns false before connect() is called", () => {
			const conn = new Connection(testUrl, testApiKey);
			expect(conn.isConnected()).toBe(false);
		});

		it("returns true after successful connect()", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();

			// Queue response for ConnectRequest (will be sent when message is received)
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();

			await connectPromise;
			expect(conn.isConnected()).toBe(true);
		});

		it("returns false after close()", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			conn.close();
			expect(conn.isConnected()).toBe(false);
		});
	});

	describe("connect()", () => {
		it("resolves on successful connection", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();

			await expect(connectPromise).resolves.toBeUndefined();
		});

		it("multiple connect() calls while connected are no-ops", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			// Second connect should be a no-op
			await expect(conn.connect()).resolves.toBeUndefined();
		});

		it("concurrent connect() calls return without error", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const promise1 = conn.connect();
			const promise2 = conn.connect();

			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();

			await promise1;
			await promise2;

			expect(conn.isConnected()).toBe(true);
		});

		it("throws on WebSocket connection error", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.simulateError();

			await expect(connectPromise).rejects.toThrow(
				"WebSocket connection error",
			);
		});

		it("throws when server rejects ConnectRequest", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			// Queue rejection response (non-zero status code)
			ws.queueResponse(createServerResponse(1, 3)); // INVALID_ARGUMENT
			ws.simulateOpen();

			await expect(connectPromise).rejects.toThrow("Connection rejected");
		});

		it("isConnected() returns false after failed connect()", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.simulateError();

			try {
				await connectPromise;
			} catch {
				// Expected to throw
			}

			expect(conn.isConnected()).toBe(false);
		});
	});

	describe("send()", () => {
		it("sends message and returns server response", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			const sendPromise = conn.send(createQueryPayload());

			// Simulate response for request_id = 2
			ws.simulateMessage(createServerResponse(2));

			const response = await sendPromise;
			expect(response.requestId).toBe(2);
		});

		it("correlates response to request via request_id", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			// Send two requests
			const promise1 = conn.send(createQueryPayload());
			const promise2 = conn.send(createQueryPayload());

			// Respond out of order
			ws.simulateMessage(createServerResponse(3)); // Second request
			ws.simulateMessage(createServerResponse(2)); // First request

			const [response1, response2] = await Promise.all([promise1, promise2]);
			expect(response1.requestId).toBe(2);
			expect(response2.requestId).toBe(3);
		});

		it("throws when not connected", () => {
			const conn = new Connection(testUrl, testApiKey);

			expect(() => conn.send(createQueryPayload())).toThrow(
				"Not connected to server",
			);
		});

		it("can send multiple concurrent requests", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			const promises = [
				conn.send(createQueryPayload()),
				conn.send(createQueryPayload()),
				conn.send(createQueryPayload()),
			];

			ws.simulateMessage(createServerResponse(2));
			ws.simulateMessage(createServerResponse(3));
			ws.simulateMessage(createServerResponse(4));

			const responses = await Promise.all(promises);
			expect(responses.map((r) => r.requestId)).toEqual([2, 3, 4]);
		});
	});

	describe("setSubscriptionHandler()", () => {
		it("handler is called when subscription update received", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			const handler = mock(() => undefined);
			conn.setSubscriptionHandler(handler);

			ws.simulateMessage(createSubscriptionUpdate(42));

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("no error when update received without handler set", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			// Should not throw
			expect(() => {
				ws.simulateMessage(createSubscriptionUpdate(42));
			}).not.toThrow();
		});
	});

	describe("close()", () => {
		it("isConnected() returns false after close()", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			expect(conn.isConnected()).toBe(true);
			conn.close();
			expect(conn.isConnected()).toBe(false);
		});

		it("pending requests are rejected with error", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			const sendPromise = conn.send(createQueryPayload());

			conn.close();

			await expect(sendPromise).rejects.toThrow("Connection closed");
		});

		it("can be called multiple times safely", async () => {
			const conn = new Connection(testUrl, testApiKey);

			const connectPromise = conn.connect();
			const ws = getMockWebSocket();
			ws.queueResponse(createServerResponse(1));
			ws.simulateOpen();
			await connectPromise;

			expect(() => {
				conn.close();
				conn.close();
				conn.close();
			}).not.toThrow();
		});

		it("can be called when not connected", () => {
			const conn = new Connection(testUrl, testApiKey);

			expect(() => {
				conn.close();
			}).not.toThrow();
		});
	});
});
