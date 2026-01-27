/**
 * Connection Class
 *
 * Manages WebSocket communication with the server using protobuf protocol.
 * Handles request/response correlation, reconnection, and subscription routing.
 *
 * Invariants:
 * - requestId is monotonically increasing within a connection
 * - Each pending request has exactly one corresponding resolve/reject
 * - Subscription handlers are invoked for matching subscription_id updates
 */

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { assert } from "../../../shared/assert.js";
import {
	type ClientMessage,
	ClientMessageSchema,
	ConnectRequestSchema,
	type QueryRequest,
	ServerMessageSchema,
	type ServerResponse,
	SubscribeRequestSchema,
	type SubscriptionUpdate,
	type Triple,
	TripleUpdateRequestSchema,
	UnsubscribeRequestSchema,
} from "../../proto/protocol_pb.js";

export type ConnectionState = "disconnected" | "connecting" | "connected";

export type SubscriptionHandler = (update: SubscriptionUpdate) => void;

type PendingRequest = {
	resolve: (response: ServerResponse) => void;
	reject: (error: Error) => void;
};

/**
 * Connection manages WebSocket communication with the server.
 *
 * Pre-conditions:
 * - url must be a valid WebSocket URL
 * - apiKey must be a non-empty string
 *
 * Post-conditions:
 * - After connect(), connection is established and ConnectRequest is sent
 * - Responses are correlated with requests via request_id
 *
 * Invariants:
 * - pendingRequests contains only requests awaiting responses
 * - subscriptionHandlers contains only active subscription handlers
 */
export class Connection {
	private ws: WebSocket | null = null;
	private readonly url: string;
	private readonly apiKey: string;
	private pendingRequests: Map<number, PendingRequest> = new Map();
	private nextRequestId: number = 1;
	private subscriptionHandlers: Map<number, SubscriptionHandler> = new Map();
	private connectionState: ConnectionState = "disconnected";
	private connectPromise: Promise<void> | null = null;
	private reconnectAttempts: number = 0;
	private readonly maxReconnectAttempts: number = 5;
	private readonly reconnectDelay: number = 1000;
	private queuedMessages: ClientMessage[] = [];

	/**
	 * Create a new Connection instance.
	 *
	 * @param url - WebSocket URL to connect to
	 * @param apiKey - API key for authentication
	 */
	constructor(url: string, apiKey: string) {
		assert(url.length > 0, "url must be a non-empty string");
		assert(apiKey.length > 0, "apiKey must be a non-empty string");
		this.url = url;
		this.apiKey = apiKey;
	}

	/**
	 * Get the current connection state.
	 *
	 * @returns The current connection state
	 */
	getState(): ConnectionState {
		return this.connectionState;
	}

	/**
	 * Connect to the server.
	 *
	 * Pre-conditions:
	 * - Connection is not already connecting
	 *
	 * Post-conditions:
	 * - WebSocket is connected
	 * - ConnectRequest has been sent and acknowledged
	 *
	 * @returns A promise that resolves when connected
	 */
	async connect(): Promise<void> {
		if (this.connectionState === "connected") {
			return;
		}

		if (this.connectPromise) {
			return this.connectPromise;
		}

		this.connectPromise = this.establishConnection();
		try {
			await this.connectPromise;
		} finally {
			this.connectPromise = null;
		}
	}

	private establishConnection(): Promise<void> {
		this.connectionState = "connecting";

		return new Promise<void>((resolve, reject) => {
			try {
				this.ws = new WebSocket(this.url);
				this.ws.binaryType = "arraybuffer";

				this.ws.onopen = () => {
					try {
						// Send ConnectRequest
						const connectRequest = create(ConnectRequestSchema, {
							appApiKey: this.apiKey,
						});

						const requestId = this.nextRequestId++;
						const message = create(ClientMessageSchema, {
							requestId,
							payload: {
								case: "connect",
								value: connectRequest,
							},
						});

						const binary = toBinary(ClientMessageSchema, message);
						this.ws?.send(binary);

						// Wait for response
						this.pendingRequests.set(requestId, {
							resolve: () => {
								this.connectionState = "connected";
								this.reconnectAttempts = 0;
								this.flushQueuedMessages();
								resolve();
							},
							reject: (error: Error) => {
								this.connectionState = "disconnected";
								reject(error);
							},
						});
					} catch (error) {
						reject(error);
					}
				};

				this.ws.onmessage = (event: MessageEvent) => {
					this.handleMessage(event);
				};

				this.ws.onerror = (_event: Event) => {
					const error = new Error("WebSocket error");
					if (this.connectionState === "connecting") {
						reject(error);
					}
					this.handleError(error);
				};

				this.ws.onclose = () => {
					this.handleClose();
				};
			} catch (error) {
				this.connectionState = "disconnected";
				reject(error);
			}
		});
	}

	private handleMessage(event: MessageEvent): void {
		try {
			const data =
				event.data instanceof ArrayBuffer
					? new Uint8Array(event.data)
					: event.data;

			const serverMessage = fromBinary(ServerMessageSchema, data);

			if (serverMessage.payload.case === "response") {
				const response = serverMessage.payload.value;
				if (response.requestId !== undefined) {
					const pending = this.pendingRequests.get(response.requestId);
					if (pending) {
						this.pendingRequests.delete(response.requestId);

						// Check for error status
						if (response.status !== undefined && response.status.code !== 0) {
							pending.reject(
								new Error(response.status.message || "Request failed"),
							);
						} else {
							pending.resolve(response);
						}
					}
				}
			} else if (serverMessage.payload.case === "subscriptionUpdate") {
				const update = serverMessage.payload.value;
				const handler = this.subscriptionHandlers.get(update.subscriptionId);
				if (handler) {
					handler(update);
				}
			}
		} catch (error) {
			console.error("Failed to parse server message:", error);
		}
	}

	private handleError(_error: Error): void {
		// Reject all pending requests
		for (const [, pending] of this.pendingRequests) {
			pending.reject(new Error("Connection error"));
		}
		this.pendingRequests.clear();
	}

	private handleClose(): void {
		const wasConnected = this.connectionState === "connected";
		this.connectionState = "disconnected";
		this.ws = null;

		// Reject all pending requests
		for (const [, pending] of this.pendingRequests) {
			pending.reject(new Error("Connection closed"));
		}
		this.pendingRequests.clear();

		// Attempt reconnection if we were connected
		if (wasConnected && this.reconnectAttempts < this.maxReconnectAttempts) {
			this.scheduleReconnect();
		}
	}

	private scheduleReconnect(): void {
		const delay = this.reconnectDelay * 2 ** this.reconnectAttempts;
		this.reconnectAttempts++;

		setTimeout(() => {
			this.connect().catch((error) => {
				console.error("Reconnection failed:", error);
			});
		}, delay);
	}

	private flushQueuedMessages(): void {
		const messages = this.queuedMessages;
		this.queuedMessages = [];

		for (const message of messages) {
			this.sendMessage(message);
		}
	}

	private sendMessage(message: ClientMessage): void {
		assert(this.ws !== null, "WebSocket is not connected");
		assert(
			this.connectionState === "connected",
			"Connection is not established",
		);

		const binary = toBinary(ClientMessageSchema, message);
		this.ws.send(binary);
	}

	/**
	 * Send a request and wait for a response.
	 *
	 * Pre-conditions:
	 * - Connection is established
	 *
	 * Post-conditions:
	 * - Returns the server response
	 *
	 * @param payload - The payload to send
	 * @returns A promise that resolves with the server response
	 */
	async send(payload: ClientMessage["payload"]): Promise<ServerResponse> {
		if (this.connectionState !== "connected") {
			await this.connect();
		}

		return new Promise<ServerResponse>((resolve, reject) => {
			const requestId = this.nextRequestId++;

			const message = create(ClientMessageSchema, {
				requestId,
				payload,
			});

			this.pendingRequests.set(requestId, { resolve, reject });

			try {
				this.sendMessage(message);
			} catch (error) {
				this.pendingRequests.delete(requestId);
				reject(error);
			}
		});
	}

	/**
	 * Send a triple update request.
	 *
	 * @param triples - The triples to update
	 * @returns A promise that resolves when the update is acknowledged
	 */
	async sendTripleUpdate(triples: Triple[]): Promise<ServerResponse> {
		const request = create(TripleUpdateRequestSchema, {
			triples,
		});

		return await this.send({
			case: "tripleUpdateRequest",
			value: request,
		});
	}

	/**
	 * Send a query request.
	 *
	 * @param query - The query to execute
	 * @returns A promise that resolves with the query results
	 */
	async sendQuery(query: QueryRequest): Promise<ServerResponse> {
		return await this.send({
			case: "query",
			value: query,
		});
	}

	/**
	 * Subscribe to changes.
	 *
	 * @param subscriptionId - Client-assigned subscription ID
	 * @param handler - Handler function for subscription updates
	 * @returns A promise that resolves when the subscription is established
	 */
	async subscribe(
		subscriptionId: number,
		handler: SubscriptionHandler,
	): Promise<void> {
		const request = create(SubscribeRequestSchema, {
			subscriptionId,
		});

		this.subscriptionHandlers.set(subscriptionId, handler);

		try {
			await this.send({
				case: "subscribe",
				value: request,
			});
		} catch (error) {
			this.subscriptionHandlers.delete(subscriptionId);
			throw error;
		}
	}

	/**
	 * Unsubscribe from changes.
	 *
	 * @param subscriptionId - The subscription ID to unsubscribe
	 * @returns A promise that resolves when unsubscribed
	 */
	async unsubscribe(subscriptionId: number): Promise<void> {
		const request = create(UnsubscribeRequestSchema, {
			subscriptionId,
		});

		await this.send({
			case: "unsubscribe",
			value: request,
		});

		this.subscriptionHandlers.delete(subscriptionId);
	}

	/**
	 * Close the connection.
	 *
	 * Post-conditions:
	 * - WebSocket is closed
	 * - All pending requests are rejected
	 */
	close(): void {
		this.reconnectAttempts = this.maxReconnectAttempts; // Prevent reconnection

		if (this.ws) {
			this.ws.close();
			this.ws = null;
		}

		this.connectionState = "disconnected";

		// Reject all pending requests
		for (const [, pending] of this.pendingRequests) {
			pending.reject(new Error("Connection closed by client"));
		}
		this.pendingRequests.clear();
		this.subscriptionHandlers.clear();
	}
}
