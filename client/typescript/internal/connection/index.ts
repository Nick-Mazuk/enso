/**
 * WebSocket Connection Management
 *
 * Manages WebSocket connection to the Enso server with:
 * - Connection lifecycle (connect, disconnect, reconnect)
 * - Binary protobuf message encoding/decoding
 * - Request/response correlation via request_id
 * - Subscription update handling
 *
 * Pre-conditions:
 * - ServerUrl and ApiKey must be validated branded types
 *
 * Post-conditions:
 * - Connection is established before any operations
 * - All requests receive responses or timeout
 *
 * Invariants:
 * - request_id is monotonically increasing
 * - Each pending request has exactly one resolve/reject
 */

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

import { assert } from "../../../shared/assert.js";
import type {
	ClientMessage,
	ServerResponse,
	SubscriptionUpdate,
} from "../../proto/protocol_pb.js";
import {
	ClientMessageSchema,
	ConnectRequestSchema,
	ServerMessageSchema,
} from "../../proto/protocol_pb.js";
import type { ApiKey, ServerUrl } from "./types.js";

/** Default timeout for requests in milliseconds */
const DEFAULT_TIMEOUT_MS = 30000;

/** Connection states */
type ConnectionState = "disconnected" | "connecting" | "connected";

/** Pending request tracking */
interface PendingRequest {
	resolve: (response: ServerResponse) => void;
	reject: (error: Error) => void;
	timeoutId: ReturnType<typeof setTimeout>;
}

/** Subscription handler function */
export type SubscriptionHandler = (update: SubscriptionUpdate) => void;

/**
 * WebSocket connection to the Enso server.
 *
 * Invariants:
 * - Only one connection attempt at a time
 * - Pending requests are resolved or rejected before disconnect
 */
export class Connection {
	private readonly url: ServerUrl;
	private readonly apiKey: ApiKey;
	private ws: WebSocket | null = null;
	private state: ConnectionState = "disconnected";
	private nextRequestId: number = 1;
	private pendingRequests: Map<number, PendingRequest> = new Map();
	private subscriptionHandler: SubscriptionHandler | null = null;
	private connectPromise: Promise<void> | null = null;

	/**
	 * Creates a new connection instance.
	 *
	 * Pre-conditions: url and apiKey are validated branded types
	 * Post-conditions: Connection instance is created but not connected
	 */
	constructor(url: ServerUrl, apiKey: ApiKey) {
		this.url = url;
		this.apiKey = apiKey;
	}

	/**
	 * Connects to the server and sends ConnectRequest.
	 *
	 * Pre-conditions: Not already connected
	 * Post-conditions: Connection is established and ready for operations
	 *
	 * @throws Error if connection fails or ConnectRequest is rejected
	 */
	async connect(): Promise<void> {
		if (this.state === "connected") {
			return;
		}

		if (this.connectPromise) {
			return this.connectPromise;
		}

		this.connectPromise = this.doConnect();
		try {
			await this.connectPromise;
		} finally {
			this.connectPromise = null;
		}
	}

	private async doConnect(): Promise<void> {
		this.state = "connecting";

		await new Promise<void>((resolve, reject) => {
			this.ws = new WebSocket(this.url.href);
			this.ws.binaryType = "arraybuffer";

			this.ws.onopen = () => {
				resolve();
			};

			this.ws.onerror = (event) => {
				this.state = "disconnected";
				reject(new Error(`WebSocket error: ${event}`));
			};

			this.ws.onclose = () => {
				this.handleDisconnect();
			};

			this.ws.onmessage = (event) => {
				this.handleMessage(event.data as ArrayBuffer);
			};
		});

		// Send ConnectRequest
		const response = await this.sendConnectRequest();
		if (response.status && response.status.code !== 0) {
			this.close();
			throw new Error(
				`Connection rejected: ${response.status.message || "Unknown error"}`,
			);
		}

		this.state = "connected";
	}

	private sendConnectRequest(): Promise<ServerResponse> {
		const connectRequest = create(ConnectRequestSchema, {
			appApiKey: this.apiKey,
		});

		const message = create(ClientMessageSchema, {
			requestId: this.nextRequestId++,
			payload: {
				case: "connect",
				value: connectRequest,
			},
		});

		return this.sendRaw(message);
	}

	/**
	 * Sends a client message and waits for response.
	 *
	 * Pre-conditions: Connected to server
	 * Post-conditions: Returns ServerResponse or throws on error/timeout
	 *
	 * @param payload - The payload to send (query, update, subscribe, etc.)
	 * @param timeoutMs - Optional timeout in milliseconds
	 */
	send(
		payload: ClientMessage["payload"],
		timeoutMs: number = DEFAULT_TIMEOUT_MS,
	): Promise<ServerResponse> {
		assert(this.state === "connected", "Not connected to server");
		assert(this.ws !== null, "WebSocket is null");

		const requestId = this.nextRequestId++;
		const message = create(ClientMessageSchema, {
			requestId,
			payload,
		});

		return this.sendRaw(message, timeoutMs);
	}

	private sendRaw(
		message: ClientMessage,
		timeoutMs: number = DEFAULT_TIMEOUT_MS,
	): Promise<ServerResponse> {
		return new Promise((resolve, reject) => {
			const requestId = message.requestId;
			assert(requestId !== undefined, "Request ID must be set");
			assert(this.ws !== null, "WebSocket is null");

			const timeoutId = setTimeout(() => {
				this.pendingRequests.delete(requestId);
				reject(
					new Error(`Request ${requestId} timed out after ${timeoutMs}ms`),
				);
			}, timeoutMs);

			this.pendingRequests.set(requestId, { resolve, reject, timeoutId });

			const bytes = toBinary(ClientMessageSchema, message);
			this.ws.send(bytes);
		});
	}

	private handleMessage(data: ArrayBuffer): void {
		const bytes = new Uint8Array(data);
		const serverMessage = fromBinary(ServerMessageSchema, bytes);

		switch (serverMessage.payload.case) {
			case "response": {
				const response = serverMessage.payload.value;
				const requestId = response.requestId;
				if (requestId !== undefined) {
					const pending = this.pendingRequests.get(requestId);
					if (pending) {
						clearTimeout(pending.timeoutId);
						this.pendingRequests.delete(requestId);
						pending.resolve(response);
					}
				}
				break;
			}
			case "subscriptionUpdate": {
				const update = serverMessage.payload.value;
				if (this.subscriptionHandler) {
					this.subscriptionHandler(update);
				}
				break;
			}
		}
	}

	private handleDisconnect(): void {
		this.state = "disconnected";
		this.ws = null;

		// Reject all pending requests
		for (const [requestId, pending] of this.pendingRequests) {
			clearTimeout(pending.timeoutId);
			pending.reject(
				new Error(`Connection closed, request ${requestId} aborted`),
			);
		}
		this.pendingRequests.clear();
	}

	/**
	 * Sets the handler for subscription updates.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Handler will be called for each SubscriptionUpdate
	 */
	setSubscriptionHandler(handler: SubscriptionHandler): void {
		this.subscriptionHandler = handler;
	}

	/**
	 * Closes the connection.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Connection is closed, all pending requests rejected
	 */
	close(): void {
		if (this.ws) {
			this.ws.close();
			this.ws = null;
		}
		this.handleDisconnect();
	}

	/**
	 * Returns true if connected to the server.
	 */
	isConnected(): boolean {
		return this.state === "connected";
	}
}
