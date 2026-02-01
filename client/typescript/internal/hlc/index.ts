/**
 * Hybrid Logical Clock (HLC) Utilities
 *
 * HLC combines physical time with logical counters for conflict resolution
 * in distributed systems. It ensures:
 * - Timestamps are always monotonically increasing
 * - Events at the same physical time can be ordered by logical counter
 * - Different nodes can be distinguished by node_id
 *
 * Pre-conditions:
 * - HlcClock must be initialized before creating timestamps
 *
 * Post-conditions:
 * - Each timestamp is strictly greater than the previous
 * - Timestamps are comparable for ordering
 *
 * Invariants:
 * - node_id is constant for the lifetime of an HlcClock instance
 * - logical_counter resets when physical time advances
 */

import { create } from "@bufbuild/protobuf";
import type { Tagged } from "type-fest";

import { assert } from "../../../shared/assert.js";
import type { HlcTimestamp } from "../../proto/protocol_pb.js";
import { HlcTimestampSchema } from "../../proto/protocol_pb.js";

/**
 * Branded type for validated node IDs.
 *
 * Invariants:
 * - Value is a valid uint32 (0 to 0xFFFFFFFF)
 * - Uses only 24 bits of randomness for practical purposes
 */
export type NodeId = Tagged<number, "NodeId">;

/**
 * Creates a validated NodeId from a number.
 *
 * Pre-conditions: value is a valid uint32
 * Post-conditions: Returns a validated NodeId
 *
 * @param value - The node ID value
 * @throws Error if value is not a valid uint32
 */
export function NodeId(value: number): NodeId {
	assert(Number.isInteger(value), "NodeId must be an integer");
	assert(value >= 0, "NodeId must be non-negative");
	assert(value <= 0xffffffff, "NodeId must fit in uint32");
	return value as NodeId;
}

/**
 * Generates a random node ID for this client instance.
 * Uses 24 bits of randomness (fits in uint32).
 *
 * Pre-conditions: None
 * Post-conditions: Returns a random NodeId
 */
function generateNodeId(): NodeId {
	const bytes = new Uint8Array(4);
	crypto.getRandomValues(bytes);
	// Use only 24 bits to leave room for other uses
	// biome-ignore lint/style/noNonNullAssertion: bytes array is always 4 elements
	const value = ((bytes[0]! << 16) | (bytes[1]! << 8) | bytes[2]!) >>> 0;
	return value as NodeId;
}

/**
 * HLC Clock for generating monotonically increasing timestamps.
 *
 * Invariants:
 * - nodeId is constant after construction
 * - Each call to now() returns a timestamp greater than the previous
 */
export class HlcClock {
	private readonly nodeId: NodeId;
	private lastPhysicalTime: bigint = 0n;
	private logicalCounter: number = 0;

	/**
	 * Creates a new HLC clock.
	 *
	 * Pre-conditions: If nodeId provided, must be a validated NodeId
	 * Post-conditions: Clock is ready to generate timestamps
	 *
	 * @param nodeId - Optional validated NodeId. If not provided, a random one is generated.
	 */
	constructor(nodeId?: NodeId) {
		this.nodeId = nodeId ?? generateNodeId();
	}

	/**
	 * Generates a new HLC timestamp that is strictly greater than the previous.
	 *
	 * Pre-conditions: None
	 * Post-conditions: Returns a timestamp > all previous timestamps from this clock
	 */
	now(): HlcTimestamp {
		const physicalTime = BigInt(Date.now());

		if (physicalTime > this.lastPhysicalTime) {
			// Physical time advanced, reset logical counter
			this.lastPhysicalTime = physicalTime;
			this.logicalCounter = 0;
		} else {
			// Same or earlier physical time, increment logical counter
			this.logicalCounter++;
			assert(
				this.logicalCounter <= 0xffffffff,
				"Logical counter overflow - too many events in same millisecond",
			);
		}

		return create(HlcTimestampSchema, {
			physicalTimeMs: this.lastPhysicalTime,
			logicalCounter: this.logicalCounter,
			nodeId: this.nodeId,
		});
	}

	/**
	 * Updates the clock based on a received timestamp.
	 * Ensures the next generated timestamp is greater than both
	 * the local clock and the received timestamp.
	 *
	 * Pre-conditions: received is a valid HlcTimestamp
	 * Post-conditions: Next call to now() returns a timestamp > received
	 */
	receive(received: HlcTimestamp): void {
		const physicalTime = BigInt(Date.now());
		const receivedTime = received.physicalTimeMs;

		if (physicalTime > this.lastPhysicalTime && physicalTime > receivedTime) {
			// Local physical time is ahead
			this.lastPhysicalTime = physicalTime;
			this.logicalCounter = 0;
		} else if (receivedTime > this.lastPhysicalTime) {
			// Received time is ahead of local
			this.lastPhysicalTime = receivedTime;
			this.logicalCounter = received.logicalCounter + 1;
		} else if (receivedTime === this.lastPhysicalTime) {
			// Same physical time, take max logical counter + 1
			this.logicalCounter =
				Math.max(this.logicalCounter, received.logicalCounter) + 1;
		}
		// else: local is already ahead, no update needed
	}

	/**
	 * Gets the node ID for this clock.
	 */
	getNodeId(): NodeId {
		return this.nodeId;
	}
}

/**
 * Compares two HLC timestamps.
 *
 * Pre-conditions: Both timestamps are valid HlcTimestamp objects
 * Post-conditions: Returns -1 if a < b, 0 if a == b, 1 if a > b
 */
export function compareHlc(a: HlcTimestamp, b: HlcTimestamp): -1 | 0 | 1 {
	// Compare physical time first
	if (a.physicalTimeMs < b.physicalTimeMs) return -1;
	if (a.physicalTimeMs > b.physicalTimeMs) return 1;

	// Same physical time, compare logical counter
	if (a.logicalCounter < b.logicalCounter) return -1;
	if (a.logicalCounter > b.logicalCounter) return 1;

	// Same logical counter, compare node ID for total ordering
	if (a.nodeId < b.nodeId) return -1;
	if (a.nodeId > b.nodeId) return 1;

	return 0;
}

/**
 * Returns true if timestamp a is strictly greater than timestamp b.
 *
 * Pre-conditions: Both timestamps are valid HlcTimestamp objects
 * Post-conditions: Returns true iff a > b
 */
export function isHlcGreater(a: HlcTimestamp, b: HlcTimestamp): boolean {
	return compareHlc(a, b) === 1;
}
