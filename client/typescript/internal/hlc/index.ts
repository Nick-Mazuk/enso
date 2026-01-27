/**
 * HLC (Hybrid Logical Clock) Timestamp Utilities
 *
 * Generates Hybrid Logical Clock timestamps for conflict resolution in
 * distributed systems. HLC combines physical time with a logical counter
 * to ensure total ordering of events even when physical clocks are skewed.
 *
 * Invariants:
 * - Each timestamp from a single HlcClock instance is monotonically increasing
 * - nodeId is unique per client instance
 * - physicalTimeMs represents milliseconds since Unix epoch
 * - logicalCounter is incremented when multiple events occur in the same millisecond
 */

export type HlcTimestamp = {
	physicalTimeMs: bigint;
	logicalCounter: number;
	nodeId: number;
};

/**
 * Generate a unique node ID for this client instance.
 * Uses random number generation to minimize collision probability.
 *
 * Post-conditions:
 * - Returns a 32-bit unsigned integer
 * - Different calls produce different values (with high probability)
 *
 * @returns A unique node ID
 */
const generateNodeId = (): number => {
	// Use crypto.getRandomValues if available (browser/Node 19+)
	if (
		typeof globalThis.crypto !== "undefined" &&
		typeof globalThis.crypto.getRandomValues === "function"
	) {
		const array = new Uint32Array(1);
		globalThis.crypto.getRandomValues(array);
		return array[0]!;
	}
	// Fallback to Math.random
	return Math.floor(Math.random() * 0xffffffff);
};

/**
 * HLC Clock for generating monotonically increasing timestamps.
 *
 * Pre-conditions:
 * - System clock should be reasonably accurate (within seconds)
 *
 * Post-conditions:
 * - Each createTimestamp call returns a timestamp greater than all previous
 * - Timestamps from this clock have the same nodeId
 *
 * Invariants:
 * - lastPhysicalTime <= current physical time (or equals it when counter increments)
 * - logicalCounter resets to 0 when physical time advances
 */
export class HlcClock {
	private readonly nodeId: number;
	private lastPhysicalTimeMs: bigint = 0n;
	private logicalCounter: number = 0;

	/**
	 * Create a new HLC clock with a unique node ID.
	 *
	 * @param nodeId - Optional node ID. If not provided, a random one is generated.
	 */
	constructor(nodeId?: number) {
		this.nodeId = nodeId ?? generateNodeId();
	}

	/**
	 * Get the node ID for this clock.
	 *
	 * @returns The node ID
	 */
	getNodeId(): number {
		return this.nodeId;
	}

	/**
	 * Create a new HLC timestamp.
	 *
	 * Pre-conditions:
	 * - None
	 *
	 * Post-conditions:
	 * - Returns a timestamp greater than all previous timestamps from this clock
	 * - physicalTimeMs >= previous physicalTimeMs
	 * - If physicalTimeMs equals previous, logicalCounter is incremented
	 *
	 * @returns A new HLC timestamp
	 */
	createTimestamp(): HlcTimestamp {
		const now = BigInt(Date.now());

		if (now > this.lastPhysicalTimeMs) {
			// Time has advanced, reset counter
			this.lastPhysicalTimeMs = now;
			this.logicalCounter = 0;
		} else {
			// Same or backwards physical time, increment counter
			this.logicalCounter++;
		}

		return {
			physicalTimeMs: this.lastPhysicalTimeMs,
			logicalCounter: this.logicalCounter,
			nodeId: this.nodeId,
		};
	}

	/**
	 * Update the clock with a received timestamp (for synchronization).
	 * Ensures local clock stays ahead of received timestamps.
	 *
	 * Pre-conditions:
	 * - received must be a valid HlcTimestamp
	 *
	 * Post-conditions:
	 * - Next createTimestamp will return a timestamp > received
	 *
	 * @param received - The received timestamp to synchronize with
	 */
	receive(received: HlcTimestamp): void {
		const now = BigInt(Date.now());
		const maxPhysicalTime =
			now > received.physicalTimeMs ? now : received.physicalTimeMs;

		if (maxPhysicalTime === this.lastPhysicalTimeMs) {
			// All three times are equal, take max counter + 1
			this.logicalCounter =
				Math.max(this.logicalCounter, received.logicalCounter) + 1;
		} else if (maxPhysicalTime === received.physicalTimeMs) {
			// Received time is ahead, use received counter + 1
			this.lastPhysicalTimeMs = maxPhysicalTime;
			this.logicalCounter = received.logicalCounter + 1;
		} else {
			// Local time is ahead, reset counter
			this.lastPhysicalTimeMs = maxPhysicalTime;
			this.logicalCounter = 0;
		}
	}
}

/**
 * Compare two HLC timestamps.
 *
 * Pre-conditions:
 * - Both a and b must be valid HlcTimestamps
 *
 * Post-conditions:
 * - Returns negative if a < b
 * - Returns positive if a > b
 * - Returns 0 if a equals b
 *
 * @param a - First timestamp
 * @param b - Second timestamp
 * @returns A negative number, zero, or positive number
 */
export const compareTimestamps = (a: HlcTimestamp, b: HlcTimestamp): number => {
	if (a.physicalTimeMs !== b.physicalTimeMs) {
		return a.physicalTimeMs < b.physicalTimeMs ? -1 : 1;
	}
	if (a.logicalCounter !== b.logicalCounter) {
		return a.logicalCounter - b.logicalCounter;
	}
	return a.nodeId - b.nodeId;
};
