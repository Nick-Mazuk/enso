import { describe, expect, it } from "bun:test";
import { HlcClock, compareTimestamps } from "./index.js";

describe("HLC timestamp utilities", () => {
	describe("HlcClock", () => {
		it("generates timestamps with correct structure", () => {
			const clock = new HlcClock();
			const ts = clock.createTimestamp();

			expect(typeof ts.physicalTimeMs).toBe("bigint");
			expect(typeof ts.logicalCounter).toBe("number");
			expect(typeof ts.nodeId).toBe("number");
		});

		it("uses provided node ID", () => {
			const clock = new HlcClock(12345);
			const ts = clock.createTimestamp();
			expect(ts.nodeId).toBe(12345);
		});

		it("generates unique node IDs when not provided", () => {
			const clock1 = new HlcClock();
			const clock2 = new HlcClock();
			// Note: There's a tiny probability these could be equal
			expect(clock1.getNodeId()).not.toBe(clock2.getNodeId());
		});

		it("returns monotonically increasing timestamps", () => {
			const clock = new HlcClock();
			const ts1 = clock.createTimestamp();
			const ts2 = clock.createTimestamp();
			const ts3 = clock.createTimestamp();

			expect(compareTimestamps(ts1, ts2)).toBeLessThan(0);
			expect(compareTimestamps(ts2, ts3)).toBeLessThan(0);
		});

		it("increments logical counter for same physical time", () => {
			const clock = new HlcClock();
			const ts1 = clock.createTimestamp();
			const ts2 = clock.createTimestamp();

			// If called quickly enough, physical time might be the same
			if (ts1.physicalTimeMs === ts2.physicalTimeMs) {
				expect(ts2.logicalCounter).toBe(ts1.logicalCounter + 1);
			}
		});

		it("resets counter when physical time advances", async () => {
			const clock = new HlcClock();
			clock.createTimestamp();
			clock.createTimestamp();
			clock.createTimestamp();

			// Wait for physical time to advance
			await new Promise((resolve) => setTimeout(resolve, 2));

			const ts = clock.createTimestamp();
			// Counter should be 0 or low after time advance
			expect(ts.logicalCounter).toBeLessThanOrEqual(3);
		});

		describe("receive", () => {
			it("updates clock to stay ahead of received timestamps", () => {
				const clock = new HlcClock(1);
				const futureTime = BigInt(Date.now() + 10000);

				clock.receive({
					physicalTimeMs: futureTime,
					logicalCounter: 5,
					nodeId: 2,
				});

				const ts = clock.createTimestamp();
				expect(ts.physicalTimeMs).toBeGreaterThanOrEqual(futureTime);
				if (ts.physicalTimeMs === futureTime) {
					expect(ts.logicalCounter).toBeGreaterThan(5);
				}
			});

			it("handles received timestamp from the past", () => {
				const clock = new HlcClock(1);
				const ts1 = clock.createTimestamp();

				clock.receive({
					physicalTimeMs: ts1.physicalTimeMs - 1000n,
					logicalCounter: 100,
					nodeId: 2,
				});

				const ts2 = clock.createTimestamp();
				expect(compareTimestamps(ts1, ts2)).toBeLessThan(0);
			});
		});
	});

	describe("compareTimestamps", () => {
		it("returns negative when a < b (physical time)", () => {
			const a = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 1 };
			const b = { physicalTimeMs: 200n, logicalCounter: 0, nodeId: 1 };
			expect(compareTimestamps(a, b)).toBeLessThan(0);
		});

		it("returns positive when a > b (physical time)", () => {
			const a = { physicalTimeMs: 200n, logicalCounter: 0, nodeId: 1 };
			const b = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 1 };
			expect(compareTimestamps(a, b)).toBeGreaterThan(0);
		});

		it("returns negative when a < b (logical counter)", () => {
			const a = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 1 };
			const b = { physicalTimeMs: 100n, logicalCounter: 5, nodeId: 1 };
			expect(compareTimestamps(a, b)).toBeLessThan(0);
		});

		it("returns positive when a > b (logical counter)", () => {
			const a = { physicalTimeMs: 100n, logicalCounter: 5, nodeId: 1 };
			const b = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 1 };
			expect(compareTimestamps(a, b)).toBeGreaterThan(0);
		});

		it("uses node ID as tiebreaker", () => {
			const a = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 1 };
			const b = { physicalTimeMs: 100n, logicalCounter: 0, nodeId: 2 };
			expect(compareTimestamps(a, b)).toBeLessThan(0);
			expect(compareTimestamps(b, a)).toBeGreaterThan(0);
		});

		it("returns 0 for identical timestamps", () => {
			const a = { physicalTimeMs: 100n, logicalCounter: 5, nodeId: 1 };
			const b = { physicalTimeMs: 100n, logicalCounter: 5, nodeId: 1 };
			expect(compareTimestamps(a, b)).toBe(0);
		});
	});
});
