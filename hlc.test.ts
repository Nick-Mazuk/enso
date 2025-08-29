import { expect, test } from "bun:test";
import { HLC } from "./hlc";

test("HLC constructor throws on invalid counter", () => {
  expect(() => new HLC(new Date(), -1)).toThrow();
  expect(() => new HLC(new Date(), 0x10000)).toThrow();
});

test("HLC.toString() creates a valid HLC string", () => {
  const hlc = new HLC(new Date("2023-01-01T00:00:00.000Z"), 0);
  expect(hlc.toString()).toBe("2023-01-01T00:00:00.000Z-0000");

  const hlc2 = new HLC(new Date("2023-01-01T00:00:00.000Z"), 0xffff);
  expect(hlc2.toString()).toBe("2023-01-01T00:00:00.000Z-FFFF");
});

test("HLC.fromString correctly parses an HLC string", () => {
  const hlcString = "2023-01-01T00:00:00.000Z-0000";
  const parsed = HLC.fromString(hlcString);
  expect(parsed?.time).toEqual(new Date("2023-01-01T00:00:00.000Z"));
  expect(parsed?.counter).toBe(0);
});

test("HLC.fromString returns undefined for invalid HLC strings", () => {
  expect(HLC.fromString("")).toBeUndefined();
  expect(HLC.fromString("not-an-hlc")).toBeUndefined();
  expect(HLC.fromString("2023-01-01T00:00:00.000Z")).toBeUndefined();
  expect(HLC.fromString("2023-01-01T00:00:00.000Z-")).toBeUndefined();
  expect(HLC.fromString("2023-01-01T00:00:00.000Z-123")).toBeUndefined();
  expect(HLC.fromString("2023-01-01T00:00:00.000Z-GHIJ")).toBeUndefined();
  expect(HLC.fromString("not-a-date-FFFF")).toBeUndefined();
});

test("increment() increments the counter", () => {
  const hlc = new HLC(new Date("2023-01-01T00:00:00.000Z"), 0);
  const newHlc = hlc.increment();
  expect(newHlc.toString()).toBe("2023-01-01T00:00:00.000Z-0001");
});

test("increment() rolls over the timestamp when counter maxes out", () => {
  const hlc = new HLC(new Date("2023-01-01T00:00:00.000Z"), 0xffff);
  const newHlc = hlc.increment();
  expect(newHlc.time.getTime()).toBe(
    new Date("2023-01-01T00:00:00.001Z").getTime()
  );
  expect(newHlc.counter).toBe(0);
});

test("compare() correctly compares two HLCs", () => {
  const hlc1 = new HLC(new Date("2023-01-01T00:00:00.000Z"), 0);
  const hlc2 = new HLC(new Date("2023-01-01T00:00:00.000Z"), 1);
  const hlc3 = new HLC(new Date("2023-01-01T00:00:00.001Z"), 0);
  const hlc4 = new HLC(new Date("2024-01-01T00:00:00.000Z"), 0);

  expect(hlc1.compare(hlc1)).toBe(0);
  expect(hlc1.compare(hlc2)).toBe(-1);
  expect(hlc2.compare(hlc1)).toBe(1);
  expect(hlc1.compare(hlc3)).toBe(-1);
  expect(hlc3.compare(hlc1)).toBe(1);
  expect(hlc1.compare(hlc4)).toBe(-1);
  expect(hlc4.compare(hlc1)).toBe(1);
  expect(hlc3.compare(hlc2)).toBe(1);
});
