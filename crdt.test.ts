import { expect, test } from "bun:test";
import { decode, encode } from "./crdt";
import type { Value } from "./store";

test("encode and decode values correctly", () => {
  const values: Value[] = [
    "hello",
    42,
    -42,
    3.14,
    true,
    false,
    new Date("2023-01-01T00:00:00.000Z"),
    null,
  ];

  for (const value of values) {
    const encoded = encode(value);
    const decoded = decode(encoded);
    expect(decoded).toEqual(value);
  }
});
