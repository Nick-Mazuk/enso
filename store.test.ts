import { expect, test } from "bun:test";
import { HLC } from "./hlc";
import type { Triple } from "./store";
import { TripleStore } from "./store";

test("TripleStore can add and retrieve a triple", () => {
  const store = new TripleStore();
  const hlc = new HLC(new Date(), 0);
  const triple: Triple = ["a", "b", "c", hlc];
  store.add(triple);

  expect(store.query(["a", "b", "c"])).toEqual([triple]);
  expect(store.query(["a", "b"])).toEqual([triple]);
  expect(store.query(["a", undefined, "c"])).toEqual([triple]);
  expect(store.query(["a"])).toEqual([triple]);
  expect(store.query([undefined, "b", "c"])).toEqual([triple]);
  expect(store.query([undefined, "b", undefined])).toEqual([triple]);
  expect(store.query([undefined, undefined, "c"])).toEqual([triple]);
  expect(store.query([])).toEqual([triple]);
});

test("TripleStore can remove a triple", () => {
  const store = new TripleStore();
  const hlc = new HLC(new Date(), 0);
  const triple: Triple = ["a", "b", "c", hlc];
  store.add(triple);
  store.remove(triple);

  expect(store.query(["a", "b", "c"])).toEqual([]);
});

test("TripleStore can handle multiple triples", () => {
  const store = new TripleStore();
  const hlc1 = new HLC(new Date(), 0);
  const hlc2 = new HLC(new Date(), 1);
  const triple1: Triple = ["a", "b", "c", hlc1];
  const triple2: Triple = ["a", "b", "d", hlc2];
  store.add(triple1);
  store.add(triple2);

  expect(store.query(["a", "b", "c"])).toEqual([triple1]);
  expect(store.query(["a", "b", "d"])).toEqual([triple2]);
  expect(store.query(["a", "b"])).toEqual([triple1, triple2]);
});

test("TripleStore conflicts - later timestamp wins", () => {
  const store = new TripleStore();
  const date = new Date();
  const hlc1 = new HLC(date, 0);
  const hlc2 = new HLC(date, 1);
  const triple1: Triple = ["a", "b", "c", hlc1];
  const triple2: Triple = ["a", "b", "d", hlc2];
  store.add(triple1);
  store.add(triple2);

  expect(store.query(["a", "b", "d"])).toEqual([triple2]);
});

test("TripleStore conflicts - later timestamp wins even if it was added first", () => {
  const store = new TripleStore();
  const date = new Date();
  const hlc1 = new HLC(date, 0);
  const hlc2 = new HLC(date, 1);
  const triple1: Triple = ["a", "b", "c", hlc1];
  const triple2: Triple = ["a", "b", "d", hlc2];
  store.add(triple2);
  store.add(triple1);

  expect(store.query(["a", "b", "d"])).toEqual([triple2]);
});
