import { expect, test } from "bun:test";
import { HLC } from "./hlc";
import type { DatalogQuery, Triple } from "./store";
import { TripleStore } from "./store";

test("TripleStore can add and retrieve a triple", () => {
  const store = new TripleStore();
  store.add(["a", "name", "test", new HLC(new Date(1), 0)]);
  expect(store.query(["a", "name", "test"])).toEqual([
    ["a", "name", "test", new HLC(new Date(1), 0)],
  ]);
  expect(store.query(["a", "name"])).toEqual([
    ["a", "name", "test", new HLC(new Date(1), 0)],
  ]);
  expect(store.query(["a"])).toHaveLength(1);
});

test("TripleStore can remove a triple", () => {
  const store = new TripleStore();
  const hlc = new HLC(new Date(), 0);
  const triple: Triple = ["a", "b", "c", hlc];
  store.add(triple);
  store.remove(triple);

  expect(store.query(["a", "b", "c"])).toEqual([]);
});

test("TripleStore can run a datalog query", () => {
  const store = new TripleStore();
  store.add(["e1", "likes", "pizza", new HLC(new Date(), 0)]);
  store.add(["e1", "age", 30, new HLC(new Date(), 1)]);
  store.add(["e2", "likes", "pizza", new HLC(new Date(), 2)]);
  store.add(["e2", "age", 20, new HLC(new Date(), 3)]);
  store.add(["e3", "likes", "ramen", new HLC(new Date(), 4)]);
  store.add(["e3", "age", 35, new HLC(new Date(), 5)]);

  const query: DatalogQuery = [
    ["?e", "likes", "pizza"],
    ["?e", "age", "?age"],
    ["?age", "greaterThan", 25],
  ];

  const results = store.datalogQuery(query);

  expect(results).toEqual([{ "?e": "e1", "?age": 30 }]);
});

test("TripleStore conflicts - later timestamp wins", () => {
  const store = new TripleStore();
  store.add(["a", "name", "old", new HLC(new Date(1), 0)]);
  store.add(["a", "name", "new", new HLC(new Date(1), 1)]);
  expect(store.query(["a", "name"])).toEqual([
    ["a", "name", "new", new HLC(new Date(1), 1)],
  ]);
});

test("TripleStore conflicts - later timestamp wins even if it was added first", () => {
  const store = new TripleStore();
  store.add(["a", "name", "new", new HLC(new Date(2), 0)]);
  store.add(["a", "name", "old", new HLC(new Date(1), 0)]);
  expect(store.query(["a", "name"])).toEqual([
    ["a", "name", "new", new HLC(new Date(2), 0)],
  ]);
});
