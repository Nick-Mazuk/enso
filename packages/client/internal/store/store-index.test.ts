import { describe, it } from "bun:test";
import { StoreIndex } from "./store-index";
import { Field, Id, Value } from "./types";

describe("add", () => {
	it("adds a triple to an empty index", () => {
		const index = new StoreIndex<Id, Field, Value>();
		index.add(Id("x"), Field("y"), Value("z"));
	});
});
