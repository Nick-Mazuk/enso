import { describe, expect, expectTypeOf, it } from "bun:test";
import { isVariable, type QueryVariable, Variable } from "./types.js";

describe("QueryVariable", () => {
	it("can create and validate a query variable", () => {
		const x = Variable("x");
		expectTypeOf(x).toEqualTypeOf<QueryVariable>();
		expect(isVariable(x)).toBe(true);
	});
	it("random things are not query variables", () => {
		expect(isVariable("x")).toBe(false);
		expectTypeOf("x").not.toEqualTypeOf<QueryVariable>();

		expect(isVariable(1)).toBe(false);
		expectTypeOf(1).not.toEqualTypeOf<QueryVariable>();
	});
});
