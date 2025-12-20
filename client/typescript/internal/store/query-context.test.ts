import { describe, expect, it } from "bun:test";
import { QueryContext } from "./query-context";
import { Id, Variable } from "./types";

describe("QueryContext", () => {
	it("can set and get a variable", () => {
		const context = new QueryContext();
		context.set(Variable("x"), Id("x"));
		expect(context.get(Variable("x"))).toEqual(Id("x"));
	});
	it("can get a variable that is not set", () => {
		const context = new QueryContext();
		expect(context.get(Variable("x"))).toBeUndefined();
		expect(context.has(Variable("x"))).toBe(false);
	});
	it("can set a variable to a different value", () => {
		const context = new QueryContext();
		context.set(Variable("x"), Id("x"));
		context.set(Variable("x"), Id("y"));
		expect(context.get(Variable("x"))).toEqual(Id("y"));
		expect(context.has(Variable("x"))).toBe(true);
	});
	it("can clone a context", () => {
		const context = new QueryContext();
		context.set(Variable("x"), Id("x"));
		const clone = context.clone();
		expect(clone.get(Variable("x"))).toEqual(Id("x"));
		expect(clone.has(Variable("x"))).toBe(true);
	});
	it("cloned context does not affect the original context", () => {
		const context = new QueryContext();
		context.set(Variable("x"), Id("x"));

		const clone = context.clone();
		clone.set(Variable("y"), Id("y"));
		expect(clone.get(Variable("x"))).toEqual(Id("x"));
		expect(clone.has(Variable("x"))).toBe(true);
		expect(clone.get(Variable("y"))).toEqual(Id("y"));
		expect(clone.has(Variable("y"))).toBe(true);

		expect(context.get(Variable("x"))).toEqual(Id("x"));
		expect(context.has(Variable("x"))).toBe(true);
		expect(context.get(Variable("y"))).toBeUndefined();
		expect(context.has(Variable("y"))).toBe(false);
	});
});
