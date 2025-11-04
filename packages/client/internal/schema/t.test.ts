import { describe, expect, expectTypeOf, it } from "bun:test";
import { t } from "./t";
import type { Field } from "./types";

describe("t.string", () => {
	it("when fallback is provided, optional is optional", () => {
		const field = t.string({ fallback: "" });
		expectTypeOf(field).toEqualTypeOf<Field<string, false>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.string({ optional: true });
		expectTypeOf(field).toEqualTypeOf<Field<string, true>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.string({ optional: true, fallback: "now" });
		expectTypeOf(field).toEqualTypeOf<Field<string, true>>();
	});

	it("when optional is false, fallback is provided", () => {
		const field = t.string({ optional: false, fallback: "now" });
		expectTypeOf(field).toEqualTypeOf<Field<string, false>>();
	});

	it("when optional is true, fallback can be provided", () => {
		const field = t.string({ optional: true, fallback: "now" });
		expectTypeOf(field).toEqualTypeOf<Field<string, true>>();
	});

	it("errors when the optional is false and fallback is not provided", () => {
		expect(() =>
			t.string(
				// @ts-expect-error - fallback is required when optional is false
				{ optional: false },
			),
		).toThrow();
	});

	it("fallback must be a string", () => {
		expect(() =>
			t.string(
				// @ts-expect-error - fallback is required when optional is false
				{ fallback: false },
			),
		).toThrow();
	});
});

describe("t.number", () => {
	it("when fallback is provided, optional is optional", () => {
		const field = t.number({ fallback: 0 });
		expectTypeOf(field).toEqualTypeOf<Field<number, false>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.number({ optional: true });
		expectTypeOf(field).toEqualTypeOf<Field<number, true>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.number({ optional: true, fallback: 1 });
		expectTypeOf(field).toEqualTypeOf<Field<number, true>>();
	});

	it("when optional is false, fallback is provided", () => {
		const field = t.number({ optional: false, fallback: 2 });
		expectTypeOf(field).toEqualTypeOf<Field<number, false>>();
	});

	it("when optional is true, fallback can be provided", () => {
		const field = t.number({ optional: true, fallback: 3 });
		expectTypeOf(field).toEqualTypeOf<Field<number, true>>();
	});

	it("typescript errors when the optional is false and fallback is not provided", () => {
		expect(() =>
			t.number(
				// @ts-expect-error - fallback is required when optional is false
				{ optional: false },
			),
		).toThrow();
	});

	it("fallback must be a number", () => {
		expect(() =>
			t.number(
				// @ts-expect-error - fallback is required when optional is false
				{ fallback: false },
			),
		).toThrow();
	});
});

describe("t.boolean", () => {
	it("when fallback is provided, optional is optional", () => {
		const field = t.boolean({ fallback: true });
		expectTypeOf(field).toEqualTypeOf<Field<boolean, false>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.boolean({ optional: true });
		expectTypeOf(field).toEqualTypeOf<Field<boolean, true>>();
	});

	it("when optional is true, fallback is optional", () => {
		const field = t.boolean({ optional: true, fallback: false });
		expectTypeOf(field).toEqualTypeOf<Field<boolean, true>>();
	});

	it("when optional is false, fallback is provided", () => {
		const field = t.boolean({ optional: false, fallback: true });
		expectTypeOf(field).toEqualTypeOf<Field<boolean, false>>();
	});

	it("when optional is true, fallback can be provided", () => {
		const field = t.boolean({ optional: true, fallback: false });
		expectTypeOf(field).toEqualTypeOf<Field<boolean, true>>();
	});

	it("errors when the optional is false and fallback is not provided", () => {
		expect(() =>
			t.boolean(
				// @ts-expect-error - fallback is required when optional is false
				{ optional: false },
			),
		).toThrow();
	});

	it("fallback must be a boolean", () => {
		expect(() =>
			t.boolean(
				// @ts-expect-error - fallback is required when optional is false
				{ fallback: "hello" },
			),
		).toThrow();
	});
});
