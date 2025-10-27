import { describe, expect, expectTypeOf, it } from "bun:test";
import { createSchema } from "./create";
import { t } from "./t";
import type { Field } from "./types";

describe("createSchema", () => {
	it("infers entity types", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
					isAdult: t.boolean({ fallback: false }),
				},
			},
		});
		expectTypeOf(schema.entities).toEqualTypeOf<{
			users: {
				name: Field<string, false>;
				age: Field<number, true>;
				isAdult: Field<boolean, false>;
			};
		}>();
	});
	it("infers multiple entity types", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
					isAdult: t.boolean({ fallback: false }),
				},
				posts: {
					name: t.string({ optional: true }),
				},
			},
		});
		expectTypeOf(schema.entities).toEqualTypeOf<{
			users: {
				name: Field<string, false>;
				age: Field<number, true>;
				isAdult: Field<boolean, false>;
			};
			posts: {
				name: Field<string, true>;
			};
		}>();
	});
	describe("reserved fields are not allowed", () => {
		it("id is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							id: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'id' is not allowed");
		});
		it("createdAt is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							createdAt: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'createdAt' is not allowed");
		});
		it("updatedAt is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							updatedAt: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'updatedAt' is not allowed");
		});
		it("createdBy is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							createdBy: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'createdBy' is not allowed");
		});
	});
});
