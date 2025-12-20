import { describe, expect, expectTypeOf, it } from "bun:test";
import { createSchema } from "./create.js";
import { t } from "./t.js";
import type { Field } from "./types.js";

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
	it("infers ref types", () => {
		const schema = createSchema({
			entities: {
				posts: {
					authorId: t.ref("users"),
				},
			},
		});
		expectTypeOf(schema.entities).toEqualTypeOf<{
			posts: {
				authorId: Field<string, true>;
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
		it("createTime is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							createTime: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'createTime' is not allowed");
		});
		it("updateTime is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							updateTime: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'updateTime' is not allowed");
		});
		it("creator is not allowed", () => {
			expect(() =>
				createSchema({
					entities: {
						users: {
							creator: t.string({ fallback: "" }),
						},
					},
				}),
			).toThrow("Reserved field 'creator' is not allowed");
		});
	});
});
