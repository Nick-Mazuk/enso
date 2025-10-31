import { describe, expect, expectTypeOf, it } from "bun:test";
import { createSchema, t } from "../../index";
import { Store } from "../store";
import { createDatabase } from "./create";
import type { DatabaseResult } from "./types";

describe("createDatabase", () => {
	it("creates a database the correct keys", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
				posts: {},
				tags: {},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);
		expect(Object.keys(database)).toEqual(["users", "posts", "tags"]);
		expectTypeOf(database).toHaveProperty("users");
		expectTypeOf(database).toHaveProperty("posts");
		expectTypeOf(database).toHaveProperty("tags");
	});
});

describe("database.entity.create", () => {
	it("argument type is inferred correctly", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
					isActive: t.boolean({ fallback: false }),
					isAuthor: t.boolean({ optional: true, fallback: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);
		expectTypeOf(database.users.create).parameters.toEqualTypeOf<
			[
				{
					name: string;
					isActive: boolean;
					age?: number | undefined;
					isAuthor?: boolean | undefined;
				},
			]
		>();
	});

	it("creates a triple in the store", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);
		const result = database.users.create({ name: "John Doe", age: 30 });
		expect(store.size()).toBe(3); // id, name, age
		expect(result.error).toBeUndefined();
		expect(result.data).toEqual({
			id: expect.any(String),
			name: "John Doe",
			age: 30,
		});
	});
});

describe("database.entity.query", () => {
	it("argument type is inferred correctly", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);
		expectTypeOf(database.users.query).parameters.toEqualTypeOf<
			[{ fields: { name?: boolean; age?: boolean; id?: boolean } }]
		>();
	});

	// DO NOT SUBMIT: reorganize the tests by scenario, not "type vs. functionality".
	// That'll make this more extensible to future scenarios.
	it("return type is inferred correctly", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);

		const result1 = database.users.query({ fields: { age: true, name: true } });
		expectTypeOf(result1).toEqualTypeOf<
			DatabaseResult<{ name: string; age?: number }[]>
		>();

		const result2 = database.users.query({ fields: { id: true } });
		expectTypeOf(result2).toEqualTypeOf<DatabaseResult<{ id: string }[]>>();

		const result3 = database.users.query({ fields: { name: true, id: true } });
		expectTypeOf(result3).toEqualTypeOf<
			DatabaseResult<{ name: string; id: string }[]>
		>();
	});

	it("return the correct data", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);

		database.users.create({ name: "John Doe", age: 30 });
		database.users.create({ name: "Jane Doe" });

		const result1 = database.users.query({ fields: { age: true, name: true } });
		expect(result1).toEqual({
			data: [{ name: "John Doe", age: 30 }, { name: "Jane Doe" }],
		});

		const result2 = database.users.query({ fields: { id: true } });
		expect(result2).toEqual({
			data: [{ id: expect.any(String) }, { id: expect.any(String) }],
		});

		const result3 = database.users.query({ fields: { name: true, id: true } });
		expect(result3).toEqual({
			data: [
				{ id: expect.any(String), name: "John Doe" },
				{ id: expect.any(String), name: "Jane Doe" },
			],
		});
	});
});
