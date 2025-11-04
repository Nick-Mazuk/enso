import { describe, expect, expectTypeOf, it } from "bun:test";
import { assert } from "../../../shared/assert";
import { createSchema, t } from "../../index";
import { Store } from "../store";
import { Field, Id, Value } from "../store/types";
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

	it("create errors if required field is not present", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});

		const store = new Store();
		const database = createDatabase(schema, store);
		const result = database.users.create(
			// @ts-expect-error
			{},
		);
		expect(result).toEqual({
			error: {
				message: `Missing required field "name" when creating entity "users"`,
			},
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

	it("can query for all schema fields", () => {
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

		const result = database.users.query({ fields: { age: true, name: true } });
		expectTypeOf(result).toEqualTypeOf<
			DatabaseResult<{ name: string; age?: number }[]>
		>();
		expect(result).toEqual({
			data: [{ name: "John Doe", age: 30 }],
		});
	});

	it("objects with missing optional fields are still returned", () => {
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
		database.users.create({ name: "John Doe" });

		const result = database.users.query({ fields: { age: true, name: true } });
		expectTypeOf(result).toEqualTypeOf<
			DatabaseResult<{ name: string; age?: number }[]>
		>();
		expect(result).toEqual({
			data: [{ name: "John Doe", age: 30 }, { name: "John Doe" }],
		});
	});

	it("can query for the auto-generated id", () => {
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

		const user = database.users.create({ name: "John Doe", age: 30 });
		assert(user.data !== undefined, "User was not created successfully");

		const result = database.users.query({ fields: { id: true } });
		expectTypeOf(result).toEqualTypeOf<DatabaseResult<{ id: string }[]>>();
		expect(result).toEqual({
			data: [{ id: user.data.id }],
		});
	});

	it("can query for auto-generated and schema fields in the same query", () => {
		it("can query for all schema fields", () => {
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

			const user = database.users.create({ name: "John Doe", age: 30 });
			assert(user.data !== undefined, "User was not created successfully");

			const result = database.users.query({
				fields: { name: true, id: true },
			});
			expectTypeOf(result).toEqualTypeOf<
				DatabaseResult<{ name: string; id: string }[]>
			>();
			expect(result).toEqual({
				data: [{ id: user.data.id, name: "John Doe" }],
			});
		});
	});

	it("should apply fallback for required fields when data is missing", () => {
		const schema = createSchema({
			entities: {
				users: {
					// 'name' is required (has a fallback, not optional)
					name: t.string({ fallback: "Anonymous" }),
					// 'age' is optional
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);

		// User 1: Create a normal user
		const user1 = database.users.create({ name: "John Doe", age: 30 });
		assert(user1.data !== undefined, "User 1 was not created successfully");

		// User 2: Simulate a user with missing 'name' triple.
		// We do this by adding triples directly to the store,
		// bypassing the database.create() guarantees.
		const user2Id = "id-user-2";
		store.add(
			[Id(user2Id), Field("users/id"), Value(user2Id)],
			[Id(user2Id), Field("users/age"), Value(40)],
			// DO NOT add the 'users/name' triple
		);

		const result = database.users.query({
			fields: { name: true, age: true },
		});

		expect(result.error).toBeUndefined();
		expect(result.data).toBeDefined();
		expect(result.data?.length).toBe(2);

		// Sort by age for stable test
		const sortedData = result.data?.sort((a, b) => (a.age || 0) - (b.age || 0));

		// User 1 should be complete
		expect(sortedData?.[0]).toEqual({
			name: "John Doe",
			age: 30,
		});

		// User 2 should have its name replaced by the fallback
		expect(sortedData?.[1]).toEqual({
			name: "Anonymous",
			age: 40,
		});
	});

	it("should apply fallback for an optional field when data is missing", () => {
		const schema = createSchema({
			entities: {
				users: {
					age: t.number({ optional: true, fallback: 42 }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);

		// User 1: Create a normal user
		database.users.create({ age: 30 });
		database.users.create({});

		const result = database.users.query({
			fields: { age: true },
		});

		expect(result.error).toBeUndefined();
		expect(result.data).toBeDefined();
		expect(result.data?.length).toBe(2);

		// Sort by age for stable test
		const sortedData = result.data?.sort((a, b) => (a.age || 0) - (b.age || 0));

		expect(sortedData).toEqual([
			{ age: 30 },
			{ age: 42 }, // Uses the fallback
		]);
	});

	it("should return an empty array when querying an empty database", () => {
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

		const result = database.users.query({ fields: { name: true, age: true } });
		expect(result.data).toEqual([]);
	});

	it("should return an empty array when querying for no fields in an empty database", () => {
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

		const result = database.users.query({ fields: {} });
		expect(result.data).toEqual([]);
	});

	it("should return an array of empty objects when querying for no fields", () => {
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

		const result = database.users.query({ fields: {} });
		expect(result.data).toEqual([{}, {}]);
	});

	it("should throw an error when querying for a field that is not in the schema", () => {
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

		const result = database.users.query({
			fields: { name: true, age: true, notInSchema: true },
		});
		expect(result.error).toBeDefined();
		expect(result.error?.message).toContain(
			"Field 'notInSchema' not found in schema",
		);
		expect(result.data).toBeUndefined();
	});

	it("should only return entities of the queried type", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
				posts: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new Store();
		const database = createDatabase(schema, store);

		database.users.create({ name: "John Doe" });
		database.posts.create({ name: "Jane Doe" });

		const result = database.users.query({ fields: { name: true } });
		expect(result.data).toEqual([{ name: "John Doe" }]);

		const result2 = database.posts.query({ fields: { name: true } });
		expect(result2.data).toEqual([{ name: "Jane Doe" }]);
	});

	it("should correctly retrieve entities that only have optional fields, even when created with no data", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ optional: true }),
					age: t.number({ optional: true }),
				},
			},
		});

		const store = new Store();
		const database = createDatabase(schema, store);

		const user1 = database.users.create({});
		assert(user1.data !== undefined, "Expected create to succeed");
		const user2 = database.users.create({});
		assert(user2.data !== undefined, "Expected create to succeed");

		const result = database.users.query({
			fields: { id: true, name: true, age: true },
		});
		result.data?.sort((a, b) => (a.id.localeCompare(b.id) ? 1 : -1));
		expect(result.data).toEqual([{ id: user1.data.id }, { id: user2.data.id }]);
	});

	it("should apply fallbacks for multiple required fields of different types", () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ optional: true, fallback: "string" }),
					isVerified: t.boolean({ optional: true, fallback: false }),
				},
			},
		});

		const store = new Store();
		const database = createDatabase(schema, store);

		const user1 = database.users.create({});
		assert(user1.data !== undefined, "Expected create to succeed");
		const user2 = database.users.create({});
		assert(user2.data !== undefined, "Expected create to succeed");

		const result = database.users.query({
			fields: { id: true, name: true, isVerified: true },
		});
		result.data?.sort((a, b) => (a.id.localeCompare(b.id) ? 1 : -1));
		expect(result.data).toEqual([
			{ id: user1.data.id, name: "string", isVerified: false },
			{ id: user2.data.id, name: "string", isVerified: false },
		]);
	});
});
