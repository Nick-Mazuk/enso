import { describe, expect, expectTypeOf, it } from "bun:test";
import { assert } from "../../../shared/assert";
import { createSchema, t } from "../../index";
import { Store } from "../store";
import { Field, Id, Value } from "../store/types";
import { createDatabase } from "./index";
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
		assert(result.success, "Expected the query to work");
		// biome-ignore lint/suspicious/noExplicitAny: needed to test runtime behavior
		expect((result as any).error).toBeUndefined();
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
			success: false,
			error: {
				message: `Missing required field "name" when creating entity "users"`,
			},
		});
	});
});

describe("database.entity.query", () => {
	it("can query for all schema fields", async () => {
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

		const result = await database.users.query({
			fields: { age: true, name: true },
		});
		expectTypeOf(result).toEqualTypeOf<
			DatabaseResult<{ name: string; age?: number }[]>
		>();
		expect(result).toEqual({
			success: true,
			data: [{ name: "John Doe", age: 30 }],
		});
	});

	it("objects with missing optional fields are still returned", async () => {
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

		const result = await database.users.query({
			fields: { age: true, name: true },
		});
		expectTypeOf(result).toEqualTypeOf<
			DatabaseResult<{ name: string; age?: number }[]>
		>();
		expect(result).toEqual({
			success: true,
			data: [{ name: "John Doe", age: 30 }, { name: "John Doe" }],
		});
	});

	it("can query for the auto-generated id", async () => {
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
		assert(user.success, "User was not created successfully");

		const result = await database.users.query({ fields: { id: true } });
		expectTypeOf(result).toEqualTypeOf<DatabaseResult<{ id: string }[]>>();
		expect(result).toEqual({
			success: true,
			data: [{ id: user.data.id }],
		});
	});

	it("can query for auto-generated and schema fields in the same query", async () => {
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
		assert(user.success, "User was not created successfully");

		const result = await database.users.query({
			fields: { name: true, id: true },
		});
		expectTypeOf(result).toEqualTypeOf<
			DatabaseResult<{ name: string; id: string }[]>
		>();
		expect(result).toEqual({
			success: true,
			data: [{ id: user.data.id, name: "John Doe" }],
		});
	});

	it("should apply fallback for required fields when data is missing", async () => {
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
		assert(user1.success, "User 1 was not created successfully");

		// User 2: Simulate a user with missing 'name' triple.
		// We do this by adding triples directly to the store,
		// bypassing the database.create() guarantees.
		const user2Id = "id-user-2";
		store.add(
			[Id(user2Id), Field("users/id"), Value(user2Id)],
			[Id(user2Id), Field("users/age"), Value(40)],
			// DO NOT add the 'users/name' triple
		);

		const result = await database.users.query({
			fields: { name: true, age: true },
		});
		assert(result.success, "expected the query to succeed");

		// biome-ignore lint/suspicious/noExplicitAny: needed to test runtime behavior
		expect((result as any).error).toBeUndefined();
		expect(result.data.length).toBe(2);

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

	it("should apply fallback for an optional field when data is missing", async () => {
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

		const result = await database.users.query({
			fields: { age: true },
		});
		assert(result.success, "expected query to succeed");

		// biome-ignore lint/suspicious/noExplicitAny: needed to test runtime behavior
		expect((result as any).error).toBeUndefined();
		expect(result.data?.length).toBe(2);

		// Sort by age for stable test
		const sortedData = result.data?.sort((a, b) => (a.age || 0) - (b.age || 0));

		expect(sortedData).toEqual([
			{ age: 30 },
			{ age: 42 }, // Uses the fallback
		]);
	});

	it("should return an empty array when querying an empty database", async () => {
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

		const result = await database.users.query({
			fields: { name: true, age: true },
		});
		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([]);
	});

	it("should return an empty array when querying for no fields in an empty database", async () => {
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

		const result = await database.users.query({ fields: {} });
		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([]);
	});

	it("should return an array of empty objects when querying for no fields", async () => {
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

		const result = await database.users.query({ fields: {} });
		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{}, {}]);
	});

	it("should throw an error when querying for a field that is not in the schema", async () => {
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

		const result = await database.users.query({
			fields: {
				name: true,
				age: true,
				// @ts-expect-error
				notInSchema: true,
			},
		});
		expect(result).toEqual({
			success: false,
			error: {
				message: "Field 'notInSchema' not found in schema",
			},
		});
	});

	it("should only return entities of the queried type", async () => {
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

		const result = await database.users.query({ fields: { name: true } });
		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ name: "John Doe" }]);

		const result2 = await database.posts.query({ fields: { name: true } });
		assert(result2.success, "expected query to succeed");
		expect(result2.data).toEqual([{ name: "Jane Doe" }]);
	});

	it("should correctly retrieve entities that only have optional fields, even when created with no data", async () => {
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
		assert(user1.success, "Expected create to succeed");
		const user2 = database.users.create({});
		assert(user2.success, "Expected create to succeed");

		const result = await database.users.query({
			fields: { id: true, name: true, age: true },
		});
		assert(result.success, "expected query to succeed");
		result.data?.sort((a, b) => (a.id.localeCompare(b.id) ? 1 : -1));
		expect(result.data).toEqual([{ id: user1.data.id }, { id: user2.data.id }]);
	});

	it("should apply fallbacks for multiple required fields of different types", async () => {
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
		assert(user1.success, "Expected create to succeed");
		const user2 = database.users.create({});
		assert(user2.success, "Expected create to succeed");

		const result = await database.users.query({
			fields: { id: true, name: true, isVerified: true },
		});
		assert(result.success, "expected query to succeed");
		result.data?.sort((a, b) => (a.id.localeCompare(b.id) ? 1 : -1));
		expect(result.data).toEqual([
			{ id: user1.data.id, name: "string", isVerified: false },
			{ id: user2.data.id, name: "string", isVerified: false },
		]);
	});

	describe("filters", () => {
		it("Boolean: is undefined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						isVerified: t.boolean({
							optional: true,
							fallback: false,
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // isVerified is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					isVerified: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user2.data.id, name: "user 2" }]);
		});

		it("Boolean: is undefined still returns fallback", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						isVerified: t.boolean({
							optional: true,
							fallback: false,
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // isVerified is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true, isVerified: true },
				where: {
					isVerified: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([
				{ id: user2.data.id, name: "user 2", isVerified: false },
			]);
		});

		it("Boolean: is defined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						isVerified: t.boolean({
							optional: true,
							fallback: false,
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // isVerified is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					isVerified: { isDefined: true },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user1.data.id, name: "user 1" }]);
		});

		it("string: is undefined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.string({
							optional: true,
							fallback: "",
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					maybeDefined: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user2.data.id, name: "user 2" }]);
		});

		it("string: is undefined still returns fallback", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.string({
							optional: true,
							fallback: "",
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true, maybeDefined: true },
				where: {
					maybeDefined: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([
				{ id: user2.data.id, name: "user 2", maybeDefined: "" },
			]);
		});

		it("string: is defined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.string({
							optional: true,
							fallback: "",
						}),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					maybeDefined: { isDefined: true },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user1.data.id, name: "user 1" }]);
		});

		it("number: is undefined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.number({ optional: true, fallback: 0 }),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					maybeDefined: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user2.data.id, name: "user 2" }]);
		});

		it("number: is undefined still returns fallback", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.number({ optional: true, fallback: 0 }),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true, maybeDefined: true },
				where: {
					maybeDefined: { isDefined: false },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([
				{ id: user2.data.id, name: "user 2", maybeDefined: 0 },
			]);
		});

		it("number: is defined", async () => {
			const schema = createSchema({
				entities: {
					users: {
						name: t.string({ optional: true, fallback: "string" }),
						maybeDefined: t.number({ optional: true, fallback: 0 }),
					},
				},
			});

			const store = new Store();
			const database = createDatabase(schema, store);

			const user1 = database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = database.users.create({ name: "user 2" }); // maybeDefined is not defined
			assert(user2.success, "Expected create to succeed");

			const result = await database.users.query({
				fields: { id: true, name: true },
				where: {
					maybeDefined: { isDefined: true },
				},
			});
			assert(result.success, "expected query to succeed");
			expect(result.data).toEqual([{ id: user1.data.id, name: "user 1" }]);
		});
	});
});

describe("database.entity.query number filters", () => {
	it("can filter by equals", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { equals: 10 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 10 }]);
	});

	it("can filter by notEquals", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { notEquals: 10 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 20 }]);
	});

	it("can filter by greaterThan", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });
		db.items.create({ val: 30 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 15 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(2);
		expect(result.data).toContainEqual({ val: 20 });
		expect(result.data).toContainEqual({ val: 30 });
	});

	it("can filter by greaterThanOrEqual", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThanOrEqual: 20 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 20 }]);
	});

	it("can filter by lessThan", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { lessThan: 15 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 10 }]);
	});

	it("can filter by lessThanOrEqual", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { lessThanOrEqual: 10 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 10 }]);
	});

	it("can combine range filters", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 5 });
		db.items.create({ val: 10 });
		db.items.create({ val: 15 });
		db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 5, lessThan: 20 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(2);
		expect(result.data).toContainEqual({ val: 10 });
		expect(result.data).toContainEqual({ val: 15 });
	});
});

describe("database.entity.query number filters edge cases", () => {
	it("supports floating point numbers", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({ val: 10.5 });
		db.items.create({ val: 20.1 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 10.6 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 20.1 }]);
	});

	it("throws runtime error if number filter applied to string field", async () => {
		const schema = createSchema({
			entities: {
				items: {
					str: t.string({ fallback: "" }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		const promise = db.items.query({
			fields: { str: true },
			// @ts-expect-error - testing runtime check
			where: { str: { greaterThan: 10 } },
		});

		expect(promise).rejects.toThrow("Field 'str' is not a number field");
	});

	it("throws runtime error if number filter applied to boolean field", async () => {
		const schema = createSchema({
			entities: {
				items: {
					bool: t.boolean({ fallback: false }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		const promise = db.items.query({
			fields: { bool: true },
			// @ts-expect-error - testing runtime check
			where: { bool: { equals: 10 } },
		});

		expect(promise).rejects.toThrow("Field 'bool' is not a number field");
	});

	it("uses fallback value for filtering if field is missing but has fallback", async () => {
		const schema = createSchema({
			entities: {
				items: {
					// Optional field with fallback
					val: t.number({ optional: true, fallback: 100 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		// Create item without the field. It should take fallback 100.
		db.items.create({});
		// Create item with explicit field.
		db.items.create({ val: 50 });

		const result = await db.items.query({
			fields: { id: true, val: true },
			where: { val: { greaterThan: 80 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(1);
		expect(result.data[0]?.val).toBe(100);
	});

	it("uses fallback value for filtering even if field is not in result projection", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ optional: true, fallback: 100 }),
				},
			},
		});
		const store = new Store();
		const db = createDatabase(schema, store);

		db.items.create({}); // Should be 100
		db.items.create({ val: 50 });

		const result = await db.items.query({
			fields: { id: true }, // Not asking for 'val'
			where: { val: { greaterThan: 80 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(1);
	});
});
