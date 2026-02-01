import { describe, expect, expectTypeOf, it } from "bun:test";
import { assert } from "../../../shared/assert.js";
import { createSchema, t } from "../../index.js";
import { MockStore } from "../store/testing/index.js";
import { Field, Id, Value } from "../store/types.js";
import { createDatabase } from "./index.js";
import type { DatabaseResult } from "./types.js";

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
		const store = new MockStore();
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
		const store = new MockStore();
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

	it("creates a triple in the store", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
					age: t.number({ optional: true }),
				},
			},
		});
		const store = new MockStore();
		const database = createDatabase(schema, store);
		const result = await database.users.create({ name: "John Doe", age: 30 });
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

	it("create errors if required field is not present", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});

		const store = new MockStore();
		const database = createDatabase(schema, store);
		const result = await database.users.create(
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		await database.users.create({ name: "John Doe", age: 30 });

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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		await database.users.create({ name: "John Doe", age: 30 });
		await database.users.create({ name: "John Doe" });

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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user = await database.users.create({ name: "John Doe", age: 30 });
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user = await database.users.create({ name: "John Doe", age: 30 });
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		// User 1: Create a normal user
		const user1 = await database.users.create({ name: "John Doe", age: 30 });
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		// User 1: Create a normal user
		await database.users.create({ age: 30 });
		await database.users.create({});

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
		const store = new MockStore();
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
		const store = new MockStore();
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		await database.users.create({ name: "John Doe", age: 30 });
		await database.users.create({ name: "Jane Doe" });

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
		const store = new MockStore();
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
		const store = new MockStore();
		const database = createDatabase(schema, store);

		await database.users.create({ name: "John Doe" });
		await database.posts.create({ name: "Jane Doe" });

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

		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user1 = await database.users.create({});
		assert(user1.success, "Expected create to succeed");
		const user2 = await database.users.create({});
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

		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user1 = await database.users.create({});
		assert(user1.success, "Expected create to succeed");
		const user2 = await database.users.create({});
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // isVerified is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // isVerified is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				isVerified: true,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // isVerified is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: "hello",
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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

			const store = new MockStore();
			const database = createDatabase(schema, store);

			const user1 = await database.users.create({
				name: "user 1",
				maybeDefined: 1,
			});
			assert(user1.success, "Expected create to succeed");
			const user2 = await database.users.create({ name: "user 2" }); // maybeDefined is not defined
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
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { equals: 10 } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 10 }]);
	});

	it("returns error for notEquals filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { notEquals: 10 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'notEquals' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for greaterThan filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });
		await db.items.create({ val: 30 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 15 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for greaterThanOrEqual filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThanOrEqual: 20 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThanOrEqual' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for lessThan filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { lessThan: 15 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'lessThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for lessThanOrEqual filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { lessThanOrEqual: 10 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'lessThanOrEqual' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for combined range filters (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 5 });
		await db.items.create({ val: 10 });
		await db.items.create({ val: 15 });
		await db.items.create({ val: 20 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 5, lessThan: 20 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});
});

describe("database.entity.create with refs", () => {
	it("can create an entity with a ref", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "Guest" }),
				},
				posts: {
					title: t.string({ fallback: "Untitled" }),
					authorId: t.ref("users"),
				},
			},
		});
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user = await database.users.create({ name: "Alice" });
		assert(user.success, "User creation failed");

		const post = await database.posts.create({
			title: "Hello World",
			authorId: user.data.id,
		});
		assert(post.success, "Post creation failed");

		expect(post.data.authorId).toBe(user.data.id);

		const result = await database.posts.query({
			fields: { title: true, authorId: true },
		});

		assert(result.success, "Query failed");
		expect(result.data).toEqual([
			{ title: "Hello World", authorId: user.data.id },
		]);
	});

	it("ref is optional", async () => {
		const schema = createSchema({
			entities: {
				posts: {
					authorId: t.ref("users"),
				},
			},
		});
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const post = await database.posts.create({});
		assert(post.success, "Post creation failed");
		expect(post.data.authorId).toBeUndefined();

		const result = await database.posts.query({
			fields: { authorId: true },
		});

		assert(result.success, "Query failed");
		expect(result.data).toEqual([{}]);
	});

	it("can filter by ref", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
				posts: {
					authorId: t.ref("users"),
				},
			},
		});
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user1 = await database.users.create({ name: "Alice" });
		const user2 = await database.users.create({ name: "Bob" });

		assert(user1.success, "User1 creation failed");
		assert(user2.success, "User2 creation failed");

		await database.posts.create({ authorId: user1.data.id });
		await database.posts.create({ authorId: user2.data.id });

		const result = await database.posts.query({
			fields: { authorId: true },
			where: { authorId: { equals: user1.data.id } },
		});

		assert(result.success, "Query failed");
		expect(result.data).toHaveLength(1);
		expect(result.data[0]?.authorId).toBe(user1.data.id);
	});

	it("throws for ref notEquals filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
				posts: {
					authorId: t.ref("users"),
				},
			},
		});
		const store = new MockStore();
		const database = createDatabase(schema, store);

		const user1 = await database.users.create({ name: "Alice" });
		const user2 = await database.users.create({ name: "Bob" });

		assert(user1.success, "User1 creation failed");
		assert(user2.success, "User2 creation failed");

		await database.posts.create({ authorId: user1.data.id });
		await database.posts.create({ authorId: user2.data.id });

		const result = await database.posts.query({
			fields: { authorId: true },
			where: { authorId: { notEquals: user1.data.id } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'notEquals' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});
});

describe("database.entity.query number filters edge cases", () => {
	it("returns error for greaterThan filter with floating point (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 10.5 });
		await db.items.create({ val: 20.1 });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { greaterThan: 10.6 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error if number filter applied to string field", async () => {
		const schema = createSchema({
			entities: {
				items: {
					str: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const result = await db.items.query({
			fields: { str: true },
			// @ts-expect-error - testing runtime check
			where: { str: { greaterThan: 10 } },
		});

		expect(result).toEqual({
			success: false,
			error: {
				message: "Filter 'greaterThan' not allowed on str which is a string",
			},
		});
	});

	it("returns error if number filter has a string config", async () => {
		const schema = createSchema({
			entities: {
				items: {
					num: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const result = await db.items.query({
			fields: { num: true },
			where: {
				num: {
					// @ts-expect-error - testing runtime check
					equals: "10",
				},
			},
		});

		expect(result).toEqual({
			success: false,
			error: {
				message: "Expected filter equals on num to be a number",
			},
		});
	});

	it("returns error if number filter applied to boolean field", async () => {
		const schema = createSchema({
			entities: {
				items: {
					bool: t.boolean({ fallback: false }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const result = await db.items.query({
			fields: { bool: true },
			// @ts-expect-error - testing runtime check
			where: { bool: { equals: 10 } },
		});

		expect(result).toEqual({
			success: false,
			error: {
				message: "Expected filter equals on bool to be a boolean",
			},
		});
	});

	it("returns error for greaterThan filter with fallback (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					// Optional field with fallback
					val: t.number({ optional: true, fallback: 100 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		// Create item without the field. It should take fallback 100.
		await db.items.create({});
		// Create item with explicit field.
		await db.items.create({ val: 50 });

		const result = await db.items.query({
			fields: { id: true, val: true },
			where: { val: { greaterThan: 80 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for greaterThan filter on non-projected field (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ optional: true, fallback: 100 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({});
		await db.items.create({ val: 50 });

		const result = await db.items.query({
			fields: { id: true }, // Not asking for 'val'
			where: { val: { greaterThan: 80 } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'greaterThan' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});
});

describe("database.entity.query boolean filters", () => {
	it("can filter by equals", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.boolean({ fallback: false }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: true });
		await db.items.create({ val: false });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { equals: true } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: true }]);
	});

	it("can filter by equals (false)", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.boolean({ fallback: false }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: true });
		await db.items.create({ val: false });

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { equals: false } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: false }]);
	});

	it("uses fallback value for filtering if field is missing but has fallback", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.boolean({ optional: true, fallback: true }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		// Create item without the field. It should take fallback true.
		await db.items.create({});
		// Create item with explicit field.
		await db.items.create({ val: false });

		const result = await db.items.query({
			fields: { id: true, val: true },
			where: { val: { equals: true } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(1);
		expect(result.data[0]?.val).toBe(true);
	});
});

describe("database.entity.query string filters", () => {
	it("can filter by equals", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "Alice" });
		await db.users.create({ name: "Bob" });

		const result = await db.users.query({
			fields: { name: true },
			where: { name: { equals: "Alice" } },
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ name: "Alice" }]);
	});

	it("returns error for notEquals filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "Alice" });
		await db.users.create({ name: "Bob" });

		const result = await db.users.query({
			fields: { name: true },
			where: { name: { notEquals: "Alice" } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'notEquals' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for contains filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "Alice" });
		await db.users.create({ name: "Alicia" });
		await db.users.create({ name: "Bob" });

		const result = await db.users.query({
			fields: { name: true },
			where: { name: { contains: "Ali" } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'contains' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for startsWith filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "Alice" });
		await db.users.create({ name: "Alicia" });
		await db.users.create({ name: "Bob" });

		const result = await db.users.query({
			fields: { name: true },
			where: { name: { startsWith: "Ali" } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'startsWith' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error for endsWith filter (not implemented)", async () => {
		const schema = createSchema({
			entities: {
				users: {
					name: t.string({ fallback: "" }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "Alice" });
		await db.users.create({ name: "Beatrice" });
		await db.users.create({ name: "Bob" });

		const result = await db.users.query({
			fields: { name: true },
			where: { name: { endsWith: "ce" } },
		});
		expect(result).toEqual({
			success: false,
			error: {
				message:
					"Filter 'endsWith' is not implemented. Only 'equals' and 'isDefined' filters are currently supported.",
			},
		});
	});

	it("returns error if string filter applied to number field", async () => {
		const schema = createSchema({
			entities: {
				items: {
					num: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const result = await db.items.query({
			fields: { num: true },
			// @ts-expect-error - testing runtime check
			where: { num: { includes: "invalid" } },
		});

		expect(result).toEqual({
			success: false,
			error: {
				message: "Filter 'includes' not allowed on num which is a number",
			},
		});
	});
});

describe("database.entity.query limit", () => {
	it("limits the number of results", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await Promise.all(
			Array.from({ length: 10 }, (_, i) => db.items.create({ val: i })),
		);

		const result = await db.items.query({
			fields: { val: true },
			limit: 5,
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(5);
	});

	it("returns all results if limit is greater than count", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 1 });
		await db.items.create({ val: 2 });

		const result = await db.items.query({
			fields: { val: true },
			limit: 10,
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(2);
	});

	it("returns empty array if limit is 0", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 1 });

		const result = await db.items.query({
			fields: { val: true },
			limit: 0,
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(0);
	});

	it("applies limit after equals filter", async () => {
		const schema = createSchema({
			entities: {
				items: {
					val: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		// Creates vals: 0,1,2,0,1,2,0,1,2,0
		await Promise.all(
			Array.from({ length: 10 }, (_, i) => db.items.create({ val: i % 3 })),
		);

		const result = await db.items.query({
			fields: { val: true },
			where: { val: { equals: 1 } },
			limit: 2,
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toHaveLength(2);
		for (const item of result.data) {
			expect(item.val).toBe(1);
		}
	});
});

describe("database.entity.query sorting", () => {
	it("sorts by string field asc", async () => {
		const schema = createSchema({
			entities: {
				users: { name: t.string({ fallback: "" }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "C" });
		await db.users.create({ name: "A" });
		await db.users.create({ name: "B" });

		const result = await db.users.query({
			fields: { name: true },
			orderBy: ["name", "asc"],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ name: "A" }, { name: "B" }, { name: "C" }]);
	});

	it("sorts by string field desc", async () => {
		const schema = createSchema({
			entities: {
				users: { name: t.string({ fallback: "" }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ name: "A" });
		await db.users.create({ name: "C" });
		await db.users.create({ name: "B" });

		const result = await db.users.query({
			fields: { name: true },
			orderBy: ["name", "desc"],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ name: "C" }, { name: "B" }, { name: "A" }]);
	});

	it("sorts by number field asc", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.number({ fallback: 0 }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 3 });
		await db.items.create({ val: 1 });
		await db.items.create({ val: 2 });

		const result = await db.items.query({
			fields: { val: true },
			orderBy: ["val", "asc"],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 1 }, { val: 2 }, { val: 3 }]);
	});

	it("sorts by number field desc", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.number({ fallback: 0 }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 1 });
		await db.items.create({ val: 3 });
		await db.items.create({ val: 2 });

		const result = await db.items.query({
			fields: { val: true },
			orderBy: ["val", "desc"],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 3 }, { val: 2 }, { val: 1 }]);
	});

	it("sorts by boolean field asc (false < true)", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.boolean({ fallback: false }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: true });
		await db.items.create({ val: false });
		await db.items.create({ val: true });

		const result = await db.items.query({
			fields: { val: true },
			orderBy: ["val", "asc"],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: false }, { val: true }, { val: true }]);
	});

	it("sorts by multiple fields", async () => {
		const schema = createSchema({
			entities: {
				users: {
					group: t.string({ fallback: "" }),
					score: t.number({ fallback: 0 }),
				},
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.users.create({ group: "A", score: 10 });
		await db.users.create({ group: "B", score: 20 });
		await db.users.create({ group: "A", score: 5 });
		await db.users.create({ group: "B", score: 15 });

		const result = await db.users.query({
			fields: { group: true, score: true },
			orderBy: [
				["group", "asc"],
				["score", "desc"],
			],
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([
			{ group: "A", score: 10 },
			{ group: "A", score: 5 },
			{ group: "B", score: 20 },
			{ group: "B", score: 15 },
		]);
	});

	it("puts undefined values last regardless of direction (asc)", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.number({ optional: true }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const i1 = await db.items.create({ val: 1 });
		const i2 = await db.items.create({}); // undefined
		const i3 = await db.items.create({ val: 2 });
		assert(i1.success, "Expect item to be created");
		assert(i2.success, "Expect item to be created");
		assert(i3.success, "Expect item to be created");

		const result = await db.items.query({
			fields: { id: true, val: true },
			orderBy: ["val", "asc"],
		});

		assert(result.success, "expected query to succeed");
		// defined values first sorted, then undefined
		expect(result.data).toEqual([
			{ id: i1.data.id, val: 1 },
			{ id: i3.data.id, val: 2 },
			{ id: i2.data.id },
		]);
	});

	it("puts undefined values last regardless of direction (desc)", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.number({ optional: true }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		const i1 = await db.items.create({ val: 1 });
		const i2 = await db.items.create({}); // undefined
		const i3 = await db.items.create({ val: 2 });
		assert(i1.success, "Expect item to be created");
		assert(i2.success, "Expect item to be created");
		assert(i3.success, "Expect item to be created");

		const result = await db.items.query({
			fields: { id: true, val: true },
			orderBy: ["val", "desc"],
		});

		assert(result.success, "expected query to succeed");
		// defined values first (sorted desc), then undefined
		expect(result.data).toEqual([
			{ id: i3.data.id, val: 2 },
			{ id: i1.data.id, val: 1 },
			{ id: i2.data.id },
		]);
	});

	it("sorts by id", async () => {
		const schema = createSchema({
			entities: {
				users: { name: t.string({ fallback: "" }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		// IDs are random, so we can't predict them, but we can verify the order is consistent with the values
		const u1 = await db.users.create({ name: "A" });
		const u2 = await db.users.create({ name: "B" });
		assert(u1.success, "Expect item to be created");
		assert(u2.success, "Expect item to be created");

		const result = await db.users.query({
			fields: { id: true, name: true },
			orderBy: ["id", "asc"],
		});

		assert(result.success, "expected query to succeed");
		const sortedIds = [u1.data.id, u2.data.id].sort();

		expect(result.data.map((d) => d.id)).toEqual(sortedIds);
	});

	it("applies sorting before limit", async () => {
		const schema = createSchema({
			entities: {
				items: { val: t.number({ fallback: 0 }) },
			},
		});
		const store = new MockStore();
		const db = createDatabase(schema, store);

		await db.items.create({ val: 5 });
		await db.items.create({ val: 1 });
		await db.items.create({ val: 10 });
		await db.items.create({ val: 2 });

		const result = await db.items.query({
			fields: { val: true },
			orderBy: ["val", "asc"],
			limit: 2,
		});

		assert(result.success, "expected query to succeed");
		expect(result.data).toEqual([{ val: 1 }, { val: 2 }]);
	});
});
