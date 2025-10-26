import { describe, expect, expectTypeOf, it } from "bun:test";
import { createSchema, t } from "../../index";
import { createDatabase } from "./create";

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
		const database = createDatabase(schema);
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
		const database = createDatabase(schema);
		expectTypeOf(database.users.create).parameters.toEqualTypeOf<
			[
				{
					name: string;
					isActive: boolean;
				} & {
					age?: number | undefined;
					isAuthor?: boolean | undefined;
				},
			]
		>();
	});
});
