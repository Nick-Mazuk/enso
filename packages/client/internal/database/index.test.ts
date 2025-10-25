// biome-ignore assist/source/organizeImports: something not working here
import { describe, expect, expectTypeOf, it } from "bun:test";
import { createDatabase } from ".";
import { createSchema } from "../../index";

describe("createDatabase", () => {
	it("creates a database the correct keys", () => {
		const schema = createSchema({
			entities: {
				users: {},
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
