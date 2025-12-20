import { describe, expect, expectTypeOf, it } from "bun:test";
import { createClient, createSchema } from "./index.js";
import type { Database } from "./internal/database/types.js";

describe("createClient", () => {
	it("client has a database", () => {
		const schema = createSchema({
			entities: {
				users: {},
				posts: {},
				tags: {},
			},
		});
		const client = createClient({ schema });
		expect(Object.keys(client.database)).toEqual(["users", "posts", "tags"]);
		expectTypeOf(client.database).toEqualTypeOf<Database<typeof schema>>();
	});
});
