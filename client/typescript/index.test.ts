import { describe, expect, expectTypeOf, it } from "bun:test";
import { createClient, createSchema } from "./index.js";
import type { Database } from "./internal/database/types.js";

describe("createClient", () => {
	it("client has a database", async () => {
		const schema = createSchema({
			entities: {
				users: {},
				posts: {},
				tags: {},
			},
		});
		const client = await createClient({ schema });
		expect(Object.keys(client.database)).toEqual(["users", "posts", "tags"]);
		expectTypeOf(client.database).toEqualTypeOf<Database<typeof schema>>();
	});

	it("throws error if serverUrl provided without apiKey", async () => {
		const schema = createSchema({ entities: {} });
		await expect(
			createClient({ schema, serverUrl: "ws://localhost:8080" }),
		).rejects.toThrow("apiKey is required when serverUrl is provided");
	});
});
