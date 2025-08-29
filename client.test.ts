import { expect, test } from "bun:test";
import { Client, createClient } from "./client";
import { createSchema, t } from "./schema";

test("createClient initializes a client with a dynamic database API", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
      },
      posts: {
        title: t.string({ fallback: "" }),
      },
    },
    rooms: {},
  });

  const client = createClient({ schema });

  expect(client).toBeInstanceOf(Client);
  expect(client.database.users).toBeDefined();
  expect(client.database.posts).toBeDefined();
  expect(client.database.tags).toBeUndefined();
});
