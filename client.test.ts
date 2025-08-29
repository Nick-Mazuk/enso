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
});

test("client.database.users.create() creates a new user", async () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
      },
    },
    rooms: {},
  });

  const client = createClient({ schema });

  const result = await client.database.users.create({
    name: "test",
  });

  expect(result.error).toBeUndefined();
  expect(result.data).toBeDefined();
  expect(result.data?.id).toBeString();
  expect(result.data?.createdAt).toBeDate();
  expect(result.data?.updatedAt).toBeDate();
  expect(result.data?.name).toBe("test");

  const result2 = await client.database.users.create({
    name: "test2",
    age: 10,
  });

  expect(result2.error).toBeUndefined();
  expect(result2.data?.age).toBe(10);

  const result3 = await client.database.users.create({
    age: 10,
  });

  expect(result3.error).toBeDefined();
  expect(result3.data).toBeUndefined();
});
