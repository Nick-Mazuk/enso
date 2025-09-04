import { expect, expectTypeOf, test } from "bun:test";
import { Client, createClient, type DatabaseAPI } from "./client";
import { createSchema, t, type StringField } from "./schema";

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

  // @ts-expect-error
  const result3 = await client.database.users.create({
    age: 10,
  });

  expect(result3.error).toBeDefined();
  expect(result3.data).toBeUndefined();
});

test("client.database.users.query() retrieves users", async () => {
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

  await client.database.users.create({ name: "Alice", age: 30 });
  await client.database.users.create({ name: "Bob", age: 25 });

  const { data: users, error } = await client.database.users.query({
    fields: {
      id: true,
      name: true,
      age: true,
      createdAt: true,
      updatedAt: true,
    },
  });

  expect(error).toBeUndefined();
  expect(users).toBeDefined();
  expect(users?.length).toBe(2);
  users?.sort((a, b) => a.name.localeCompare(b.name));
  expect(users?.[0]).toEqual({
    id: expect.any(String),
    name: "Alice",
    age: 30,
    createdAt: expect.any(Date),
    updatedAt: expect.any(Date),
  });
});

test("client.database.users.query() projects fields correctly", async () => {
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

  await client.database.users.create({ name: "Alice", age: 30 });

  const { data: users, error } = await client.database.users.query({
    fields: {
      name: true,
    },
  });

  expect(error).toBeUndefined();
  expect(users).toBeDefined();
  expect(users?.[0]).toEqual({ name: "Alice" });

  const { data: usersWithId, error: idError } =
    await client.database.users.query({
      fields: {
        id: true,
        name: true,
      },
    });

  expect(idError).toBeUndefined();
  expect(usersWithId?.[0]).toEqual({
    id: expect.any(String),
    name: "Alice",
  });
});

test("Client database only has defined entities", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
      },
    },
  });
  const client = createClient({ schema });
  expectTypeOf(client.database).toEqualTypeOf<
    DatabaseAPI<{
      entities: {
        users: {
          name: StringField;
        };
      };
    }>
  >();
});

test("client.database correctly enforces optional and required fields", () => {
  const schema = createSchema({
    entities: {
      users: {
        // required
        name: t.string({ fallback: "" }),

        // optional
        email: t.string({ optional: true }),
        verified: t.boolean({ optional: true, fallback: false }),
      },
    },
    rooms: {},
  });

  const client = createClient({ schema });

  expectTypeOf(client.database.users.create).parameters.toEqualTypeOf<
    [
      {
        name: string;
        email?: string;
        verified?: boolean;
      }
    ]
  >();
});
