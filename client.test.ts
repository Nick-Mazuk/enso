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

test("client.database.users.create() creates a new user", () => {
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

  const result = client.database.users.create({
    name: "test",
  });

  expect(result.error).toBeUndefined();
  expect(result.data).toBeDefined();
  expect(result.data?.id).toBeString();
  expect(result.data?.createdAt).toBeDate();
  expect(result.data?.updatedAt).toBeDate();
  expect(result.data?.name).toBe("test");

  const result2 = client.database.users.create({
    name: "test2",
    age: 10,
  });

  expect(result2.error).toBeUndefined();
  expect(result2.data?.age).toBe(10);

  // @ts-expect-error
  const result3 = client.database.users.create({
    age: 10,
  });

  expect(result3.error).toStrictEqual({
    code: "VALIDATION_FAILED",
    message: "Validation failed",
  });
  expect(result3.data).toBeUndefined();
});

test("client.database.users.query() retrieves users", () => {
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

  client.database.users.create({ name: "Alice", age: 30 });
  client.database.users.create({ name: "Bob", age: 25 });

  const { data: users, error } = client.database.users.query({
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

test("client.database.users.query() projects fields correctly", () => {
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

  client.database.users.create({ name: "Alice", age: 30 });

  const { data: users, error } = client.database.users.query({
    fields: {
      name: true,
    },
  });

  expectTypeOf(users).toEqualTypeOf<{ name: string }[] | undefined>();
  expect(error).toBeUndefined();
  expect(users).toBeDefined();
  expect(users?.[0]).toEqual({ name: "Alice" });

  const { data: usersWithId, error: idError } = client.database.users.query({
    fields: {
      id: true,
      name: true,
    },
  });

  expectTypeOf(usersWithId).toEqualTypeOf<
    { id: string; name: string }[] | undefined
  >();
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

test("client.database.users.update() updates a user", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
      },
    },
  });
  const client = createClient({ schema });
  const { data: user } = client.database.users.create({ name: "test" });
  expect(user).toBeDefined();
  if (!user) return;

  const { error } = client.database.users.update({
    id: user.id,
    fields: { name: "updated" },
  });
  expect(error).toBeUndefined();

  const { data: users } = client.database.users.query({
    fields: { name: true },
  });
  expect(users?.[0]?.name).toBe("updated");
});

test("client.database.users.update() with functional update", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
        age: t.number({ fallback: 0 }),
      },
    },
  });
  const client = createClient({ schema });
  const { data: user } = client.database.users.create({
    name: "test",
    age: 10,
  });
  if (!user) return;

  const { error } = client.database.users.update({
    id: user.id,
    fields: (prev) => ({ age: prev.age + 5 }),
  });
  expect(error).toBeUndefined();

  const { data: users } = client.database.users.query({
    fields: { age: true },
  });
  expect(users?.[0]?.age).toBe(15);
});

test("client.database.users.replace() replaces a user", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
      },
    },
  });
  const client = createClient({ schema });
  const { data: user } = client.database.users.create({
    name: "test",
    age: 10,
  });
  expect(user).toBeDefined();
  if (!user) return;

  const { error } = client.database.users.replace({
    id: user.id,
    fields: { name: "replaced" },
  });
  expect(error).toBeUndefined();

  const { data: users } = client.database.users.query({
    fields: { name: true, age: true },
  });
  expect(users?.[0]?.name).toBe("replaced");
  expect(users?.[0]?.age).toBeUndefined();
});

test("client.database.users.replace() with functional update", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
        age: t.number({ fallback: 0 }),
      },
    },
  });

  const client = createClient({ schema });
  const { data: user } = client.database.users.create({
    name: "test",
    age: 10,
  });
  if (!user) return;

  const { error } = client.database.users.replace({
    id: user.id,
    fields: (prev) => ({ name: "replaced", age: prev.age + 5 }),
  });

  expect(error).toBeUndefined();

  const { data: users } = client.database.users.query({
    fields: { name: true, age: true },
  });

  expect(users?.[0]?.name).toBe("replaced");
  expect(users?.[0]?.age).toBe(15);
});

test("client.database.users.delete() deletes a user", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
      },
    },
  });
  const client = createClient({ schema });
  const { data: user1 } = client.database.users.create({
    name: "test1",
  });
  const { data: user2 } = client.database.users.create({
    name: "test2",
  });
  expect(user1).toBeDefined();
  expect(user2).toBeDefined();
  if (!user1 || !user2) return;

  const { error } = client.database.users.delete(user1.id);
  expect(error).toBeUndefined();

  const { data: users } = client.database.users.query({
    fields: { name: true },
  });
  expect(users?.length).toBe(1);
  expect(users?.[0]?.name).toBe("test2");
});

test("client.database.users.delete() with non-existent user", () => {
  const schema = createSchema({
    entities: {
      users: {
        name: t.string({ fallback: "" }),
      },
    },
  });
  const client = createClient({ schema });
  const { error } = client.database.users.delete("non-existent-id");
  expect(error).toBeUndefined();
});

test("client.database.users.query() retrieves users", () => {
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

  client.database.users.create({ name: "Alice", age: 30 });
  client.database.users.create({ name: "Bob", age: 25 });

  const { data: users, error } = client.database.users.query({
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

test("client.database.users.query() projects fields correctly", () => {
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

  client.database.users.create({ name: "Alice", age: 30 });

  const { data: users, error } = client.database.users.query({
    fields: {
      name: true,
    },
  });

  expectTypeOf(users).toEqualTypeOf<{ name: string }[] | undefined>();
  expect(error).toBeUndefined();
  expect(users).toBeDefined();
  expect(users?.[0]).toEqual({ name: "Alice" });

  const { data: usersWithId, error: idError } = client.database.users.query({
    fields: {
      id: true,
      name: true,
    },
  });

  expectTypeOf(usersWithId).toEqualTypeOf<
    { id: string; name: string }[] | undefined
  >();
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
