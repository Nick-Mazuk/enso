import { expect, expectTypeOf, test } from "bun:test";
import {
  type RefField,
  Schema,
  type StringField,
  createSchema,
  t,
} from "./schema";

test("createSchema returns an instance of Schema", () => {
  const schemaDefinition = {
    entities: {
      $users: {
        name: t.string({ fallback: "" }),
        email: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
      },
      posts: {
        title: t.string({ fallback: "" }),
        authorId: t.ref("$users"),
        tagIds: t.refMany("tags"),
      },
      tags: {
        name: t.string({ fallback: "" }),
      },
    },
    rooms: {
      documentEditor: {
        events: {
          like: t.object({
            targetId: t.string({ fallback: "" }),
            userId: t.string({ fallback: "" }),
          }),
        },
        userStatus: {
          cursor: t.object({
            x: t.number({ fallback: 0 }),
            y: t.number({ fallback: 0 }),
          }),
        },
        roomStatus: {
          documentTitle: t.string({ optional: true }),
        },
      },
    },
  };

  const schema = createSchema(schemaDefinition);
  expect(schema).toBeInstanceOf(Schema);
  expect(schema.definition).toEqual(schemaDefinition);
});

test("createSchema produces correct types", () => {
  const schemaDefinition = {
    entities: {
      $users: {
        name: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
      },
      posts: {
        authorId: t.ref("$users"),
      },
    },
    rooms: {},
  };

  const schema = createSchema(schemaDefinition);

  expectTypeOf(schema.definition).toEqualTypeOf(schemaDefinition);
  expectTypeOf(
    schema.definition.entities.$users.name
  ).toEqualTypeOf<StringField>();
  expectTypeOf(schema.definition.entities.posts.authorId).toEqualTypeOf<
    RefField<"$users">
  >();
  expectTypeOf(
    schema.definition.entities.posts.authorId.refType
  ).toEqualTypeOf<"$users">();
});

test("Schema.validate correctly validates an object", () => {
  const schema = createSchema({
    entities: {
      user: {
        name: t.string({ fallback: "" }),
        age: t.number({ optional: true }),
        posts: t.refMany("post"),
      },
    },
    rooms: {},
  });

  const validUser = { name: "test", age: 10 };
  const invalidUser = { name: "test", age: "10" };
  const missingRequired = { age: 10 };

  expect(schema.validate("user", validUser)).toBe(true);
  expect(schema.validate("user", invalidUser)).toBe(false);
  expect(schema.validate("user", missingRequired)).toBe(false);
  expect(schema.validate("nonexistent", validUser)).toBe(false);
});

test("Can create a schema without entities", () => {
  const schema = createSchema({
    rooms: {},
  });

  expect(schema.validate("user", { name: "test", age: 10 })).toBe(false);
});

test("Can create a schema without rooms", () => {
  const schema = createSchema({
    entities: {},
  });

  expect(schema.validate("user", { name: "test", age: 10 })).toBe(false);
});

test("Can create an empty schema", () => {
  const schema = createSchema({});

  expect(schema.validate("user", { name: "test", age: 10 })).toBe(false);
});
