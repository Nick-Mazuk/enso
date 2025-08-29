import { expect, test } from "bun:test";
import { Schema, createSchema, t } from "./schema";

test("createSchema creates a valid schema from the client-api.md definition", () => {
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
