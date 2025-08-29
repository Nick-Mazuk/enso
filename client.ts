import { nanoid } from "nanoid";
import { HLC } from "./hlc";
import { type Schema, type SchemaDefinition } from "./schema";
import { TripleStore } from "./store";

type CreateResult<T> = Promise<
  { data: T; error: undefined } | { data: undefined; error: Error }
>;

type EntityAPI<Def> = {
  create: (fields: any) => CreateResult<any>;
};

type DatabaseAPI<S extends SchemaDefinition> = {
  [K in keyof S["entities"]]: EntityAPI<S["entities"][K]>;
};

export class Client<S extends Schema<any>> {
  public schema: S;
  public store: TripleStore;
  public database: DatabaseAPI<S["definition"]>;
  private hlc: HLC;

  constructor({ schema }: { schema: S }) {
    this.schema = schema;
    this.store = new TripleStore();
    this.hlc = new HLC(new Date(), 0);
    this.database = this.createDatabaseAPI();
  }

  private createDatabaseAPI(): DatabaseAPI<S["definition"]> {
    const db: any = {};
    const entityNames = Object.keys(this.schema.definition.entities);

    for (const entityName of entityNames) {
      db[entityName] = {
        create: async (fields: Record<string, unknown>) => {
          if (!this.schema.validate(entityName, fields)) {
            return {
              data: undefined,
              error: new Error("Validation failed"),
            };
          }

          const id = nanoid();
          const now = new Date();

          for (const [key, value] of Object.entries(fields)) {
            this.hlc = this.hlc.increment();
            this.store.add([
              id,
              `${entityName}/${key}`,
              value as any,
              this.hlc,
            ]);
          }

          const data = {
            id,
            createdAt: now,
            updatedAt: now,
            ...fields,
          };

          return { data, error: undefined };
        },
      };
    }

    return db;
  }
}

export const createClient = <S extends Schema<any>>({
  schema,
}: {
  schema: S;
}): Client<S> => {
  return new Client({ schema });
};
