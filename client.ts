import { nanoid } from "nanoid";
import { HLC } from "./hlc";
import {
  type EntityDefinition,
  type FieldDefinition,
  type Schema,
  type SchemaDefinition,
} from "./schema";
import { TripleStore, type Object } from "./store";

type UnwrapField<F extends FieldDefinition> = F extends { type: "string" }
  ? string
  : F extends { type: "number" }
  ? number
  : F extends { type: "boolean" }
  ? boolean
  : F extends { type: "date" }
  ? Date
  : F extends { type: "ref" }
  ? string
  : F extends { type: "refMany" }
  ? string[]
  : never;

type BaseEntity = { id: string; createdAt: Date; updatedAt: Date };

type Entity<Def extends EntityDefinition> = Prettify<
  {
    [K in RequiredKeys<Def>]: UnwrapField<Def[K]>;
  } & {
    [K in OptionalKeys<Def>]?: UnwrapField<Def[K]>;
  } & BaseEntity
>;

type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

type OptionalKeys<T> = {
  [K in keyof T]: T[K] extends { optional: true } ? K : never;
}[keyof T];
type RequiredKeys<T> = Exclude<keyof T, OptionalKeys<T>>;
type CreateFields<Def extends EntityDefinition> = Prettify<
  {
    [K in RequiredKeys<Def>]: UnwrapField<Def[K]>;
  } & {
    [K in OptionalKeys<Def>]?: UnwrapField<Def[K]>;
  }
>;

type CreateResult<Def extends EntityDefinition> = Promise<
  { data: Entity<Def>; error: undefined } | { data: undefined; error: Error }
>;

type QueryResult<
  Def extends EntityDefinition,
  Fields extends { [K in keyof any]?: boolean }
> = Promise<
  | {
      data: Pick<Entity<Def>, keyof Fields & keyof Entity<Def>>[];
      error: undefined;
    }
  | { data: undefined; error: Error }
>;

type EntityAPI<Def extends EntityDefinition> = {
  create: (fields: CreateFields<Def>) => CreateResult<Def>;
  query: <
    Fields extends {
      [K in keyof Entity<Def>]?: true;
    }
  >(opts: {
    fields: Fields;
  }) => QueryResult<Def, Fields>;
};

export type DatabaseAPI<S extends SchemaDefinition> = {
  [K in keyof S["entities"]]: EntityAPI<NonNullable<S["entities"][K]>>;
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
        create: async (
          fields: CreateFields<any>
        ): Promise<{ data: any; error: any }> => {
          if (!this.schema.validate(entityName, fields)) {
            return {
              data: undefined,
              error: new Error("Validation failed"),
            };
          }

          const id = nanoid();
          const now = new Date();

          this.hlc = this.hlc.increment();
          this.store.add([id, "entityType", entityName, this.hlc]);
          this.hlc = this.hlc.increment();
          this.store.add([id, `${entityName}/id`, id, this.hlc]);
          this.hlc = this.hlc.increment();
          this.store.add([id, `${entityName}/createdAt`, now, this.hlc]);
          this.hlc = this.hlc.increment();
          this.store.add([id, `${entityName}/updatedAt`, now, this.hlc]);

          for (const [key, value] of Object.entries(fields)) {
            this.hlc = this.hlc.increment();
            this.store.add([
              id,
              `${entityName}/${key}`,
              value as Object,
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
        query: async (opts: any) => {
          const subjects = this.store.querySubjects({
            predicate: "entityType",
            object: entityName,
          });

          const data = subjects
            .map((subject) => {
              const triples = this.store.query([subject]);
              const entity: Record<string, unknown> = {};

              const triplesByPredicate = triples.reduce((acc, [, p, o]) => {
                acc[p] = o;
                return acc;
              }, {} as Record<string, Object>);

              for (const field of Object.keys(opts.fields)) {
                const predicate = `${entityName}/${String(field)}`;
                if (triplesByPredicate[predicate] !== undefined) {
                  entity[field as string] = triplesByPredicate[predicate];
                }
              }
              return entity;
            })
            .sort((a, b) => {
              if (a.createdAt && b.createdAt) {
                return (
                  (a.createdAt as Date).getTime() -
                  (b.createdAt as Date).getTime()
                );
              }
              return 0;
            });

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
