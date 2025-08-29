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
  : never;

type BaseEntity = { id: string; createdAt: Date; updatedAt: Date };

type Entity<Def extends EntityDefinition> = {
  [K in keyof Def]: Def[K] extends FieldDefinition
    ? UnwrapField<Def[K]>
    : never;
} & BaseEntity;

type CreateFields<Def extends EntityDefinition> = {
  [K in keyof Def as Def[K] extends { optional: true } | { fallback: any }
    ? never
    : K]: UnwrapField<Def[K]>;
} & {
  [K in keyof Def as Def[K] extends { optional: true } | { fallback: any }
    ? K
    : never]?: UnwrapField<Def[K]>;
};

type CreateResult<Def extends EntityDefinition> = Promise<
  { data: Entity<Def>; error: undefined } | { data: undefined; error: Error }
>;

type QueryOptions<Def extends EntityDefinition> = {
  fields: { [K in keyof Entity<Def>]?: true };
};

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
