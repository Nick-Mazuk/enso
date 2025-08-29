import { Schema } from "./schema";
import { TripleStore } from "./store";

export class Client<S extends Schema<any>> {
  public schema: S;
  public store: TripleStore;
  public database: any;

  constructor({ schema }: { schema: S }) {
    this.schema = schema;
    this.store = new TripleStore();
    this.database = this.createDatabaseProxy();
  }

  private createDatabaseProxy() {
    return new Proxy(
      {},
      {
        get: (target, prop, receiver) => {
          if (
            typeof prop === "string" &&
            prop in this.schema.definition.entities
          ) {
            return {}; // Placeholder for entity API
          }
          return undefined;
        },
      }
    );
  }
}

export const createClient = <S extends Schema<any>>({
  schema,
}: {
  schema: S;
}): Client<S> => {
  return new Client({ schema });
};
