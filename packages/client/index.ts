import { createDatabase } from "./internal/database/create";
import type { Database } from "./internal/database/types";
import type { Field, FieldValue, Schema } from "./internal/schema/types";
import { Store } from "./internal/store";

export { createSchema } from "./internal/schema/create";
export { t } from "./internal/schema/t";

export const createClient = <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(opts: {
	schema: S;
}): Client<S> => {
	return new Client(opts.schema);
};

class Client<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> {
	public readonly database: Database<S>;
	private readonly store: Store;

	constructor(schema: S) {
		this.store = new Store();
		this.database = createDatabase(schema, this.store);
	}
}
