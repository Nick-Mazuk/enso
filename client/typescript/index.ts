import { createDatabase } from "./internal/database/index.js";
import type { Database } from "./internal/database/types.js";
import type { Field, FieldValue, Schema } from "./internal/schema/types.js";
import { Store } from "./internal/store/index.js";

export { createSchema } from "./internal/schema/create.js";
export { t } from "./internal/schema/t.js";

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
