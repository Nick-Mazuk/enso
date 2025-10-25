import { createDatabase, type Database } from "./internal/database";
import type { Field, FieldValue, Schema } from "./internal/schema/types";

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

	constructor(schema: S) {
		this.database = createDatabase(schema);
	}
}
