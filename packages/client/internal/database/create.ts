import type { Field, FieldValue, Schema } from "../schema/types";
import type { Database } from "./types";

export const createDatabase = <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	schema: S,
): Database<S> => {
	const database: Partial<Database<S>> = {};
	for (const entity in schema.entities) {
		database[entity as keyof S["entities"]] = {
			create: () => ({ data: undefined, error: undefined }),
		};
	}
	return database as Database<S>;
};
