import type { Field, FieldValue, Schema } from "../schema/types";

export type Database<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> = Record<keyof S["entities"], boolean>;

export const createDatabase = <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	schema: S,
): Database<S> => {
	const database: Partial<Database<S>> = {};
	for (const entity in schema.entities) {
		database[entity as keyof S["entities"]] = false;
	}
	return database as Database<S>;
};
