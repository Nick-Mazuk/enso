import type { Field, FieldValue, Schema } from "./types";

export const createSchema = <
	Entities extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(
	schema: Schema<Entities>,
): Schema<Entities> => {
	return schema;
};
