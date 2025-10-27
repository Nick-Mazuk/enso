import { type ReservedField, reservedFields } from "./reserved-fields";
import type { Field, FieldValue, Schema } from "./types";

// TODO: update the type definition to disallow reserved fields
export const createSchema = <
	Entities extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(
	schema: Schema<Entities>,
): Schema<Entities> => {
	for (const entity in schema.entities) {
		for (const field in schema.entities[entity]) {
			if (reservedFields.includes(field as ReservedField)) {
				throw new Error(`Reserved field '${field}' is not allowed`);
			}
		}
	}
	return schema;
};
