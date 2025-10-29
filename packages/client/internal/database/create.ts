import { nanoid } from "nanoid";
import type { Field, FieldValue, Schema } from "../schema/types";
import type { Store } from "../store";
import { Id, Field as StoreField, type Triple, Value } from "../store/types";
import type { Database } from "./types";

export const createDatabase = <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	schema: S,
	store: Store,
): Database<S> => {
	const database: Partial<Database<S>> = {};
	for (const entity in schema.entities) {
		database[entity as keyof S["entities"]] = {
			create: (fields) => {
				const id = Id(nanoid());
				const triples: Triple[] = [];
				for (const field in fields) {
					triples.push([
						id,
						StoreField(field),
						Value(fields[field as keyof typeof fields]),
					]);
				}
				store.add(...triples);
				// biome-ignore lint/suspicious/noExplicitAny: need future debugging why this doesn't type check
				return { data: { ...fields, id } } as any;
			},
			delete: (id) => {
				store.deleteAllById(Id(id));
				return { data: undefined };
			},
		};
	}
	return database as Database<S>;
};
