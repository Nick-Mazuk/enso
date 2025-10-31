import { nanoid } from "nanoid";
import type { Field, FieldValue, Schema } from "../schema/types";
import type { Store } from "../store";
import {
	Id,
	Field as StoreField,
	type Triple,
	Value,
	Variable,
} from "../store/types";
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
						StoreField(`${entity}/${field}`),
						Value(fields[field as keyof typeof fields]),
					]);
				}
				triples.push([id, StoreField(`${entity}/id`), Value(id)]);
				store.add(...triples);
				// biome-ignore lint/suspicious/noExplicitAny: need future debugging why this doesn't type check
				return { data: { ...fields, id } } as any;
			},
			query: (opts) => {
				const fields = Object.entries(opts.fields)
					.filter(([_, value]) => value)
					.map(([key]) => key);
				// DO NOT SUBMIT: make it so if the triple for the field doesn't exist, this returns undefined
				const response = store.query({
					find: fields.map(Variable),
					where: fields
						.filter((field) => field !== "id")
						.map((field) => [
							Variable("id"),
							StoreField(`${entity}/${field}`),
							Variable(field),
						]),
				});
				console.log(response);
				return {
					data: response.map((data) => {
						const result: Record<string, string> = {};
						for (let i = 0; i < fields.length; i++) {
							// DO NOT SUBMIT: for required fields without data, set this to the fallback.
							result[fields[i]] = data[i];
						}
						return result;
					}),
				};
			},
			delete: (id) => {
				store.deleteAllById(Id(id));
				return { data: undefined };
			},
		};
	}
	return database as Database<S>;
};
