import { nanoid } from "nanoid";
import type { Field, FieldValue, Schema } from "../schema/types";
import type { Store } from "../store";
import {
	Id,
	type QueryPattern,
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
				const find = fields.map(Variable);
				const where: QueryPattern[] = [
					[
						Variable("id"),
						StoreField(`${entity}/id`),
						// binding to a separate value here enables the developer to query for just an objects id
						Variable("$$$id_val$$$"),
					],
				];
				fields
					.filter((field) => field !== "id")
					.forEach((field) => {
						where.push([
							Variable("id"),
							StoreField(`${entity}/${field}`),
							Variable(field),
						]);
					});

				// DO NOT SUBMIT: make it so if the triple for the field doesn't exist, this returns undefined
				const response = store.query({ find, where });
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
	return Object.freeze(database) as Database<S>;
};
