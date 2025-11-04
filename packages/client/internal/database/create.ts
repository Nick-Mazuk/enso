import { nanoid } from "nanoid";
import { assert } from "../../../shared/assert";
import type { Field, FieldValue, Schema } from "../schema/types";
import type { Store } from "../store";
import {
	type Datom,
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
				const entitySchema = schema.entities[entity];
				assert(
					entitySchema !== undefined,
					`Entity '${entity}' not found in schema`,
				);
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
				const optional: QueryPattern[] = [];
				for (const field of fields) {
					if (field === "id") continue;
					optional.push([
						Variable("id"),
						StoreField(`${entity}/${field}`),
						Variable(field),
					]);
				}

				const response = store.query({ find, where, optional });
				return {
					data: response.map((data) => {
						const result: Record<string, Datom> = {};
						for (let i = 0; i < fields.length; i++) {
							const field = fields[i];
							if (field === undefined) continue;
							const dataItem = data[i];
							if (dataItem !== undefined) {
								result[field] = dataItem;
								continue;
							}
							if (field === "id") continue;
							const fieldSchema =
								entitySchema[field as keyof typeof entitySchema];
							assert(
								fieldSchema !== undefined,
								`Field '${field}' not found in schema`,
							);
							if (fieldSchema.fallback !== undefined && !fieldSchema.optional) {
								result[field] = Value(fieldSchema.fallback);
							}
						}
						// biome-ignore lint/suspicious/noExplicitAny: need future debugging why this doesn't type check
						return result as any;
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
