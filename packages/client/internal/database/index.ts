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
	type Filter,
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
		const entitySchema = schema.entities[entity];
		assert(
			entitySchema !== undefined,
			`Entity '${entity}' not found in schema`,
		);
		database[entity as keyof S["entities"]] = {
			create: (fields) => {
				for (const schemaField in entitySchema) {
					const fieldDefinition = entitySchema[schemaField];
					assert(
						fieldDefinition !== undefined,
						`Field definition for ${schemaField} does not exist in entity ${entity}`,
					);
					const isRequired = fieldDefinition.optional !== true;
					if (isRequired && !(schemaField in fields)) {
						return {
							success: false,
							error: {
								message: `Missing required field "${schemaField}" when creating entity "${entity}"`,
							},
						};
					}
				}

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
				return { success: true, data: { ...fields, id } } as any;
			},
			query: async (opts) => {
				const fields = Object.entries(opts.fields)
					.filter(([_, value]) => value)
					.map(([key]) => key);
				// validate fields
				for (const field of fields) {
					if (field === "id") continue;
					const fieldSchema = entitySchema[field as keyof typeof entitySchema];
					if (fieldSchema === undefined) {
						return {
							success: false,
							error: { message: `Field '${field}' not found in schema` },
						};
					}
				}
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

				// Apply filters
				const whereNot: QueryPattern[] = [];
				const filters: Filter[] = [];
				for (const field in opts.where) {
					const config = opts.where[field];
					if (!config) continue;

					// biome-ignore lint/suspicious/noExplicitAny: config is typed as CommonFilters but at runtime can correspond to NumberFilters
					const conf = config as any;

					const hasValueFilter =
						conf.equals !== undefined ||
						conf.notEquals !== undefined ||
						conf.greaterThan !== undefined ||
						conf.greaterThanOrEqual !== undefined ||
						conf.lessThan !== undefined ||
						conf.lessThanOrEqual !== undefined;

					if (conf.isDefined || hasValueFilter) {
						where.push([
							Variable("id"),
							StoreField(`${entity}/${field}`),
							Variable(field),
						]);
					} else if (conf.isDefined === false) {
						whereNot.push([
							Variable("id"),
							StoreField(`${entity}/${field}`),
							Variable(field),
						]);
					}

					const selector = Variable(field);

					if (conf.equals !== undefined) {
						filters.push({
							selector,
							filter: (v) => v === conf.equals,
						});
					}
					if (conf.notEquals !== undefined) {
						filters.push({
							selector,
							filter: (v) => v !== conf.notEquals,
						});
					}
					if (conf.greaterThan !== undefined) {
						filters.push({
							selector,
							filter: (v) => typeof v === "number" && v > conf.greaterThan,
						});
					}
					if (conf.greaterThanOrEqual !== undefined) {
						filters.push({
							selector,
							filter: (v) => typeof v === "number" && v >= conf.greaterThanOrEqual,
						});
					}
					if (conf.lessThan !== undefined) {
						filters.push({
							selector,
							filter: (v) => typeof v === "number" && v < conf.lessThan,
						});
					}
					if (conf.lessThanOrEqual !== undefined) {
						filters.push({
							selector,
							filter: (v) => typeof v === "number" && v <= conf.lessThanOrEqual,
						});
					}
				}

				const response = store.query({
					find,
					where,
					optional,
					whereNot,
					filters,
				});
				return {
					success: true,
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
							if (fieldSchema.fallback !== undefined) {
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
				return { data: undefined, success: true };
			},
		};
	}
	return Object.freeze(database) as Database<S>;
};
