import { nanoid } from "nanoid";
import { assert } from "../../../shared/assert";
import type { Field, FieldValue, Schema } from "../schema/types";
import type { Store } from "../store";
import {
	type Datom,
	type Filter,
	Id,
	type QueryPattern,
	Field as StoreField,
	type Triple,
	Value,
	Variable,
} from "../store/types";
import type { Database } from "./types";

const getValue = (v: unknown, schema: { fallback?: unknown }) => {
	if (v !== undefined) return v;
	return schema.fallback;
};

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
				const selectedFields = Object.keys(opts.fields);
				// validate all selected fields are in the schema
				for (const field of selectedFields) {
					if (field === "id") continue;
					const fieldSchema = entitySchema[field as keyof typeof entitySchema];
					if (fieldSchema === undefined) {
						return {
							success: false,
							error: {
								message: `Field '${field}' not found in schema`,
							},
						};
					}
				}

				// Initialize store.query options
				const find = selectedFields.map(Variable);
				const where: QueryPattern[] = [
					[
						Variable("id"),
						StoreField(`${entity}/id`),
						// binding to a separate value here enables the developer to query for just an objects id
						Variable("$$$id_val$$$"),
					],
				];
				const optional: QueryPattern[] = [];
				const whereNot: QueryPattern[] = [];
				const filters: Filter[] = [];

				// Query for fields. Make them optional because they may not be defined.
				// Even if the schema says the field is required, the underlying data
				// could be missing.
				for (const field of selectedFields) {
					if (field === "id") continue;
					optional.push([
						Variable("id"),
						StoreField(`${entity}/${field}`),
						Variable(field),
					]);
				}

				// Apply filters
				for (const filteredField in opts.where) {
					// Ensure all filtered fields are in the schema.
					const fieldSchema = entitySchema[filteredField];
					if (fieldSchema === undefined) {
						return {
							success: false,
							error: {
								message: `Field '${filteredField}' not found in schema`,
							},
						} as const;
					}

					const config = opts.where[filteredField];
					if (!config) continue;

					for (const filter in config) {
						// Validate config is not undefined
						const filterValue = config[filter as keyof typeof config];
						if (typeof filterValue === "undefined") continue;

						// Filters common to all field kinds
						if (filter === "isDefined") {
							if (filterValue) {
								where.push([
									Variable("id"),
									StoreField(`${entity}/${filteredField}`),
									Variable(filteredField),
								]);
								continue;
							}
							whereNot.push([
								Variable("id"),
								StoreField(`${entity}/${filteredField}`),
								Variable(filteredField),
							]);
							continue;
						}

						// Constants for multiple filters
						const selector = Variable(filteredField);
						const filterError = {
							success: false,
							error: {
								message: `Filter '${filter}' not allowed on ${filteredField} which is a ${fieldSchema.kind}`,
							},
						} as const;
						const filterValueTypeError = {
							success: false,
							error: {
								message: `Expected filter ${filter} on ${filteredField} to be a ${fieldSchema.kind}`,
							},
						} as const;

						// Filters specific to different field kinds
						switch (fieldSchema.kind) {
							case "number": {
								if (typeof filterValue !== "number")
									return filterValueTypeError;
								switch (filter) {
									case "equals":
										filters.push({
											selector,
											filter: (v) => getValue(v, fieldSchema) === filterValue,
										});
										continue;
									case "notEquals":
										filters.push({
											selector,
											filter: (v) => getValue(v, fieldSchema) !== filterValue,
										});
										continue;
									case "greaterThan":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return typeof val === "number" && val > filterValue;
											},
										});
										continue;
									case "greaterThanOrEqual":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return typeof val === "number" && val >= filterValue;
											},
										});
										continue;
									case "lessThan":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return typeof val === "number" && val < filterValue;
											},
										});
										continue;
									case "lessThanOrEqual":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return typeof val === "number" && val <= filterValue;
											},
										});
										continue;
									default:
										return filterError;
								}
							}
							case "boolean":
								if (filter !== "equals") return filterError;
								if (typeof filterValue !== "boolean")
									return filterValueTypeError;
								filters.push({
									selector,
									filter: (v) => getValue(v, fieldSchema) === filterValue,
								});
								continue;
							case "string":
								if (
									![
										"equals",
										"notEquals",
										"contains",
										"startsWith",
										"endsWith",
									].includes(filter)
								) {
									return filterError;
								}
								if (typeof filterValue !== "string")
									return filterValueTypeError;
								switch (filter) {
									case "equals":
										filters.push({
											selector,
											filter: (v) => getValue(v, fieldSchema) === filterValue,
										});
										continue;
									case "notEquals":
										filters.push({
											selector,
											filter: (v) => getValue(v, fieldSchema) !== filterValue,
										});
										continue;
									case "contains":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return (
													typeof val === "string" && val.includes(filterValue)
												);
											},
										});
										continue;
									case "startsWith":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return (
													typeof val === "string" && val.startsWith(filterValue)
												);
											},
										});
										continue;
									case "endsWith":
										filters.push({
											selector,
											filter: (v) => {
												const val = getValue(v, fieldSchema);
												return (
													typeof val === "string" && val.endsWith(filterValue)
												);
											},
										});
										continue;
									default:
										return filterError;
								}
							default:
								return filterError;
						}
					}

					// If the schema has a fallback, we still want to use that fallback for filtering.
					// Therefore we need to make sure the query still looks for this field.
					if (fieldSchema.fallback && !selectedFields.includes(filteredField)) {
						optional.push([
							Variable("id"),
							StoreField(`${entity}/${filteredField}`),
							Variable(filteredField),
						]);
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
						for (let i = 0; i < selectedFields.length; i++) {
							const field = selectedFields[i];
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
