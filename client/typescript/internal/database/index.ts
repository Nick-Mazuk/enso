import { nanoid } from "nanoid";
import { assert } from "../../../shared/assert.js";
import type { Field, FieldKind, FieldValue, Schema } from "../schema/types.js";
import type { StoreInterface } from "../store/store-interface.js";
import {
	type Datom,
	type Filter,
	Id,
	type QueryPattern,
	Field as StoreField,
	type Triple,
	Value,
	Variable,
} from "../store/types.js";
import type {
	BooleanFilters,
	CommonFilters,
	Database,
	DatabaseResult,
	NumberFilters,
	RefFilters,
	StringFilters,
} from "./types.js";

const getValue = (
	v: Datom | undefined,
	schema: Field<FieldValue, boolean>,
): Datom | undefined => {
	if (v !== undefined) return v;
	return schema.fallback as Datom | undefined;
};

export const createDatabase = <
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
>(
	schema: S,
	store: StoreInterface,
): Database<S> => {
	const database: Partial<Database<S>> = {};
	for (const entity in schema.entities) {
		if (!Object.hasOwn(schema.entities, entity)) continue;
		const entitySchema = schema.entities[entity];
		assert(
			entitySchema !== undefined,
			`Entity '${entity}' not found in schema`,
		);
		database[entity as keyof S["entities"]] = {
			create: async (fields) => {
				for (const schemaField in entitySchema) {
					if (!Object.hasOwn(entitySchema, schemaField)) continue;
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
					if (!Object.hasOwn(fields, field)) continue;
					triples.push([
						id,
						StoreField(`${entity}/${field}`),
						Value(fields[field as keyof typeof fields]),
					]);
				}
				triples.push([id, StoreField(`${entity}/id`), Value(id)]);
				await store.add(...triples);
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
				const filterResult = computeFilters({
					entity,
					entitySchema,
					where: opts.where,
				});
				if (!filterResult.success) return filterResult;
				const where = filterResult.data.where;
				const whereNot = filterResult.data.whereNot;
				const filters = filterResult.data.filters;
				const optional: QueryPattern[] = [];
				where.push([
					Variable("id"),
					StoreField(`${entity}/id`),
					// binding to a separate value here enables the developer to query for just an objects id
					Variable("$$$id_val$$$"),
				]);

				for (const field of selectedFields) {
					optional.push([
						Variable("id"),
						StoreField(`${entity}/${field}`),
						Variable(field),
					]);
				}
				for (const filteredField in opts.where) {
					if (!Object.hasOwn(opts.where, filteredField)) continue;
					// If the schema has a fallback, we still want to use that fallback for filtering.
					// Therefore we need to make sure the query still looks for this field.
					optional.push([
						Variable("id"),
						StoreField(`${entity}/${filteredField}`),
						Variable(filteredField),
					]);
				}

				const response = await store.query({
					find,
					where,
					optional,
					whereNot,
					filters,
				});
				let sortParams: [string, "asc" | "desc"][];
				if (!opts.orderBy || opts.orderBy.length === 0) {
					sortParams = [];
				} else if (Array.isArray(opts.orderBy[0])) {
					// It is an array of tuples: [['name', 'asc'], ['age', 'desc']]
					sortParams = opts.orderBy as [string, "asc" | "desc"][];
				} else {
					// It is a single tuple: ['name', 'asc']
					sortParams = [opts.orderBy as [string, "asc" | "desc"]];
				}
				if (sortParams.length > 0) {
					response.sort((a, b) => {
						for (const [field, dir] of sortParams) {
							const index = selectedFields.indexOf(field);
							const valA = a[index];
							const valB = b[index];

							if (valA === valB) continue;
							if (valA === undefined) return 1;
							if (valB === undefined) return -1;

							if (valA < valB) return dir === "asc" ? -1 : 1;
							if (valA > valB) return dir === "asc" ? 1 : -1;
						}
						return 0;
					});
				}
				const rows =
					opts.limit !== undefined ? response.slice(0, opts.limit) : response;
				return {
					success: true,
					data: rows.map((data) => {
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
			delete: async (id) => {
				await store.deleteAllById(Id(id));
				return { data: undefined, success: true };
			},
		};
	}
	return Object.freeze(database) as Database<S>;
};

const computeFilters = (opts: {
	where?: Partial<
		Record<
			string,
			Partial<CommonFilters & NumberFilters & StringFilters & BooleanFilters>
		>
	>;
	entity: string;
	entitySchema: Record<string, Field<FieldValue, boolean>>;
}): DatabaseResult<{
	where: QueryPattern[];
	whereNot: QueryPattern[];
	filters: Filter[];
}> => {
	const where: QueryPattern[] = [];
	const whereNot: QueryPattern[] = [];
	const filters: Filter[] = [];

	for (const filteredField in opts.where) {
		if (!Object.hasOwn(opts.where, filteredField)) continue;
		// Ensure all filtered fields are in the schema.
		const fieldSchema = opts.entitySchema[filteredField];
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
			if (!Object.hasOwn(config, filter)) continue;
			// Validate config is not undefined
			const filterValue = config[filter as keyof typeof config];
			if (typeof filterValue === "undefined") continue;

			// Filters common to all field kinds
			if (filter === "isDefined") {
				const pattern: QueryPattern = [
					Variable("id"),
					StoreField(`${opts.entity}/${filteredField}`),
					Variable(filteredField),
				];
				if (filterValue) {
					where.push(pattern);
					continue;
				}
				whereNot.push(pattern);
				continue;
			}

			// Filters specific to different field kinds
			assert(
				fieldSchema.kind in filtersByKind,
				`Internal error: fieldSchema.kind "${fieldSchema.kind}" not in filtersByKind`,
			);
			const typeFilters = filtersByKind[fieldSchema.kind];
			if (!(filter in typeFilters)) {
				return {
					success: false,
					error: {
						message: `Filter '${filter}' not allowed on ${filteredField} which is a ${fieldSchema.kind}`,
					},
				} as const;
			}
			const expectedType =
				fieldSchema.kind === "ref" ? "string" : fieldSchema.kind;

			if (typeof filterValue !== expectedType) {
				return {
					success: false,
					error: {
						message: `Expected filter ${filter} on ${filteredField} to be a ${expectedType}`,
					},
				} as const;
			}
			const predicate = typeFilters[filter as keyof typeof typeFilters];
			assert(typeof predicate !== "undefined", "predicate not in typeFilters");
			filters.push({
				selector: Variable(filteredField),
				filter: (v) => predicate(getValue(v, fieldSchema), filterValue),
			});
		}
	}
	return { success: true, data: { where, whereNot, filters } };
};

type FilterPredicate<T extends FieldValue> = (
	currentValue: Datom | undefined,
	comparison: T,
) => boolean;

const numberFilters: Record<keyof NumberFilters, FilterPredicate<number>> = {
	equals: (currentValue, comparison) => currentValue === comparison,
	notEquals: (currentValue, comparison) => currentValue !== comparison,
	greaterThan: (currentValue, comparison) =>
		typeof currentValue === "number" && currentValue > comparison,
	greaterThanOrEqual: (currentValue, comparison) =>
		typeof currentValue === "number" && currentValue >= comparison,
	lessThan: (currentValue, comparison) =>
		typeof currentValue === "number" && currentValue < comparison,
	lessThanOrEqual: (currentValue, comparison) =>
		typeof currentValue === "number" && currentValue <= comparison,
};

const stringFilters: Record<keyof StringFilters, FilterPredicate<string>> = {
	equals: (currentValue, comparison) => currentValue === comparison,
	notEquals: (currentValue, comparison) => currentValue !== comparison,
	contains: (currentValue, comparison) =>
		typeof currentValue === "string" && currentValue.includes(comparison),
	startsWith: (currentValue, comparison) =>
		typeof currentValue === "string" && currentValue.startsWith(comparison),
	endsWith: (currentValue, comparison) =>
		typeof currentValue === "string" && currentValue.endsWith(comparison),
};

const booleanFilters: Record<keyof BooleanFilters, FilterPredicate<boolean>> = {
	equals: (currentValue, comparison) => currentValue === comparison,
};

const refFilters: Record<keyof RefFilters, FilterPredicate<string>> = {
	equals: (currentValue, comparison) => currentValue === comparison,
	notEquals: (currentValue, comparison) => currentValue !== comparison,
};

// biome-ignore lint/suspicious/noExplicitAny: need future debugging why this doesn't type check
const filtersByKind: Record<FieldKind, Record<string, FilterPredicate<any>>> = {
	number: numberFilters,
	string: stringFilters,
	boolean: booleanFilters,
	ref: refFilters,
};
