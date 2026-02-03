import { type ReservedField, reservedFields } from "./reserved-fields.js";
import {
	type EntityDefinition,
	type Field,
	type FieldValue,
	type LegacySchema,
	type LegacySchemaDefinition,
	type Schema,
	type SchemaDefinition,
	isLegacySchemaDefinition,
} from "./types.js";

/**
 * Validates that all fields in an entity definition do not use reserved field names.
 *
 * Pre-condition: entityDef is a valid EntityDefinition.
 * Post-condition: Throws if any field name is reserved.
 */
const validateEntityFields = (
	entityName: string,
	entityDef: EntityDefinition,
): void => {
	for (const field in entityDef) {
		if (!Object.hasOwn(entityDef, field)) continue;
		if (reservedFields.includes(field as ReservedField)) {
			throw new Error(`Reserved field '${field}' is not allowed`);
		}
	}
};

/**
 * Validates entities in a scope (shared or user).
 *
 * Pre-condition: entities is a valid record of EntityDefinitions or undefined.
 * Post-condition: Throws if any field name is reserved.
 */
const validateScope = (
	entities: Record<string, EntityDefinition> | undefined,
): void => {
	if (!entities) return;
	for (const entityName in entities) {
		if (!Object.hasOwn(entities, entityName)) continue;
		validateEntityFields(entityName, entities[entityName]);
	}
};

/**
 * Validates that entity names are unique across shared and user scopes.
 *
 * Pre-condition: shared and user are valid records of EntityDefinitions or undefined.
 * Post-condition: Throws if any entity name appears in both scopes.
 */
const validateUniqueEntityNames = (
	shared: Record<string, EntityDefinition> | undefined,
	user: Record<string, EntityDefinition> | undefined,
): void => {
	if (!shared || !user) return;
	for (const entityName in shared) {
		if (!Object.hasOwn(shared, entityName)) continue;
		if (Object.hasOwn(user, entityName)) {
			throw new Error(
				`Entity name '${entityName}' must be unique across shared and user scopes`,
			);
		}
	}
};

/**
 * Creates a validated and frozen schema from a schema definition.
 * Supports both legacy { entities: {...} } format and new { shared: {...}, user: {...} } format.
 *
 * Pre-condition: definition contains valid EntityDefinitions.
 * Pre-condition: No field names are reserved.
 * Pre-condition: Entity names are unique across shared and user scopes (for new format).
 * Post-condition: Returns a frozen Schema with shared, user, and flattened entities.
 *
 * @param definition - The schema definition (legacy or new format).
 * @returns A frozen Schema object with readonly shared, user, and entities properties.
 * @throws Error if reserved fields are used or entity names are duplicated.
 */
// TODO: update the type definition to disallow reserved fields
export function createSchema<
	Entities extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(definition: LegacySchemaDefinition<Entities>): LegacySchema<LegacySchemaDefinition<Entities>>;
export function createSchema<
	Shared extends Record<string, Record<string, Field<FieldValue, boolean>>>,
	User extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(definition: SchemaDefinition<Shared, User>): Schema<SchemaDefinition<Shared, User>>;
export function createSchema<
	Shared extends Record<string, Record<string, Field<FieldValue, boolean>>>,
	User extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(
	definition: SchemaDefinition<Shared, User> | LegacySchemaDefinition<Shared>,
): Schema<SchemaDefinition<Shared, User>> | LegacySchema<LegacySchemaDefinition<Shared>> {
	// Handle legacy { entities: {...} } format by treating all entities as shared
	if (isLegacySchemaDefinition<Shared>(definition)) {
		const entities = definition.entities;

		// Validate reserved fields in entities
		validateScope(entities);

		// Create schema with all entities in shared scope
		const schema = {
			shared: entities,
			user: {} as Record<string, never>,
			entities,
		} as LegacySchema<LegacySchemaDefinition<Shared>>;

		return Object.freeze(schema);
	}

	// Handle new { shared: {...}, user: {...} } format
	const shared = (definition.shared ?? {}) as Shared;
	const user = (definition.user ?? {}) as User;

	// Validate reserved fields in both scopes
	validateScope(shared);
	validateScope(user);

	// Validate entity name uniqueness across scopes
	validateUniqueEntityNames(shared, user);

	// Create the schema with all three views
	const schema = {
		shared,
		user,
		entities: { ...shared, ...user },
	} as Schema<SchemaDefinition<Shared, User>>;

	return Object.freeze(schema);
}
