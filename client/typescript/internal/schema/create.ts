import { type ReservedField, reservedFields } from "./reserved-fields.js";
import type {
	EntityDefinition,
	Field,
	FieldValue,
	Schema,
	SchemaDefinition,
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
 *
 * Pre-condition: definition contains valid EntityDefinitions in shared and/or user.
 * Pre-condition: No field names are reserved.
 * Pre-condition: Entity names are unique across shared and user scopes.
 * Post-condition: Returns a frozen Schema with shared, user, and flattened entities.
 *
 * @param definition - The schema definition with optional shared and user sections.
 * @returns A frozen Schema object with readonly shared, user, and entities properties.
 * @throws Error if reserved fields are used or entity names are duplicated.
 */
// TODO: update the type definition to disallow reserved fields
export const createSchema = <
	Shared extends Record<string, Record<string, Field<FieldValue, boolean>>>,
	User extends Record<string, Record<string, Field<FieldValue, boolean>>>,
>(
	definition: SchemaDefinition<Shared, User>,
): Schema<SchemaDefinition<Shared, User>> => {
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
};
