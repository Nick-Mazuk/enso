export type FieldKind = "string" | "number" | "boolean" | "ref";
export type FieldValue = string | number | boolean;

export type FieldOptions<
	T extends FieldValue,
	Optional extends boolean,
> = Optional extends true
	? {
			optional: true;
			fallback?: T;
		}
	: {
			optional?: false;
			fallback: T;
		};

export type Field<T extends FieldValue, Optional extends boolean> = {
	kind: FieldKind;
	entity?: string;
} & FieldOptions<T, Optional>;

export type Entity<Fields extends Record<string, Field<FieldValue, boolean>>> =
	{
		[K in keyof Fields]: Fields[K];
	};

/**
 * A record of entity names to their field definitions.
 *
 * Invariant: Each entity name maps to a record of field definitions.
 */
export type EntityDefinition = Record<string, Field<FieldValue, boolean>>;

/**
 * Input definition for creating a schema with shared and user scopes.
 *
 * Pre-condition: Entity names must be unique across both 'shared' and 'user' sections.
 * Invariant: At least one of 'shared' or 'user' must be provided.
 */
export type SchemaDefinition<
	Shared extends Record<string, EntityDefinition> = Record<
		string,
		EntityDefinition
	>,
	User extends Record<string, EntityDefinition> = Record<
		string,
		EntityDefinition
	>,
> = {
	shared?: Shared;
	user?: User;
};

/**
 * Helper type to check if two record types have overlapping keys.
 * Returns `never` if keys overlap, otherwise returns the union of both records.
 */
type AssertNoOverlappingKeys<
	A extends Record<string, unknown>,
	B extends Record<string, unknown>,
> = keyof A & keyof B extends never ? A & B : never;

/**
 * A validated schema with shared, user, and flattened entities views.
 *
 * Invariant: Entity names are unique across both shared and user scopes.
 * Invariant: The 'entities' property is a flattened view of both shared and user entities.
 * Post-condition: All properties are readonly and the schema is immutable.
 */
export type Schema<
	S extends SchemaDefinition<
		Record<string, EntityDefinition>,
		Record<string, EntityDefinition>
	>,
> = S extends SchemaDefinition<infer Shared, infer User>
	? AssertNoOverlappingKeys<Shared, User> extends never
		? never // Entity names overlap between shared and user
		: {
				readonly shared: Shared;
				readonly user: User;
				readonly entities: Shared & User;
			}
	: never;
