// Type options
type Options<T> =
  | { fallback: T; optional?: undefined }
  | { fallback?: T; optional: true };
type StringOptions = Options<string>;
type NumberOptions = Options<number>;
type BooleanOptions = Options<boolean>;
type DateOptions = Options<Date | "now">;

// Structural field types
export type StringField = { type: "string" } & StringOptions;
export type NumberField = { type: "number" } & NumberOptions;
export type BooleanField = { type: "boolean" } & BooleanOptions;
export type DateField = { type: "date" } & DateOptions;
export type RefField<T extends string> = {
  type: "ref";
  refType: T;
  optional: true;
};
export type RefManyField<T extends string> = {
  type: "refMany";
  refType: T;
  optional: true;
};
export type ObjectField<T extends Record<string, unknown>> = {
  type: "object";
  fields: T;
  optional?: true;
};
export type ArrayField<T> = {
  type: "array";
  itemType: T;
  optional?: true;
};

// Main FieldDefinition union type (recursive)
export type FieldDefinition<
  Obj extends Record<string, unknown> = Record<string, unknown>
> =
  | StringField
  | NumberField
  | BooleanField
  | DateField
  | RefField<string>
  | RefManyField<string>
  | ObjectField<Obj>
  | ArrayField<any>;

export const t = {
  string: <O extends StringOptions>(options: O): { type: "string" } & O => ({
    type: "string",
    ...options,
  }),
  number: <O extends NumberOptions>(options: O): { type: "number" } & O => ({
    type: "number",
    ...options,
  }),
  boolean: <O extends BooleanOptions>(options: O): { type: "boolean" } & O => ({
    type: "boolean",
    ...options,
  }),
  date: <O extends DateOptions>(options: O): { type: "date" } & O => ({
    type: "date",
    ...options,
  }),
  object: <T extends { [key: string]: FieldDefinition }>(
    fields: T,
    options?: { optional: true }
  ) => ({
    type: "object" as const,
    fields,
    ...options,
  }),
  array: <T extends FieldDefinition>(
    itemType: T,
    options?: { optional: true }
  ) => ({
    type: "array" as const,
    itemType,
    ...options,
  }),
  ref: <T extends string>(refType: T): RefField<T> => ({
    type: "ref",
    refType,
    optional: true,
  }),
  refMany: <T extends string>(refType: T): RefManyField<T> => ({
    type: "refMany",
    refType,
    optional: true,
  }),
};

export type EntityDefinition = {
  [key: string]: FieldDefinition;
};

export type RoomDefinition = {
  events?: { [key: string]: ObjectField<{ [key: string]: FieldDefinition }> };
  userStatus?: { [key: string]: FieldDefinition };
  roomStatus?: { [key: string]: FieldDefinition };
};

export type SchemaDefinition = {
  entities?: { [key: string]: EntityDefinition };
  rooms?: { [key: string]: RoomDefinition };
};

export class Schema<T extends SchemaDefinition> {
  public definition: T;
  private parsedSchema: Map<string, Map<string, FieldDefinition>>;

  constructor(definition: T) {
    this.definition = definition;
    this.parsedSchema = this.parse(definition);
  }

  private parse(definition: T): Map<string, Map<string, FieldDefinition>> {
    const parsed = new Map<string, Map<string, FieldDefinition>>();
    for (const entityName in definition.entities) {
      const entityDef = definition.entities[entityName];
      const fields = new Map<string, FieldDefinition>();
      for (const fieldName in entityDef) {
        if (entityDef[fieldName]) fields.set(fieldName, entityDef[fieldName]);
      }
      parsed.set(entityName, fields);
    }
    return parsed;
  }

  validate(entityType: string, object: unknown): boolean {
    const entityDef = this.parsedSchema.get(entityType);
    if (!entityDef) {
      return false;
    }

    if (typeof object !== "object" || object === null) {
      return false;
    }

    for (const [fieldName, fieldDef] of entityDef.entries()) {
      const value = (object as Record<string, unknown>)[fieldName];

      if (value === undefined) {
        if ("optional" in fieldDef && fieldDef.optional) {
          continue;
        }
        return false;
      }

      switch (fieldDef.type) {
        case "string":
          if (typeof value !== "string") return false;
          break;
        case "number":
          if (typeof value !== "number") return false;
          break;
        case "boolean":
          if (typeof value !== "boolean") return false;
          break;
        case "date":
          if (!(value instanceof Date)) return false;
          break;
        case "ref":
          if (typeof value !== "string") return false;
          break;
        case "refMany":
          if (
            !Array.isArray(value) ||
            !value.every((v) => typeof v === "string")
          ) {
            return false;
          }
          break;
      }
    }

    return true;
  }
}

export const createSchema = <T extends SchemaDefinition>(
  definition: T
): Schema<T> => {
  return new Schema(definition);
};
