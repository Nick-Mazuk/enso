// Type options
type StringOptions =
  | { fallback: string; optional?: undefined }
  | { fallback?: string; optional: true };
type NumberOptions =
  | { fallback: number; optional?: undefined }
  | { fallback?: number; optional: true };
type BooleanOptions =
  | { fallback: boolean; optional?: undefined }
  | { fallback?: boolean; optional: true };
type DateOptions =
  | { fallback: Date | "now"; optional?: undefined }
  | { fallback?: Date | "now"; optional: true };

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
export type ObjectField<T extends { [key: string]: FieldDefinition }> = {
  type: "object";
  fields: T;
};
export type ArrayField<T extends FieldDefinition> = {
  type: "array";
  itemType: T;
};

// Main FieldDefinition union type (recursive)
export type FieldDefinition =
  | StringField
  | NumberField
  | BooleanField
  | DateField
  | RefField<string>
  | RefManyField<string>
  | ObjectField<{ [key: string]: any }>
  | ArrayField<any>;

export const t = {
  string: (options: StringOptions): StringField => ({
    type: "string",
    ...options,
  }),
  number: (options: NumberOptions): NumberField => ({
    type: "number",
    ...options,
  }),
  boolean: (options: BooleanOptions): BooleanField => ({
    type: "boolean",
    ...options,
  }),
  date: (options: DateOptions): DateField => ({
    type: "date",
    ...options,
  }),
  object: <T extends { [key: string]: FieldDefinition }>(
    fields: T
  ): ObjectField<T> => ({
    type: "object",
    fields,
  }),
  array: <T extends FieldDefinition>(itemType: T): ArrayField<T> => ({
    type: "array",
    itemType,
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
  entities: { [key: string]: EntityDefinition };
  rooms: { [key: string]: RoomDefinition };
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
}

export const createSchema = <T extends SchemaDefinition>(
  definition: T
): Schema<T> => {
  return new Schema(definition);
};
