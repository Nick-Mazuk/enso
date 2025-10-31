export type FieldKind = "string" | "number" | "boolean";
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
} & FieldOptions<T, Optional>;

export type Entity<Fields extends Record<string, Field<FieldValue, boolean>>> =
	{
		[K in keyof Fields]: Fields[K];
	};

export type Schema<
	Entities extends Record<string, Record<string, Field<FieldValue, boolean>>>,
> = {
	readonly entities: Entities;
};
