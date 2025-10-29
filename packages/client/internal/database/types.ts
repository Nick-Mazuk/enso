import type { Field, FieldValue, Schema } from "../schema/types";

export type DbEntity<E extends Record<string, Field<FieldValue, boolean>>> = {
	create: (
		entity: {
			[K in keyof E as E[K] extends Field<FieldValue, false>
				? K
				: never]: E[K] extends Field<infer V, boolean> ? V : never;
		} & {
			[K in keyof E as E[K] extends Field<FieldValue, true>
				? K
				: never]?: E[K] extends Field<infer V, boolean> ? V : never;
		},
	) => DatabaseResult<
		{
			[K in keyof E as E[K] extends Field<FieldValue, false>
				? K
				: never]: E[K] extends Field<infer V, boolean> ? V : never;
		} & {
			[K in keyof E as E[K] extends Field<FieldValue, true>
				? K
				: never]?: E[K] extends Field<infer V, boolean> ? V : never;
		} & { id: string }
	>;
	delete: (id: string) => DatabaseResult<void>;
};

export type Database<
	S extends Schema<Record<string, Record<string, Field<FieldValue, boolean>>>>,
> = {
	[K in keyof S["entities"]]: DbEntity<S["entities"][K]>;
};

type DatabaseError = {
	message: string;
};

export type DatabaseResult<T> =
	| {
			data: T;
			error?: never;
	  }
	| {
			data?: never;
			error: DatabaseError;
	  };
