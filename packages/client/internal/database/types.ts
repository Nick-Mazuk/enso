import type { Simplify } from "type-fest";
import type { Field, FieldValue, Schema } from "../schema/types";

type GeneratedFields = { id: string };

export type DbEntity<E extends Record<string, Field<FieldValue, boolean>>> = {
	create: (
		entity: Simplify<
			{
				[K in keyof E as E[K] extends Field<FieldValue, false>
					? K
					: never]: E[K] extends Field<infer V, boolean> ? V : never;
			} & {
				[K in keyof E as E[K] extends Field<FieldValue, true>
					? K
					: never]?: E[K] extends Field<infer V, boolean> ? V : never;
			}
		>,
	) => DatabaseResult<
		Simplify<
			{
				[K in keyof E as E[K] extends Field<FieldValue, false>
					? K
					: never]: E[K] extends Field<infer V, boolean> ? V : never;
			} & {
				[K in keyof E as E[K] extends Field<FieldValue, true>
					? K
					: never]?: E[K] extends Field<infer V, boolean> ? V : never;
			} & GeneratedFields
		>
	>;
	query: <
		Fields extends {
			[K in keyof Fields]?: K extends keyof (E & GeneratedFields)
				? boolean | undefined
				: never;
		},
	>(opts: {
		fields: Fields;
	}) => Promise<
		DatabaseResult<
			Simplify<
				{
					[K in keyof Fields as Fields[K] extends true
						? K extends keyof E
							? E[K] extends Field<FieldValue, false>
								? K
								: never
							: never
						: never]: K extends keyof E
						? E[K] extends Field<infer V, boolean>
							? V
							: never
						: never;
				} & {
					[K in keyof Fields as Fields[K] extends true
						? K extends keyof E
							? E[K] extends Field<FieldValue, true>
								? K
								: never
							: never
						: never]?: K extends keyof E
						? E[K] extends Field<infer V, boolean>
							? V
							: never
						: never;
				} & {
					[K in keyof Fields as Fields[K] extends true
						? K extends keyof GeneratedFields
							? K
							: never
						: never]: K extends keyof GeneratedFields
						? GeneratedFields[K]
						: never;
				}
			>[]
		>
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
			success: true;
			data: T;
	  }
	| {
			success: false;
			error: DatabaseError;
	  };
