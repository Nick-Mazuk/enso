import type { Field, FieldOptions } from "./types";

// Helper type to correctly infer the 'Optional' boolean from the options
type InferOptional<Opts> = Opts extends { optional: true } ? true : false;

// Helper types to make the 'opts' parameter constraint cleaner
type StringOptions = FieldOptions<string, true> | FieldOptions<string, false>;
type NumberOptions = FieldOptions<number, true> | FieldOptions<number, false>;
type BooleanOptions =
	| FieldOptions<boolean, true>
	| FieldOptions<boolean, false>;

export const t = {
	string: <Opts extends StringOptions>(
		opts: Opts,
	): Field<string, InferOptional<Opts>> => {
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "string",
			...opts,
		};
	},
	number: <Opts extends NumberOptions>(
		opts: Opts,
	): Field<number, InferOptional<Opts>> => {
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "number",
			...opts,
		};
	},
	boolean: <Opts extends BooleanOptions>(
		opts: Opts,
	): Field<boolean, InferOptional<Opts>> => {
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "boolean",
			...opts,
		};
	},
};
