import { assert } from "../../../shared/assert";
import type { Field, FieldOptions, FieldValue } from "./types";

// Helper type to correctly infer the 'Optional' boolean from the options
type InferOptional<Opts> = Opts extends { optional: true } ? true : false;

// Helper types to make the 'opts' parameter constraint cleaner
type StringOptions = FieldOptions<string, true> | FieldOptions<string, false>;
type NumberOptions = FieldOptions<number, true> | FieldOptions<number, false>;
type BooleanOptions =
	| FieldOptions<boolean, true>
	| FieldOptions<boolean, false>;

const verifyOpts = (opts: {
	opts: FieldOptions<FieldValue, boolean>;
	fallbackType: "string" | "number" | "boolean";
}) => {
	if (!opts.opts.optional) {
		assert(
			opts.opts.fallback !== undefined,
			"Required fields must have a fallback",
		);
	}
	if (opts.opts.fallback !== undefined) {
		assert(
			typeof opts.opts.fallback === opts.fallbackType,
			`Fallback must be of type ${opts.fallbackType}`,
		);
	}
};

export const t = {
	string: <Opts extends StringOptions>(
		opts: Opts,
	): Field<string, InferOptional<Opts>> => {
		verifyOpts({ opts, fallbackType: "string" });
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "string",
			...opts,
		};
	},
	number: <Opts extends NumberOptions>(
		opts: Opts,
	): Field<number, InferOptional<Opts>> => {
		verifyOpts({ opts, fallbackType: "number" });
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "number",
			...opts,
		};
	},
	boolean: <Opts extends BooleanOptions>(
		opts: Opts,
	): Field<boolean, InferOptional<Opts>> => {
		verifyOpts({ opts, fallbackType: "boolean" });
		// @ts-expect-error - the typescript types do wizardry
		return {
			kind: "boolean",
			...opts,
		};
	},
};
