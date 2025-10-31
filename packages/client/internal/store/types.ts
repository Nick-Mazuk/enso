import type { Tagged } from "type-fest";

export type Id = Tagged<string, "TripleId">;
export const Id = (id: string) => id as Id;
export type Field = Tagged<string, "TripleField">;
export const Field = (field: string) => field as Field;
export type Value = Tagged<string | number | boolean, "TripleValue"> | Id;
export const Value = (value: string | number | boolean) => value as Value;
export type Datom = Id | Field | Value;
export type Triple = [Id, Field, Value];

const variableSymbol = Symbol("QueryVariable");
export type QueryVariable = {
	name: string;
	__brand: typeof variableSymbol;
};
export const Variable = (name: string): QueryVariable => ({
	name,
	__brand: variableSymbol,
});
export const isVariable = (value: unknown): value is QueryVariable => {
	return (
		typeof value === "object" &&
		value !== null &&
		"__brand" in value &&
		value.__brand === variableSymbol
	);
};
export type QueryPattern = [
	Id | QueryVariable,
	Field | QueryVariable,
	Value | QueryVariable,
];
