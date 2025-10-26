import { type Brand, make } from "ts-brand";

export type Id = Brand<string, "TripleId">;
export const Id = make<Id>();
export type Field = Brand<string, "TripleField">;
export const Field = make<Field>();
export type Value = Brand<string | number | boolean, "TripleValue"> | Id;
export const Value = make<Value>();
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
