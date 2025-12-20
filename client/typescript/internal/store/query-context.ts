import type { Datom, QueryVariable } from "./types";

export class QueryContext {
	private context: Map<string, Datom>;

	constructor(context: Map<string, Datom> = new Map()) {
		this.context = new Map(context);
	}

	// Sets a variable to a datom in the context.
	// If the variable is already set, it will be overwritten.
	set(variable: QueryVariable, datom: Datom) {
		this.context.set(variable.name, datom);
	}

	// Gets a variable from the context.
	// If the variable is not set, it returns undefined.
	get(variable: QueryVariable): Datom | undefined {
		return this.context.get(variable.name);
	}

	// Checks if a variable is set in the context.
	has(variable: QueryVariable): boolean {
		return this.context.has(variable.name);
	}

	// Creates a new context with the same variables and values.
	clone(): QueryContext {
		return new QueryContext(new Map(this.context));
	}

	size(): number {
		return this.context.size;
	}
}
