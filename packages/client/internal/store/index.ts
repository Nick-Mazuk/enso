import { assert } from "../../../shared/assert";
import { QueryContext } from "./query-context";
import {
	type Field,
	type Id,
	isVariable,
	type QueryPattern,
	type QueryVariable,
	type Triple,
} from "./types";

type Query<Find extends QueryVariable[]> = {
	find: Find;
	where: QueryPattern[];
};

/**
 * The Store is a class that manages the local data store for the client.
 */
export class Store {
	private triples: Triple[] = [];

	add(...triples: Triple[]) {
		// TODO: Check for duplicates -- if the id/field already exists, update the value
		this.triples.push(...triples);
	}

	delete(triple: [Id, Field]) {}

	query<Find extends QueryVariable[]>(query: Query<Find>) {
		const contexts = this.queryMultiplePatterns(query.where);
		return contexts.map((context) => {
			return query.find.map((datom) => {
				return isVariable(datom) ? context.get(datom) : datom;
			});
		});
	}

	// For a given pattern and triple, it determines if the pattern can match the triple.
	// It matches if:
	//   - For all non-variable parts, they must be equal
	//   - For all variable parts, they must either equal what's in the context or not be set in the context
	//
	// If it matches, it returns an updated context with the newly determined values.
	// If it doesn't match, it returns undefined.
	private matchPattern(
		pattern: QueryPattern,
		triple: Triple,
		context: QueryContext,
	) {
		// TODO: figure out if there's an algorithmic way to do this without cloning.
		const newContext = context.clone();
		for (let i = 0; i < pattern.length && i < triple.length; i++) {
			const patternPart = pattern[i];
			const triplePart = triple[i];
			assert(
				patternPart !== undefined && triplePart !== undefined,
				"Pattern and triple must have the same length",
			);
			if (isVariable(patternPart)) {
				if (
					newContext.has(patternPart) &&
					newContext.get(patternPart) !== triplePart
				) {
					return undefined;
				}
				newContext.set(patternPart, triplePart);
				continue;
			}
			if (patternPart !== triplePart) {
				return undefined;
			}
		}
		return newContext;
	}

	private querySinglePattern(pattern: QueryPattern, context: QueryContext) {
		const contexts: QueryContext[] = [];
		for (const triple of this.triples) {
			const newContext = this.matchPattern(pattern, triple, context);
			if (newContext) {
				contexts.push(newContext);
			}
		}
		return contexts;
	}

	private queryMultiplePatterns(patterns: QueryPattern[]) {
		let contexts: QueryContext[] = [new QueryContext()];
		for (const pattern of patterns) {
			contexts = contexts.flatMap((context) =>
				this.querySinglePattern(pattern, context),
			);
			if (contexts.length === 0) {
				// No need to process more patterns if we have no matches.
				return [];
			}
		}
		// We should not return the initial empty context if no patterns were matched
		// and the database is empty.
		if (
			patterns.length > 0 &&
			contexts.length === 1 &&
			contexts[0]?.size() === 0
		) {
			return [];
		}
		return contexts;
	}

	size() {
		return this.triples.length;
	}
}
