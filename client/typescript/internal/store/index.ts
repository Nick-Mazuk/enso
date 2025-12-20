import { assert } from "../../../shared/assert";
import { QueryContext } from "./query-context";
import {
	type Datom,
	type Field,
	type Filter,
	type Id,
	isVariable,
	type QueryPattern,
	type QueryVariable,
	type Triple,
	type Value,
} from "./types";

type Query<Find extends QueryVariable[]> = {
	find: Find;
	where: QueryPattern[];
	optional?: QueryPattern[];
	filters?: Filter[];
	whereNot?: QueryPattern[];
};

/**
 * The Store is a class that manages the local data store for the client.
 */
export class Store {
	// private triples: Triple[] = [];
	private idIndex: Map<Id, Triple[]> = new Map();
	private fieldIndex: Map<Field, Triple[]> = new Map();
	private valueIndex: Map<Value, Triple[]> = new Map();
	private tripleCount = 0;

	add(...triples: Triple[]) {
		// TODO: Check for duplicates -- if the id/field already exists, update the value
		for (const triple of triples) {
			this.addToIndex(this.idIndex, triple[0], triple);
			this.addToIndex(this.fieldIndex, triple[1], triple);
			this.addToIndex(this.valueIndex, triple[2], triple);
		}
		this.tripleCount += triples.length;
	}

	private addToIndex(index: Map<Datom, Triple[]>, key: Datom, triple: Triple) {
		if (!index.has(key)) {
			index.set(key, [triple]);
		} else {
			index.get(key)?.push(triple);
		}
	}

	query<Find extends QueryVariable[]>(query: Query<Find>) {
		let contexts = this.queryMultiplePatterns(query.where);
		if (query.optional && query.optional.length > 0 && contexts.length > 0) {
			for (const pattern of query.optional) {
				contexts = this.queryOptionalPattern(pattern, contexts);
			}
		}
		if (query.whereNot && query.whereNot.length > 0 && contexts.length > 0) {
			const whereNot = query.whereNot;
			contexts = contexts.filter((context) => {
				const notMatches = this.queryMultiplePatterns(whereNot, [
					context.clone(),
				]);
				// Keep if no "not" patterns matched
				return notMatches.length === 0;
			});
		}
		if (query.filters && query.filters.length > 0 && contexts.length > 0) {
			const filters = query.filters;
			contexts = contexts.filter((context) =>
				filters.every(({ selector, filter }) => {
					const value = context.get(selector);
					return filter(value);
				}),
			);
		}
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

	private queryOptionalPattern(
		pattern: QueryPattern,
		contexts: QueryContext[],
	) {
		const newContexts: QueryContext[] = [];

		for (const context of contexts) {
			const matches = this.querySinglePattern(pattern, context);

			if (matches.length > 0) {
				newContexts.push(...matches);
			} else {
				newContexts.push(context);
			}
		}
		return newContexts;
	}

	private querySinglePattern(pattern: QueryPattern, context: QueryContext) {
		const contexts: QueryContext[] = [];
		for (const triple of this.relevantTriples(pattern)) {
			const newContext = this.matchPattern(pattern, triple, context);
			if (newContext) {
				contexts.push(newContext);
			}
		}
		return contexts;
	}

	private relevantTriples(pattern: QueryPattern): Triple[] {
		if (!isVariable(pattern[0])) {
			return this.idIndex.get(pattern[0]) ?? [];
		}
		if (!isVariable(pattern[1])) {
			return this.fieldIndex.get(pattern[1]) ?? [];
		}
		if (!isVariable(pattern[2])) {
			return this.valueIndex.get(pattern[2]) ?? [];
		}
		return [];
	}

	private queryMultiplePatterns(
		patterns: QueryPattern[],
		contexts: QueryContext[] = [new QueryContext()],
	) {
		let newContexts = contexts;
		for (const pattern of patterns) {
			newContexts = newContexts.flatMap((context) =>
				this.querySinglePattern(pattern, context),
			);
			if (newContexts.length === 0) {
				// No need to process more patterns if we have no matches.
				return [];
			}
		}
		// We should not return the initial empty context if no patterns were matched
		// and the database is empty.
		if (
			patterns.length > 0 &&
			newContexts.length === 1 &&
			newContexts[0]?.size() === 0
		) {
			return [];
		}
		return newContexts;
	}

	deleteAllById(id: Id) {
		// TODO: potentially instead just add a tombstone triple to handle replication.
		const triples = this.idIndex.get(id) ?? [];
		this.tripleCount -= triples.length;
		this.idIndex.delete(id);
		for (const triple of triples) {
			// The field and value indexes may have
			// triples that both reference this id and
			// reference other ids.
			const fieldTriples = this.fieldIndex.get(triple[1]);
			if (fieldTriples) {
				const newFieldTriples = fieldTriples.filter((t) => t[0] !== id);
				if (newFieldTriples.length > 0) {
					this.fieldIndex.set(triple[1], newFieldTriples);
				} else {
					this.fieldIndex.delete(triple[1]);
				}
			}
			const valueTriples = this.valueIndex.get(triple[2]);
			if (valueTriples) {
				const newValueTriples = valueTriples.filter((t) => t[0] !== id);
				if (newValueTriples.length > 0) {
					this.valueIndex.set(triple[2], newValueTriples);
				} else {
					this.valueIndex.delete(triple[2]);
				}
			}
		}
	}

	size() {
		return this.tripleCount;
	}
}
