import { HLC } from "./hlc";

export type Value = string | number | boolean | Date | { [key: string]: any } | null | undefined;

export type Triple = [subject: string, predicate: string, object: Value];
export type TripleWithHLC = [
  subject: string,
  predicate: string,
  object: Value,
  hlc: HLC,
];

// Datalog Querying Types
export type DatalogVariable = `?${string}`;

// Operator Expressions for Clauses
export type NumericOperator =
  | "equals"
  | "greaterThan"
  | "lessThan"
  | "notEquals"
  | "greaterThanOrEqual"
  | "lessThanOrEqual";
export type NumericExpression = [operator: NumericOperator, value: number];

export type StringOperator =
  | "contains"
  | "startsWith"
  | "endsWith"
  | "equals"
  | "notEquals";
export type StringExpression = [operator: StringOperator, value: string];

export type DatalogClause = [
  subject: string | DatalogVariable,
  predicate: string | DatalogVariable,
  object: Value | DatalogVariable | NumericExpression | StringExpression,
];

// Aggregation Expressions for Find
export type AggregationOperator = "count" | "min" | "max" | "avg" | "sum";
export type AggregationExpression = [
  operator: AggregationOperator,
  variable: DatalogVariable,
];

export type DatalogQuery = {
  find: (DatalogVariable | AggregationExpression)[];
  where: DatalogClause[];
};

export type DatalogResult = Record<string, Value>;

export class TripleStore {
  private triples: TripleWithHLC[] = [];

  private _valuesAreEqual(a: Value, b: Value): boolean {
    if (a === null && b === null) return true;
    if (a === null || b === null) return false;

    if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) {
      return true;
    }

    if (a instanceof Date && b instanceof Date) {
      return a.getTime() === b.getTime();
    }

    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) {
        if (!this._valuesAreEqual(a[i], b[i])) return false;
      }
      return true;
    }

    if (typeof a === 'object' && typeof b === 'object' && !Array.isArray(a) && !Array.isArray(b)) {
      const aKeys = Object.keys(a).sort();
      const bKeys = Object.keys(b).sort();
      if (aKeys.length !== bKeys.length) return false;
      if (aKeys.join('') !== bKeys.join('')) return false;
      for (const key of aKeys) {
        if (!this._valuesAreEqual((a as any)[key], (b as any)[key])) return false;
      }
      return true;
    }
    return a === b;
  }

  public set(subject: string, predicate: string, object: Value, hlc: HLC): void {
    const existingIndex = this.triples.findIndex(
      (t) =>
        t[0] === subject &&
        t[1] === predicate &&
        this._valuesAreEqual(t[2], object),
    );

    if (existingIndex !== -1) {
      // If a triple with the same subject, predicate, and object exists
      const existingTriple = this.triples[existingIndex];
      const existingTripleHlc = existingTriple?.[3];
      if (existingTripleHlc && hlc.compare(existingTripleHlc) === 1) {
        // If the new HLC is newer, update the existing triple's HLC
        this.triples[existingIndex] = [subject, predicate, object, hlc];
      }
    } else {
      // If no such triple exists, add the new one
      this.triples.push([subject, predicate, object, hlc]);
    }
  }

  public delete(
    subject: string,
    predicate: string,
    object: Value,
    hlc: HLC,
  ): void {
    const existingIndex = this.triples.findIndex(
      (t) =>
        t[0] === subject &&
        t[1] === predicate &&
        this._valuesAreEqual(t[2], object),
    );

    if (existingIndex !== -1) {
      const existingTriple = this.triples[existingIndex];
      const existingTripleHlc = existingTriple?.[3];
      if (existingTripleHlc && hlc.compare(existingTripleHlc) === 1) {
        this.triples.splice(existingIndex, 1);
      }
    }
  }

  private _matchesClause(
    triple: TripleWithHLC,
    clause: DatalogClause,
    bindings: DatalogResult,
  ): DatalogResult | null {
    const newBindings = { ...bindings };
    const [sClause, pClause, oClause] = clause;
    const [sTriple, pTriple, oTriple] = triple;

    // Temporarily bind subject, predicate, and object for potential use in find
    newBindings['_s'] = sTriple;
    newBindings['_p'] = pTriple;
    newBindings['_o'] = oTriple;

    const matchPart = (clausePart: any, triplePart: Value): boolean => {
      if (typeof clausePart === 'string' && clausePart.startsWith('?')) {
        const varName = clausePart as DatalogVariable;
        if (newBindings[varName] === undefined) {
          newBindings[varName] = triplePart;
          return true; // Successfully bound
        }
        return this._valuesAreEqual(newBindings[varName]!, triplePart);
      }
      return this._valuesAreEqual(clausePart, triplePart);
    };

    if (!matchPart(sClause, sTriple)) return null;
    if (!matchPart(pClause, pTriple)) return null;

    let oMatch = false; // Flag to track if the object part matches

    if (Array.isArray(oClause)) {
      const isNumericOperator = (op: string): op is NumericOperator =>
        ['equals', 'greaterThan', 'lessThan', 'notEquals', 'greaterThanOrEqual', 'lessThanOrEqual'].includes(op);
      const isStringOperator = (op: string): op is StringOperator =>
        ['contains', 'startsWith', 'endsWith', 'equals', 'notEquals'].includes(op);

      const isOperatorExpression =
        (oClause.length === 2 && typeof oClause[0] === 'string' && (isNumericOperator(oClause[0]) || isStringOperator(oClause[0])));

      if (isOperatorExpression) {
        const [operator, operand] = oClause;

        if (typeof oTriple === 'number' && typeof operand === 'number') {
          switch (operator) {
            case 'equals': oMatch = oTriple === operand; break;
            case 'greaterThan': oMatch = oTriple > operand; break;
            case 'lessThan': oMatch = oTriple < operand; break;
            case 'notEquals': oMatch = oTriple !== operand; break;
            case 'greaterThanOrEqual': oMatch = oTriple >= operand; break;
            case 'lessThanOrEqual': oMatch = oTriple <= operand; break;
            default: oMatch = false; // Unknown numeric operator
          }
        } else if (typeof oTriple === 'string' && typeof operand === 'string') {
          const lowerOTriple = oTriple.toLowerCase();
          const lowerOperand = operand.toLowerCase();
          switch (operator) {
            case 'equals': oMatch = oTriple === operand; break; // Case-sensitive equals
            case 'notEquals': oMatch = oTriple !== operand; break; // Case-sensitive notEquals
            case 'contains': oMatch = lowerOTriple.includes(lowerOperand); break;
            case 'startsWith': oMatch = lowerOTriple.startsWith(lowerOperand); break;
            case 'endsWith': oMatch = lowerOTriple.endsWith(lowerOperand); break;
            default: oMatch = false; // Unknown string operator
          }
        }
        if (!oMatch) return null;

      } else {
        // It's a literal array value, treat it as a Value
        if (!matchPart(oClause, oTriple)) return null;
      }
    } else {
      // Not an array, so it's either a Value or DatalogVariable
      if (!matchPart(oClause, oTriple)) return null;
    }
    return newBindings;
  }

  private getSolutionsForClause(clause: DatalogClause): DatalogResult[] {
    const solutions: DatalogResult[] = [];
    for (const triple of this.triples) {
      const solution = this._matchesClause(triple, clause, {});
      if (solution) {
        solutions.push(solution);
      }
    }
    return solutions;
  }

  private joinSolutions(solutionsA: DatalogResult[], solutionsB: DatalogResult[]): DatalogResult[] {
    const joined: DatalogResult[] = [];
    for (const solA of solutionsA) {
      for (const solB of solutionsB) {
        const commonVars = Object.keys(solA).filter(
          v => v.startsWith('?') && Object.keys(solB).includes(v)
        );
        let compatible = true;
        for (const v of commonVars) {
          if (!this._valuesAreEqual(solA[v]!, solB[v]!)) {
            compatible = false;
            break;
          }
        }
        if (compatible) {
          joined.push({ ...solA, ...solB });
        }
      }
    }
    return joined;
  }

  public find(query: DatalogQuery): DatalogResult[] {
    const { where, find } = query;

    if (where.length === 0) {
      const regularFindVariables: DatalogVariable[] = [];
      const aggregationExpressions: AggregationExpression[] = [];

      for (const item of find) {
        if (typeof item === 'string' && item.startsWith('?')) {
          regularFindVariables.push(item as DatalogVariable);
        } else if (Array.isArray(item)) {
          aggregationExpressions.push(item as AggregationExpression);
        }
      }

      const results: DatalogResult[] = [];
      if (regularFindVariables.length > 0) {
        for (const triple of this.triples) {
          const result: DatalogResult = {};
          for (const variable of regularFindVariables) {
            if (variable === '?s') result[variable] = triple[0];
            else if (variable === '?p') result[variable] = triple[1];
            else if (variable === '?o') result[variable] = triple[2];
          }
          results.push(result);
        }
      }

      const aggregationResults: DatalogResult = {};
      if (aggregationExpressions.length > 0) {
        for (const [operator, variable] of aggregationExpressions) {
          const values: number[] = [];
          for (const triple of this.triples) {
            let val: Value | undefined;
            if (variable === '?s') val = triple[0];
            else if (variable === '?p') val = triple[1];
            else if (variable === '?o') val = triple[2];

            if (typeof val === 'number') {
              values.push(val);
            }
          }

          let aggResult: Value | null;
          switch (operator) {
            case 'count':
              const distinctValues = new Set();
              for (const triple of this.triples) {
                let val: Value | undefined;
                if (variable === '?s') val = triple[0];
                else if (variable === '?p') val = triple[1];
                else if (variable === '?o') val = triple[2];
                if (val !== undefined) {
                  distinctValues.add(val);
                }
              }
              aggResult = distinctValues.size; // Return 0 for empty set
              break;
            case 'min':
              aggResult = values.length > 0 ? Math.min(...values) : null;
              break;
            case 'max':
              aggResult = values.length > 0 ? Math.max(...values) : null;
              break;
            case 'avg':
              aggResult = values.length > 0 ? values.reduce((sum, v) => sum + v, 0) / values.length : null;
              break;
            case 'sum':
              aggResult = values.length > 0 ? values.reduce((sum, v) => sum + v, 0) : null;
              break;
          }
          aggregationResults[`?${operator}_${variable.substring(1)}`] = aggResult;
        }
      }

      if (regularFindVariables.length > 0 && aggregationExpressions.length > 0) {
        return results.map(r => ({ ...r, ...aggregationResults }));
      } else if (aggregationExpressions.length > 0) {
        return [aggregationResults];
      } else {
        return results;
      }
    }

    let solutions: DatalogResult[] = [];
    solutions = this.getSolutionsForClause(where[0]!);
    for (let i = 1; i < where.length; i++) {
      const nextSolutions = this.getSolutionsForClause(where[i]!);
      solutions = this.joinSolutions(solutions, nextSolutions);
    }

    const finalResults: DatalogResult[] = [];
    const aggregationResults: DatalogResult = {};
    const regularFindVariables: DatalogVariable[] = [];
    const aggregationExpressions: AggregationExpression[] = [];

    for (const item of find) {
      if (typeof item === 'string' && item.startsWith('?')) {
        regularFindVariables.push(item as DatalogVariable);
      } else if (Array.isArray(item)) {
        aggregationExpressions.push(item as AggregationExpression);
      }
    }

    // Process regular variables
    const mappedResults = solutions.map(solution => {
      const result: DatalogResult = {};
      for (const variable of regularFindVariables) {
        if (solution[variable] !== undefined) {
          result[variable] = solution[variable];
        } else {
          // Handle cases where find variables are not explicitly bound in where, but might be temporary
          if (variable === '?s' && solution['_s'] !== undefined) result[variable] = solution['_s'];
          if (variable === '?p' && solution['_p'] !== undefined) result[variable] = solution['_p'];
          if (variable === '?o' && solution['_o'] !== undefined) result[variable] = solution['_o'];
        }
      }
      return result;
    });

    // Process aggregations
    for (const [operator, variable] of aggregationExpressions) {
      const values: number[] = [];
      for (const solution of solutions) {
        const val = solution[variable];
        if (typeof val === 'number') { // Only aggregate numbers for now
          values.push(val);
        }
      }

      let aggResult: Value | null;
      switch (operator) {
        case 'count':
          const distinctValues = new Set();
          for (const solution of solutions) {
            if (solution[variable] !== undefined) {
              distinctValues.add(solution[variable]);
            }
          }
          aggResult = distinctValues.size; // Return 0 for empty set
          break;
        case 'min':
          aggResult = values.length > 0 ? Math.min(...values) : null;
          break;
        case 'max':
          aggResult = values.length > 0 ? Math.max(...values) : null;
          break;
        case 'avg':
          aggResult = values.length > 0 ? values.reduce((sum, v) => sum + v, 0) / values.length : null;
          break;
        case 'sum':
          aggResult = values.length > 0 ? values.reduce((sum, v) => sum + v, 0) : null;
          break;
      }
      aggregationResults[`?${operator}_${variable.substring(1)}`] = aggResult;
    }

    // Combine results
    if (regularFindVariables.length > 0) {
      // If there are regular variables, each mappedResult gets aggregationResults merged
      for (const res of mappedResults) {
        finalResults.push({ ...res, ...aggregationResults });
      }
    } else if (aggregationExpressions.length > 0) {
      // If only aggregations are requested, return a single result object
      if (Object.keys(aggregationResults).length > 0) {
        finalResults.push(aggregationResults);
      }
    } else {
      // If find is empty, return all solutions (after cleaning up temp bindings)
      for (const solution of solutions) {
        delete solution['_s'];
        delete solution['_p'];
        delete solution['_o'];
        finalResults.push(solution);
      }
    }

    const uniqueResults = [];
    const seen = new Set();
    for (const result of finalResults) { // Use finalResults here
        // Clean up temporary bindings before finalizing results
        delete result['_s'];
        delete result['_p'];
        delete result['_o'];

      if (Object.keys(result).length > 0 || find.length === 0) {
        const key = JSON.stringify(Object.entries(result).sort());
        if (!seen.has(key)) {
          uniqueResults.push(result);
          seen.add(key);
        }
      }
    }

    return uniqueResults;
  }

  public findOne(query: DatalogQuery): DatalogResult | undefined {
    const results = this.find(query);
    return results.length > 0 ? results[0] : undefined;
  }
}
