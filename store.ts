import { HLC } from "./hlc";

export type Subject = string;
export type Predicate = string;
export type Value = string | number | boolean | Date | null;
export type Ref = { id: string };
export type RefMany = Ref[];
export type Object = Value | Ref | RefMany;
export type Triple = [Subject, Predicate, Object, HLC];

export type DatalogVariable = `?${string}`;
export type DatalogPattern = [
  Subject | DatalogVariable,
  Predicate | DatalogVariable,
  Object | DatalogVariable
];
export type DatalogClause = DatalogPattern | [DatalogPattern, boolean];
export type DatalogQuery = DatalogClause[];
export type DatalogBindingValue = Subject | Predicate | Object;
export type DatalogBinding = Record<DatalogVariable, DatalogBindingValue>;

export class TripleStore {
  private spo = new Map<Subject, Map<Predicate, Map<Object, HLC>>>();
  private pos = new Map<Predicate, Map<Object, Map<Subject, HLC>>>();
  private osp = new Map<Object, Map<Subject, Map<Predicate, HLC>>>();

  add(triple: Triple) {
    const [subject, predicate, object, hlc] = triple;

    const predMap = this.spo.get(subject);
    const objMap = predMap?.get(predicate);

    if (objMap) {
      let latestHlc: HLC | undefined = undefined;
      for (const existingHlc of objMap.values()) {
        if (!latestHlc || existingHlc.compare(latestHlc) > 0) {
          latestHlc = existingHlc;
        }
      }

      if (latestHlc && hlc.compare(latestHlc) <= 0) {
        return;
      }

      const objectsToRemove = [...objMap.keys()];
      for (const obj of objectsToRemove) {
        const oldHlc = objMap.get(obj)!;
        this.remove([subject, predicate, obj, oldHlc]);
      }
    }

    this.addToIndex(this.spo, subject, predicate, object, hlc);
    this.addToIndex(this.pos, predicate, object, subject, hlc);
    this.addToIndex(this.osp, object, subject, predicate, hlc);
  }

  remove(triple: Triple) {
    const [subject, predicate, object] = triple;

    this.removeFromIndex(this.spo, subject, predicate, object);
    this.removeFromIndex(this.pos, predicate, object, subject);
    this.removeFromIndex(this.osp, object, subject, predicate);
  }

  querySubjects(query: { predicate: Predicate; object: Object }): Subject[] {
    const subjectMap = this.pos.get(query.predicate)?.get(query.object);
    return subjectMap ? [...subjectMap.keys()] : [];
  }

  query(q: Partial<Triple>): Triple[] {
    const [s, p, o] = [q[0], q[1], q[2]];
    if (s !== undefined) {
      if (p !== undefined) {
        return o !== undefined ? this._querySPO(s, p, o) : this._querySP(s, p);
      }
      return o !== undefined ? this._querySO(s, o) : this._queryS(s);
    }
    if (p !== undefined) {
      return o !== undefined ? this._queryPO(p, o) : this._queryP(p);
    }
    return o !== undefined ? this._queryO(o) : this._queryAll();
  }

  private _querySPO(s: Subject, p: Predicate, o: Object): Triple[] {
    const hlc = this.spo.get(s)?.get(p)?.get(o);
    return hlc ? [[s, p, o, hlc]] : [];
  }

  private _querySP(s: Subject, p: Predicate): Triple[] {
    const results: Triple[] = [];
    const objMap = this.spo.get(s)?.get(p);
    if (objMap) {
      for (const [o, hlc] of objMap.entries()) {
        results.push([s, p, o, hlc]);
      }
    }
    return results;
  }

  private _querySO(s: Subject, o: Object): Triple[] {
    const results: Triple[] = [];
    const predMap = this.osp.get(o)?.get(s);
    if (predMap) {
      for (const [p, hlc] of predMap.entries()) {
        results.push([s, p, o, hlc]);
      }
    }
    return results;
  }

  private _queryS(s: Subject): Triple[] {
    const results: Triple[] = [];
    const predMap = this.spo.get(s);
    if (predMap) {
      for (const [p, objMap] of predMap.entries()) {
        for (const [o, hlc] of objMap.entries()) {
          results.push([s, p, o, hlc]);
        }
      }
    }
    return results;
  }

  private _queryPO(p: Predicate, o: Object): Triple[] {
    const results: Triple[] = [];
    const subMap = this.pos.get(p)?.get(o);
    if (subMap) {
      for (const [s, hlc] of subMap.entries()) {
        results.push([s, p, o, hlc]);
      }
    }
    return results;
  }

  private _queryP(p: Predicate): Triple[] {
    const results: Triple[] = [];
    const objMap = this.pos.get(p);
    if (objMap) {
      for (const [o, subMap] of objMap.entries()) {
        for (const [s, hlc] of subMap.entries()) {
          results.push([s, p, o, hlc]);
        }
      }
    }
    return results;
  }

  private _queryO(o: Object): Triple[] {
    const results: Triple[] = [];
    const subMap = this.osp.get(o);
    if (subMap) {
      for (const [s, predMap] of subMap.entries()) {
        for (const [p, hlc] of predMap.entries()) {
          results.push([s, p, o, hlc]);
        }
      }
    }
    return results;
  }

  private _queryAll(): Triple[] {
    const results: Triple[] = [];
    for (const [s, predMap] of this.spo.entries()) {
      for (const [p, objMap] of predMap.entries()) {
        for (const [o, hlc] of objMap.entries()) {
          results.push([s, p, o, hlc]);
        }
      }
    }
    return results;
  }

  datalogQuery(query: DatalogQuery): DatalogBinding[] {
    return this._datalogQuery(query, this._getLatestTriples());
  }

  private _getLatestTriples(): Triple[] {
    const latestTriples = new Map<string, Triple>();
    for (const triple of this._queryAll()) {
      const key = `${triple[0]}-${triple[1]}`;
      const existing = latestTriples.get(key);
      if (!existing || triple[3].compare(existing[3]) > 0) {
        latestTriples.set(key, triple);
      }
    }
    return Array.from(latestTriples.values());
  }

  private _isVariable(term: any): term is `?${string}` {
    return typeof term === "string" && term.startsWith("?");
  }

  private _matchPattern(
    pattern: DatalogPattern,
    triple: Triple,
    context: DatalogBinding
  ): DatalogBinding | null {
    let newContext: DatalogBinding | null = { ...context };

    for (let i = 0; i < 3; i++) {
      const term = pattern[i];
      const value = triple[i as 0 | 1 | 2]!;

      if (this._isVariable(term)) {
        const variable = term as DatalogVariable;
        const boundValue = newContext[variable];
        if (boundValue === undefined) {
          newContext[variable] = value;
        } else if (boundValue !== value) {
          newContext = null;
          break;
        }
      } else if (term !== value) {
        newContext = null;
        break;
      }
    }

    return newContext;
  }

  private _querySingleClause(
    clause: DatalogPattern,
    triples: Triple[],
    contexts: DatalogBinding[]
  ): DatalogBinding[] {
    const nextBindings: DatalogBinding[] = [];

    for (const context of contexts) {
      for (const triple of triples) {
        const newContext = this._matchPattern(clause, triple, context);
        if (newContext) {
          nextBindings.push(newContext);
        }
      }
    }

    return nextBindings;
  }

  private _datalogQuery(
    query: DatalogQuery,
    triples: Triple[]
  ): DatalogBinding[] {
    const bindings = query.reduce(
      (contexts, clause) => {
        const isOptional = Array.isArray(clause[1]) && clause[1] === true;
        const actualClause = (
          isOptional ? clause[0] : clause
        ) as DatalogPattern;
        const [s, p, o] = actualClause;

        if (p === "greaterThan") {
          return contexts.filter((context) => {
            const val = context[s as DatalogVariable];
            return typeof val === "number" && val > (o as number);
          });
        }

        const nextContexts = this._querySingleClause(
          actualClause,
          triples,
          contexts
        );

        if (isOptional) {
          const matchedContextSubjects = new Set(
            nextContexts.map((c) => c["?e"])
          );
          const unmatchedContexts = contexts.filter(
            (c) => !matchedContextSubjects.has(c["?e"])
          );
          return [...nextContexts, ...unmatchedContexts];
        }

        return nextContexts;
      },
      [{}] as DatalogBinding[]
    );

    const uniqueBindings = Array.from(
      new Map(bindings.map((b) => [JSON.stringify(b), b])).values()
    );

    return uniqueBindings;
  }

  private addToIndex<A, B, C>(
    index: Map<A, Map<B, Map<C, HLC>>>,
    a: A,
    b: B,
    c: C,
    hlc: HLC
  ) {
    if (!index.has(a)) {
      index.set(a, new Map());
    }
    const mapA = index.get(a)!;

    if (!mapA.has(b)) {
      mapA.set(b, new Map());
    }
    const mapB = mapA.get(b)!;

    mapB.set(c, hlc);
  }

  private removeFromIndex<A, B, C>(
    index: Map<A, Map<B, Map<C, HLC>>>,
    a: A,
    b: B,
    c: C
  ) {
    const mapA = index.get(a);
    if (mapA) {
      const mapB = mapA.get(b);
      if (mapB) {
        mapB.delete(c);
        if (mapB.size === 0) {
          mapA.delete(b);
          if (mapA.size === 0) {
            index.delete(a);
          }
        }
      }
    }
  }
}
