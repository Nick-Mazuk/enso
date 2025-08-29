import { HLC } from "./hlc";

export type Subject = string;
export type Predicate = string;
export type Value = string | number | boolean | Date | null;
export type Ref = { id: string };
export type RefMany = Ref[];

export type Object = Value | Ref | RefMany;
export type Triple = [Subject, Predicate, Object, HLC];

export class TripleStore {
  private spo = new Map<Subject, Map<Predicate, Map<Object, HLC>>>();
  private pos = new Map<Predicate, Map<Object, Map<Subject, HLC>>>();
  private osp = new Map<Object, Map<Subject, Map<Predicate, HLC>>>();

  add(triple: Triple) {
    const [subject, predicate, object, hlc] = triple;

    const existingHlc = this.spo.get(subject)?.get(predicate)?.get(object);
    if (existingHlc && hlc.compare(existingHlc) <= 0) {
      return;
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

  query(triple: Partial<Triple>): Triple[] {
    const [s, p, o] = triple;
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
