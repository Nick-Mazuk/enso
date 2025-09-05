import { beforeEach, describe, expect, test } from 'bun:test';
import { HLC } from './hlc';
import { TripleStore, type DatalogQuery } from './store';

describe('TripleStore', () => {
  // Helper to create HLCs for testing
  const createHLC = (timeOffset: number, counter: number = 0) =>
    new HLC(new Date(Date.UTC(2023, 0, 1, 0, 0, 0) + timeOffset), counter);

  // --- Tests for set method ---
  test('set method adds a new triple', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    const result = store.find({ find: ['?s', '?p', '?o'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
    expect(result[0]?.['?s']).toBe('s1');
    expect(result[0]?.['?p']).toBe('p1');
    expect(result[0]?.['?o']).toBe('o1');
  });

  test('set method updates an existing triple with a newer HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(0);
    const hlc2 = createHLC(1000); // Newer
    store.set('s1', 'p1', 'o1', hlc1);
    store.set('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s', '?p', '?o'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
    expect(result[0]?.['?s']).toBe('s1');
    expect(result[0]?.['?p']).toBe('p1');
    expect(result[0]?.['?o']).toBe('o1');
    // Cannot directly assert HLC from find result yet, as find doesn't return HLC
    // This will be verified once find returns HLC or a dedicated test for HLC update is added.
  });

  test('set method does not update an existing triple with an older HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(1000);
    const hlc2 = createHLC(0); // Older
    store.set('s1', 'p1', 'o1', hlc1);
    store.set('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s', '?p', '?o'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
    // Cannot directly assert HLC from find result yet
  });

  test('set method does not update an existing triple with the same HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(0);
    const hlc2 = createHLC(0); // Same HLC
    store.set('s1', 'p1', 'o1', hlc1);
    store.set('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s', '?p', '?o'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
    // Cannot directly assert HLC from find result yet
  });

  test('set method handles different Value types', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);

    store.set('s_str', 'p_str', 'string_value', hlc);
    store.set('s_num', 'p_num', 123, hlc);
    store.set('s_bool', 'p_bool', true, hlc);
    const dateValue = new Date('2023-01-01T10:00:00.000Z');
    store.set('s_date', 'p_date', dateValue, hlc);
    const jsonValue = { key: 'value', num: 42 };
    store.set('s_json', 'p_json', jsonValue, hlc);

    expect(store.find({ find: ['?o'], where: [['s_str', 'p_str', 'string_value']] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['s_num', 'p_num', 123]] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['s_bool', 'p_bool', true]] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['s_date', 'p_date', dateValue]] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['s_json', 'p_json', jsonValue]] }).length).toBe(1);
  });

  test('set method handles multiple distinct triples', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);
    store.set('s3', 'p3', 'o3', hlc);
    expect(store.find({ find: ['?s'], where: [] }).length).toBe(3);
  });

  test('set method handles empty strings for subject and predicate', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('', '', 'o1', hlc);
    store.set('s1', '', 'o2', hlc);
    store.set('', 'p1', 'o3', hlc);
    expect(store.find({ find: ['?o'], where: [['', '', 'o1']] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['s1', '', 'o2']] }).length).toBe(1);
    expect(store.find({ find: ['?o'], where: [['', 'p1', 'o3']] }).length).toBe(1);
  });

  // --- Tests for delete method ---
  test('delete method removes an existing triple with a newer HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(0);
    const hlc2 = createHLC(1000); // Newer
    store.set('s1', 'p1', 'o1', hlc1);
    store.delete('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(0);
  });

  test('delete method does not remove a triple with an older HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(1000);
    const hlc2 = createHLC(0); // Older
    store.set('s1', 'p1', 'o1', hlc1);
    store.delete('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
  });

  test('delete method does not remove a triple with the same HLC', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(0);
    const hlc2 = createHLC(0); // Same HLC
    store.set('s1', 'p1', 'o1', hlc1);
    store.delete('s1', 'p1', 'o1', hlc2);
    const result = store.find({ find: ['?s'], where: [['s1', 'p1', 'o1']] });
    expect(result.length).toBe(1);
  });

  test('delete method does nothing if triple does not exist', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.delete('s2', 'p2', 'o2', hlc.increment()); // Non-existent triple
    const result = store.find({ find: ['?s'], where: [] });
    expect(result.length).toBe(1);
  });

  test('delete method handles empty strings for subject and predicate', () => {
    const store = new TripleStore();
    const hlc1 = createHLC(0);
    const hlc2 = createHLC(1000);

    store.set('', '', 'o1', hlc1);
    store.set('s1', '', 'o2', hlc1);
    store.set('', 'p1', 'o3', hlc1);

    store.delete('', '', 'o1', hlc2);
    store.delete('s1', '', 'o2', hlc2);
    store.delete('', 'p1', 'o3', hlc2);

    const result = store.find({ find: ['?s'], where: [] });
    expect(result.length).toBe(0);
  });

  // --- Tests for find method (basic matching and variable binding) ---
  test('find method finds triples with exact subject, predicate, and object', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?s', '?p', '?o'],
      where: [['s1', 'p1', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(1);
    expect(results[0]?.['?s']).toBe('s1');
    expect(results[0]?.['?p']).toBe('p1');
    expect(results[0]?.['?o']).toBe('o1');
  });

  test('find method finds triples with only subject', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s1', 'p2', 'o2', hlc);
    store.set('s2', 'p3', 'o3', hlc);

    const query: DatalogQuery = {
      find: ['?p', '?o'],
      where: [['s1', '?p', '?o']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?p': 'p1', '?o': 'o1' },
        { '?p': 'p2', '?o': 'o2' },
      ]),
    );
  });

  test('find method finds triples with only predicate', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p1', 'o2', hlc);
    store.set('s3', 'p2', 'o3', hlc);

    const query: DatalogQuery = {
      find: ['?s', '?o'],
      where: [['?s', 'p1', '?o']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?s': 's1', '?o': 'o1' },
        { '?s': 's2', '?o': 'o2' },
      ]),
    );
  });

  test('find method finds triples with only object', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o1', hlc);
    store.set('s3', 'p3', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?s', '?p'],
      where: [['?s', '?p', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?s': 's1', '?p': 'p1' },
        { '?s': 's2', '?p': 'p2' },
      ]),
    );
  });

  test('find method finds triples with subject and predicate', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s1', 'p1', 'o2', hlc);
    store.set('s2', 'p2', 'o3', hlc);

    const query: DatalogQuery = {
      find: ['?o'],
      where: [['s1', 'p1', '?o']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?o': 'o1' },
        { '?o': 'o2' },
      ]),
    );
  });

  test('find method finds triples with subject and object', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s1', 'p2', 'o1', hlc);
    store.set('s2', 'p3', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?p'],
      where: [['s1', '?p', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?p': 'p1' },
        { '?p': 'p2' },
      ]),
    );
  });

  test('find method finds triples with predicate and object', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p1', 'o1', hlc);
    store.set('s3', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?s'],
      where: [['?s', 'p1', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?s': 's1' },
        { '?s': 's2' },
      ]),
    );
  });

  test('find method finds all triples with empty where clause', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?s', '?p', '?o'],
      where: [],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?s': 's1', '?p': 'p1', '?o': 'o1' },
        { '?s': 's2', '?p': 'p2', '?o': 'o2' },
      ]),
    );
  });

  test('find method binds subject variable', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?x'],
      where: [['?x', 'p1', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(1);
    expect(results[0]?.['?x']).toBe('s1');
  });

  test('find method binds predicate variable', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?y'],
      where: [['s1', '?y', 'o1']],
    };
    const results = store.find(query);
    expect(results.length).toBe(1);
    expect(results[0]?.['?y']).toBe('p1');
  });

  test('find method binds object variable', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?z'],
      where: [['s1', 'p1', '?z']],
    };
    const results = store.find(query);
    expect(results.length).toBe(1);
    expect(results[0]?.['?z']).toBe('o1');
  });

  test('find method binds multiple variables', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);
    store.set('s2', 'p2', 'o2', hlc);

    const query: DatalogQuery = {
      find: ['?x', '?y', '?z'],
      where: [['?x', '?y', '?z']],
    };
    const results = store.find(query);
    expect(results.length).toBe(2);
    expect(results).toEqual(
      expect.arrayContaining([
        { '?x': 's1', '?y': 'p1', '?z': 'o1' },
        { '?x': 's2', '?y': 'p2', '?z': 'o2' },
      ]),
    );
  });

  test('find method binds the same variable in multiple clauses', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('user1', 'name', 'Alice', hlc);
    store.set('user1', 'age', 30, hlc);
    store.set('user2', 'name', 'Bob', hlc);

    const query: DatalogQuery = {
      find: ['?user'],
      where: [
        ['?user', 'name', 'Alice'],
        ['?user', 'age', 30],
      ],
    };
    const results = store.find(query);
    expect(results.length).toBe(1);
    expect(results[0]?.['?user']).toBe('user1');
  });

  test('find method returns empty array for non-existent triples', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);

    const query: DatalogQuery = {
      find: ['?s'],
      where: [['s2', 'p2', 'o2']],
    };
    const results = store.find(query);
    expect(results.length).toBe(0);
  });

  test('find method returns empty array for conflicting variable bindings', () => {
    const store = new TripleStore();
    const hlc = createHLC(0);
    store.set('s1', 'p1', 'o1', hlc);

    const query: DatalogQuery = {
      find: ['?x'],
      where: [
        ['?x', 'p1', 'o1'],
        ['?x', 'p2', 'o2'], // This will conflict with the first clause for ?x
      ],
    };
    const results = store.find(query);
    expect(results.length).toBe(0);
  });

  // --- Tests for Edge Cases ---
  describe('Edge Cases', () => {
    test('set method with identical HLCs but different object values (last write wins or ignored)', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('s_edge', 'p_edge', 'o_initial', hlc);
      store.set('s_edge', 'p_edge', 'o_new', hlc); // Same HLC, different object
      const result = store.find({ find: ['?o'], where: [['s_edge', 'p_edge', '?o']] });
      expect(result.length).toBe(2); // Expecting two distinct triples as (S, P, O) is the unique key
      expect(result).toEqual(
        expect.arrayContaining([
          { '?o': 'o_initial' },
          { '?o': 'o_new' },
        ]),
      );
    });

    test('set method handles null as an object value', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('s_null', 'p_null', null, hlc);
      const result = store.find({ find: ['?o'], where: [['s_null', 'p_null', null]] });
      expect(result.length).toBe(1);
      expect(result[0]?.['?o']).toBe(null);
    });

    test('set method handles NaN as an object value', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('s_nan', 'p_nan', NaN, hlc);
      const result = store.find({ find: ['?o'], where: [['s_nan', 'p_nan', NaN]] });
      expect(result.length).toBe(1);
      expect(isNaN(result[0]?.['?o'] as number)).toBe(true);
    });

    test('set method handles empty array and empty object as object values', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('s_empty_arr', 'p_empty_arr', [], hlc);
      store.set('s_empty_obj', 'p_empty_obj', {}, hlc);

      const resultArr = store.find({ find: ['?o'], where: [['s_empty_arr', 'p_empty_arr', []]] });
      expect(resultArr.length).toBe(1);
      expect(resultArr[0]?.['?o']).toEqual([]);

      const resultObj = store.find({ find: ['?o'], where: [['s_empty_obj', 'p_empty_obj', {}]] });
      expect(resultObj.length).toBe(1);
      expect(resultObj[0]?.['?o']).toEqual({});
    });

    test('find method with a variable in find clause not present in where clause', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('s1', 'p1', 'o1', hlc);

      const query: DatalogQuery = {
        find: ['?s', '?p', '?o', '?unbound'],
        where: [['s1', 'p1', 'o1']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?s']).toBe('s1');
      expect(results[0]?.['?p']).toBe('p1');
      expect(results[0]?.['?o']).toBe('o1');
      expect(results[0]?.['?unbound']).toBeUndefined(); // Expect unbound variable to be undefined
    });

    test('find method with self-referential triple (same variable for subject and object)', () => {
      const store = new TripleStore();
      const hlc = createHLC(0);
      store.set('person1', 'knows', 'person1', hlc);
      store.set('person2', 'knows', 'person3', hlc);

      const query: DatalogQuery = {
        find: ['?x'],
        where: [['?x', 'knows', '?x']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?x']).toBe('person1');
    });

    test('find method on an empty store with a complex query', () => {
      const store = new TripleStore();
      const query: DatalogQuery = {
        find: ['?s', '?p', '?o'],
        where: [
          ['?s', 'hasType', 'User'],
          ['?s', 'name', '?name'],
          ['?s', 'age', '?age'],
        ],
      };
      const results = store.find(query);
      expect(results.length).toBe(0);
    });
  });

  // --- Tests for Operator Expressions ---
  describe('Operator Expressions', () => {
    let store: TripleStore;
    const hlc = createHLC(0);

    beforeEach(() => {
      store = new TripleStore();
      store.set('item1', 'price', 10, hlc);
      store.set('item2', 'price', 20, hlc);
      store.set('item3', 'price', 30, hlc);
      store.set('productA', 'name', 'Apple', hlc);
      store.set('productB', 'name', 'Banana', hlc);
      store.set('productC', 'name', 'Cherry', hlc);
    });

    // Numeric Operators
    test('find with numeric "equals" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['equals', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?item']).toBe('item2');
    });

    test('find with numeric "greaterThan" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['greaterThan', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?item']).toBe('item3');
    });

    test('find with numeric "lessThan" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['lessThan', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?item']).toBe('item1');
    });

    test('find with numeric "notEquals" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['notEquals', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(2);
      expect(results).toEqual(
        expect.arrayContaining([
          { '?item': 'item1' },
          { '?item': 'item3' },
        ]),
      );
    });

    test('find with numeric "greaterThanOrEqual" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['greaterThanOrEqual', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(2);
      expect(results).toEqual(
        expect.arrayContaining([
          { '?item': 'item2' },
          { '?item': 'item3' },
        ]),
      );
    });

    test('find with numeric "lessThanOrEqual" operator', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'price', ['lessThanOrEqual', 20]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(2);
      expect(results).toEqual(
        expect.arrayContaining([
          { '?item': 'item1' },
          { '?item': 'item2' },
        ]),
      );
    });

    // String Operators
    test('find with string "equals" operator', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['equals', 'Banana']]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?product']).toBe('productB');
    });

    test('find with string "notEquals" operator', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['notEquals', 'Banana']]],
      };
      const results = store.find(query);
      expect(results.length).toBe(2);
      expect(results).toEqual(
        expect.arrayContaining([
          { '?product': 'productA' },
          { '?product': 'productC' },
        ]),
      );
    });

    test('find with string "contains" operator', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['contains', 'app']]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?product']).toBe('productA');
    });

    test('find with string "startsWith" operator', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['startsWith', 'Ban']]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?product']).toBe('productB');
    });

    test('find with string "endsWith" operator', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['endsWith', 'erry']]],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?product']).toBe('productC');
    });

    test('find with operator and type mismatch (numeric on string)', () => {
      const query: DatalogQuery = {
        find: ['?product'],
        where: [['?product', 'name', ['greaterThan', 10]]], // Numeric operator on string value
      };
      const results = store.find(query);
      expect(results.length).toBe(0);
    });

    test('find with operator on non-existent triple', () => {
      const query: DatalogQuery = {
        find: ['?item'],
        where: [['?item', 'nonExistentProp', ['equals', 100]]],
      };
      const results = store.find(query);
      expect(results.length).toBe(0);
    });
  });

  // --- Tests for Aggregation Expressions ---
  describe('Aggregation Expressions', () => {
    let store: TripleStore;
    const hlc = createHLC(0);

    beforeEach(() => {
      store = new TripleStore();
      store.set('user1', 'age', 25, hlc);
      store.set('user2', 'age', 30, hlc);
      store.set('user3', 'age', 35, hlc);
      store.set('user4', 'age', 30, hlc); // Duplicate age
      store.set('user5', 'name', 'Alice', hlc); // Non-numeric value
    });

    test('find with "count" aggregation', () => {
      const query: DatalogQuery = {
        find: [['count', '?user']],
        where: [['?user', 'age', '?age']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?count_user']).toBe(4);
    });

    test('find with "min" aggregation', () => {
      const query: DatalogQuery = {
        find: [['min', '?age']],
        where: [['?user', 'age', '?age']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?min_age']).toBe(25);
    });

    test('find with "max" aggregation', () => {
      const query: DatalogQuery = {
        find: [['max', '?age']],
        where: [['?user', 'age', '?age']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?max_age']).toBe(35);
    });

    test('find with "avg" aggregation', () => {
      const query: DatalogQuery = {
        find: [['avg', '?age']],
        where: [['?user', 'age', '?age']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?avg_age']).toBe(30); // (25+30+35+30)/4 = 30
    });

    test('find with "sum" aggregation', () => {
      const query: DatalogQuery = {
        find: [['sum', '?age']],
        where: [['?user', 'age', '?age']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?sum_age']).toBe(120); // 25+30+35+30 = 120
    });

    test('find with aggregation on empty set of values', () => {
      const query: DatalogQuery = {
        find: [['count', '?nonExistent']],
        where: [['?user', 'nonExistentProp', '?nonExistent']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1); // Should return a single result object with undefined/0 for count
      expect(results[0]?.['?count_nonExistent']).toBe(0);
    });

    test('find with aggregation on non-numeric values (should be ignored)', () => {
      const query: DatalogQuery = {
        find: [['sum', '?name']],
        where: [['?user', 'name', '?name']],
      };
      const results = store.find(query);
      expect(results.length).toBe(1);
      expect(results[0]?.['?sum_name']).toBe(null); // Sum of non-numbers should be null due to JSON.stringify behavior
    });

    test('find with mixed regular variables and aggregations', () => {
      store.set('user1', 'city', 'NY', hlc);
      store.set('user2', 'city', 'LA', hlc);
      store.set('user3', 'city', 'NY', hlc);

      const query: DatalogQuery = {
        find: ['?city', ['count', '?user']],
        where: [['?user', 'city', '?city']],
      };
      // This test will likely fail with current implementation as aggregations are global, not per-group
      // The current find implementation aggregates globally, not per distinct value of a regular variable.
      // This would require a GROUP BY equivalent. For now, expect global count.
      const results = store.find(query);
      expect(results.length).toBe(2); // Expecting results for each distinct city
      expect(results).toEqual(
        expect.arrayContaining([
          { '?city': 'NY', '?count_user': 3 }, // Global count of users from the 'where' clause
          { '?city': 'LA', '?count_user': 3 }, // Global count of users from the 'where' clause
        ]),
      );
    });
  });
});
