// tests/query.test.ts — QueryDescriptor helpers, QueryBuilder, and serializer

import {
  FilterOp, LogicalOp, ScalarKind, ScalarValue,
  eq, neq, gt, gte, lt, lte, startsWith, contains, inList,
  and, or, not,
  BinaryFilter, LogicalFilter, UnaryFilter, FilterNode,
  QueryDescriptor,
} from '../src/query/descriptor';
import { QueryBuilder } from '../src/query/builder';
import { serializeDescriptor } from '../src/query/serializer';
import { decode } from '@msgpack/msgpack';

// ─── ScalarValue factory ──────────────────────────────────────────────────────

describe('ScalarValue', () => {
  test('null()', () => {
    expect(ScalarValue.null().kind).toBe(ScalarKind.Null);
  });

  test('bool()', () => {
    const v = ScalarValue.bool(true);
    expect(v.kind).toBe(ScalarKind.Bool);
    expect(v.boolVal).toBe(true);
  });

  test('int32()', () => {
    const v = ScalarValue.int32(42);
    expect(v.kind).toBe(ScalarKind.Int32);
    expect(v.int32Val).toBe(42);
  });

  test('double()', () => {
    const v = ScalarValue.double(3.14);
    expect(v.kind).toBe(ScalarKind.Double);
    expect(v.doubleVal).toBeCloseTo(3.14, 5);
  });

  test('string()', () => {
    const v = ScalarValue.string('hello');
    expect(v.kind).toBe(ScalarKind.String);
    expect(v.stringVal).toBe('hello');
  });

  test('dateTime()', () => {
    const d = new Date();
    const v = ScalarValue.dateTime(d);
    expect(v.kind).toBe(ScalarKind.DateTime);
    expect(v.dateTimeVal).toBe(d);
  });

  test('array()', () => {
    const v = ScalarValue.array([ScalarValue.int32(1), ScalarValue.int32(2)]);
    expect(v.kind).toBe(ScalarKind.Array);
    expect(v.arrayVal).toHaveLength(2);
  });

  describe('from()', () => {
    test('null / undefined → Null', () => {
      expect(ScalarValue.from(null).kind).toBe(ScalarKind.Null);
      expect(ScalarValue.from(undefined).kind).toBe(ScalarKind.Null);
    });

    test('boolean → Bool', () => {
      expect(ScalarValue.from(false).kind).toBe(ScalarKind.Bool);
    });

    test('small integer → Int32', () => {
      const v = ScalarValue.from(100);
      expect(v.kind).toBe(ScalarKind.Int32);
      expect(v.int32Val).toBe(100);
    });

    test('large integer → Double', () => {
      expect(ScalarValue.from(2_147_483_648).kind).toBe(ScalarKind.Double);
    });

    test('float → Double', () => {
      expect(ScalarValue.from(1.5).kind).toBe(ScalarKind.Double);
    });

    test('bigint → Int64', () => {
      expect(ScalarValue.from(99n).kind).toBe(ScalarKind.Int64);
    });

    test('string → String', () => {
      expect(ScalarValue.from('x').kind).toBe(ScalarKind.String);
    });

    test('Date → DateTime', () => {
      expect(ScalarValue.from(new Date()).kind).toBe(ScalarKind.DateTime);
    });

    test('array → Array', () => {
      const v = ScalarValue.from([1, 2]);
      expect(v.kind).toBe(ScalarKind.Array);
      expect(v.arrayVal).toHaveLength(2);
    });

    test('unknown type throws', () => {
      expect(() => ScalarValue.from(Symbol())).toThrow(TypeError);
    });
  });
});

// ─── Filter factory functions ─────────────────────────────────────────────────

describe('Filter factories', () => {
  test('eq', () => {
    const f = eq('name', 'Alice') as BinaryFilter;
    expect(f.kind).toBe('binary');
    expect(f.field).toBe('name');
    expect(f.op).toBe(FilterOp.Eq);
    expect(f.value.stringVal).toBe('Alice');
  });

  test('neq', () => {
    const f = neq('status', 'banned') as BinaryFilter;
    expect(f.op).toBe(FilterOp.NotEq);
  });

  test('gt', () => {
    const f = gt('age', 18) as BinaryFilter;
    expect(f.op).toBe(FilterOp.Gt);
    expect(f.value.int32Val).toBe(18);
  });

  test('gte', () => {
    expect((gte('age', 18) as BinaryFilter).op).toBe(FilterOp.GtEq);
  });

  test('lt', () => {
    expect((lt('age', 65) as BinaryFilter).op).toBe(FilterOp.Lt);
  });

  test('lte', () => {
    expect((lte('age', 65) as BinaryFilter).op).toBe(FilterOp.LtEq);
  });

  test('startsWith', () => {
    const f = startsWith('name', 'Ali') as BinaryFilter;
    expect(f.op).toBe(FilterOp.StartsWith);
    expect(f.value.stringVal).toBe('Ali');
  });

  test('contains', () => {
    const f = contains('bio', 'developer') as BinaryFilter;
    expect(f.op).toBe(FilterOp.Contains);
  });

  test('inList', () => {
    const f = inList('status', ['active', 'pending']) as BinaryFilter;
    expect(f.op).toBe(FilterOp.In);
    expect(f.value.kind).toBe(ScalarKind.Array);
    expect(f.value.arrayVal).toHaveLength(2);
    expect(f.value.arrayVal![0].stringVal).toBe('active');
  });

  test('and', () => {
    const f = and(eq('a', 1), eq('b', 2)) as LogicalFilter;
    expect(f.kind).toBe('logical');
    expect(f.op).toBe(LogicalOp.And);
    expect(f.children).toHaveLength(2);
  });

  test('or', () => {
    const f = or(eq('status', 'a'), eq('status', 'b')) as LogicalFilter;
    expect(f.op).toBe(LogicalOp.Or);
    expect(f.children).toHaveLength(2);
  });

  test('not', () => {
    const f = not(eq('active', false)) as UnaryFilter;
    expect(f.kind).toBe('unary');
    expect((f.operand as BinaryFilter).field).toBe('active');
  });
});

// ─── QueryBuilder ─────────────────────────────────────────────────────────────

describe('QueryBuilder', () => {
  function makeBuilder(): QueryBuilder<Record<string, unknown>> {
    // Provide a dummy executor that never yields anything
    return new QueryBuilder('users', async function* () {});
  }

  test('build() with no filters produces undefined where', () => {
    const d = makeBuilder().build();
    expect(d.where).toBeUndefined();
  });

  test('single whereEq → BinaryFilter', () => {
    const d = makeBuilder().whereEq('name', 'Alice').build();
    expect((d.where as BinaryFilter).kind).toBe('binary');
    expect((d.where as BinaryFilter).field).toBe('name');
    expect((d.where as BinaryFilter).value.stringVal).toBe('Alice');
  });

  test('multiple where predicates → AND-ed', () => {
    const d = makeBuilder()
      .whereEq('status', 'active')
      .whereGte('age', 18)
      .build();
    const lf = d.where as LogicalFilter;
    expect(lf.kind).toBe('logical');
    expect(lf.op).toBe(LogicalOp.And);
    expect(lf.children).toHaveLength(2);
  });

  test('orderBy ascending', () => {
    const d = makeBuilder().orderBy('name').build();
    expect(d.orderBy).toHaveLength(1);
    expect(d.orderBy![0].field).toBe('name');
    expect(d.orderBy![0].descending).toBe(false);
  });

  test('orderByDescending', () => {
    const d = makeBuilder().orderByDescending('createdAt').build();
    expect(d.orderBy![0].descending).toBe(true);
  });

  test('skip and take', () => {
    const d = makeBuilder().skip(10).take(20).build();
    expect(d.skip).toBe(10);
    expect(d.take).toBe(20);
  });

  test('collection name preserved', () => {
    const d = makeBuilder().build();
    expect(d.collection).toBe('users');
  });

  test('chained methods return the same builder', () => {
    const b = makeBuilder();
    const b2 = b.whereEq('x', 1).orderBy('y').skip(0).take(5);
    expect(b2).toBe(b);
  });

  test('full chain builds correct descriptor', () => {
    const d = makeBuilder()
      .whereGt('age', 21)
      .whereIn('status', ['active', 'vip'])
      .orderBy('name')
      .skip(0)
      .take(10)
      .build();

    const lf = d.where as LogicalFilter;
    expect(lf.op).toBe(LogicalOp.And);
    expect(lf.children).toHaveLength(2);
    expect((lf.children[0] as BinaryFilter).op).toBe(FilterOp.Gt);
    expect((lf.children[1] as BinaryFilter).op).toBe(FilterOp.In);
    expect(d.skip).toBe(0);
    expect(d.take).toBe(10);
  });

  test('toArray() collects results', async () => {
    const items = [{ id: 1 }, { id: 2 }];
    let idx = 0;
    const b = new QueryBuilder<{ id: number }>('col', async function* () {
      yield items[idx++]!;
      yield items[idx++]!;
    });
    const result = await b.toArray();
    expect(result).toEqual([{ id: 1 }, { id: 2 }]);
  });

  test('first() returns the first item', async () => {
    const b = new QueryBuilder<{ id: number }>('col', async function* () {
      yield { id: 99 };
    });
    const r = await b.first();
    expect(r?.id).toBe(99);
  });

  test('first() on empty stream returns undefined', async () => {
    const b = new QueryBuilder<{ id: number }>('col', async function* () {});
    const r = await b.first();
    expect(r).toBeUndefined();
  });
});

// ─── QueryDescriptor serializer ───────────────────────────────────────────────

describe('serializeDescriptor', () => {
  function serialize(d: QueryDescriptor): unknown[] {
    const bytes = serializeDescriptor(d);
    return decode(bytes) as unknown[];
  }

  test('returns a non-empty Uint8Array', () => {
    const bytes = serializeDescriptor({ collection: 'test' });
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBeGreaterThan(0);
  });

  test('encoded array[0] = collection name', () => {
    const arr = serialize({ collection: 'products' });
    expect(arr[0]).toBe('products');
  });

  test('encoded array[1] = null when no where clause', () => {
    const arr = serialize({ collection: 'c' });
    expect(arr[1]).toBeNull();
  });

  test('binary filter encoding — [0, [field, op, scalar]]', () => {
    const arr = serialize({ collection: 'c', where: eq('price', 100) });
    const whereUnion = arr[1] as unknown[];
    expect(whereUnion[0]).toBe(0); // tag = BinaryFilter
    const inner = whereUnion[1] as unknown[];
    expect(inner[0]).toBe('price');
    expect(inner[1]).toBe(FilterOp.Eq);
  });

  test('logical filter encoding — [1, [op, children]]', () => {
    const arr = serialize({
      collection: 'c',
      where: and(eq('a', 1), eq('b', 2)),
    });
    const whereUnion = arr[1] as unknown[];
    expect(whereUnion[0]).toBe(1); // tag = LogicalFilter
    const inner = whereUnion[1] as unknown[];
    expect(inner[0]).toBe(LogicalOp.And);
    expect((inner[1] as unknown[]).length).toBe(2);
  });

  test('unary filter encoding — [2, [operand]]', () => {
    const arr = serialize({ collection: 'c', where: not(eq('active', false)) });
    const whereUnion = arr[1] as unknown[];
    expect(whereUnion[0]).toBe(2); // tag = UnaryFilter
  });

  test('sort spec encoded correctly', () => {
    const arr = serialize({
      collection: 'c',
      orderBy: [{ field: 'name', descending: false }, { field: 'age', descending: true }],
    });
    const sorts = arr[3] as unknown[][];
    expect(sorts[0]).toEqual(['name', false]);
    expect(sorts[1]).toEqual(['age', true]);
  });

  test('skip and take encoded at indices [5] and [4]', () => {
    const arr = serialize({ collection: 'c', skip: 10, take: 25 });
    expect(arr[4]).toBe(25); // take
    expect(arr[5]).toBe(10); // skip
  });

  test('scalar int32 is in slot [2]', () => {
    const arr = serialize({ collection: 'c', where: eq('n', 42) });
    const scalarArr = (arr[1] as unknown[][])[1][2] as unknown[];
    expect(scalarArr[0]).toBe(ScalarKind.Int32); // kind
    expect(scalarArr[2]).toBe(42);               // int32Val at index 2
  });

  test('scalar string is in slot [6]', () => {
    const arr = serialize({ collection: 'c', where: eq('s', 'hello') });
    const scalarArr = (arr[1] as unknown[][])[1][2] as unknown[];
    expect(scalarArr[0]).toBe(ScalarKind.String);
    expect(scalarArr[6]).toBe('hello');
  });

  test('scalar bool is in slot [1]', () => {
    const arr = serialize({ collection: 'c', where: eq('active', true) });
    const scalarArr = (arr[1] as unknown[][])[1][2] as unknown[];
    expect(scalarArr[0]).toBe(ScalarKind.Bool);
    expect(scalarArr[1]).toBe(true);
  });

  test('null where → null scalar kind 0', () => {
    const arr = serialize({ collection: 'c', where: eq('ref', null) });
    const scalarArr = (arr[1] as unknown[][])[1][2] as unknown[];
    expect(scalarArr[0]).toBe(ScalarKind.Null);
  });

  test('In scalar array encoded as nested arrays', () => {
    const arr = serialize({ collection: 'c', where: inList('status', ['a', 'b']) });
    const scalarArr = (arr[1] as unknown[][])[1][2] as unknown[];
    expect(scalarArr[0]).toBe(ScalarKind.Array);
    const items = scalarArr[10] as unknown[][];
    expect(items).toHaveLength(2);
    expect(items[0][6]).toBe('a');
    expect(items[1][6]).toBe('b');
  });
});
