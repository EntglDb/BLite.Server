// tests/cbson.test.ts — C-BSON encode/decode round-trip tests

import { encodeCbson } from '../src/cbson/writer';
import { decodeCbson } from '../src/cbson/reader';
import { BsonDocument } from '../src/cbson/types';

// Build a forward/reverse key map from a list of field names
function makeKeyMaps(names: string[]): { fwd: Map<string, number>; rev: Map<number, string> } {
  const fwd = new Map<string, number>();
  const rev = new Map<number, string>();
  names.forEach((n, i) => {
    const id = i + 1;
    fwd.set(n, id);
    rev.set(id, n);
  });
  return { fwd, rev };
}

function roundTrip(doc: BsonDocument, fields: string[]): BsonDocument {
  const { fwd, rev } = makeKeyMaps(fields);
  const buf = encodeCbson(doc, fwd);
  return decodeCbson(buf, rev);
}

// ─── BsonId ───────────────────────────────────────────────────────────────────

describe('BsonId', () => {
  const { BsonId, BsonIdType } = require('../src/cbson/types');

  test('fromString / toString', () => {
    const id = BsonId.fromString('hello');
    expect(id.type).toBe(BsonIdType.String);
    expect(id.toString()).toBe('hello');
  });

  test('fromInt32 / toString', () => {
    const id = BsonId.fromInt32(42);
    expect(id.type).toBe(BsonIdType.Int32);
    expect(id.toString()).toBe('42');
  });

  test('fromInt64 / toString', () => {
    const id = BsonId.fromInt64(12345678901234n);
    expect(id.type).toBe(BsonIdType.Int64);
    expect(id.toString()).toBe('12345678901234');
  });

  test('fromObjectId / toString', () => {
    const hex = '507f1f77bcf86cd799439011';
    const id = BsonId.fromObjectId(hex);
    expect(id.type).toBe(BsonIdType.ObjectId);
    expect(id.toString()).toBe(hex);
  });

  test('toProto / fromProto round-trip', () => {
    const original = BsonId.fromString('test-id');
    const proto = original.toProto();
    const restored = BsonId.fromProto(proto);
    expect(restored.toString()).toBe(original.toString());
    expect(restored.type).toBe(original.type);
  });
});

// ─── Primitive round-trips ────────────────────────────────────────────────────

describe('C-BSON primitives', () => {
  test('string field', () => {
    const doc = roundTrip({ name: 'Alice' }, ['name']);
    expect(doc.name).toBe('Alice');
  });

  test('int32 field', () => {
    const doc = roundTrip({ age: 30 }, ['age']);
    expect(doc.age).toBe(30);
  });

  test('negative int32', () => {
    const doc = roundTrip({ score: -1 }, ['score']);
    expect(doc.score).toBe(-1);
  });

  test('large int forces double', () => {
    const doc = roundTrip({ big: 2_147_483_648 }, ['big']); // > INT32_MAX
    expect(typeof doc.big).toBe('number');
    expect(doc.big).toBe(2_147_483_648);
  });

  test('bigint within MAX_SAFE_INTEGER → decoded as number', () => {
    const n = BigInt(Number.MAX_SAFE_INTEGER); // 2^53 - 1, within safe range
    const doc = roundTrip({ n }, ['n']);
    expect(doc.n).toBe(Number.MAX_SAFE_INTEGER); // returned as JS number
  });

  test('bigint beyond MAX_SAFE_INTEGER → decoded as bigint', () => {
    const n = 9_007_199_254_740_992n; // 2^53, exceeds safe range
    const doc = roundTrip({ n }, ['n']);
    expect(doc.n).toBe(9_007_199_254_740_992n); // returned as bigint
  });

  test('boolean true/false', () => {
    const doc = roundTrip({ active: true, disabled: false }, ['active', 'disabled']);
    expect(doc.active).toBe(true);
    expect(doc.disabled).toBe(false);
  });

  test('double field', () => {
    const doc = roundTrip({ price: 3.14 }, ['price']);
    expect(doc.price).toBeCloseTo(3.14, 5);
  });

  test('null field', () => {
    const doc = roundTrip({ ref: null }, ['ref']);
    expect(doc.ref).toBeNull();
  });

  test('Date field decoded as Date', () => {
    const d = new Date('2024-06-01T12:00:00Z');
    const doc = roundTrip({ ts: d }, ['ts']);
    expect(doc.ts).toBeInstanceOf(Date);
    expect((doc.ts as Date).getTime()).toBe(d.getTime());
  });
});

// ─── Complex types ────────────────────────────────────────────────────────────

describe('C-BSON complex types', () => {
  test('ObjectId round-trip via $oid', () => {
    const hex = '507f1f77bcf86cd799439011';
    const doc = roundTrip({ _id: { $oid: hex } }, ['_id']);
    expect((doc._id as { $oid: string }).$oid).toBe(hex);
  });

  test('Buffer (binary) round-trip', () => {
    const data = Buffer.from([0x01, 0x02, 0x03]);
    const doc = roundTrip({ blob: data }, ['blob']);
    expect(Buffer.from(doc.blob as Buffer)).toEqual(data);
  });

  test('nested document', () => {
    const doc = roundTrip(
      { address: { city: 'Rome', zip: '00100' } },
      ['address', 'city', 'zip'],
    );
    const addr = doc.address as BsonDocument;
    expect(addr.city).toBe('Rome');
    expect(addr.zip).toBe('00100');
  });

  test('string array', () => {
    const doc = roundTrip({ tags: ['a', 'b', 'c'] }, ['tags']);
    expect(doc.tags).toEqual(['a', 'b', 'c']);
  });

  test('mixed array', () => {
    const doc = roundTrip({ items: [1, 'x', true, null] }, ['items']);
    expect(doc.items).toEqual([1, 'x', true, null]);
  });

  test('multiple fields preserved', () => {
    const doc = roundTrip(
      { name: 'Alice', age: 30, active: true, score: 9.5 },
      ['name', 'age', 'active', 'score'],
    );
    expect(doc.name).toBe('Alice');
    expect(doc.age).toBe(30);
    expect(doc.active).toBe(true);
    expect(doc.score).toBeCloseTo(9.5, 5);
  });
});

// ─── Key-map enforcement ──────────────────────────────────────────────────────

describe('C-BSON key-map enforcement', () => {
  test('throws when field not registered', () => {
    const fwd = new Map<string, number>([['name', 1]]);
    expect(() => encodeCbson({ unknown: 'x' }, fwd)).toThrow(/unknown/i);
  });

  test('unknown key id decoded as numeric fallback', () => {
    const { fwd, rev } = makeKeyMaps(['name']);
    // Write with a known key, then decode with an empty reverse map
    const buf = encodeCbson({ name: 'Alice' }, fwd);
    const doc = decodeCbson(buf, new Map()); // empty reverse map → fallback
    expect(Object.keys(doc)).toHaveLength(1);
    expect(Object.values(doc)[0]).toBe('Alice');
  });
});

// ─── Encoded size sanity ──────────────────────────────────────────────────────

describe('C-BSON encoded size', () => {
  test('empty document encodes to 5 bytes (4-byte size + end marker)', () => {
    const buf = encodeCbson({}, new Map());
    expect(buf.length).toBe(5);
    // First 4 bytes = size (little-endian)
    expect(buf.readInt32LE(0)).toBe(5);
    // Last byte = end-of-document marker
    expect(buf[4]).toBe(0x00);
  });
});
