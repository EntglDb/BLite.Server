// tests/kv-store.test.ts — KvBatch builder

import { KvBatch } from '../src/kv-store';

const buf = (s: string) => Buffer.from(s);

describe('KvBatch', () => {
  test('starts empty', () => {
    const b = new KvBatch();
    expect(b.ops()).toHaveLength(0);
  });

  test('set() adds a non-delete op', () => {
    const b = new KvBatch();
    b.set('foo', buf('bar'));
    const ops = b.ops();
    expect(ops).toHaveLength(1);
    expect(ops[0]!.key).toBe('foo');
    expect(ops[0]!.value).toEqual(buf('bar'));
    expect(ops[0]!.isDelete).toBeUndefined();
  });

  test('set() with ttlMs stores the ttl', () => {
    const b = new KvBatch();
    b.set('k', buf('v'), 5000);
    expect(b.ops()[0]!.ttlMs).toBe(5000);
  });

  test('set() without ttlMs stores 0', () => {
    const b = new KvBatch();
    b.set('k', buf('v'));
    expect(b.ops()[0]!.ttlMs).toBe(0);
  });

  test('delete() adds a delete op', () => {
    const b = new KvBatch();
    b.delete('foo');
    const ops = b.ops();
    expect(ops).toHaveLength(1);
    expect(ops[0]!.key).toBe('foo');
    expect(ops[0]!.isDelete).toBe(true);
  });

  test('set() is chainable', () => {
    const b = new KvBatch();
    const ret = b.set('a', buf('1'));
    expect(ret).toBe(b);
  });

  test('delete() is chainable', () => {
    const b = new KvBatch();
    const ret = b.delete('a');
    expect(ret).toBe(b);
  });

  test('multiple ops accumulate in order', () => {
    const b = new KvBatch();
    b.set('a', buf('1')).set('b', buf('2')).delete('c').set('d', buf('4'), 1000);
    const ops = b.ops();
    expect(ops).toHaveLength(4);
    expect(ops[0]!.key).toBe('a');
    expect(ops[1]!.key).toBe('b');
    expect(ops[2]!.isDelete).toBe(true);
    expect(ops[3]!.ttlMs).toBe(1000);
  });
});
