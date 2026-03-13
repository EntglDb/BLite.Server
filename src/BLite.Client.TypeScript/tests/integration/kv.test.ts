// tests/integration/kv.test.ts
//
// Integration tests for KvStore and KvBatch.
// Requires a live BLite server (npm run test:integration).

import { BLiteClient } from '../../src/client';
import { createClient, checkAvailability } from './helpers';

let client: BLiteClient;
let available = false;

// Use a unique prefix so parallel runs never collide
const PREFIX = `ts_kv_${Date.now()}_`;
const k = (n: string) => `${PREFIX}${n}`;

beforeAll(async () => {
  client    = createClient();
  available = await checkAvailability(client);
  if (!available) {
    console.warn('⚠️  BLite server not reachable — kv integration tests skipped');
  }
}, 8000);

afterAll(async () => {
  if (!available) return;
  // Clean up all test keys
  const keys = await client.kv.scanKeys(PREFIX);
  if (keys.length > 0) {
    await client.kv.batch((b) => { for (const key of keys) b.delete(key); });
  }
});

function run(name: string, fn: () => Promise<void>) {
  test(name, async () => {
    if (!available) return;
    await fn();
  });
}

const buf = (s: string) => Buffer.from(s);

// ─── set / get / exists ────────────────────────────────────────────────────────

run('set and get a value', async () => {
  await client.kv.set(k('a'), buf('hello'));
  const val = await client.kv.get(k('a'));
  expect(val).not.toBeNull();
  expect(val!.toString()).toBe('hello');
});

run('get returns null for a missing key', async () => {
  const val = await client.kv.get(k('no-such-key-xyz'));
  expect(val).toBeNull();
});

run('exists returns true for an existing key', async () => {
  await client.kv.set(k('exists-yes'), buf('1'));
  expect(await client.kv.exists(k('exists-yes'))).toBe(true);
});

run('exists returns false for a missing key', async () => {
  expect(await client.kv.exists(k('exists-no-xyz'))).toBe(false);
});

// ─── delete ───────────────────────────────────────────────────────────────────

run('delete removes the key', async () => {
  await client.kv.set(k('del'), buf('bye'));
  const ok = await client.kv.delete(k('del'));
  expect(ok).toBe(true);
  expect(await client.kv.get(k('del'))).toBeNull();
});

run('delete returns false for a missing key', async () => {
  const ok = await client.kv.delete(k('del-ghost-xyz'));
  expect(ok).toBe(false);
});

// ─── scanKeys ────────────────────────────────────────────────────────────────

run('scanKeys returns keys matching the prefix', async () => {
  const scanPfx = k('scan_');
  await client.kv.set(`${scanPfx}1`, buf('a'));
  await client.kv.set(`${scanPfx}2`, buf('b'));
  await client.kv.set(`${scanPfx}3`, buf('c'));

  const keys = await client.kv.scanKeys(scanPfx);
  expect(keys.length).toBeGreaterThanOrEqual(3);
  for (const key of keys) expect(key.startsWith(scanPfx)).toBe(true);
});

run('scanKeys with empty prefix returns all keys (or at least the test ones)', async () => {
  const keys = await client.kv.scanKeys('');
  expect(keys.length).toBeGreaterThan(0);
});

// ─── TTL / refresh ────────────────────────────────────────────────────────────

run('TTL key is present immediately after set', async () => {
  // 30-second TTL — will definitely still exist in the test window
  await client.kv.set(k('ttl'), buf('expires'), 30_000);
  expect(await client.kv.exists(k('ttl'))).toBe(true);
});

run('refresh updates the TTL of an existing key', async () => {
  await client.kv.set(k('refresh-me'), buf('data'), 30_000);
  const ok = await client.kv.refresh(k('refresh-me'), 60_000);
  expect(ok).toBe(true);
});

run('refresh returns false for a missing key', async () => {
  const ok = await client.kv.refresh(k('refresh-ghost'), 5000);
  expect(ok).toBe(false);
});

// ─── batch ────────────────────────────────────────────────────────────────────

run('batch set operations persist', async () => {
  await client.kv.batch((b) => {
    b.set(k('bx1'), buf('one'));
    b.set(k('bx2'), buf('two'));
    b.set(k('bx3'), buf('three'));
  });

  expect((await client.kv.get(k('bx1')))!.toString()).toBe('one');
  expect((await client.kv.get(k('bx2')))!.toString()).toBe('two');
  expect((await client.kv.get(k('bx3')))!.toString()).toBe('three');
});

run('batch delete operations remove keys', async () => {
  await client.kv.set(k('bd1'), buf('del1'));
  await client.kv.set(k('bd2'), buf('del2'));

  await client.kv.batch((b) => {
    b.delete(k('bd1'));
    b.delete(k('bd2'));
  });

  expect(await client.kv.get(k('bd1'))).toBeNull();
  expect(await client.kv.get(k('bd2'))).toBeNull();
});

run('batch mixing set and delete in one call', async () => {
  await client.kv.set(k('mix-del'), buf('old'));

  await client.kv.batch((b) => {
    b.set(k('mix-new'), buf('fresh'));
    b.delete(k('mix-del'));
  });

  expect((await client.kv.get(k('mix-new')))!.toString()).toBe('fresh');
  expect(await client.kv.get(k('mix-del'))).toBeNull();
});

run('batch returns affected count', async () => {
  const count = await client.kv.batch((b) => {
    b.set(k('cnt1'), buf('a'));
    b.set(k('cnt2'), buf('b'));
  });
  expect(count).toBe(2);
});

run('empty batch returns 0', async () => {
  const count = await client.kv.batch(() => {});
  expect(count).toBe(0);
});

// ─── overwrite ────────────────────────────────────────────────────────────────

run('set overwrites an existing key', async () => {
  await client.kv.set(k('overwrite'), buf('v1'));
  await client.kv.set(k('overwrite'), buf('v2'));
  const val = await client.kv.get(k('overwrite'));
  expect(val!.toString()).toBe('v2');
});
