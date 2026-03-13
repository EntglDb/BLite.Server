// tests/integration/transaction.test.ts
//
// Integration tests for BLiteTransaction: commit, rollback, isolation.
// Requires a live BLite server (npm run test:integration).

import { BLiteClient } from '../../src/client';
import { DynamicCollection } from '../../src/dynamic-collection';
import { BsonId } from '../../src/cbson/types';
import { BLiteError } from '../../src/errors';
import { createClient, checkAvailability, uniqueCol } from './helpers';

let client: BLiteClient;
let col: DynamicCollection;
let colName: string;
let available = false;

beforeAll(async () => {
  client    = createClient();
  available = await checkAvailability(client);
  if (!available) {
    console.warn('⚠️  BLite server not reachable — transaction integration tests skipped');
    return;
  }
  colName = uniqueCol('txn');
  col     = client.getCollection(colName);
}, 8000);

afterAll(async () => {
  if (available) await client.dropCollection(colName).catch(() => {});
});

function run(name: string, fn: () => Promise<void>) {
  test(name, async () => {
    if (!available) return;
    await fn();
  });
}

// ─── Commit ──────────────────────────────────────────────────────────────────

run('committed transaction persists the insert', async () => {
  const tx  = await client.beginTransaction();
  const id  = await col.insert({ name: 'TxnAlice', committed: true }, tx);
  await tx.commit();

  const doc = await col.findById(id);
  expect(doc).not.toBeNull();
  expect(doc!['name']).toBe('TxnAlice');
});

run('committed transaction persists multiple inserts', async () => {
  const tx   = await client.beginTransaction();
  const ids  = await col.insertBulk([
    { name: 'Multi1', tx: true },
    { name: 'Multi2', tx: true },
  ], tx);
  await tx.commit();

  for (const id of ids) {
    const doc = await col.findById(id);
    expect(doc).not.toBeNull();
  }
});

run('commit sets transaction state correctly', async () => {
  const tx = await client.beginTransaction();
  await col.insert({ ping: true }, tx);
  await tx.commit();

  expect(tx.committed).toBe(true);
  expect(tx.active).toBe(false);
  expect(tx.rolledBack).toBe(false);
});

// ─── Rollback ────────────────────────────────────────────────────────────────

run('rolled-back transaction does not persist the insert', async () => {
  const tx = await client.beginTransaction();
  const id = await col.insert({ name: 'TxnGhost', committed: false }, tx);
  await tx.rollback();

  const doc = await col.findById(id);
  expect(doc).toBeNull();
});

run('rollback sets transaction state correctly', async () => {
  const tx = await client.beginTransaction();
  await col.insert({ temp: true }, tx);
  await tx.rollback();

  expect(tx.rolledBack).toBe(true);
  expect(tx.active).toBe(false);
  expect(tx.committed).toBe(false);
});

// ─── Double-commit guard ──────────────────────────────────────────────────────

run('committing twice throws BLiteError', async () => {
  const tx = await client.beginTransaction();
  await col.insert({ val: 1 }, tx);
  await tx.commit();
  await expect(tx.commit()).rejects.toBeInstanceOf(BLiteError);
});

// ─── AsyncDispose (await using) ───────────────────────────────────────────────

run('await using auto-rolls back when not committed', async () => {
  let savedId: BsonId;

  {
    // Scope block simulating `await using`
    const tx  = await client.beginTransaction();
    savedId   = await col.insert({ autopurge: true }, tx);
    await tx[Symbol.asyncDispose](); // simulates end-of-scope dispose without commit
  }

  const doc = await col.findById(savedId!);
  expect(doc).toBeNull();
});

// ─── Committed update/delete ──────────────────────────────────────────────────

run('committed update persists the change', async () => {
  const id = await col.insert({ val: 0 });

  const tx = await client.beginTransaction();
  await col.update(id, { val: 42 }, tx);
  await tx.commit();

  const doc = await col.findById(id);
  expect(doc!['val']).toBe(42);
});

run('rolled-back update does not change the document', async () => {
  const id = await col.insert({ val: 100 });

  const tx = await client.beginTransaction();
  await col.update(id, { val: 999 }, tx);
  await tx.rollback();

  const doc = await col.findById(id);
  expect(doc!['val']).toBe(100);
});

run('committed delete removes the document', async () => {
  const id = await col.insert({ toDelete: true });

  const tx = await client.beginTransaction();
  await col.delete(id, tx);
  await tx.commit();

  expect(await col.findById(id)).toBeNull();
});

run('rolled-back delete preserves the document', async () => {
  const id = await col.insert({ keep: true });

  const tx = await client.beginTransaction();
  await col.delete(id, tx);
  await tx.rollback();

  expect(await col.findById(id)).not.toBeNull();
});
