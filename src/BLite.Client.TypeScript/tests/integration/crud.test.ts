// tests/integration/crud.test.ts
//
// Integration tests for DynamicCollection CRUD and bulk operations.
// Requires a live BLite server (npm run test:integration).

import { BLiteClient } from '../../src/client';
import { DynamicCollection } from '../../src/dynamic-collection';
import { BsonId } from '../../src/cbson/types';
import { createClient, checkAvailability, uniqueCol } from './helpers';

let client: BLiteClient;
let col: DynamicCollection;
let colName: string;
let available = false;

beforeAll(async () => {
  client    = createClient();
  available = await checkAvailability(client);
  if (!available) {
    console.warn('⚠️  BLite server not reachable — crud integration tests skipped');
    return;
  }
  colName = uniqueCol('crud');
  col     = client.getCollection(colName);
}, 8000);

afterAll(async () => {
  if (available) await client.dropCollection(colName).catch(() => {});
});

// Helper: vacuous pass when server is offline
function run(name: string, fn: () => Promise<void>) {
  test(name, async () => {
    if (!available) return;
    await fn();
  });
}

// ─── insert / findById ────────────────────────────────────────────────────────

run('insert returns a valid BsonId', async () => {
  const id = await col.insert({ name: 'Alice', age: 30 });
  expect(id).toBeInstanceOf(BsonId);
  expect(id.toString()).toBeTruthy();
});

run('findById returns the inserted document', async () => {
  const id  = await col.insert({ name: 'Bob', age: 25 });
  const doc = await col.findById(id);
  expect(doc).not.toBeNull();
  expect(doc!['name']).toBe('Bob');
  expect(doc!['age']).toBe(25);
});

run('findById returns null for unknown id', async () => {
  const ghostId = await col.insert({ _placeholder: 1 });
  await col.delete(ghostId);
  const doc = await col.findById(ghostId);
  expect(doc).toBeNull();
});

// ─── update ───────────────────────────────────────────────────────────────────

run('update changes fields and findById reflects the change', async () => {
  const id = await col.insert({ name: 'Carol', score: 10 });
  const ok = await col.update(id, { name: 'Carol', score: 99 });
  expect(ok).toBe(true);
  const doc = await col.findById(id);
  expect(doc!['score']).toBe(99);
});

run('update returns false for unknown id', async () => {
  const ghostId = await col.insert({ _placeholder: 2 });
  await col.delete(ghostId);
  const ok = await col.update(ghostId, { name: 'Ghost' });
  expect(ok).toBe(false);
});

// ─── delete ───────────────────────────────────────────────────────────────────

run('delete removes the document', async () => {
  const id = await col.insert({ name: 'Dave', temp: true });
  const ok = await col.delete(id);
  expect(ok).toBe(true);
  const doc = await col.findById(id);
  expect(doc).toBeNull();
});

run('delete returns false for unknown id', async () => {
  const ghostId = await col.insert({ _placeholder: 3 });
  await col.delete(ghostId);
  const ok = await col.delete(ghostId);
  expect(ok).toBe(false);
});

// ─── insertBulk ───────────────────────────────────────────────────────────────

run('insertBulk returns one id per document', async () => {
  const docs = [
    { name: 'Eva', city: 'Rome' },
    { name: 'Frank', city: 'Milan' },
    { name: 'Grace', city: 'Turin' },
  ];
  const ids = await col.insertBulk(docs);
  expect(ids).toHaveLength(3);
  for (const id of ids) expect(id).toBeInstanceOf(BsonId);
});

run('insertBulk with empty array returns empty list', async () => {
  const ids = await col.insertBulk([]);
  expect(ids).toHaveLength(0);
});

// ─── deleteBulk ───────────────────────────────────────────────────────────────

run('deleteBulk removes multiple documents at once', async () => {
  const ids = await col.insertBulk([
    { name: 'X1', batch: true },
    { name: 'X2', batch: true },
    { name: 'X3', batch: true },
  ]);
  const count = await col.deleteBulk(ids);
  expect(count).toBe(3);
  for (const id of ids) expect(await col.findById(id)).toBeNull();
});

// ─── updateBulk ───────────────────────────────────────────────────────────────

run('updateBulk updates multiple documents', async () => {
  const ids = await col.insertBulk([
    { name: 'Bulk1', score: 0 },
    { name: 'Bulk2', score: 0 },
  ]);
  const count = await col.updateBulk([
    { id: ids[0]!, doc: { name: 'Bulk1', score: 10 } },
    { id: ids[1]!, doc: { name: 'Bulk2', score: 20 } },
  ]);
  expect(count).toBe(2);
  const doc1 = await col.findById(ids[0]!);
  expect(doc1!['score']).toBe(10);
});

// ─── findAll ─────────────────────────────────────────────────────────────────

run('findAll streams all documents in the collection', async () => {
  // Drop and recreate a fresh collection for a predictable count
  const freshName = uniqueCol('findall');
  const fresh     = client.getCollection(freshName);
  await fresh.insertBulk([{ n: 1 }, { n: 2 }, { n: 3 }]);

  const docs: Record<string, unknown>[] = [];
  for await (const doc of fresh.findAll()) docs.push(doc);
  expect(docs.length).toBeGreaterThanOrEqual(3);

  await client.dropCollection(freshName);
});

// ─── listCollections / dropCollection ────────────────────────────────────────

run('listCollections includes the test collection', async () => {
  const names = await client.listCollections();
  expect(names).toContain(colName);
});
