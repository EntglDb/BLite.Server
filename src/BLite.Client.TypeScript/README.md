# @blite/client

TypeScript/Node.js client for [BLite](https://github.com/EntglDb/BLite.Server) — the embedded document database server.

## Install

```bash
npm install @blite/client
```

## Quick start

```ts
import { BLiteClient, BsonId } from '@blite/client';

const client = new BLiteClient({
  host:   'localhost',
  port:   2626,          // default gRPC port
  apiKey: 'your-api-key',
  useTls: true,          // set false only for local dev
});

const users = client.getCollection('users');

// Insert
const id = await users.insert({ name: 'Alice', age: 30, active: true });
console.log(id.toString()); // hex ObjectId or string, etc.

// Find by ID
const doc = await users.findById(id);

// Query (fluent builder)
const results = await users.query()
  .whereEq('active', true)
  .whereGte('age', 18)
  .orderBy('name')
  .skip(0).take(20)
  .toArray();

// Stream large result sets
for await (const doc of users.query().whereEq('status', 'pending').execute()) {
  console.log(doc);
}

// Update
await users.update(id, { ...doc, age: 31 });

// Delete
await users.delete(id);
```

## Typed collection

Use a `CollectionMapper<T>` to map between a TypeScript type and `BsonDocument`. This gives you compile-time safety on read/write paths.

```ts
import { BLiteClient, CollectionMapper, BsonId, BsonDocument } from '@blite/client';

interface User {
  id?: BsonId;
  name: string;
  email: string;
  age: number;
}

const userMapper: CollectionMapper<User> = {
  collectionName: 'users',
  getId(u) { return u.id!; },
  toDocument(u): BsonDocument {
    return { _id: u.id?.toString(), name: u.name, email: u.email, age: u.age };
  },
  fromDocument(doc): User {
    return {
      id:    doc._id ? BsonId.fromString(String(doc._id)) : undefined,
      name:  String(doc.name),
      email: String(doc.email),
      age:   Number(doc.age),
    };
  },
};

const client = new BLiteClient({ host: 'localhost', apiKey: 'key' });
const users  = client.getTypedCollection(userMapper);

const id    = await users.insert({ name: 'Bob', email: 'bob@example.com', age: 25 });
const user  = await users.findById(id);            // → User | null
const list  = await users.query().whereGt('age', 20).toArray(); // → User[]
```

## Transactions

```ts
const tx = await client.beginTransaction();
try {
  await col.insert({ key: 'a' }, tx);
  await col.insert({ key: 'b' }, tx);
  await tx.commit();
} catch {
  await tx.rollback();
}

// Or with AsyncDisposable (Node.js 18.2+, TypeScript 5.2+):
await using tx = await client.beginTransaction();
await col.insert({ key: 'a' }, tx);
await col.insert({ key: 'b' }, tx);
await tx.commit();
```

## Indexes

```ts
// BTree index (supports range queries)
await users.createIndex('email', { unique: true });

// Vector index (HNSW)
await products.createVectorIndex('embedding', 1536, { metric: 'Cosine' });

// Vector similarity search
for await (const doc of products.vectorSearch(queryEmbedding, { k: 10 })) {
  console.log(doc);
}

// Spatial index
await locations.createSpatialIndex('coordinates');
```

## Change Data Capture (CDC)

```ts
// Stream all write events for a collection
for await (const event of users.watch(true /* capture payload */)) {
  console.log(event.operation, event.documentId.toString(), event.payload);
}
```

## Key-Value store

```ts
const kv = client.kv;

await kv.set('session:abc', Buffer.from(JSON.stringify({ userId: 1 })), 3_600_000); // 1 h TTL
const buf = await kv.get('session:abc');
const keys = await kv.scanKeys('session:');

// Batch
await kv.batch((b) => {
  b.set('k1', Buffer.from('v1'));
  b.set('k2', Buffer.from('v2'), 60_000);
  b.delete('k3');
});
```

## Admin API

```ts
import { BLiteOperation } from '@blite/client';

const admin = client.admin;

// Create a user with scoped permissions
const apiKey = await admin.createUser('bob', {
  permissions: [
    { collection: 'orders', ops: BLiteOperation.Query | BLiteOperation.Insert },
  ],
});

// Rotate key
const newKey = await admin.rotateKey('bob');

// Tenant provisioning (multi-tenant mode)
await admin.provisionTenant('tenant-acme');
await admin.deprovisionTenant('tenant-old', /* deleteFiles */ true);
```

## Schema management

```ts
// Define a schema (enforced on write)
await users.setSchema([
  { name: 'name',  typeCode: 0x02 /* String */,  nullable: false },
  { name: 'age',   typeCode: 0x10 /* Int32 */,   nullable: true  },
  { name: 'email', typeCode: 0x02 /* String */,  nullable: false },
], 'UserSchema');

const info = await users.getSchema();
console.log(info.title, info.version, info.fields);
```

## Time series

```ts
await metrics.configureTimeSeries('timestamp', 7 * 24 * 3_600_000); // 7-day retention
const info = await metrics.getTimeSeriesInfo();
await metrics.forcePrune(); // prune expired documents on demand
```

## Configuration reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `'localhost'` | Server hostname |
| `port` | number | `2626` | gRPC port |
| `apiKey` | string | **required** | API key sent as `x-api-key` |
| `useTls` | boolean | `true` | Enable TLS |
| `address` | string | — | Full address override; skips host/port/useTls |

## Key-map refresh

BLite uses a compact binary format (C-BSON) where field names are replaced by 2-byte IDs.  
The client keeps a local cache of `fieldName ↔ ID` mappings and registers new fields automatically on every write.

If you need to read documents written by other clients before making any writes yourself, pre-populate the cache:

```ts
await client.refreshKeyMap('users');  // any collection you have Query access to
```

## Notes

- **Node.js 18+** required (AsyncDisposable, `Buffer.writeBigInt64LE`, ES2020 target).
- **int64** values in filter predicates: pass as `bigint` for values outside `[-2^53, 2^53]`.
- **Decimal128** fields are decoded as a raw string representation for lossless round-trip; no arithmetic support.
- The TypeScript client uses `DynamicService` only; `DocumentService` (typed BSON path with CLR type hints) is a C#-only concept.

## License

AGPL-3.0 © EntglDb
