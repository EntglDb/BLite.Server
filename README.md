# ⚡ BLite Server

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/EntglDb/BLite.Server)

**BLite Server** is a high-performance, self-hosted database server built on top of the [BLite](https://github.com/EntglDb/BLite) embedded engine.  
It exposes BLite's full capabilities — schema-less BSON documents and typed strongly-typed collections — over a **gRPC / Protocol Buffers** transport, hosted on **ASP.NET Core / Kestrel**.

> **Status**: Active development — core server + auth + observability + transactions complete. Client SDK in progress.

---

## Architecture

```
Client (BLite.Client SDK)
  ├── IBLiteQueryable<T>  ──── ExpressionToDescriptorVisitor ──┐
  └── RemoteDynamicClient ─── BsonDocument ↔ bytes ────────────┤
                                                               │
                      gRPC (HTTP/2, TLS) — port 2626           │
                           API-Key header                      │
                                                               ▼
BLite.Server (ASP.NET Core + Kestrel)
  ├── ApiKeyMiddleware      → resolve BLiteUser + namespace
  ├── TelemetryInterceptor  → OTel trace + metrics for every RPC
  ├── DynamicService        (schema-less CRUD + streaming Query + Bulk)
  ├── DocumentService       (typed Query / Insert / Update / Delete / BulkInsert)
  ├── AdminService          (user management, permissions)
  ├── TransactionService    (Begin / Commit / Rollback)
  └── QueryDescriptorExecutor
        └── BLiteEngine / DynamicCollection (BTree, WAL, HNSW, RTree)
```

### Projects

| Project | Description |
|---|---|
| `BLite.Proto` | Shared `.proto` contracts + `QueryDescriptor` (MessagePack) |
| `BLite.Server` | ASP.NET Core gRPC host, service implementations, auth, observability, transactions |
| `BLite.Client` _(in progress)_ | .NET client SDK with `RemoteQueryProvider` and `IBLiteQueryable<T>` |

---

## Why gRPC?

| Feature | REST/JSON | gRPC/Protobuf |
|---|---|---|
| Transport | HTTP/1.1 | HTTP/2 (multiplexed frames) |
| Payload | JSON (double serialization) | Raw BSON bytes |
| Result streaming | SSE/WebSocket (workaround) | **Native server-streaming** |
| Expression trees | Custom JSON DSL | `QueryDescriptor` in `bytes` |
| Cross-language | ✅ | ✅ (proto3 generates all languages) |

---

## QueryDescriptor

Expression trees cannot be sent over the wire directly.  
BLite Server defines a `QueryDescriptor` — a serializable DTO (MessagePack) that maps 1:1 to BLite's internal `QueryModel`:

```csharp
public sealed class QueryDescriptor
{
    public string Collection  { get; set; }
    public FilterNode? Where  { get; set; }   // predicate tree
    public ProjectionSpec? Select { get; set; } // scalar field list
    public List<SortSpec> OrderBy { get; set; }
    public int? Take { get; set; }
    public int? Skip { get; set; }
}
```

Filter nodes support `BinaryFilter` (field op value) and `LogicalFilter` (AND/OR/NOT), covering the full range of BLite LINQ predicates.  
The server rebuilds a `Func<BsonSpanReader, bool>` predicate from the descriptor and delegates directly to `DynamicCollection.Scan` / `BsonProjectionCompiler` — **`T` is never instantiated on the server for typed queries**.

---

## Service Contract (proto)

```protobuf
service DynamicService {
  rpc Insert      (InsertRequest)     returns (InsertResponse);
  rpc FindById    (FindByIdRequest)   returns (DocumentResponse);
  rpc Update      (UpdateRequest)     returns (MutationResponse);
  rpc Delete      (DeleteRequest)     returns (MutationResponse);
  rpc Query       (QueryRequest)      returns (stream DocumentResponse);
  rpc InsertBulk  (BulkInsertRequest) returns (BulkInsertResponse);
  rpc UpdateBulk  (BulkUpdateRequest) returns (BulkMutationResponse);
  rpc DeleteBulk  (BulkDeleteRequest) returns (BulkMutationResponse);
  rpc ListCollections (Empty)                returns (CollectionListResponse);
  rpc DropCollection  (DropCollectionRequest) returns (MutationResponse);
}

service DocumentService {
  rpc Query      (QueryRequest)          returns (stream TypedDocumentResponse);
  rpc Insert     (TypedInsertRequest)    returns (InsertResponse);
  rpc Update     (TypedUpdateRequest)    returns (MutationResponse);
  rpc Delete     (DeleteRequest)         returns (MutationResponse);
  rpc InsertBulk (TypedBulkInsertRequest) returns (BulkInsertResponse);
}

service AdminService {
  rpc CreateUser  (CreateUserRequest)  returns (MutationResponse);
  rpc DeleteUser  (DeleteUserRequest)  returns (MutationResponse);
  rpc ListUsers   (Empty)              returns (UserListResponse);
  rpc UpdatePerms (UpdatePermsRequest) returns (MutationResponse);
}

service TransactionService {
  rpc Begin    (BeginTransactionRequest) returns (BeginTransactionResponse);
  rpc Commit   (TransactionRequest)      returns (MutationResponse);
  rpc Rollback (TransactionRequest)      returns (MutationResponse);
}
```

All write RPCs (Insert / Update / Delete / Bulk variants) accept an optional `transaction_id` field.  
When set, the operation participates in the named server-side transaction instead of auto-committing.

---

## Authentication & Multi-tenancy

Every request must carry an `x-api-key` header.  
The server resolves the caller to a `BLiteUser` with a scoped permission set:

| Permission | Operations |
|---|---|
| `Query` | read-only (FindById, Query) |
| `Insert` / `Update` / `Delete` | write |
| `Drop` | DropCollection |
| `Admin` | full access + user management |

Collections are automatically **namespaced by user** — user `alice` accessing `orders` operates on the isolated logical collection `alice::orders`.  
The `root` user bypasses namespacing and has full access.

---

## Transactions

BLite Server supports explicit, token-scoped transactions over gRPC:

```
1. client calls TransactionService.Begin → receives transaction_id (UUID)
2. client passes transaction_id in any write RPC → writes are not auto-committed
3. client calls Commit or Rollback to finalise
```

At most one transaction can be active at a time (BLiteEngine constraint).  
Sessions that idle longer than `Transactions:TimeoutSeconds` (default 60 s) are rolled back automatically by a background cleanup service.

---

## Observability

BLite Server ships with built-in OpenTelemetry support:

- **Traces** — every gRPC RPC generates a span via `TelemetryInterceptor`
- **Metrics** — `blite.server.rpc.total`, `blite.server.rpc.duration`, `blite.server.documents.streamed`, `blite.server.active_transactions`
- **Exporters** — Console (dev) + OTLP/gRPC (Jaeger, Grafana, Datadog, …)

Configure in `appsettings.json`:

```json
"Telemetry": {
  "Enabled": true,
  "ServiceName": "blite-server",
  "Console": null,
  "Otlp": { "Endpoint": "http://localhost:4317" }
}
```

---

## Implementation Roadmap

| Step | Scope | Status |
|---|---|---|
| **1 – BLite.Proto** | `.proto` + `QueryDescriptor` + MessagePack serialization | ✅ Complete |
| **2 – BLite.Server** | ASP.NET Core host, `DynamicService` + `DocumentService` end-to-end | ✅ Complete |
| **3 – BLite.Client** | `RemoteQueryProvider`, `ExpressionToDescriptorVisitor`, `IBLiteQueryable<T>` | 🔄 In progress |
| **4 – Typed path** | `TypeManifest` schema registration, typed query push-down | 🔜 Planned |
| **5 – Auth & multi-tenancy** | API Key middleware, `AdminService`, tenant namespacing | ✅ Complete |
| **6 – Observability** | OpenTelemetry traces + metrics, Console + OTLP exporters | ✅ Complete |
| **7 – Transactions** | `TransactionService`, token-scoped sessions, TTL cleanup | ✅ Complete |

---

## Getting Started

```bash
# Run the server (binds on all interfaces, port 2626)
dotnet run --project src/BLite.Server

# Or with a custom database path
dotnet run --project src/BLite.Server -- --db /data/mydb.db
```

The server listens on `https://*:2626` (HTTP/2 only).

```csharp
// Client usage (BLite.Client SDK — coming soon)
var client = new BLiteRemoteClient("https://myhost:2626", apiKey: "your-key");
var users  = client.GetCollection<User>("users");

var result = await users.AsQueryable()
    .Where(u => u.Age > 25)
    .Select(u => new { u.Name, u.Age })
    .ToListAsync();

// Explicit transaction
var txn = await client.BeginTransactionAsync();
await users.InsertAsync(new User { Name = "Alice" }, txn);
await orders.InsertAsync(new Order { UserId = alice.Id }, txn);
await txn.CommitAsync();
```

---

## License

Licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.  
See [LICENSE](LICENSE) for the full text.

> The AGPL-3.0 requires that any modified version of this software that is
> made available over a network must also make its source code available.
> If you need a commercial license without this restriction, please contact the authors.

---

## Related Projects

- [BLite](https://github.com/EntglDb/BLite) — the embedded engine (MIT)
- [BLite documentation](https://blitedb.com/docs/getting-started)


**BLite Server** is a high-performance, self-hosted database server built on top of the [BLite](https://github.com/EntglDb/BLite) embedded engine.  
It exposes BLite's full capabilities — schema-less BSON documents and typed strongly-typed collections — over a **gRPC / Protocol Buffers** transport, hosted on **ASP.NET Core / Kestrel**.

> **Status**: Active development — not yet ready for production use.

---

## Architecture

```
Client (BLite.Client SDK)
  ├── IBLiteQueryable<T>  ──── ExpressionToDescriptorVisitor ──┐
  └── RemoteDynamicClient ─── BsonDocument ↔ bytes ────────────┤
                                                               │
                           gRPC (HTTP/2, TLS)                  │
                                                               ▼
BLite.Server (ASP.NET Core + Kestrel)
  ├── DynamicService   (schema-less CRUD + streaming Query)
  ├── DocumentService  (typed Query via QueryDescriptor)
  └── QueryDescriptorExecutor
        └── BLiteEngine / DynamicCollection (same kernel: BTree, WAL, HNSW, RTree)
```

### Projects

| Project | Description |
|---|---|
| `BLite.Proto` | Shared `.proto` contracts + `QueryDescriptor` (MessagePack) |
| `BLite.Server` | ASP.NET Core gRPC host, service implementations, auth middleware |
| `BLite.Client` _(planned)_ | .NET client SDK with `RemoteQueryProvider` and `IBLiteQueryable<T>` |

---

## Why gRPC?

| Feature | REST/JSON | gRPC/Protobuf |
|---|---|---|
| Transport | HTTP/1.1 | HTTP/2 (multiplexed frames) |
| Payload | JSON (double serialization) | Raw BSON bytes |
| Result streaming | SSE/WebSocket (workaround) | **Native server-streaming** |
| Expression trees | Custom JSON DSL | `QueryDescriptor` in `bytes` |
| Cross-language | ✅ | ✅ (proto3 generates all languages) |

---

## QueryDescriptor

Expression trees cannot be sent over the wire directly.  
BLite Server defines a `QueryDescriptor` — a serializable DTO (MessagePack) that maps 1:1 to BLite's internal `QueryModel`:

```csharp
public sealed class QueryDescriptor
{
    public string Collection  { get; set; }
    public FilterNode? Where  { get; set; }   // predicate tree
    public ProjectionSpec? Select { get; set; } // scalar field list
    public List<SortSpec> OrderBy { get; set; }
    public int? Take { get; set; }
    public int? Skip { get; set; }
}
```

Filter nodes support `BinaryFilter` (field op value) and `LogicalFilter` (AND/OR/NOT), covering the full range of BLite LINQ predicates.  
The server rebuilds a `Func<BsonSpanReader, bool>` predicate from the descriptor and delegates directly to `DynamicCollection.Scan` / `BsonProjectionCompiler` — **`T` is never instantiated on the server for typed queries**.

---

## Service Contract (proto)

```protobuf
service DynamicService {
  rpc Insert      (InsertRequest)  returns (InsertResponse);
  rpc FindById    (FindByIdRequest) returns (DocumentResponse);
  rpc Update      (UpdateRequest)  returns (MutationResponse);
  rpc Delete      (DeleteRequest)  returns (MutationResponse);
  rpc Query       (QueryRequest)   returns (stream DocumentResponse);  // server-streaming
  rpc ExecuteBulk (BulkRequest)    returns (BulkResponse);
}

service DocumentService {
  rpc Query  (QueryRequest)        returns (stream TypedDocumentResponse);
  rpc Insert (TypedInsertRequest)  returns (InsertResponse);
  rpc Update (TypedUpdateRequest)  returns (MutationResponse);
  rpc Delete (DeleteRequest)       returns (MutationResponse);
}
```

---

## Implementation Roadmap

| Step | Scope | Status |
|---|---|---|
| **1 – BLite.Proto** | `.proto` + `QueryDescriptor` + MessagePack serialization | 🔄 In progress |
| **2 – BLite.Server** | ASP.NET Core host, `DynamicService` end-to-end, API key auth | 🔜 Planned |
| **3 – BLite.Client** | `RemoteQueryProvider`, `ExpressionToDescriptorVisitor`, `IBLiteQueryable<T>` | 🔜 Planned |
| **4 – Typed path** | `TypeManifest` schema registration, typed query push-down | 🔜 Planned |
| **5 – Auth & multi-tenancy** | JWT/API Key middleware, tenant namespacing | 🔜 Planned |
| **6 – Observability** | OpenTelemetry traces, Prometheus metrics | 🔜 Planned |

---

## Getting Started (coming soon)

```bash
# Run the server
dotnet run --project src/BLite.Server -- --db /data/mydb.db --port 2626

# Connect from a client
var client = new BLiteRemoteClient("https://localhost:2626");
var users  = client.GetCollection<User>("users");

var result = await users.AsQueryable()
    .Where(u => u.Age > 25)
    .Select(u => new { u.Name, u.Age })
    .ToListAsync();
```

---

## License

Licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.  
See [LICENSE](LICENSE) for the full text.

> The AGPL-3.0 requires that any modified version of this software that is
> made available over a network must also make its source code available.
> If you need a commercial license without this restriction, please contact the authors.

---

## Related Projects

- [BLite](https://github.com/EntglDb/BLite) — the embedded engine (MIT)
- [BLite documentation](https://blitedb.com/docs/getting-started)
