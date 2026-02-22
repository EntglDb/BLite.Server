# ⚡ BLite Server

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Build Status](https://img.shields.io/badge/build-in%20development-orange)](https://github.com/EntglDb/BLite.Server)

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
