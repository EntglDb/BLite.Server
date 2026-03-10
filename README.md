# ⚡ BLite Server

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/EntglDb/BLite.Server)

**BLite Server** is a high-performance, self-hosted database server built on top of the [BLite](https://github.com/EntglDb/BLite) embedded engine.  
It exposes BLite's full capabilities over three interfaces — a **gRPC** endpoint for the .NET SDK, a **REST API** for cross-language access, and a **Blazor Studio** web UI for administration — all hosted on **ASP.NET Core / Kestrel**.

---

## Endpoints

| Endpoint | Default Port | Protocol | Purpose |
|---|---|---|---|
| gRPC | `2626` | HTTP/2, TLS | BLite.Client SDK, high-throughput streaming |
| REST API | `2627` | HTTP/1.1 + HTTP/2 | Cross-language clients, tooling, CI |
| Studio | `2628` | HTTP/1.1 + HTTP/2 | Blazor Server admin UI |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                            BLite.Server                              │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────────┐  ┌────────────────┐  │
│  │   gRPC :2626     │  │    REST /api/v1 :2627 │  │  Studio :2628  │  │
│  │  DynamicService  │  │  Minimal API endpoints│  │  Blazor Server │  │
│  │  DocumentService │  │  PermissionFilter     │  │  StudioService │  │
│  │  AdminService    │  │  RestAuthFilter       │  │  StudioSession │  │
│  │  MetadataService │  │  OpenAPI / Scalar     │  └───────┬────────┘  │
│  │  TransactionSvc  │  └───────────┬───────────┘          │           │
│  │  KvService       │              │                      │           │
│  └────────┬─────────┘              │                      │           │
│           └──────────────┬─────────┘                      │           │
│                          ▼                                 │           │
│                 ┌─────────────────┐◄───────────────────────┘           │
│                 │  EngineRegistry │                                    │
│                 │  (singleton)    │                                    │
│                 └────────┬────────┘                                    │
│                          │  one BLiteEngine per database              │
│                          ▼                                             │
│                 ┌─────────────────┐                                    │
│                 │   BLiteEngine   │  (sibling repo: BLite)            │
│                 │   + Collections │                                    │
│                 │   + KvStore     │                                    │
│                 └─────────────────┘                                    │
└──────────────────────────────────────────────────────────────────────┘
```

### Projects

| Project | Description |
|---|---|
| `BLite.Proto` | Shared `.proto` contracts + `QueryDescriptor` (MessagePack) |
| `BLite.Server` | ASP.NET Core host — gRPC services, REST API, Blazor Studio, auth, observability |
| `BLite.Client` | .NET client SDK — `RemoteDynamicCollection`, `RemoteCollection<TId,T>`, `RemoteKvStore` |

---

## gRPC Services

All gRPC calls require an `x-api-key` header.  
The server enforces per-collection permission checks and transparent namespace isolation.

```protobuf
service DynamicService {
  // CRUD
  rpc Insert      (InsertRequest)       returns (InsertResponse);
  rpc FindById    (FindByIdRequest)     returns (DocumentResponse);
  rpc Update      (UpdateRequest)       returns (MutationResponse);
  rpc Delete      (DeleteRequest)       returns (MutationResponse);
  // Streaming query + vector search
  rpc Query        (QueryRequest)        returns (stream DocumentResponse);
  rpc VectorSearch (VectorSearchRequest) returns (stream DocumentResponse);
  // Bulk operations
  rpc InsertBulk  (BulkInsertRequest)   returns (BulkInsertResponse);
  rpc UpdateBulk  (BulkUpdateRequest)   returns (BulkMutationResponse);
  rpc DeleteBulk  (BulkDeleteRequest)   returns (BulkMutationResponse);
  // Collection management
  rpc ListCollections     (Empty)                      returns (CollectionListResponse);
  rpc DropCollection      (DropCollectionRequest)      returns (MutationResponse);
  rpc CreateIndex         (CreateIndexRequest)         returns (MutationResponse);
  rpc DropIndex           (DropIndexRequest)           returns (MutationResponse);
  rpc ListIndexes         (CollectionRequest)          returns (ListIndexesResponse);
  // VectorSource (embedding worker)
  rpc SetVectorSource     (SetVectorSourceRequest)     returns (MutationResponse);
  rpc GetVectorSource     (CollectionRequest)          returns (GetVectorSourceResponse);
  // TimeSeries
  rpc ConfigureTimeSeries (ConfigureTimeSeriesRequest) returns (MutationResponse);
  rpc GetTimeSeriesInfo   (CollectionRequest)          returns (TimeSeriesResponse);
  rpc ForcePrune          (CollectionRequest)          returns (MutationResponse);
  // Schema
  rpc GetSchema (CollectionRequest)  returns (CollectionSchemaResponse);
  rpc SetSchema (SetSchemaRequest)   returns (MutationResponse);
}

service DocumentService {
  // Typed (BSON payload) path — mirrors DynamicService for typed collections
  rpc Query      (QueryRequest)           returns (stream TypedDocumentResponse);
  rpc Insert     (TypedInsertRequest)     returns (InsertResponse);
  rpc Update     (TypedUpdateRequest)     returns (MutationResponse);
  rpc Delete     (DeleteRequest)          returns (MutationResponse);
  rpc InsertBulk (TypedBulkInsertRequest) returns (BulkInsertResponse);
}

service AdminService {
  rpc CreateUser        (CreateUserRequest)        returns (CreateUserResponse);
  rpc RevokeUser        (UsernameRequest)          returns (MutationResponse);
  rpc RotateKey         (UsernameRequest)          returns (RotateKeyResponse);
  rpc ListUsers         (Empty)                    returns (ListUsersResponse);
  rpc UpdatePerms       (UpdatePermsRequest)       returns (MutationResponse);
  rpc ProvisionTenant   (ProvisionTenantRequest)   returns (ProvisionTenantResponse);
  rpc DeprovisionTenant (DeprovisionTenantRequest) returns (DeprovisionTenantResponse);
  rpc ListTenants       (Empty)                    returns (ListTenantsResponse);
}

service TransactionService {
  rpc Begin    (BeginTransactionRequest) returns (BeginTransactionResponse);
  rpc Commit   (TransactionRequest)      returns (MutationResponse);
  rpc Rollback (TransactionRequest)      returns (MutationResponse);
}

service KvService {
  // Read
  rpc Get      (KvGetRequest)  returns (KvGetResponse);
  rpc Exists   (KvKeyRequest)  returns (KvExistsResponse);
  rpc ScanKeys (KvScanRequest) returns (KvScanResponse);
  // Write
  rpc Set      (KvSetRequest)     returns (MutationResponse);
  rpc Delete   (KvDeleteRequest)  returns (MutationResponse);
  rpc Refresh  (KvRefreshRequest) returns (MutationResponse);  // extend TTL
  rpc Batch    (KvBatchRequest)   returns (KvBatchResponse);
  // Admin
  rpc PurgeExpired (KvDbRequest) returns (KvPurgeResponse);
}
```

All write RPCs accept an optional `transaction_id` field.

---

## REST API

Base path: `/api/v1`. All endpoints require `x-api-key` or `Authorization: Bearer <key>`.  
Interactive docs available at `/scalar` when Studio is enabled.

### Databases (tenant management)

| Method | Path | Description |
|---|---|---|
| `GET` | `/databases` | List all tenant databases |
| `POST` | `/databases` | Provision a new tenant database |
| `DELETE` | `/databases/{dbId}` | Deprovision a tenant database |
| `GET` | `/databases/{dbId}/backup` | Download a hot backup as a ZIP file |

### Collections

| Method | Path | Description |
|---|---|---|
| `GET` | `/{dbId}/collections` | List collections |
| `POST` | `/{dbId}/collections` | Create a collection |
| `DELETE` | `/{dbId}/{collection}` | Drop a collection |
| `GET` | `/{dbId}/{collection}/vectorsource` | Get VectorSource config |
| `PUT` | `/{dbId}/{collection}/vectorsource` | Set VectorSource config |
| `DELETE` | `/{dbId}/{collection}/vectorsource` | Clear VectorSource config |
| `GET` | `/{dbId}/{collection}/timeseries` | Get TimeSeries config |
| `PUT` | `/{dbId}/{collection}/timeseries` | Configure TimeSeries + retention |
| `POST` | `/{dbId}/{collection}/timeseries/prune` | Force retention prune |
| `GET` | `/{dbId}/{collection}/schema` | Get collection schema |
| `PUT` | `/{dbId}/{collection}/schema` | Set / append schema version |

### Documents

| Method | Path | Description |
|---|---|---|
| `GET` | `/{dbId}/{collection}/documents` | List documents (paginated) |
| `POST` | `/{dbId}/{collection}/documents` | Insert a document |
| `GET` | `/{dbId}/{collection}/documents/{id}` | Get document by ID |
| `PUT` | `/{dbId}/{collection}/documents/{id}` | Replace document |
| `DELETE` | `/{dbId}/{collection}/documents/{id}` | Delete document |
| `POST` | `/{dbId}/{collection}/documents/vector-search` | kNN vector search |

### BLQL Query

| Method | Path | Description |
|---|---|---|
| `POST` | `/{dbId}/{collection}/query` | BLQL filter + sort query (JSON body) |
| `GET` | `/{dbId}/{collection}/query` | BLQL query via query-string params |
| `POST` | `/{dbId}/{collection}/count` | Count documents matching a filter |

### Key-Value Store

| Method | Path | Description |
|---|---|---|
| `GET` | `/{dbId}/kv` | Scan keys (optional `?prefix=`) |
| `GET` | `/{dbId}/kv/{key}` | Get value (Base64-encoded) |
| `PUT` | `/{dbId}/kv/{key}` | Set value (`{ value, ttlMs? }`) |
| `DELETE` | `/{dbId}/kv/{key}` | Delete a key |
| `PATCH` | `/{dbId}/kv/{key}` | Refresh TTL (`{ ttlMs }`) |
| `POST` | `/{dbId}/kv/purge` | Purge all expired entries |
| `POST` | `/{dbId}/kv/batch` | Atomic batch set/delete |

### Users

| Method | Path | Description |
|---|---|---|
| `GET` | `/users` | List all users |
| `POST` | `/users` | Create a user |
| `DELETE` | `/users/{username}` | Delete a user |
| `PUT` | `/users/{username}/permissions` | Replace user permissions |

---

## BLite.Client SDK

```csharp
await using var client = new BLiteClient(new BLiteClientOptions
{
    Host   = "myserver",
    Port   = 2626,
    ApiKey = "blt_...",
    UseTls = true
});

// Schema-less collection
var sensors = client.GetDynamicCollection("sensors");
var id = await sensors.InsertAsync(new BsonDocument { ["temp"] = 22.5 });
await foreach (var doc in sensors.QueryAsync(descriptor))
    Console.WriteLine(doc);

// Typed collection (requires [BLiteMapper])
var users = client.GetCollection<ObjectId, User>(new UserMapper());
await users.InsertAsync(new User { Name = "Alice", Age = 30 });
var result = await users.AsQueryable()
    .Where(u => u.Age > 25)
    .OrderBy(u => u.Name)
    .ToListAsync();

// Explicit transaction
await using var tx = await client.BeginTransactionAsync();
await sensors.InsertAsync(doc, tx);
await tx.CommitAsync();

// Key-Value store
await client.Kv.SetAsync("session:xyz", Encoding.UTF8.GetBytes("data"), ttl: TimeSpan.FromHours(1));
var bytes = await client.Kv.GetAsync("session:xyz");
var count = await client.Kv.BatchAsync(b => b
    .Set("a", Encoding.UTF8.GetBytes("1"))
    .Delete("b"));

// Admin
var apiKey = await client.Admin.CreateUserAsync("alice", namespace: null,
    permissions: [new UserPermission { Collection = "orders", Ops = BLiteOperation.Write }]);
await client.Admin.ProvisionTenantAsync("tenant-42");
```

---

## Studio

The **Blazor Studio** is a built-in administration UI served from the same process.  
Enable it in `appsettings.json`:

```json
"Studio": { "Enabled": true },
"Kestrel": {
  "Endpoints": {
    "Grpc":   { "Url": "https://*:2626", "Protocols": "Http2" },
    "Rest":   { "Url": "https://*:2627", "Protocols": "Http1AndHttp2" },
    "Studio": { "Url": "https://*:2628", "Protocols": "Http1AndHttp2" }
  }
}
```

Studio pages:

| Page | Path | Description |
|---|---|---|
| Dashboard | `/` | Server uptime, version, tenant/user counts |
| Tenants | `/tenants` | Provision / deprovision tenant databases, download backups |
| Users | `/users` | Create, revoke, rotate keys, manage permissions |
| Collections | `/collections` | Browse, create, drop collections; insert JSON documents |
| Collection detail | `/collection/{name}` | Indexes, VectorSource, TimeSeries, Schema tabs |
| Documents | `/documents/{name}` | Browse, edit, delete documents; BLQL query |
| Key-Value | `/kv` | Browse keys, set/edit/delete entries, purge expired |
| Embedding | `/embedding` | Load ONNX model, test embeddings, cosine-similarity sandbox |

First startup navigates to `/setup` to create the `root` admin user.

---

## Authentication & Multi-tenancy

Every request must carry an `x-api-key` header (or `Authorization: Bearer <key>` for REST).

**Permissions** (`[Flags]` enum — composable):

| Flag | Allowed operations |
|---|---|
| `Query` | FindById, Query, VectorSearch, ScanKeys, Get, Exists |
| `Insert` | Insert, InsertBulk, KV Set |
| `Update` | Update, UpdateBulk, KV Refresh, KV Batch |
| `Delete` | Delete, DeleteBulk, KV Delete |
| `Drop` | DropCollection |
| `Admin` | All of the above + user management + tenant management + PurgeExpired |

**Namespace isolation** — users with a `Namespace` field operate inside a transparent prefix.  
Physical name: `"<namespace>:<logical_name>"`.  Always resolved via `NamespaceResolver.Resolve`.

**Database isolation** — users with a `DatabaseId` are restricted to one tenant engine.

---

## QueryDescriptor

Expression trees are serialized as a `QueryDescriptor` (MessagePack) sent in the `query_descriptor` bytes field:

```csharp
public sealed class QueryDescriptor
{
    public string         Collection { get; set; }
    public FilterNode?    Where      { get; set; }   // BinaryFilter | LogicalFilter
    public ProjectionSpec? Select    { get; set; }
    public List<SortSpec> OrderBy    { get; set; }
    public int?           Skip       { get; set; }
    public int?           Take       { get; set; }
}
```

The server rebuilds native BLite predicates from the descriptor — `T` is never instantiated on the server.

---

## Transactions

```
1. TransactionService.Begin → receives transaction_id (UUID)
2. Pass transaction_id in any write RPC → writes are buffered, not auto-committed
3. TransactionService.Commit or Rollback
```

At most one transaction per database at a time (enforced by `SemaphoreSlim(1,1)`).  
Sessions idle longer than `Transactions:TimeoutSeconds` (default: 60 s) are rolled back automatically.

---

## Observability

Built-in OpenTelemetry support:

- **Traces** — every gRPC RPC generates a span via `TelemetryInterceptor`
- **Metrics** — `blite.server.rpc.total`, `blite.server.rpc.duration`, `blite.server.documents.streamed`, `blite.server.active_transactions`
- **Exporters** — Console (dev) + OTLP/gRPC (Jaeger, Grafana, Datadog, …)

```json
"Telemetry": {
  "Enabled": true,
  "ServiceName": "blite-server",
  "Otlp": { "Endpoint": "http://localhost:4317" }
}
```

---

## Getting Started

```bash
dotnet run --project src/BLite.Server
```

On first run, navigate to `https://localhost:2628/setup` to create the root admin user.

| Service | URL |
|---|---|
| gRPC | `https://localhost:2626` |
| REST API | `https://localhost:2627/api/v1` |
| OpenAPI (Scalar) | `https://localhost:2627/scalar` |
| Studio | `https://localhost:2628` |

---

## License

Licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.  
See [LICENSE](LICENSE) for the full text.

> The AGPL-3.0 requires that any modified version of this software made available over
> a network also makes its source code available.  
> For a commercial license without this restriction, please contact the authors.

---

## Related Projects

- [BLite](https://github.com/EntglDb/BLite) — the embedded engine (MIT)
- [BLite documentation](https://blitedb.com/docs/getting-started)
