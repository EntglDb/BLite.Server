# Implement: Query Result Cache

**Task for AI agent.** Implement an opt-in, collection-scoped, write-invalidated
query result cache using `IMemoryCache`. This document is self-contained: read it
completely before touching any file.

Read `AGENTS.md` first for project conventions, naming rules, and invariants.

---

## Scope

| In scope | Out of scope |
|---|---|
| Cache REST BLQL queries | `FindById` (point lookup, already O(1)) |
| Cache gRPC `Query` streaming calls | Queries inside an active transaction |
| Write-triggered invalidation on all mutation paths | Cache stampede protection (future) |
| Per-collection `CancellationChangeToken` eviction | Distributed / multi-node cache |
| Configuration via `appsettings.json` | Vector search endpoints |

---

## Files to create

```
src/BLite.Server/Caching/
  QueryCacheService.cs      — core cache service
  QueryCacheOptions.cs      — strongly-typed config
  QueryCacheKeys.cs         — deterministic cache key builders
```

---

## Files to modify

```
src/BLite.Server/Program.cs                             — register services
src/BLite.Server/Rest/RestApiDocumentsExtensions.cs     — cache GET /documents
src/BLite.Server/Rest/RestApiBlqlExtensions.cs          — cache POST+GET /query, POST /query/count
src/BLite.Server/Rest/RestApiDocumentsExtensions.cs     — invalidate on POST/PUT/DELETE /documents
src/BLite.Server/Rest/RestApiCollectionsExtensions.cs   — invalidate on DELETE /collections/{col}
src/BLite.Server/Rest/RestApiDatabasesExtensions.cs     — invalidate on DELETE /databases/{dbId}
src/BLite.Server/Services/DynamicServiceImpl.cs         — cache Query; invalidate writes
src/BLite.Server/Services/DocumentServiceImpl.cs        — cache Query; invalidate writes
src/BLite.Server/Transactions/TransactionManager.cs     — invalidate on CommitAsync
appsettings.json                                        — add QueryCache section
```

---

## Step 1 — Create `QueryCacheOptions.cs`

```csharp
// BLite.Server — Query cache configuration
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.Caching;

public sealed class QueryCacheOptions
{
    public bool   Enabled                   { get; init; } = false;
    public int    SlidingExpirationSeconds  { get; init; } = 30;
    public int    AbsoluteExpirationSeconds { get; init; } = 300;
    public long   MaxSizeBytes              { get; init; } = 64 * 1024 * 1024; // 64 MB
    public int    MaxResultSetSize          { get; init; } = 500; // skip cache if result > N docs
}
```

---

## Step 2 — Create `QueryCacheKeys.cs`

Cache keys must be **deterministic** and **include `dbId`** (cross-tenant isolation).
The physical collection name (after `NamespaceResolver.Resolve`) is always used.

```csharp
// BLite.Server — Cache key builders
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Security.Cryptography;
using System.Text;

namespace BLite.Server.Caching;

/// <summary>
/// Builds cache keys for every query variant.
/// All keys include the database id to prevent cross-tenant cache hits.
/// Physical collection names (post-namespace-resolution) are always used.
/// </summary>
public static class QueryCacheKeys
{
    // REST — GET /documents
    public static string DocumentsList(string? dbId, string collection, int skip, int limit)
        => $"docs:{N(dbId)}:{collection}:{skip}:{limit}";

    // REST — POST /query  (body is the raw JSON request body)
    public static string BlqlPost(string? dbId, string collection, string body)
        => $"blql-post:{N(dbId)}:{collection}:{Hash(body)}";

    // REST — GET /query  (all query-string params concatenated)
    public static string BlqlGet(string? dbId, string collection,
        string? filter, string? sort, int skip, int limit)
        => $"blql-get:{N(dbId)}:{collection}:{Hash($"{filter}|{sort}|{skip}|{limit}")}";

    // REST — POST /query/count
    public static string BlqlCount(string? dbId, string collection, string body)
        => $"blql-count:{N(dbId)}:{collection}:{Hash(body)}";

    // gRPC — Query (bytes are the serialized QueryDescriptor)
    public static string GrpcQuery(string? dbId, string collection, byte[] descriptorBytes)
        => $"grpc:{N(dbId)}:{collection}:{HashBytes(descriptorBytes)}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string N(string? dbId)
        => string.IsNullOrWhiteSpace(dbId) ? "_sys" : dbId.Trim().ToLowerInvariant();

    private static string Hash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input ?? string.Empty));
        return Convert.ToHexString(bytes)[..16]; // first 64 bits is enough for a cache key
    }

    private static string HashBytes(byte[] input)
    {
        var bytes = SHA256.HashData(input);
        return Convert.ToHexString(bytes)[..16];
    }
}
```

---

## Step 3 — Create `QueryCacheService.cs`

`IMemoryCache` does not support prefix/tag-based eviction. The solution is a
`CancellationTokenSource` per `(dbId, collection)`: cancelling it expires every
`IMemoryCache` entry that was registered with the corresponding `CancellationChangeToken`.

```csharp
// BLite.Server — Query result cache
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Registered as Singleton. Wraps IMemoryCache with per-(dbId, collection)
// CancellationChangeToken-based invalidation.

using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;

namespace BLite.Server.Caching;

/// <summary>
/// Thread-safe query result cache with collection-scoped write invalidation.
/// </summary>
public sealed class QueryCacheService(
    IMemoryCache cache,
    IOptions<QueryCacheOptions> opts)
{
    private readonly QueryCacheOptions _opts = opts.Value;

    // One CTS per canonical (dbId, collection) key.
    // Cancelling it expires all cache entries tagged with that collection.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();

    public bool Enabled => _opts.Enabled;

    // ── Read ──────────────────────────────────────────────────────────────────

    public bool TryGet<T>(string key, out T? value) =>
        cache.TryGetValue(key, out value);

    // ── Write ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Stores <paramref name="value"/> in the cache, linked to the given collection
    /// so it is evicted the next time any write targets that collection.
    /// </summary>
    public void Set<T>(string key, T value, string? dbId, string collection)
    {
        var collKey = CollectionKey(dbId, collection);
        var cts     = _tokens.GetOrAdd(collKey, _ => new CancellationTokenSource());

        var entry = new MemoryCacheEntryOptions()
            .SetSlidingExpiration(TimeSpan.FromSeconds(_opts.SlidingExpirationSeconds))
            .SetAbsoluteExpiration(TimeSpan.FromSeconds(_opts.AbsoluteExpirationSeconds))
            .AddExpirationToken(new CancellationChangeToken(cts.Token));

        if (_opts.MaxSizeBytes > 0)
            entry.SetSize(EstimateSize(value));

        cache.Set(key, value, entry);
    }

    // ── Invalidation ─────────────────────────────────────────────────────────

    /// <summary>Evicts all cached results for one (dbId, collection) pair.</summary>
    public void Invalidate(string? dbId, string collection)
    {
        var key = CollectionKey(dbId, collection);
        if (_tokens.TryRemove(key, out var cts))
            cts.Cancel();
    }

    /// <summary>Evicts all cached results for every collection in a database.</summary>
    public void InvalidateDatabase(string? dbId)
    {
        var prefix = $"{N(dbId)}:";
        foreach (var key in _tokens.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
            if (_tokens.TryRemove(key, out var cts))
                cts.Cancel();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string CollectionKey(string? dbId, string collection)
        => $"{N(dbId)}:{collection}";

    private static string N(string? dbId)
        => string.IsNullOrWhiteSpace(dbId) ? "_sys" : dbId.Trim().ToLowerInvariant();

    private static long EstimateSize<T>(T value) => value switch
    {
        string s                     => (long)s.Length * 2,
        System.Collections.ICollection c => (long)c.Count * 512,
        byte[] b                     => b.Length,
        _                            => 1024
    };
}
```

---

## Step 4 — Register services in `Program.cs`

Add the following block **before** the Studio block, inside the services section:

```csharp
// Query cache (optional — disabled by default, enabled via QueryCache:Enabled)
builder.Services.Configure<QueryCacheOptions>(
    builder.Configuration.GetSection("QueryCache"));
builder.Services.AddMemoryCache(opts =>
{
    var maxBytes = builder.Configuration.GetValue<long?>("QueryCache:MaxSizeBytes");
    if (maxBytes > 0) opts.SizeLimit = maxBytes;
});
builder.Services.AddSingleton<QueryCacheService>();
```

---

## Step 5 — Add `QueryCache` to `appsettings.json`

```json
"QueryCache": {
    "Enabled": false,
    "SlidingExpirationSeconds": 30,
    "AbsoluteExpirationSeconds": 300,
    "MaxSizeBytes": 67108864,
    "MaxResultSetSize": 500
}
```

`Enabled` defaults to `false`. Operators opt in explicitly.

---

## Step 6 — Cache REST read endpoints

Inject `QueryCacheService` via the handler's parameter list (Minimal API DI).
The pattern is identical for every read endpoint:

```
1. If !cache.Enabled → skip cache entirely, execute as before
2. Build the cache key
3. If cache.TryGet(key, out result) → return cached result
4. Execute the real query
5. If result count <= MaxResultSetSize → cache.Set(key, result, dbId, physicalCollection)
6. Return result
```

### 6a — `GET /{dbId}/{collection}/documents` (RestApiDocumentsExtensions.cs)

```csharp
group.MapGet("/{dbId}/{collection}/documents",
    async (HttpContext ctx,
           EngineRegistry registry,
           QueryCacheService cache,
           string dbId,
           string collection,
           int skip = 0,
           int limit = 50,
           CancellationToken ct = default) =>
    {
        if (limit is <= 0 or > 1000) limit = 50;

        var user     = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
        var physical = NamespaceResolver.Resolve(user, collection);
        var realDb   = RestApiExtensions.NullIfDefault(dbId);

        if (cache.Enabled)
        {
            var cacheKey = QueryCacheKeys.DocumentsList(realDb, physical, skip, limit);
            if (cache.TryGet(cacheKey, out List<object>? cached))
                return Results.Ok(cached);
        }

        try
        {
            var engine = registry.GetEngine(realDb);
            var col    = engine.GetOrCreateCollection(physical);
            var docs   = new List<object>();
            int index  = 0;

            await foreach (var doc in col.FindAllAsync(ct))
            {
                if (index++ < skip) continue;
                if (docs.Count >= limit) break;
                docs.Add(BsonJsonConverter.ToJson(doc, indented: false));
            }

            if (cache.Enabled && docs.Count <= cache_opts.MaxResultSetSize)  // see note below
            {
                var cacheKey = QueryCacheKeys.DocumentsList(realDb, physical, skip, limit);
                cache.Set(cacheKey, docs, realDb, physical);
            }

            return Results.Ok(docs);
        }
        catch (InvalidOperationException ex) { … }
    })
```

> **Note**: to avoid calling `QueryCacheKeys.*` twice, build the key once before
> the query and store it in a local variable regardless of whether you take the
> cache hit branch.

### 6b — `POST /{dbId}/{collection}/query` (RestApiBlqlExtensions.cs)

The body is read from `ctx.Request.Body`. Read it once, use it both for the cache
key and for the actual query:

```csharp
// Read body once
string body;
try
{
    using var reader = new StreamReader(ctx.Request.Body);
    body = await reader.ReadToEndAsync(ct);
}
catch (Exception ex) { return BLiteErrors.InvalidJson(ex.Message).ToResult(); }

var realDb   = RestApiExtensions.NullIfDefault(dbId);
var cacheKey = QueryCacheKeys.BlqlPost(realDb, physical, body ?? string.Empty);

if (cache.Enabled && cache.TryGet(cacheKey, out object? hit))
    return Results.Ok(hit);

// … parse body and execute query as before …

if (cache.Enabled && docs.Count <= opts.MaxResultSetSize)
    cache.Set(cacheKey, new { count = docs.Count, skip, limit, documents = docs }, realDb, physical);
```

### 6c — `GET /{dbId}/{collection}/query` (RestApiBlqlExtensions.cs)

```csharp
var cacheKey = QueryCacheKeys.BlqlGet(realDb, physical, filter, sort, skip, limit);

if (cache.Enabled && cache.TryGet(cacheKey, out object? hit))
    return Results.Ok(hit);
// … execute as before …
if (cache.Enabled && docs.Count <= opts.MaxResultSetSize)
    cache.Set(cacheKey, new { count = docs.Count, skip, limit, documents = docs }, realDb, physical);
```

### 6d — `POST /{dbId}/{collection}/query/count` (RestApiBlqlExtensions.cs)

```csharp
// Read body first (same as 6b)
var cacheKey = QueryCacheKeys.BlqlCount(realDb, physical, body ?? string.Empty);

if (cache.Enabled && cache.TryGet(cacheKey, out object? hit))
    return Results.Ok(hit);
// … execute count …
if (cache.Enabled)
    cache.Set(cacheKey, new { count }, realDb, physical);
// count is always a scalar — no MaxResultSetSize check needed
```

---

## Step 7 — Invalidate REST write endpoints

Add `QueryCacheService cache` to the handler parameter list of every write
endpoint. Call `cache.Invalidate` **after** a successful write.

### Pattern

```csharp
// After successful insert/update/delete:
if (cache.Enabled)
    cache.Invalidate(RestApiExtensions.NullIfDefault(dbId), physical);
```

### Invalidation points in `RestApiDocumentsExtensions.cs`

| Endpoint | When to invalidate |
|---|---|
| `POST /{dbId}/{collection}/documents` | After `engine.InsertAsync` succeeds |
| `PUT /{dbId}/{collection}/documents/{id}` | After `engine.UpdateAsync` returns `true` |
| `DELETE /{dbId}/{collection}/documents/{id}` | After `engine.DeleteAsync` returns `true` |

### Invalidation points in `RestApiCollectionsExtensions.cs`

| Endpoint | When to invalidate |
|---|---|
| `DELETE /{dbId}/collections/{collection}` | After `engine.DropCollection` returns `true` |

### Invalidation points in `RestApiDatabasesExtensions.cs`

| Endpoint | When to invalidate |
|---|---|
| `DELETE /databases/{dbId}` | After `registry.DeprovisionAsync` succeeds; call `cache.InvalidateDatabase(dbId)` |

---

## Step 8 — Cache gRPC `Query` endpoints

Both `DynamicServiceImpl` and `DocumentServiceImpl` have a `Query` method that
streams results via `IServerStreamWriter`. To cache them, **materialize the result
first**, then stream from the in-memory list.

Inject `QueryCacheService` via constructor injection in both service classes.

### Cache type for gRPC

Do **not** cache `JsonNode` or `BsonDocument`. Cache the already-serialized BSON
bytes to avoid double-deserialization:

```csharp
// Cache entry type
List<byte[]>  // each element = BsonPayloadSerializer.Serialize(doc)
```

### Pattern for `DynamicServiceImpl.Query`

```csharp
public override async Task Query(
    QueryRequest request,
    IServerStreamWriter<DocumentResponse> responseStream,
    ServerCallContext context)
{
    // … deserialize descriptor, authorize, resolve collection as before …

    var realDb   = user.DatabaseId;
    var cacheKey = QueryCacheKeys.GrpcQuery(realDb, descriptor.Collection,
                                            request.QueryDescriptor.ToByteArray());

    if (_cache.Enabled && _cache.TryGet(cacheKey, out List<byte[]>? cachedPayloads))
    {
        foreach (var payload in cachedPayloads!)
            await responseStream.WriteAsync(
                new DocumentResponse { BsonPayload = ByteString.CopyFrom(payload), Found = true },
                context.CancellationToken);
        return;
    }

    // Execute and collect
    var payloads = new List<byte[]>();
    await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(engine, descriptor, ct))
    {
        var payload = BsonPayloadSerializer.Serialize(doc);
        payloads.Add(payload);
        await responseStream.WriteAsync(
            new DocumentResponse { BsonPayload = ByteString.CopyFrom(payload), Found = true },
            ct);
    }

    if (_cache.Enabled && payloads.Count <= _cacheOpts.MaxResultSetSize)
        _cache.Set(cacheKey, payloads, realDb, descriptor.Collection);
}
```

Apply the same pattern to `DocumentServiceImpl.Query`, using
`TypedDocumentResponse` and including the `TypeName` field.

---

## Step 9 — Invalidate gRPC write endpoints

Add `_cache.Invalidate(user.DatabaseId, col)` after every successful write in
both `DynamicServiceImpl` and `DocumentServiceImpl`:

| Method | When |
|---|---|
| `Insert` | After `engine.InsertAsync` / `session.Engine…InsertAsync` |
| `Update` | After `engine.UpdateAsync` returns `true` |
| `Delete` | After `engine.DeleteAsync` returns `true` |
| `InsertBulk` | After `engine.InsertBulkAsync` / `session.Engine…InsertBulkAsync` |

For transactional writes (`request.TransactionId` non-empty), do **not**
invalidate immediately — invalidation happens at Commit (Step 10).

```csharp
// Non-transactional write example
id = await engine.InsertAsync(col, doc, context.CancellationToken);
if (_cache.Enabled)
    _cache.Invalidate(user.DatabaseId, col);
```

---

## Step 10 — Invalidate on Transaction Commit

In `TransactionManager.CommitAsync`, collect the collections written during the
transaction and invalidate them after the engine commit.

The `TransactionSession` already knows `DatabaseId`. The set of written collections
must be tracked in `TransactionSession`:

### 10a — Modify `TransactionSession`

Add a thread-safe set for tracking written collections:

```csharp
// Add to TransactionSession
private readonly ConcurrentBag<string> _dirtyCollections = [];

public void MarkDirty(string physicalCollection)
    => _dirtyCollections.Add(physicalCollection);

public IReadOnlyCollection<string> DirtyCollections
    => _dirtyCollections.Distinct().ToList();
```

### 10b — Call `MarkDirty` from transactional writes

In `DynamicServiceImpl` and `DocumentServiceImpl`, when `request.TransactionId`
is non-empty, call `session.MarkDirty(col)` after each write.

### 10c — Invalidate in `TransactionManager.CommitAsync`

```csharp
public async Task CommitAsync(string txnId, BLiteUser caller, CancellationToken ct)
{
    var session = RemoveSession(txnId, caller);
    try
    {
        await session.Engine.CommitAsync(ct);

        // Invalidate cache for every collection written in this transaction
        if (_cache.Enabled)
            foreach (var col in session.DirtyCollections)
                _cache.Invalidate(session.DatabaseId, col);

        _logger.LogInformation("Transaction {TxnId} committed by '{User}'.", txnId, caller.Username);
    }
    finally
    {
        GetLock(session.DatabaseId).Release();
    }
}
```

Inject `QueryCacheService` into `TransactionManager` via constructor.
`TransactionManager` is a singleton; `QueryCacheService` is also a singleton — no
lifetime violation.

Transaction `RollbackAsync` and `CleanupExpiredAsync`: **do not invalidate**.
The data was never committed so the cache is still consistent.

---

## Step 11 — `MaxResultSetSize` access in Minimal API handlers

The REST handlers need `_opts.MaxResultSetSize` to decide whether to cache.
Since Minimal API handlers cannot hold state, resolve `QueryCacheOptions` via DI:

```csharp
// Add QueryCacheOptions to the handler parameter list
(…, QueryCacheService cache, IOptions<QueryCacheOptions> cacheOpts, …)

// Then use:
if (cache.Enabled && docs.Count <= cacheOpts.Value.MaxResultSetSize)
    cache.Set(cacheKey, result, realDb, physical);
```

---

## Transaction-aware skipping (read endpoints)

Do **not** read from cache when the request's database has an active transaction.
A transaction may have written data that is not yet committed; reading from the
cache would return stale (pre-transaction) results.

Add a `HasActiveTransaction(string? dbId)` method to `TransactionManager`:

```csharp
public bool HasActiveTransaction(string? dbId)
{
    var key = CanonicalDbId(dbId);
    return _sessions.Values.Any(s => s.DatabaseId == key);
}
```

Then in every read handler:

```csharp
if (cache.Enabled
    && !txnManager.HasActiveTransaction(realDb)
    && cache.TryGet(cacheKey, out T? cached))
    return Results.Ok(cached);
```

Inject `TransactionManager txnManager` into each read handler's parameter list
(it is a singleton, so Minimal API DI will resolve it).

---

## Invariants to respect

- **Always** use the **physical** (post-namespace) collection name in cache keys
  and invalidation calls. Logical names are user-facing only.
- **Always** include `realDb` (the value after `NullIfDefault`) in the cache key,
  not the raw `dbId` from the route.
- **Never** cache `FindById` results.
- **Never** cache a result set larger than `MaxResultSetSize`.
- **Never** cache when a transaction is active on that database.
- **Never** invalidate on `Rollback` — the engine discarded the writes.
- The cache is **read-through from the caller's perspective** only. The engine has
  no knowledge of the cache; the cache layer lives entirely in the server.

---

## Acceptance checklist

- [ ] `QueryCacheService`, `QueryCacheOptions`, `QueryCacheKeys` created
- [ ] `IMemoryCache` + `QueryCacheService` registered in `Program.cs`
- [ ] `QueryCache` section added to `appsettings.json` with `"Enabled": false`
- [ ] REST `GET /documents` caches + `POST/PUT/DELETE` invalidates
- [ ] REST `POST /query`, `GET /query`, `POST /query/count` cache + invalidate
- [ ] REST `DELETE /collections/{col}` invalidates
- [ ] REST `DELETE /databases/{dbId}` calls `InvalidateDatabase`
- [ ] gRPC `DynamicService.Query` caches materialized BSON bytes
- [ ] gRPC `DocumentService.Query` caches materialized BSON bytes
- [ ] gRPC `Insert/Update/Delete/InsertBulk` invalidates (non-transactional path)
- [ ] `TransactionSession.MarkDirty` + `DirtyCollections` added
- [ ] Transactional writes call `MarkDirty`
- [ ] `TransactionManager.CommitAsync` invalidates dirty collections
- [ ] `TransactionManager.HasActiveTransaction` guards all read-cache hits
- [ ] `cache.Enabled == false` results in zero overhead (no key computation)
- [ ] Build passes with `dotnet build src/BLite.Server/BLite.Server.csproj`
