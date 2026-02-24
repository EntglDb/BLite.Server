// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks use AuthorizationService + principal stored in HttpContext.Items.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using System.Collections.Concurrent;
using System.IO.Compression;
using System.Text.Json.Nodes;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;
using BLite.Server.Auth;
using ErrorOr;

namespace BLite.Server.Rest;

/// <summary>Extension method that registers all BLite REST endpoints.</summary>
public static class RestApiExtensions
{
    // ── Entry point ────────────────────────────────────────────────────────────

    public static void MapRestApi(this WebApplication app)
    {
        var api = app.MapGroup("/api/v1")
                     .AddEndpointFilter<RestAuthFilter>()
                     .WithTags("BLite REST API")
                     .WithOpenApi();

        MapDatabases(api);
        MapCollections(api);
        MapDocuments(api);
        MapBlql(api);
        MapUsers(api);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Databases
    // ────────────────────────────────────────────────────────────────────────────

    private static void MapDatabases(RouteGroupBuilder g)
    {
        // GET /api/v1/databases — list all tenant databases
        g.MapGet("/databases",
            (HttpContext ctx,
             EngineRegistry registry,
             AuthorizationService authz) =>
        {
            var result = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (result.IsError) return result.ToResult(_ => Results.Ok());

            var tenants = registry.ListTenants()
                .Select(t => new { t.DatabaseId, t.IsActive })
                .ToList();
            return Results.Ok(tenants);
        });

        // POST /api/v1/databases — provision a new database
        //   Body: { "databaseId": "acme" }
        g.MapPost("/databases",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   ProvisionDatabaseRequest req,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            if (string.IsNullOrWhiteSpace(req.DatabaseId))
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["databaseId"] = ["databaseId is required."]
                });

            try
            {
                await registry.ProvisionAsync(req.DatabaseId.Trim(), ct);
                return Results.Created($"/api/v1/{req.DatabaseId.Trim()}/collections",
                    new { databaseId = req.DatabaseId.Trim() });
            }
            catch (InvalidOperationException)
            {
                return BLiteErrors.DatabaseAlreadyExists(req.DatabaseId).ToResult();
            }
        });

        // DELETE /api/v1/databases/{dbId}?deleteFiles=false
        g.MapDelete("/databases/{dbId}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   bool deleteFiles,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            try
            {
                await registry.DeprovisionAsync(dbId, deleteFiles, ct);
                return Results.NoContent();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // GET /api/v1/databases/{dbId}/backup — hot backup as a ZIP download
        //   Use dbId "_system" for the system (default) database.
        g.MapGet("/databases/{dbId}/backup",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var realId  = dbId.Equals("_system", StringComparison.OrdinalIgnoreCase) ? null : dbId;
            var label   = realId ?? "system";
            var stamp   = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var zipName = $"blite-backup-{label}-{stamp}.zip";
            var tempDb  = Path.Combine(Path.GetTempPath(), $"blite-bkp-{Guid.NewGuid():N}.db");
            try
            {
                var engine = registry.GetEngine(realId);
                await engine.BackupAsync(tempDb, ct);

                var ms = new MemoryStream();
                using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
                {
                    var entry = zip.CreateEntry($"{label}.db", CompressionLevel.Fastest);
                    await using var es = entry.Open();
                    await using var fs = new FileStream(
                        tempDb, FileMode.Open, FileAccess.Read, FileShare.None);
                    await fs.CopyToAsync(es, ct);
                }
                ms.Position = 0;
                return Results.File(ms, "application/zip", zipName);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
            finally
            {
                if (File.Exists(tempDb)) File.Delete(tempDb);
            }
        });
    }
    // ────────────────────────────────────────────────────────────────────────────

    private static void MapCollections(RouteGroupBuilder g)
    {
        // GET /api/v1/{dbId}/collections
        g.MapGet("/{dbId}/collections",
            (HttpContext ctx,
             EngineRegistry registry,
             AuthorizationService authz,
             string dbId) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, "*", BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user = auth.Value;
            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var cols = engine.ListCollections()
                    .Where(n => NamespaceResolver.BelongsTo(user, n))
                    .Select(n => NamespaceResolver.Strip(user, n))
                    .Where(n => !n.StartsWith("_"))   // hide reserved collections
                    .Select(n => new { name = n })
                    .ToList();
                return Results.Ok(cols);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // POST /api/v1/{dbId}/collections — create (ensure) a collection
        //   Body: { "collection": "orders" }
        g.MapPost("/{dbId}/collections",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   CreateCollectionRequest req,
                   CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(req.Collection))
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["collection"] = ["collection is required."]
                });

            var logical = req.Collection.Trim();
            var auth    = AuthorizedForDb(ctx, authz, dbId, logical, BLiteOperation.Insert);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, logical);

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var col    = engine.GetOrCreateCollection(physical);
                // Insert + delete a marker doc to materialise the collection on disk
                var doc = col.CreateDocument(["__rest_init"], b => b.AddBoolean("__rest_init", true));
                var id  = await col.InsertAsync(doc, ct);
                await col.DeleteAsync(id, ct);
                await engine.CommitAsync(ct);
                return Results.Created(
                    $"/api/v1/{dbId}/{logical}/documents",
                    new { databaseId = dbId, collection = logical });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // DELETE /api/v1/{dbId}/collections/{collection}
        g.MapDelete("/{dbId}/collections/{collection}",
            (HttpContext ctx,
             EngineRegistry registry,
             AuthorizationService authz,
             string dbId,
             string collection) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Drop);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            try
            {
                var engine  = registry.GetEngine(NullIfDefault(dbId));
                var dropped = engine.DropCollection(physical);
                return dropped
                    ? Results.NoContent()
                    : Results.Problem(
                        title:      "Not Found",
                        detail:     $"Collection '{collection}' does not exist.",
                        statusCode: StatusCodes.Status404NotFound);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Documents
    // ────────────────────────────────────────────────────────────────────────────

    private static void MapDocuments(RouteGroupBuilder g)
    {
        // GET /api/v1/{dbId}/{collection}/documents?skip=0&limit=50
        g.MapGet("/{dbId}/{collection}/documents",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   int skip = 0,
                   int limit = 50,
                   CancellationToken ct = default) =>
        {
            if (limit is <= 0 or > 1000) limit = 50;

            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var col    = engine.GetOrCreateCollection(physical);
                var docs   = new List<object>();
                int index  = 0;

                await foreach (var doc in col.FindAllAsync(ct))
                {
                    if (index++ < skip) continue;
                    if (docs.Count >= limit) break;
                    docs.Add(BsonJsonConverter.ToJson(doc, indented: false));
                }

                return Results.Ok(docs);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // POST /api/v1/{dbId}/{collection}/documents — insert a document
        //   Body: any valid JSON object
        g.MapPost("/{dbId}/{collection}/documents",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Insert);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            string json;
            try
            {
                using var reader = new StreamReader(ctx.Request.Body);
                json = await reader.ReadToEndAsync(ct);
                if (string.IsNullOrWhiteSpace(json))
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["body"] = ["Request body must be a non-empty JSON object."]
                    });
            }
            catch (Exception ex)
            {
                return BLiteErrors.InvalidJson(ex.Message).ToResult();
            }

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                engine.RegisterKeys(CollectJsonKeys(json));
                var keyMap  = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
                var revMap  = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
                var doc     = BsonJsonConverter.FromJson(json, keyMap, revMap);
                var id      = await engine.InsertAsync(physical, doc, ct);
                return Results.Created(
                    $"/api/v1/{dbId}/{collection}/documents/{id}",
                    new { id = id.ToString() });
            }
            catch (System.Text.Json.JsonException ex)
            {
                return BLiteErrors.InvalidJson(ex.Message).ToResult();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // GET /api/v1/{dbId}/{collection}/documents/{id}
        g.MapGet("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);
            var bsonId   = ParseId(id);

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var doc    = await engine.FindByIdAsync(physical, bsonId, ct);
                return doc is null
                    ? Results.Problem(
                        title:      "Not Found",
                        detail:     $"Document '{id}' not found in '{collection}'.",
                        statusCode: StatusCodes.Status404NotFound)
                    : Results.Content(BsonJsonConverter.ToJson(doc, indented: true),
                        "application/json");
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // PUT /api/v1/{dbId}/{collection}/documents/{id} — replace a document
        g.MapPut("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Update);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);
            var bsonId   = ParseId(id);

            string json;
            try
            {
                using var reader = new StreamReader(ctx.Request.Body);
                json = await reader.ReadToEndAsync(ct);
            }
            catch (Exception ex)
            {
                return BLiteErrors.InvalidJson(ex.Message).ToResult();
            }

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                engine.RegisterKeys(CollectJsonKeys(json));
                var keyMap = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
                var revMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
                var doc    = BsonJsonConverter.FromJson(json, keyMap, revMap);
                var ok     = await engine.UpdateAsync(physical, bsonId, doc, ct);
                return ok
                    ? Results.NoContent()
                    : Results.Problem(
                        title:      "Not Found",
                        detail:     $"Document '{id}' not found in '{collection}'.",
                        statusCode: StatusCodes.Status404NotFound);
            }
            catch (System.Text.Json.JsonException ex)
            {
                return BLiteErrors.InvalidJson(ex.Message).ToResult();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });

        // DELETE /api/v1/{dbId}/{collection}/documents/{id}
        g.MapDelete("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Delete);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);
            var bsonId   = ParseId(id);

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var ok     = await engine.DeleteAsync(physical, bsonId, ct);
                return ok
                    ? Results.NoContent()
                    : Results.Problem(
                        title:      "Not Found",
                        detail:     $"Document '{id}' not found in '{collection}'.",
                        statusCode: StatusCodes.Status404NotFound);
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        });
    }

    // ────────────────────────────────────────────────────────────────────────────
    // BLQL — BLite Query Language
    // ────────────────────────────────────────────────────────────────────────────

    private static void MapBlql(RouteGroupBuilder g)
    {
        // POST /api/v1/{dbId}/{collection}/query
        //
        // Body (all fields optional):
        //   {
        //     "filter":  { "status": "active", "age": { "$gt": 18 } },
        //     "sort":    { "name": 1, "age": -1 },
        //     "skip":    0,
        //     "limit":   50,
        //     "include": ["name", "email"],   // or "exclude": ["password"]
        //   }
        //
        // Response: { "count": N, "skip": N, "limit": N, "documents": [ {...}, ... ] }
        g.MapPost("/{dbId}/{collection}/query",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            string body;
            try
            {
                using var reader = new StreamReader(ctx.Request.Body);
                body = await reader.ReadToEndAsync(ct);
            }
            catch (Exception ex) { return BLiteErrors.InvalidJson(ex.Message).ToResult(); }

            BlqlFilter     filter     = BlqlFilter.Empty;
            BlqlSort?      sort       = null;
            int            skip       = 0;
            int            limit      = 50;
            BlqlProjection projection = BlqlProjection.All;

            if (!string.IsNullOrWhiteSpace(body))
            {
                try
                {
                    using var jdoc = System.Text.Json.JsonDocument.Parse(body);
                    var root = jdoc.RootElement;

                    if (root.TryGetProperty("filter", out var filterEl) &&
                        filterEl.ValueKind == System.Text.Json.JsonValueKind.Object)
                        filter = BlqlFilterParser.Parse(filterEl.GetRawText());

                    if (root.TryGetProperty("sort", out var sortEl) &&
                        sortEl.ValueKind == System.Text.Json.JsonValueKind.Object)
                        sort = BlqlSortParser.Parse(sortEl.GetRawText());

                    if (root.TryGetProperty("skip", out var skipEl))
                        skip = Math.Max(0, skipEl.GetInt32());

                    if (root.TryGetProperty("limit", out var limitEl))
                        limit = limitEl.GetInt32();
                    if (limit <= 0 || limit > 1000) limit = 50;

                    if (root.TryGetProperty("include", out var includeEl) &&
                        includeEl.ValueKind == System.Text.Json.JsonValueKind.Array)
                    {
                        var fields = includeEl.EnumerateArray()
                            .Where(e => e.ValueKind == System.Text.Json.JsonValueKind.String)
                            .Select(e => e.GetString()!)
                            .ToArray();
                        if (fields.Length > 0) projection = BlqlProjection.Include(fields);
                    }
                    else if (root.TryGetProperty("exclude", out var excludeEl) &&
                        excludeEl.ValueKind == System.Text.Json.JsonValueKind.Array)
                    {
                        var fields = excludeEl.EnumerateArray()
                            .Where(e => e.ValueKind == System.Text.Json.JsonValueKind.String)
                            .Select(e => e.GetString()!)
                            .ToArray();
                        if (fields.Length > 0) projection = BlqlProjection.Exclude(fields);
                    }
                }
                catch (FormatException ex)
                {
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["filter"] = [ex.Message]
                    });
                }
                catch (System.Text.Json.JsonException ex)
                {
                    return BLiteErrors.InvalidJson(ex.Message).ToResult();
                }
            }

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var col    = engine.GetOrCreateCollection(physical);

                var query = col.Query(filter);
                if (sort is not null) query = query.Sort(sort);
                query = query.Skip(skip).Take(limit).Project(projection);

                var docs = query.ToList()
                    .Select(d => JsonNode.Parse(BsonJsonConverter.ToJson(d, indented: false)))
                    .ToList();

                return Results.Ok(new { count = docs.Count, skip, limit, documents = docs });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        })
        .WithSummary("BLQL query — filter, sort, page, project")
        .WithDescription(
            "Executes a BLQL (BLite Query Language) query on a collection. " +
            "The request body is a JSON object with optional fields: " +
            "`filter` (MQL-style filter object), `sort` ({field: 1|-1}), " +
            "`skip` (int), `limit` (int, max 1000), " +
            "`include` (string[]) or `exclude` (string[]) for projection.");

        // GET /api/v1/{dbId}/{collection}/query?filter=...&sort=...&skip=0&limit=50
        //
        // Lightweight query variant for simple filters expressible in a URL.
        // `filter` and `sort` are URL-encoded JSON strings.
        g.MapGet("/{dbId}/{collection}/query",
            (HttpContext ctx,
             EngineRegistry registry,
             AuthorizationService authz,
             string dbId,
             string collection,
             string? filter,
             string? sort,
             int skip = 0,
             int limit = 50) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            if (limit <= 0 || limit > 1000) limit = 50;

            BlqlFilter blqlFilter = BlqlFilter.Empty;
            BlqlSort?  blqlSort   = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(filter))
                    blqlFilter = BlqlFilterParser.Parse(filter);
                if (!string.IsNullOrWhiteSpace(sort))
                    blqlSort = BlqlSortParser.Parse(sort);
            }
            catch (FormatException ex)
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["filter"] = [ex.Message]
                });
            }
            catch (System.Text.Json.JsonException ex)
            {
                return BLiteErrors.InvalidJson(ex.Message).ToResult();
            }

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var col    = engine.GetOrCreateCollection(physical);

                var query = col.Query(blqlFilter);
                if (blqlSort is not null) query = query.Sort(blqlSort);
                query = query.Skip(skip).Take(limit);

                var docs = query.ToList()
                    .Select(d => JsonNode.Parse(BsonJsonConverter.ToJson(d, indented: false)))
                    .ToList();

                return Results.Ok(new { count = docs.Count, skip, limit, documents = docs });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        })
        .WithSummary("BLQL query via query string")
        .WithDescription(
            "Executes a BLQL query using URL query parameters. " +
            "`filter` and `sort` are URL-encoded JSON strings (e.g. `filter={\"status\":\"active\"}`).");

        // POST /api/v1/{dbId}/{collection}/query/count
        //
        // Body (filter only):
        //   { "filter": { "status": "active" } }
        //
        // Response: { "count": 42 }
        g.MapPost("/{dbId}/{collection}/query/count",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   AuthorizationService authz,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
        {
            var auth = AuthorizedForDb(ctx, authz, dbId, collection, BLiteOperation.Query);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var user     = auth.Value;
            var physical = NamespaceResolver.Resolve(user, collection);

            BlqlFilter filter = BlqlFilter.Empty;

            string body;
            try
            {
                using var reader = new StreamReader(ctx.Request.Body);
                body = await reader.ReadToEndAsync(ct);
            }
            catch (Exception ex) { return BLiteErrors.InvalidJson(ex.Message).ToResult(); }

            if (!string.IsNullOrWhiteSpace(body))
            {
                try
                {
                    using var jdoc = System.Text.Json.JsonDocument.Parse(body);
                    var root = jdoc.RootElement;
                    if (root.TryGetProperty("filter", out var filterEl) &&
                        filterEl.ValueKind == System.Text.Json.JsonValueKind.Object)
                        filter = BlqlFilterParser.Parse(filterEl.GetRawText());
                }
                catch (FormatException ex)
                {
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["filter"] = [ex.Message]
                    });
                }
                catch (System.Text.Json.JsonException ex)
                {
                    return BLiteErrors.InvalidJson(ex.Message).ToResult();
                }
            }

            try
            {
                var engine = registry.GetEngine(NullIfDefault(dbId));
                var col    = engine.GetOrCreateCollection(physical);
                var count  = col.Query(filter).Count();
                return Results.Ok(new { count });
            }
            catch (InvalidOperationException ex)
            {
                return Results.Problem(
                    title:      "Not Found",
                    detail:     ex.Message,
                    statusCode: StatusCodes.Status404NotFound);
            }
        })
        .WithSummary("BLQL count — count documents matching a filter")
        .WithDescription(
            "Counts the documents matching a BLQL filter without returning payloads. " +
            "Body: `{ \"filter\": { ... } }`. Omit the body (or send `{}`) to count all documents.");
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Users
    // ────────────────────────────────────────────────────────────────────────────

    private static void MapUsers(RouteGroupBuilder g)
    {
        // GET /api/v1/users — list all users
        g.MapGet("/users",
            (HttpContext ctx,
             UserRepository users,
             AuthorizationService authz) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var list = users.ListAll().Select(u => new
            {
                u.Username,
                u.Namespace,
                u.DatabaseId,
                u.Active,
                CreatedAt = u.CreatedAt.ToString("O"),
                Permissions = u.Permissions.Select(p => new { p.Collection, Ops = p.Ops.ToString() })
            });
            return Results.Ok(list);
        });

        // POST /api/v1/users — create a user
        //   Body: { "username": "alice", "namespace": null, "databaseId": null }
        g.MapPost("/users",
            async (HttpContext ctx,
                   UserRepository users,
                   AuthorizationService authz,
                   CreateUserRequest req,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            if (string.IsNullOrWhiteSpace(req.Username))
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["username"] = ["username is required."]
                });

            try
            {
                var perms = new List<PermissionEntry>
                {
                    new("*", BLiteOperation.All)
                };
                var ns   = string.IsNullOrWhiteSpace(req.Namespace)   ? null : req.Namespace.Trim();
                var dbId = string.IsNullOrWhiteSpace(req.DatabaseId)  ? null : req.DatabaseId.Trim();
                var (_, plainKey) = await users.CreateAsync(req.Username.Trim(), ns, perms, dbId, ct);
                return Results.Created($"/api/v1/users/{req.Username.Trim()}",
                    new { username = req.Username.Trim(), apiKey = plainKey });
            }
            catch (InvalidOperationException)
            {
                return BLiteErrors.UserAlreadyExists(req.Username).ToResult();
            }
        });

        // DELETE /api/v1/users/{username}
        g.MapDelete("/users/{username}",
            async (HttpContext ctx,
                   UserRepository users,
                   AuthorizationService authz,
                   string username,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var deleted = await users.DeleteUserAsync(username, ct);
            return deleted
                ? Results.NoContent()
                : BLiteErrors.UserNotFound(username).ToResult();
        });

        // PUT /api/v1/users/{username}/permissions
        //   Body: [{ "collection": "*", "ops": 63 }, ...]
        g.MapPut("/users/{username}/permissions",
            async (HttpContext ctx,
                   UserRepository users,
                   AuthorizationService authz,
                   string username,
                   List<PermissionRequest> perms,
                   CancellationToken ct) =>
        {
            var auth = Authorized(ctx, authz, "*", BLiteOperation.Admin);
            if (auth.IsError) return auth.ToResult(_ => Results.Ok());

            var entries = perms
                .Where(p => !string.IsNullOrWhiteSpace(p.Collection))
                .Select(p => new PermissionEntry(p.Collection.Trim(), (BLiteOperation)p.Ops))
                .ToList();

            var ok = await users.UpdatePermissionsAsync(username, entries, ct);
            return ok
                ? Results.NoContent()
                : BLiteErrors.UserNotFound(username).ToResult();
        });
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Auth helpers
    // ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the authenticated user from the current request context, checking
    /// that they have <paramref name="op"/> on <paramref name="collection"/>.
    /// Returns an <see cref="Error"/> if the user is absent or lacks permission.
    /// </summary>
    private static ErrorOr<BLiteUser> Authorized(
        HttpContext ctx, AuthorizationService authz,
        string collection, BLiteOperation op)
    {
        if (ctx.Items[nameof(BLiteUser)] is not BLiteUser user)
            return BLiteErrors.MissingKey();

        if (!user.Active)
            return BLiteErrors.InactiveUser(user.Username);

        if (!authz.CheckPermission(user, collection, op))
            return BLiteErrors.PermissionDenied(user.Username, op, collection);

        return user;
    }

    /// <summary>
    /// Like <see cref="Authorized"/> but also verifies the user is allowed to access
    /// <paramref name="dbId"/> (user's <c>DatabaseId</c>, if set, must match).
    /// </summary>
    private static ErrorOr<BLiteUser> AuthorizedForDb(
        HttpContext ctx, AuthorizationService authz,
        string dbId, string collection, BLiteOperation op)
    {
        var base_ = Authorized(ctx, authz, collection, op);
        if (base_.IsError) return base_;

        var user          = base_.Value;
        var normalizedReq = NullIfDefault(dbId);

        if (user.DatabaseId is not null &&
            !user.DatabaseId.Equals(normalizedReq, StringComparison.OrdinalIgnoreCase))
        {
            return BLiteErrors.PermissionDenied(
                user.Username, op,
                $"database '{dbId}' (user is restricted to '{user.DatabaseId}')");
        }

        return user;
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Utility helpers
    // ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Routes use "default" as a sentinel for the system database (empty string key).
    /// </summary>
    private static string? NullIfDefault(string dbId)
        => string.IsNullOrEmpty(dbId) || dbId.Equals("default", StringComparison.OrdinalIgnoreCase)
            ? null
            : dbId;

    /// <summary>
    /// Best-effort parse of a URL segment into a <see cref="BsonId"/>.
    /// Order: Guid → Int64 → ObjectId (24 hex) → String.
    /// </summary>
    private static BsonId ParseId(string s)
    {
        if (Guid.TryParse(s, out var guid))              return new BsonId(guid);
        if (long.TryParse(s, out var lng))               return new BsonId(lng);
        if (s.Length == 24 && IsAllHex(s))
        {
            try { return new BsonId(new ObjectId(Convert.FromHexString(s))); }
            catch { /* fall through to string */ }
        }
        return new BsonId(s);
    }

    private static bool IsAllHex(string s)
    {
        foreach (var c in s)
            if (!Uri.IsHexDigit(c)) return false;
        return true;
    }

    /// <summary>Collects every property name from a JSON tree for key pre-registration.</summary>
    private static IEnumerable<string> CollectJsonKeys(string json)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var jdoc = System.Text.Json.JsonDocument.Parse(json);
            CollectElementKeys(jdoc.RootElement, keys);
        }
        catch { /* invalid JSON — let the converter surface the real error */ }
        return keys;
    }

    private static void CollectElementKeys(System.Text.Json.JsonElement el, HashSet<string> keys)
    {
        if (el.ValueKind == System.Text.Json.JsonValueKind.Object)
            foreach (var prop in el.EnumerateObject())
            {
                if (prop.Name != "_id") keys.Add(prop.Name);
                CollectElementKeys(prop.Value, keys);
            }
        else if (el.ValueKind == System.Text.Json.JsonValueKind.Array)
            foreach (var item in el.EnumerateArray())
                CollectElementKeys(item, keys);
    }

    private static async Task ZipAddFileAsync(
        ZipArchive zip, string filePath, string entryName, CancellationToken ct)
    {
        var entry = zip.CreateEntry(entryName, CompressionLevel.Fastest);
        await using var entryStream = entry.Open();
        await using var fs = new FileStream(
            filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        await fs.CopyToAsync(entryStream, ct);
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Request DTOs
// ────────────────────────────────────────────────────────────────────────────

public sealed record ProvisionDatabaseRequest(string DatabaseId);
public sealed record CreateCollectionRequest(string Collection);
public sealed record CreateUserRequest(string Username, string? Namespace, string? DatabaseId);
public sealed record PermissionRequest(string Collection, int Ops);
