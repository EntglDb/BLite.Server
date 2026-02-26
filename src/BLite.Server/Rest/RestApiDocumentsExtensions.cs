// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Server.Auth;
using BLite.Server.Caching;
using BLite.Server.Transactions;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;

namespace BLite.Server.Rest;

/// <summary>
/// Provides extension methods for mapping document-related REST API endpoints to a route group, enabling CRUD
/// operations on documents within a database collection.
/// </summary>
/// <remarks>Use this class to configure endpoints for retrieving, inserting, updating, and deleting documents in
/// a collection. The mapped endpoints include authorization checks and handle common error scenarios, such as invalid
/// input or missing documents. This class is intended for internal use to expose document operations in a RESTful API
/// for a database collection.</remarks>
internal static class RestApiDocumentsExtensions
{
    /// <summary>
    /// Maps document-related API endpoints to the specified route group, enabling CRUD operations for documents within
    /// a database collection.
    /// </summary>
    /// <remarks>This method configures endpoints for retrieving, inserting, updating, and deleting documents
    /// in a collection. Each endpoint includes authorization checks and handles common error scenarios, such as invalid
    /// input or missing documents. Use this method to expose document operations in a RESTful API for a database
    /// collection.</remarks>
    /// <param name="g">The route group builder used to define and organize the document API endpoints.</param>
    internal static void MapDocuments(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Documents");

        // GET /api/v1/{dbId}/{collection}/documents?skip=0&limit=50
        group.MapGet("/{dbId}/{collection}/documents",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   QueryCacheService cache,
                   IOptions<QueryCacheOptions> cacheOpts,
                   TransactionManager txnManager,
                   string dbId,
                   string collection,
                   int skip = 0,
                   int limit = 50,
                   CancellationToken ct = default) =>
            {
                if (limit is <= 0 or > 1000) limit = 50;

                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var realDb = RestApiExtensions.NullIfDefault(dbId);

                var cacheKey = QueryCacheKeys.DocumentsList(realDb, physical, skip, limit);
                if (cache.Enabled
                    && !txnManager.HasActiveTransaction(realDb))
                {
                    if (cache.TryGet(cacheKey, out List<object>? cached))
                        return Results.Ok(cached);
                }

                try
                {
                    var engine = registry.GetEngine(realDb);
                    var col = engine.GetOrCreateCollection(physical);
                    var docs = new List<object>();
                    int index = 0;

                    await foreach (var doc in col.FindAllAsync(ct))
                    {
                        if (index++ < skip) continue;
                        if (docs.Count >= limit) break;
                        docs.Add(BsonJsonConverter.ToJson(doc, indented: false));
                    }

                    if (cache.Enabled && docs.Count <= cacheOpts.Value.MaxResultSetSize)
                        cache.Set(cacheKey, docs, realDb, physical);

                    return Results.Ok(docs);
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, checkDb: true))
            .WithSummary("List documents")
            .WithDescription("Returns a page of raw JSON documents from the collection. Use `skip` and `limit` (max 1000) for pagination.");

        // POST /api/v1/{dbId}/{collection}/documents
        //   Body: any valid JSON object
        group.MapPost("/{dbId}/{collection}/documents",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   QueryCacheService cache,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
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
                    var realDb = RestApiExtensions.NullIfDefault(dbId);
                    var engine = registry.GetEngine(realDb);
                    engine.RegisterKeys(RestApiExtensions.CollectJsonKeys(json));
                    var keyMap = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
                    var revMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
                    var doc = BsonJsonConverter.FromJson(json, keyMap, revMap);
                    var id = await engine.InsertAsync(physical, doc, ct);
                    if (cache.Enabled)
                        cache.Invalidate(realDb, physical);
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
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Insert, checkDb: true))
            .WithSummary("Insert a document")
            .WithDescription("Inserts a JSON document into the collection and returns its generated id. The body must be a non-empty JSON object.");

        // GET /api/v1/{dbId}/{collection}/documents/{id}
        group.MapGet("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var bsonId = RestApiExtensions.ParseId(id);

                try
                {
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var doc = await engine.FindByIdAsync(physical, bsonId, ct);
                    return doc is null
                        ? Results.Problem(
                            title: "Not Found",
                            detail: $"Document '{id}' not found in '{collection}'.",
                            statusCode: StatusCodes.Status404NotFound)
                        : Results.Content(BsonJsonConverter.ToJson(doc, indented: true),
                            "application/json");
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, checkDb: true))
            .WithSummary("Get a document by id")
            .WithDescription("Returns a single document by its id. The id segment is parsed as Guid, Int64, ObjectId (24-char hex), or plain string, in that order.");

        // PUT /api/v1/{dbId}/{collection}/documents/{id}
        group.MapPut("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   QueryCacheService cache,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var bsonId = RestApiExtensions.ParseId(id);

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
                    var realDb = RestApiExtensions.NullIfDefault(dbId);
                    var engine = registry.GetEngine(realDb);
                    engine.RegisterKeys(RestApiExtensions.CollectJsonKeys(json));
                    var keyMap = (ConcurrentDictionary<string, ushort>)engine.GetKeyMap();
                    var revMap = (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
                    var doc = BsonJsonConverter.FromJson(json, keyMap, revMap);
                    var ok = await engine.UpdateAsync(physical, bsonId, doc, ct);
                    if (ok && cache.Enabled)
                        cache.Invalidate(realDb, physical);
                    return ok
                        ? Results.NoContent()
                        : Results.Problem(
                            title: "Not Found",
                            detail: $"Document '{id}' not found in '{collection}'.",
                            statusCode: StatusCodes.Status404NotFound);
                }
                catch (System.Text.Json.JsonException ex)
                {
                    return BLiteErrors.InvalidJson(ex.Message).ToResult();
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Update, checkDb: true))
            .WithSummary("Replace a document")
            .WithDescription("Fully replaces the document with the given id. The body must be a valid JSON object. Returns 404 if the document does not exist.");

        // DELETE /api/v1/{dbId}/{collection}/documents/{id}
        group.MapDelete("/{dbId}/{collection}/documents/{id}",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   QueryCacheService cache,
                   string dbId,
                   string collection,
                   string id,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var bsonId = RestApiExtensions.ParseId(id);

                try
                {
                    var realDb = RestApiExtensions.NullIfDefault(dbId);
                    var engine = registry.GetEngine(realDb);
                    var ok = await engine.DeleteAsync(physical, bsonId, ct);
                    if (ok && cache.Enabled)
                        cache.Invalidate(realDb, physical);
                    return ok
                        ? Results.NoContent()
                        : Results.Problem(
                            title: "Not Found",
                            detail: $"Document '{id}' not found in '{collection}'.",
                            statusCode: StatusCodes.Status404NotFound);
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Delete, checkDb: true))
            .WithSummary("Delete a document")
            .WithDescription("Permanently deletes the document with the given id. Returns 404 if the document does not exist.");

        // POST /api/v1/{dbId}/{collection}/vector-search
        //   Body: { "vector": [...], "k": 10, "indexName": "myIdx", "efSearch": 100 }
        group.MapPost("/{dbId}/{collection}/vector-search",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   string dbId,
                   string collection,
                   VectorSearchBody body,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var realDb = RestApiExtensions.NullIfDefault(dbId);

                if (body.Vector is not { Length: > 0 })
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["vector"] = ["vector is required and must be non-empty."]
                    });

                try
                {
                    var engine = registry.GetEngine(realDb);
                    var col = engine.GetOrCreateCollection(physical);

                    // Resolve index name
                    var indexName = body.IndexName;
                    if (string.IsNullOrEmpty(indexName))
                    {
                        var indexes = engine.GetIndexDescriptors(physical);
                        var vec = indexes.FirstOrDefault(d => d.Type == IndexType.Vector);
                        if (vec == null)
                            return Results.Problem(
                                title: "No vector index",
                                detail: $"Collection '{collection}' has no vector index.",
                                statusCode: StatusCodes.Status422UnprocessableEntity);
                        indexName = vec.Name;
                    }

                    var k = body.K > 0 ? body.K : 10;
                    var efSearch = body.EfSearch > 0 ? body.EfSearch : 100;

                    var results = col.VectorSearch(indexName, body.Vector, k, efSearch)
                        .Select(doc => BsonJsonConverter.ToJson(doc, indented: false))
                        .ToList();

                    return Results.Ok(new { count = results.Count, documents = results });
                }
                catch (ArgumentException ex)
                {
                    return Results.Problem(
                        title: "Invalid argument",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status422UnprocessableEntity);
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, checkDb: true))
            .WithSummary("Vector similarity search")
            .WithDescription("Returns the k nearest documents to the given query vector using the HNSW index.");
    }
}

internal sealed class VectorSearchBody
{
    public float[] Vector    { get; set; } = [];
    public int     K         { get; set; } = 10;
    public string? IndexName { get; set; }
    public int     EfSearch  { get; set; } = 100;
}
