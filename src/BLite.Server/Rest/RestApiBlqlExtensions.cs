// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using BLite.Bson;
using BLite.Core;
using BLite.Core.Query.Blql;
using BLite.Server.Auth;
using System.Text.Json.Nodes;

namespace BLite.Server.Rest;

/// <summary>
/// Provides extension methods for configuring BLQL (BLite Query Language) routes in a REST API.
/// </summary>
/// <remarks>This class contains methods to map BLQL query endpoints for executing queries and counting documents
/// in a specified collection. It handles both POST and GET requests, allowing for flexible querying options including
/// filtering, sorting, pagination, and projection.</remarks>
internal static class RestApiBlqlExtensions
{
    /// <summary>
    /// Maps BLQL query and count routes to the specified route group builder, enabling support for filtering, sorting,
    /// pagination, projection, and document counting operations on collections via HTTP endpoints.
    /// </summary>
    /// <remarks>This method configures endpoints for executing BLQL (BLite Query Language) queries and
    /// counting documents in a collection. It supports both POST and GET requests, allowing clients to specify filters,
    /// sorting, pagination, and projection options through JSON request bodies or URL query parameters. The endpoints
    /// are designed to provide flexible querying and counting capabilities for collection data, with input validation
    /// and error handling for malformed requests.</remarks>
    /// <param name="g">The route group builder to which BLQL query and count routes will be added.</param>
    internal static void MapBlql(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("BLQL");

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
        group.MapPost("/{dbId}/{collection}/query",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);

                string body;
                try
                {
                    using var reader = new StreamReader(ctx.Request.Body);
                    body = await reader.ReadToEndAsync(ct);
                }
                catch (Exception ex) { return BLiteErrors.InvalidJson(ex.Message).ToResult(); }

                BlqlFilter filter = BlqlFilter.Empty;
                BlqlSort? sort = null;
                int skip = 0;
                int limit = 50;
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
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var col = engine.GetOrCreateCollection(physical);

                    var query = col.Query().Filter(filter);
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
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, checkDb: true))
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
        group.MapGet("/{dbId}/{collection}/query",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection,
             string? filter,
             string? sort,
             int skip = 0,
             int limit = 50) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);

                if (limit <= 0 || limit > 1000) limit = 50;

                BlqlFilter blqlFilter = BlqlFilter.Empty;
                BlqlSort? blqlSort = null;
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
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var col = engine.GetOrCreateCollection(physical);

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
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, checkDb: true))
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
        group.MapPost("/{dbId}/{collection}/query/count",
            async (HttpContext ctx,
                   EngineRegistry registry,
                   string dbId,
                   string collection,
                   CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
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
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var col = engine.GetOrCreateCollection(physical);
                    var count = col.Query(filter).Count();
                    return Results.Ok(new { count });
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
            .WithSummary("BLQL count — count documents matching a filter")
            .WithDescription(
                "Counts the documents matching a BLQL filter without returning payloads. " +
                "Body: `{ \"filter\": { ... } }`. Omit the body (or send `{}`) to count all documents.");
    }
}
