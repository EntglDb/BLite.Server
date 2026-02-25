// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using BLite.Server.Auth;
using BLite.Server.Caching;

namespace BLite.Server.Rest;

/// <summary>
/// Provides extension methods for mapping REST API routes related to collections in a database.
/// </summary>
/// <remarks>This class defines endpoints for managing collections within a specified database, including listing,
/// creating, and deleting collections. Authorization checks are performed for each operation, and appropriate HTTP
/// responses are returned for validation errors or missing resources. These extensions are intended for internal use
/// when configuring REST API route groups.</remarks>
internal static class RestApiCollectionsExtensions
{
    /// <summary>
    /// Configures API routes for managing collections within a specified database group.
    /// </summary>
    /// <remarks>This method sets up routes for listing, creating, and deleting collections. Each route
    /// enforces authorization and returns appropriate HTTP responses based on the outcome of the operation. Use this
    /// method to expose collection management functionality in a RESTful API.</remarks>
    /// <param name="g">The <paramref name="g"/> parameter is the <see cref="RouteGroupBuilder"/> instance used to define the route
    /// group for collection-related endpoints.</param>
    internal static void MapCollections(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Collections");

        // GET /api/v1/{dbId}/collections
        group.MapGet("/{dbId}/collections",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
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
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, "*", checkDb: true))
            .WithSummary("List collections")
            .WithDescription("Returns all non-reserved collection names visible to the caller within the specified database. Collections whose names start with `_` are hidden.");

        // POST /api/v1/{dbId}/collections
        //   Body: { "collection": "orders" }
        group.MapPost("/{dbId}/collections",
            async (HttpContext ctx,
                   EngineRegistry registry,
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
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, logical);

                try
                {
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var col = engine.GetOrCreateCollection(physical);
                    // Insert + delete a marker doc to materialise the collection on disk
                    var doc = col.CreateDocument(["__rest_init"], b => b.AddBoolean("__rest_init", true));
                    var id = await col.InsertAsync(doc, ct);
                    await col.DeleteAsync(id, ct);
                    await engine.CommitAsync(ct);
                    return Results.Created(
                        $"/api/v1/{dbId}/{logical}/documents",
                        new { databaseId = dbId, collection = logical });
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
            .WithSummary("Create a collection")
            .WithDescription("Ensures a named collection exists in the specified database. If the collection already exists, the call is idempotent. The collection is materialised on disk immediately.");

        // DELETE /api/v1/{dbId}/collections/{collection}
        group.MapDelete("/{dbId}/collections/{collection}",
            (HttpContext ctx,
             EngineRegistry registry,
             QueryCacheService cache,
             string dbId,
             string collection) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var physical = NamespaceResolver.Resolve(user, collection);
                var realDb = RestApiExtensions.NullIfDefault(dbId);

                try
                {
                    var engine = registry.GetEngine(realDb);
                    var dropped = engine.DropCollection(physical);
                    if (dropped && cache.Enabled)
                        cache.Invalidate(realDb, physical);
                    return dropped
                        ? Results.NoContent()
                        : Results.Problem(
                            title: "Not Found",
                            detail: $"Collection '{collection}' does not exist.",
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
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Drop, checkDb: true))
            .WithSummary("Drop a collection")
            .WithDescription("Permanently removes a collection and all its documents from the specified database. Returns 404 if the collection does not exist.");
    }
}
