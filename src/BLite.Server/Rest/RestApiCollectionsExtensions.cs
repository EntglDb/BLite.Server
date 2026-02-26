// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using BLite.Core.Storage;
using BLite.Server.Auth;
using BLite.Server.Caching;
using BLite.Server.Embedding;

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

        // GET /api/v1/{dbId}/{collection}/vector-source
        group.MapGet("/{dbId}/{collection}/vector-source",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var config = engine.GetVectorSource(physical);
                    if (config == null)
                        return Results.NoContent();

                    // Serialize to a simple DTO
                    var dto = new
                    {
                        separator = config.Separator,
                        fields = config.Fields.Select(f => new
                        {
                            path = f.Path,
                            prefix = f.Prefix,
                            suffix = f.Suffix
                        }).ToList()
                    };
                    return Results.Ok(dto);
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
            .WithSummary("Get VectorSource configuration")
            .WithDescription("Retrieves the embedding configuration for automatic text composition. Returns 204 if not configured.");

        // PUT /api/v1/{dbId}/{collection}/vector-source
        group.MapPut("/{dbId}/{collection}/vector-source",
            async (HttpContext ctx,
             EngineRegistry registry,
             EmbeddingQueuePopulator populator,
             string dbId,
             string collection,
             VectorSourceConfigDto? dto,
             CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var realDb = RestApiExtensions.NullIfDefault(dbId);
                    var engine = registry.GetEngine(realDb);

                    // dto == null → clear configuration
                    if (dto == null)
                    {
                        engine.SetVectorSource(physical, null);
                        await populator.RefreshSubscriptionAsync(realDb, physical, ct);
                        return Results.NoContent();
                    }

                    // Build VectorSourceConfig from DTO
                    var config = new VectorSourceConfig
                    {
                        Separator = string.IsNullOrWhiteSpace(dto.Separator) ? " " : dto.Separator
                    };

                    if (dto.Fields != null)
                    {
                        foreach (var fieldDto in dto.Fields)
                        {
                            if (!string.IsNullOrWhiteSpace(fieldDto.Path))
                            {
                                config.Fields.Add(new VectorSourceField
                                {
                                    Path = fieldDto.Path.Trim(),
                                    Prefix = string.IsNullOrWhiteSpace(fieldDto.Prefix) ? null : fieldDto.Prefix,
                                    Suffix = string.IsNullOrWhiteSpace(fieldDto.Suffix) ? null : fieldDto.Suffix
                                });
                            }
                        }
                    }

                    if (config.Fields.Count == 0)
                        return Results.BadRequest(new { error = "At least one field is required." });

                    engine.SetVectorSource(physical, config);
                    await populator.RefreshSubscriptionAsync(realDb, physical, ct);
                    return Results.NoContent();
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
            .WithSummary("Set VectorSource configuration")
            .WithDescription("Updates the embedding configuration for automatic text composition. Send null body to clear.");

        // DELETE /api/v1/{dbId}/{collection}/vector-source
        group.MapDelete("/{dbId}/{collection}/vector-source",
            async (HttpContext ctx,
             EngineRegistry registry,
             EmbeddingQueuePopulator populator,
             string dbId,
             string collection,
             CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var realDb = RestApiExtensions.NullIfDefault(dbId);
                    var engine = registry.GetEngine(realDb);
                    engine.SetVectorSource(physical, null);
                    await populator.RefreshSubscriptionAsync(realDb, physical, ct);
                    return Results.NoContent();
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
            .WithSummary("Clear VectorSource configuration")
            .WithDescription("Removes the embedding configuration from the collection.");

        // GET /api/v1/{dbId}/{collection}/timeseries
        group.MapGet("/{dbId}/{collection}/timeseries",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var (isTs, retentionMs, ttlField) = engine.GetTimeSeriesConfig(physical);
                    return Results.Ok(new
                    {
                        isTimeSeries = isTs,
                        ttlFieldName = ttlField,
                        retentionMs
                    });
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
            .WithSummary("Get TimeSeries configuration")
            .WithDescription("Returns the TimeSeries configuration for the collection. isTimeSeries is false when the collection has not been configured as a TimeSeries.");

        // PUT /api/v1/{dbId}/{collection}/timeseries
        group.MapPut("/{dbId}/{collection}/timeseries",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection,
             ConfigureTimeSeriesDto req) =>
            {
                if (string.IsNullOrWhiteSpace(req.TtlFieldName))
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["ttlFieldName"] = ["ttlFieldName is required."]
                    });
                if (req.RetentionMs <= 0)
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["retentionMs"] = ["retentionMs must be a positive value."]
                    });

                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    engine.SetTimeSeries(physical, req.TtlFieldName.Trim(),
                        TimeSpan.FromMilliseconds(req.RetentionMs));
                    return Results.NoContent();
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, checkDb: true))
            .WithSummary("Configure TimeSeries")
            .WithDescription("Configures the collection as a TimeSeries with a TTL field and retention policy. This operation is irreversible.");

        // GET /api/v1/{dbId}/{collection}/schema
        group.MapGet("/{dbId}/{collection}/schema",
            (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var schemas = engine.GetSchemas(physical);
                    if (schemas.Count == 0)
                        return Results.Ok(new { hasSchema = false, title = (string?)null, version = (int?)null, versionCount = 0, fields = Array.Empty<object>() });

                    var latest = schemas[^1];
                    return Results.Ok(new
                    {
                        hasSchema = true,
                        title = latest.Title,
                        version = latest.Version,
                        versionCount = schemas.Count,
                        fields = latest.Fields.Select(f => new
                        {
                            name = f.Name,
                            type = f.Type.ToString(),
                            typeCode = (int)f.Type,
                            nullable = f.IsNullable
                        }).ToArray()
                    });
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
            .WithSummary("Get collection schema")
            .WithDescription("Returns the latest schema version for the collection. hasSchema is false when no schema has been defined.");

        // PUT /api/v1/{dbId}/{collection}/schema
        group.MapPut("/{dbId}/{collection}/schema",
            async (HttpContext ctx,
             EngineRegistry registry,
             string dbId,
             string collection,
             SetSchemaDto req,
             CancellationToken ct) =>
            {
                var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                try
                {
                    var physical = NamespaceResolver.Resolve(user, collection);
                    var engine = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId));
                    var schema = new BLite.Bson.BsonSchema
                    {
                        Title = string.IsNullOrWhiteSpace(req.Title) ? null : req.Title.Trim()
                    };
                    foreach (var f in req.Fields ?? [])
                    {
                        if (string.IsNullOrWhiteSpace(f.Name)) continue;
                        if (!Enum.TryParse<BLite.Bson.BsonType>(f.Type, ignoreCase: true, out var bsonType))
                            return Results.ValidationProblem(new Dictionary<string, string[]>
                            {
                                ["type"] = [$"Unknown BsonType '{f.Type}'."]
                            });
                        schema.Fields.Add(new BLite.Bson.BsonField
                        {
                            Name = f.Name.Trim(),
                            Type = bsonType,
                            IsNullable = f.Nullable
                        });
                    }
                    engine.SetSchema(physical, schema);
                    await engine.CommitAsync(ct);
                    return Results.NoContent();
                }
                catch (InvalidOperationException ex)
                {
                    return Results.Problem(
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, checkDb: true))
            .WithSummary("Set collection schema")
            .WithDescription("Appends a new schema version to the collection. Each call creates a new version; existing versions are retained.");
    }
}

/// <summary>
/// DTO for VectorSource configuration (REST API serialization).
/// </summary>
internal class VectorSourceConfigDto
{
    public string? Separator { get; set; }
    public List<VectorSourceFieldDto>? Fields { get; set; }
}

internal class VectorSourceFieldDto
{
    public string? Path { get; set; }
    public string? Prefix { get; set; }
    public string? Suffix { get; set; }
}

/// <summary>
/// DTO for configuring TimeSeries on a collection (REST API).
/// </summary>
internal class ConfigureTimeSeriesDto
{
    public string? TtlFieldName { get; set; }
    public long RetentionMs { get; set; }
}

/// <summary>
/// DTO for setting a schema on a collection (REST API).
/// </summary>
internal class SetSchemaDto
{
    public string? Title { get; set; }
    public List<SetSchemaFieldDto>? Fields { get; set; }
}

internal class SetSchemaFieldDto
{
    public string? Name { get; set; }
    public string Type { get; set; } = "String";  // BsonType name (e.g. "String", "Int32")
    public bool Nullable { get; set; } = true;
}
