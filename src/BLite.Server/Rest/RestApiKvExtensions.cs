// BLite.Server — REST API: Key-Value store endpoints
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1/{dbId}/kv.
// Values are Base64-encoded in JSON because KV payloads are arbitrary bytes.

using BLite.Server.Auth;

namespace BLite.Server.Rest;

internal static class RestApiKvExtensions
{
    internal static IEndpointRouteBuilder MapKv(this IEndpointRouteBuilder g)
    {
        // GET /api/v1/{dbId}/kv?prefix=...     → scan keys
        g.MapGet("/{dbId}/kv", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            string? prefix) =>
        {
            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv             = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var physicalPrefix = NamespaceResolver.Resolve(user, prefix ?? "");
                var keys = kv.ScanKeys(physicalPrefix)
                              .Where(k  => NamespaceResolver.BelongsTo(user, k))
                              .Select(k => NamespaceResolver.Strip(user, k))
                              .ToArray();
                return Results.Ok(new { keys });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, "*", checkDb: true))
        .WithSummary("Scan KV keys")
        .WithDescription("Returns all keys visible to the caller (optionally filtered by prefix). " +
                         "Namespace isolation is applied automatically.");

        // GET /api/v1/{dbId}/kv/{key}          → get value
        g.MapGet("/{dbId}/kv/{key}", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            string key) =>
        {
            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv    = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var value = kv.Get(NamespaceResolver.Resolve(user, key));
                if (value is null)
                    return Results.NotFound(new { key, error = "Key not found or has expired." });
                return Results.Ok(new { key, value = Convert.ToBase64String(value) });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, "*", checkDb: true))
        .WithSummary("Get KV entry")
        .WithDescription("Returns the value for a key as a Base64-encoded string. " +
                         "Returns 404 if the key does not exist or has expired.");

        // PUT /api/v1/{dbId}/kv/{key}           → set value
        g.MapPut("/{dbId}/kv/{key}", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            string key,
            KvSetDto req) =>
        {
            if (string.IsNullOrEmpty(req.Value))
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["value"] = ["value is required and must be a non-empty Base64 string."]
                });

            byte[] bytes;
            try { bytes = Convert.FromBase64String(req.Value); }
            catch
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["value"] = ["value must be a valid Base64-encoded string."]
                });
            }

            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv  = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var ttl = req.TtlMs is > 0 ? TimeSpan.FromMilliseconds(req.TtlMs.Value) : (TimeSpan?)null;
                kv.Set(NamespaceResolver.Resolve(user, key), bytes, ttl);
                return Results.NoContent();
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Write, "*", checkDb: true))
        .WithSummary("Set KV entry")
        .WithDescription("Stores a value for a key. " +
                         "The value must be Base64-encoded. " +
                         "ttlMs specifies the time-to-live in milliseconds (0 or omit for no expiry). " +
                         "Creates the entry if absent, overwrites if present.");

        // DELETE /api/v1/{dbId}/kv/{key}        → delete key
        g.MapDelete("/{dbId}/kv/{key}", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            string key) =>
        {
            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var ok = kv.Delete(NamespaceResolver.Resolve(user, key));
                return ok ? Results.NoContent() : Results.NotFound(new { key, error = "Key not found." });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Write, "*", checkDb: true))
        .WithSummary("Delete KV entry")
        .WithDescription("Removes a key from the store. Returns 404 if the key does not exist.");

        // PATCH /api/v1/{dbId}/kv/{key}         → refresh TTL
        g.MapPatch("/{dbId}/kv/{key}", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            string key,
            KvRefreshDto req) =>
        {
            if (req.TtlMs <= 0)
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["ttlMs"] = ["ttlMs must be a positive value."]
                });

            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var ok = kv.Refresh(NamespaceResolver.Resolve(user, key),
                                    TimeSpan.FromMilliseconds(req.TtlMs));
                return ok
                    ? Results.NoContent()
                    : Results.NotFound(new { key, error = "Key not found or has already expired." });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Write, "*", checkDb: true))
        .WithSummary("Refresh KV TTL")
        .WithDescription("Extends the time-to-live of an existing key without changing its value. " +
                         "ttlMs must be a positive number of milliseconds. Returns 404 if the key has already expired.");

        // POST /api/v1/{dbId}/kv/purge          → purge expired entries
        g.MapPost("/{dbId}/kv/purge", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId) =>
        {
            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv    = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var count = kv.PurgeExpired();
                return Results.Ok(new { purgedCount = count });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*", checkDb: true))
        .WithSummary("Purge expired KV entries")
        .WithDescription("Removes all soft-deleted and expired entries from disk pages and rebuilds the in-memory index. " +
                         "Returns the number of entries purged. Requires Admin permission.");

        // POST /api/v1/{dbId}/kv/batch          → batch operations
        g.MapPost("/{dbId}/kv/batch", (
            HttpContext ctx,
            EngineRegistry registry,
            string dbId,
            KvBatchDto req) =>
        {
            if (req.Operations is not { Count: > 0 })
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["operations"] = ["At least one operation is required."]
                });

            var user = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
            try
            {
                var kv    = registry.GetEngine(RestApiExtensions.NullIfDefault(dbId)).KvStore;
                var batch = kv.Batch();

                foreach (var op in req.Operations)
                {
                    if (string.IsNullOrEmpty(op.Key))
                        return Results.ValidationProblem(new Dictionary<string, string[]>
                        {
                            ["operations[].key"] = ["Each operation must have a non-empty key."]
                        });

                    var physicalKey = NamespaceResolver.Resolve(user, op.Key);

                    if (op.IsDelete)
                    {
                        batch.Delete(physicalKey);
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(op.Value))
                            return Results.ValidationProblem(new Dictionary<string, string[]>
                            {
                                [$"operations[key={op.Key}].value"] = ["value is required for Set operations."]
                            });

                        byte[] bytes;
                        try { bytes = Convert.FromBase64String(op.Value); }
                        catch
                        {
                            return Results.ValidationProblem(new Dictionary<string, string[]>
                            {
                                [$"operations[key={op.Key}].value"] = ["value must be a valid Base64-encoded string."]
                            });
                        }

                        var ttl = op.TtlMs is > 0 ? TimeSpan.FromMilliseconds(op.TtlMs.Value) : (TimeSpan?)null;
                        batch.Set(physicalKey, bytes, ttl);
                    }
                }

                return Results.Ok(new { affectedCount = batch.Execute() });
            }
            catch (Exception ex)
            {
                return Results.Problem(title: "Internal Error", detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .AddEndpointFilter(new PermissionFilter(BLiteOperation.Write, "*", checkDb: true))
        .WithSummary("Batch KV operations")
        .WithDescription("Executes multiple Set and Delete operations atomically. " +
                         "All operations execute under a single write-lock acquisition. " +
                         "Values must be Base64-encoded.");

        return g;
    }
}

// ─── DTOs ─────────────────────────────────────────────────────────────────────

/// <summary>Request body for setting a KV entry.</summary>
internal sealed class KvSetDto
{
    /// <summary>Base64-encoded value payload.</summary>
    public string Value { get; set; } = "";

    /// <summary>Time-to-live in milliseconds. 0 or omitted means no expiry.</summary>
    public long? TtlMs { get; set; }
}

/// <summary>Request body for refreshing the TTL of a KV entry.</summary>
internal sealed class KvRefreshDto
{
    /// <summary>New time-to-live in milliseconds. Must be positive.</summary>
    public long TtlMs { get; set; }
}

/// <summary>Request body for a KV batch operation.</summary>
internal sealed class KvBatchDto
{
    public List<KvBatchOpDto>? Operations { get; set; }
}

/// <summary>A single operation in a KV batch.</summary>
internal sealed class KvBatchOpDto
{
    public string Key { get; set; } = "";

    /// <summary>Base64-encoded value. Required when IsDelete is false.</summary>
    public string? Value { get; set; }

    /// <summary>Time-to-live in milliseconds. 0 or omitted means no expiry.</summary>
    public long? TtlMs { get; set; }

    /// <summary>When true, the operation deletes the key (Value is ignored).</summary>
    public bool IsDelete { get; set; }
}
