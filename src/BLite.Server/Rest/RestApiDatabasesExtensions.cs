// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks are enforced by PermissionFilter before handlers run.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using System.IO.Compression;
using BLite.Server.Auth;
using BLite.Server.Caching;

namespace BLite.Server.Rest;

/// <summary>
/// Provides extension methods for configuring database-related routes in a REST API.
/// </summary>
/// <remarks>This class contains methods to map routes for listing, provisioning, deleting, and backing up
/// databases. It is intended for use in setting up API endpoints that interact with tenant databases, ensuring proper
/// authorization and handling of requests.</remarks>
internal static class RestApiDatabasesExtensions
{
    // ────────────────────────────────────────────────────────────────────────────
    // Databases
    // ────────────────────────────────────────────────────────────────────────────

    internal static void MapDatabases(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Databases")
                     .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*"));

        // GET /api/v1/databases — list all tenant databases
        group.MapGet("/databases",
            (EngineRegistry registry) =>
            {
                var tenants = registry.ListTenants()
                    .Select(t => new { t.DatabaseId, t.IsActive })
                    .ToList();
                return Results.Ok(tenants);
            })
            .WithSummary("List databases")
            .WithDescription("Returns all provisioned tenant databases with their active status.");

        // POST /api/v1/databases — provision a new database
        //   Body: { "databaseId": "acme" }
        group.MapPost("/databases",
            async (EngineRegistry registry,
                   ProvisionDatabaseRequest req,
                   CancellationToken ct) =>
            {
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
            })
            .WithSummary("Provision a database")
            .WithDescription("Creates a new tenant database with the given identifier. Returns 409 if a database with that id already exists.");

        // DELETE /api/v1/databases/{dbId}?deleteFiles=false
        group.MapDelete("/databases/{dbId}",
            async (EngineRegistry registry,
                   QueryCacheService cache,
                   string dbId,
                   bool deleteFiles,
                   CancellationToken ct) =>
            {
                try
                {
                    var realId = RestApiExtensions.NullIfDefault(dbId);
                    await registry.DeprovisionAsync(dbId, deleteFiles, ct);
                    if (cache.Enabled)
                        cache.InvalidateDatabase(realId);
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
            .WithSummary("Deprovision a database")
            .WithDescription("Removes a tenant database from the registry. Pass `deleteFiles=true` to also delete the underlying data files from disk.");

        // GET /api/v1/databases/{dbId}/backup
        //   Use dbId "_system" for the system (default) database.
        group.MapGet("/databases/{dbId}/backup",
            async (EngineRegistry registry,
                   string dbId,
                   CancellationToken ct) =>
            {
                var realId = dbId.Equals("_system", StringComparison.OrdinalIgnoreCase) ? null : dbId;
                var label = realId ?? "system";
                var stamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
                var zipName = $"blite-backup-{label}-{stamp}.zip";
                var tempDb = Path.Combine(Path.GetTempPath(), $"blite-bkp-{Guid.NewGuid():N}.db");
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
                        title: "Not Found",
                        detail: ex.Message,
                        statusCode: StatusCodes.Status404NotFound);
                }
                finally
                {
                    if (File.Exists(tempDb)) File.Delete(tempDb);
                }
            })
            .WithSummary("Download a database backup")
            .WithDescription("Performs a hot backup of the specified database and streams the result as a ZIP file. Use `_system` as `dbId` to target the default (system) database.");
    }
}
