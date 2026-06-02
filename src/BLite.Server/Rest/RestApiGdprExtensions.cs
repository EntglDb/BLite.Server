// BLite.Server — GDPR REST endpoints
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Exposes BLite 5 GDPR primitives over REST:
//   GET  /{dbId}/gdpr/inspect              — Art. 30 database inspection report
//   POST /{dbId}/{collection}/gdpr/export-subject — Art. 15/20 subject data export

using BLite.Core;
using BLite.Core.GDPR;
using BLite.Server.Auth;

namespace BLite.Server.Rest;

internal static class RestApiGdprExtensions
{
    internal static void MapGdpr(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("GDPR");

        // ── GET /{dbId}/gdpr/inspect ──────────────────────────────────────────
        // Requires Admin on "*"; returns the Art. 30 DatabaseInspectionReport.
        group.MapGet("/{dbId}/gdpr/inspect",
            (EngineRegistry registry,
             HttpContext ctx,
             string dbId) =>
            {
                var user   = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var realId = RestApiExtensions.NullIfDefault(dbId);
                BLiteEngine engine;
                try   { engine = registry.GetEngine(realId); }
                catch { return BLiteErrors.DatabaseNotFound(dbId).ToResult(); }

                var report = GdprEngineExtensions.InspectDatabase(engine);

                return Results.Ok(new
                {
                    report.DatabasePath,
                    report.IsEncrypted,
                    report.IsAuditEnabled,
                    report.IsMultiFileMode,
                    Collections = report.Collections.Select(c => new
                    {
                        c.Name,
                        c.DocumentCount,
                        StorageSizeKb     = c.StorageSizeBytes / 1024,
                        c.PersonalDataFields,
                        RetentionPolicy   = c.RetentionPolicy is null ? null : new
                        {
                            MaxAge          = c.RetentionPolicy.MaxAge?.ToString(),
                            c.RetentionPolicy.MaxDocumentCount,
                            Triggers        = c.RetentionPolicy.Triggers.ToString()
                        }
                    }).ToList()
                });
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*", checkDb: true))
            .WithSummary("Inspect database (Art. 30)")
            .WithDescription(
                "Returns a GDPR Art. 30 record-of-processing snapshot: encryption status, " +
                "audit configuration, layout, and per-collection personal-data fields and retention policy.");

        // ── POST /{dbId}/{collection}/gdpr/export-subject ────────────────────
        // Requires Query on the collection; streams a JSON subject-data report.
        group.MapPost("/{dbId}/{collection}/gdpr/export-subject",
            async (EngineRegistry registry,
                   HttpContext ctx,
                   string dbId,
                   string collection,
                   SubjectExportRequest req,
                   CancellationToken ct) =>
            {
                var user   = (BLiteUser)ctx.Items[nameof(BLiteUser)]!;
                var realId = RestApiExtensions.NullIfDefault(dbId);
                BLiteEngine engine;
                try   { engine = registry.GetEngine(realId); }
                catch { return BLiteErrors.DatabaseNotFound(dbId).ToResult(); }

                var physical = NamespaceResolver.Resolve(user, collection);

                if (string.IsNullOrWhiteSpace(req.FieldName) ||
                    string.IsNullOrWhiteSpace(req.FieldValue))
                    return Results.ValidationProblem(new Dictionary<string, string[]>
                    {
                        ["fieldName"]  = ["fieldName is required."],
                        ["fieldValue"] = ["fieldValue is required."]
                    });

                var query = new SubjectQuery
                {
                    FieldName   = req.FieldName,
                    FieldValue  = BLite.Bson.BsonValue.FromString(req.FieldValue),
                    Collections = [physical],
                    Format      = SubjectExportFormat.Json
                };

                await using var report = await GdprEngineExtensions.ExportSubjectDataAsync(engine, query, ct);

                var ms = new MemoryStream();
                await report.ExportAsJsonAsync(ms, ct);
                ms.Position = 0;

                return Results.File(
                    ms,
                    contentType:  "application/json",
                    fileDownloadName: $"subject-export-{req.FieldValue}-{DateTime.UtcNow:yyyyMMdd}.json");
            })
            .AddEndpointFilter(new PermissionFilter(BLiteOperation.Query, null, checkDb: true))
            .WithSummary("Export subject data (Art. 15/20)")
            .WithDescription(
                "Produces a portable subject-data report (JSON) for a given field/value pair, " +
                "satisfying GDPR Art. 15 (right of access) and Art. 20 (data portability).");
    }
}

// ── Request DTOs ──────────────────────────────────────────────────────────────

/// <summary>Request body for the subject-export endpoint.</summary>
public sealed record SubjectExportRequest(string FieldName, string FieldValue);
