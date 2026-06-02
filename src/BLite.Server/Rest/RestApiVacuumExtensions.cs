// BLite.Server — Vacuum REST endpoint
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// POST /api/v1/{dbId}/vacuum — compact a tenant database and reclaim free space.

using BLite.Core;
using BLite.Server.Auth;

namespace BLite.Server.Rest;

internal static class RestApiVacuumExtensions
{
    internal static void MapVacuum(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Maintenance")
                     .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*"));

        // ── POST /api/v1/{dbId}/vacuum ─────────────────────────────────────────
        group.MapPost("/{dbId}/vacuum",
            async (EngineRegistry registry, string dbId, CancellationToken ct) =>
            {
                var realId = RestApiExtensions.NullIfDefault(dbId);
                BLiteEngine engine;
                try   { engine = registry.GetEngine(realId); }
                catch { return BLiteErrors.DatabaseNotFound(dbId).ToResult(); }

                await engine.VacuumAsync(ct: ct);

                return Results.Ok(new
                {
                    DatabaseId = string.IsNullOrEmpty(realId) ? "default" : realId,
                    Message    = "Vacuum completed. Free space has been reclaimed."
                });
            })
            .WithSummary("Run VACUUM on a database")
            .WithDescription(
                "Compacts the specified tenant database, reclaiming pages freed by deletes. " +
                "This is a blocking operation — run during a maintenance window. " +
                "Requires Admin permission.");
    }
}
