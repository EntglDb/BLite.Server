// BLite.Server — Metrics REST endpoint
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// GET /api/v1/metrics — returns BLiteMetrics aggregated across all active engines.
// GET /api/v1/{dbId}/metrics — returns metrics for a specific tenant engine.

using BLite.Core;
using BLite.Core.Audit;
using BLite.Server.Auth;

namespace BLite.Server.Rest;

internal static class RestApiMetricsExtensions
{
    internal static void MapMetrics(this RouteGroupBuilder g)
    {
        var group = g.MapGroup("").WithTags("Metrics")
                     .AddEndpointFilter(new PermissionFilter(BLiteOperation.Admin, "*"));

        // ── GET /api/v1/metrics ────────────────────────────────────────────────
        // Returns aggregated metrics across all active engines.
        group.MapGet("/metrics",
            (EngineRegistry registry) =>
            {
                var all = registry.GetAllActiveEngines()
                    .Select(entry => new
                    {
                        DatabaseId = entry.DbId ?? "default",
                        Metrics    = MapMetrics(entry.Engine.AuditMetrics)
                    })
                    .ToList();

                return Results.Ok(all);
            })
            .WithSummary("List metrics for all databases")
            .WithDescription(
                "Returns BLiteMetrics counters (inserts, queries, commits, cache hit rate, " +
                "average latencies) for each active engine. Metrics are null when the audit " +
                "subsystem is not configured for that engine.");

        // ── GET /api/v1/{dbId}/metrics ─────────────────────────────────────────
        group.MapGet("/{dbId}/metrics",
            (EngineRegistry registry, string dbId) =>
            {
                var realId = RestApiExtensions.NullIfDefault(dbId);
                BLiteEngine engine;
                try   { engine = registry.GetEngine(realId); }
                catch { return BLiteErrors.DatabaseNotFound(dbId).ToResult(); }

                return Results.Ok(new
                {
                    DatabaseId = string.IsNullOrEmpty(realId) ? "default" : realId,
                    Metrics    = MapMetrics(engine.AuditMetrics)
                });
            })
            .WithSummary("Get metrics for a specific database")
            .WithDescription(
                "Returns BLiteMetrics counters for the specified tenant engine.");
    }

    private static object? MapMetrics(BLiteMetrics? m)
    {
        if (m is null) return null;
        return new
        {
            m.TotalInserts,
            m.TotalQueries,
            m.TotalQueriesIndexScan,
            m.TotalQueriesFullScan,
            m.TotalCommits,
            AvgInsertMs = Math.Round(m.AvgInsertMs, 3),
            AvgQueryMs  = Math.Round(m.AvgQueryMs, 3)
        };
    }
}
