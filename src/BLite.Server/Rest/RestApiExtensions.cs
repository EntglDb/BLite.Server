// BLite.Server — REST API minimal-endpoint mappings
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// All endpoints live under /api/v1 and are authenticated via RestAuthFilter.
// Permission checks use AuthorizationService + principal stored in HttpContext.Items.
// Errors are modelled with ErrorOr and mapped to RFC-9457 ProblemDetails.

using System.IO.Compression;
using System.Text.Json;
using BLite.Bson;

namespace BLite.Server.Rest;

/// <summary>Extension method that registers all BLite REST endpoints.</summary>
public static class RestApiExtensions
{
    // ── Entry point ────────────────────────────────────────────────────────────

    public static void MapBliteRestApi(this WebApplication app)
    {
        var api = app.MapGroup("/api/v1")
                     .AddEndpointFilter<RestAuthFilter>();

        api.MapDatabases();
        api.MapCollections();
        api.MapDocuments();
        api.MapBlql();
        api.MapUsers();
    }

    // ── Utility helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Routes use "default" as a sentinel for the system database (empty string key).
    /// </summary>
    internal static string? NullIfDefault(string dbId) =>
        string.IsNullOrEmpty(dbId) || dbId.Equals("default", StringComparison.OrdinalIgnoreCase)
            ? null
            : dbId;

    /// <summary>
    /// Best-effort parse of a URL segment into a <see cref="BsonId"/>.
    /// Order: Guid → Int64 → ObjectId (24 hex) → String.
    /// </summary>
    internal static BsonId ParseId(string s)
    {
        if (Guid.TryParse(s, out var guid)) return new BsonId(guid);
        if (long.TryParse(s, out var lng)) return new BsonId(lng);

        if (s.Length == 24 && IsAllHex(s))
        {
            try { return new BsonId(new ObjectId(Convert.FromHexString(s))); }
            catch { /* fall through to string */ }
        }

        return new BsonId(s);
    }

    internal static bool IsAllHex(string s)
    {
        foreach (var c in s)
            if (!Uri.IsHexDigit(c)) return false;
        return true;
    }

    /// <summary>Collects every property name from a JSON tree for key pre-registration.</summary>
    internal static IEnumerable<string> CollectJsonKeys(string json)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var jdoc = JsonDocument.Parse(json);
            CollectElementKeys(jdoc.RootElement, keys);
        }
        catch { /* invalid JSON — let the converter surface the real error */ }
        return keys;
    }

    internal static void CollectElementKeys(JsonElement el, HashSet<string> keys)
    {
        if (el.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in el.EnumerateObject())
            {
                if (prop.Name != "_id") keys.Add(prop.Name);
                CollectElementKeys(prop.Value, keys);
            }
        }
        else if (el.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in el.EnumerateArray())
            {
                CollectElementKeys(item, keys);
            }
        }
    }

    internal static async Task ZipAddFileAsync(
        ZipArchive zip,
        string filePath,
        string entryName,
        CancellationToken ct)
    {
        var entry = zip.CreateEntry(entryName, CompressionLevel.Fastest);
        await using var entryStream = entry.Open();
        await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        await fs.CopyToAsync(entryStream, ct);
    }
}

// ── Request DTOs ───────────────────────────────────────────────────────────────

public sealed record ProvisionDatabaseRequest(string DatabaseId);
public sealed record CreateCollectionRequest(string Collection);
public sealed record CreateUserRequest(string Username, string? Namespace, string? DatabaseId);
public sealed record PermissionRequest(string Collection, int Ops);