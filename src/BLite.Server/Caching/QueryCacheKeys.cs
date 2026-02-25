// BLite.Server — Cache key builders
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Security.Cryptography;
using System.Text;

namespace BLite.Server.Caching;

/// <summary>
/// Builds cache keys for every query variant.
/// All keys include the database id to prevent cross-tenant cache hits.
/// Physical collection names (post-namespace-resolution) are always used.
/// </summary>
public static class QueryCacheKeys
{
    // REST — GET /documents
    public static string DocumentsList(string? dbId, string collection, int skip, int limit)
        => $"docs:{N(dbId)}:{collection}:{skip}:{limit}";

    // REST — POST /query  (body is the raw JSON request body)
    public static string BlqlPost(string? dbId, string collection, string body)
        => $"blql-post:{N(dbId)}:{collection}:{Hash(body)}";

    // REST — GET /query  (all query-string params concatenated)
    public static string BlqlGet(string? dbId, string collection,
        string? filter, string? sort, int skip, int limit)
        => $"blql-get:{N(dbId)}:{collection}:{Hash($"{filter}|{sort}|{skip}|{limit}")}";

    // REST — POST /query/count
    public static string BlqlCount(string? dbId, string collection, string body)
        => $"blql-count:{N(dbId)}:{collection}:{Hash(body)}";

    // gRPC — Query (bytes are the serialized QueryDescriptor)
    public static string GrpcQuery(string? dbId, string collection, byte[] descriptorBytes)
        => $"grpc:{N(dbId)}:{collection}:{HashBytes(descriptorBytes)}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string N(string? dbId)
        => string.IsNullOrWhiteSpace(dbId) ? "_sys" : dbId.Trim().ToLowerInvariant();

    private static string Hash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input ?? string.Empty));
        return Convert.ToHexString(bytes)[..16]; // first 64 bits is enough for a cache key
    }

    private static string HashBytes(byte[] input)
    {
        var bytes = SHA256.HashData(input);
        return Convert.ToHexString(bytes)[..16];
    }
}
