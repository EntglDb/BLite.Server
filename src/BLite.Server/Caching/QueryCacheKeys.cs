// BLite.Server — Cache key builders
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.IO.Hashing;
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
    // Accepts ReadOnlySpan<byte> so the caller can pass ByteString.Span directly,
    // avoiding the ByteString.ToByteArray() allocation on every request.
    public static string GrpcQuery(string? dbId, string collection, ReadOnlySpan<byte> descriptorBytes)
        => $"grpc:{N(dbId)}:{collection}:{XxHash64.HashToUInt64(descriptorBytes):x16}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string N(string? dbId)
        => string.IsNullOrWhiteSpace(dbId) ? "_sys" : dbId.Trim().ToLowerInvariant();

    private static string Hash(string input)
    {
        // XxHash64 is ~5–10× faster than SHA-256 and sufficient for a cache key.
        var maxLen = Encoding.UTF8.GetMaxByteCount(input?.Length ?? 0);
        if (maxLen <= 512)
        {
            Span<byte> buf = stackalloc byte[maxLen];
            int len = Encoding.UTF8.GetBytes(input ?? string.Empty, buf);
            return XxHash64.HashToUInt64(buf[..len]).ToString("x16");
        }
        var heapBuf = Encoding.UTF8.GetBytes(input ?? string.Empty);
        return XxHash64.HashToUInt64(heapBuf).ToString("x16");
    }
}
