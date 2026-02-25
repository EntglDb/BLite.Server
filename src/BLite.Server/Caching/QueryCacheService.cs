// BLite.Server — Query result cache
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Registered as Singleton. Wraps IMemoryCache with per-(dbId, collection)
// CancellationChangeToken-based invalidation.

using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace BLite.Server.Caching;

/// <summary>
/// Thread-safe query result cache with collection-scoped write invalidation.
/// </summary>
public sealed class QueryCacheService(
    IMemoryCache cache,
    IOptions<QueryCacheOptions> opts)
{
    private readonly QueryCacheOptions _opts = opts.Value;

    // One CTS per canonical (dbId, collection) key.
    // Cancelling it expires all cache entries tagged with that collection.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();

    public bool Enabled => _opts.Enabled;

    // ── Read ──────────────────────────────────────────────────────────────────

    public bool TryGet<T>(string key, out T? value) =>
        cache.TryGetValue(key, out value);

    // ── Write ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Stores <paramref name="value"/> in the cache, linked to the given collection
    /// so it is evicted the next time any write targets that collection.
    /// </summary>
    public void Set<T>(string key, T value, string? dbId, string collection)
    {
        var collKey = CollectionKey(dbId, collection);
        var cts     = _tokens.GetOrAdd(collKey, _ => new CancellationTokenSource());

        var entry = new MemoryCacheEntryOptions()
            .SetSlidingExpiration(TimeSpan.FromSeconds(_opts.SlidingExpirationSeconds))
            .SetAbsoluteExpiration(TimeSpan.FromSeconds(_opts.AbsoluteExpirationSeconds))
            .AddExpirationToken(new CancellationChangeToken(cts.Token));

        if (_opts.MaxSizeBytes > 0)
            entry.SetSize(EstimateSize(value));

        cache.Set(key, value, entry);
    }

    // ── Invalidation ─────────────────────────────────────────────────────────

    /// <summary>Evicts all cached results for one (dbId, collection) pair.</summary>
    public void Invalidate(string? dbId, string collection)
    {
        var key = CollectionKey(dbId, collection);
        if (_tokens.TryRemove(key, out var cts))
            cts.Cancel();
    }

    /// <summary>Evicts all cached results for every collection in a database.</summary>
    public void InvalidateDatabase(string? dbId)
    {
        var prefix = $"{N(dbId)}:";
        foreach (var key in _tokens.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
            if (_tokens.TryRemove(key, out var cts))
                cts.Cancel();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string CollectionKey(string? dbId, string collection)
        => $"{N(dbId)}:{collection}";

    private static string N(string? dbId)
        => string.IsNullOrWhiteSpace(dbId) ? "_sys" : dbId.Trim().ToLowerInvariant();

    private static long EstimateSize<T>(T value) => value switch
    {
        string s                                   => (long)s.Length * 2,
        List<byte[]> payloads                      => payloads.Sum(b => (long)b.Length),
        System.Collections.ICollection c           => (long)c.Count * 512,
        _                                          => EstimateFallback(value)
    };

    private static long EstimateFallback<T>(T value)
    {
        // Anonymous REST result objects { count, skip, limit, documents }.
        // Reflect on the 'documents' property to get a better size estimate.
        var prop = value?.GetType().GetProperty("documents");
        if (prop?.GetValue(value) is System.Collections.ICollection docs)
            return (long)docs.Count * 512;
        return 1024;
    }
}
