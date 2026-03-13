// BLite.Client — RemoteCollectionIndex<TId, T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// ICollectionIndex<TId,T> backed by remote index metadata.
// Query/QueryAsync delegate to the owning collection's QueryIndexAsync so they
// start working automatically once the QueryIndex RPC is implemented.

using BLite.Core.Indexing;
using System.Runtime.CompilerServices;

namespace BLite.Client.Collections;

/// <summary>
/// Remote implementation of <see cref="ICollectionIndex{TId,T}"/>.
/// Carries index metadata returned by the server.
/// <para>
/// <see cref="Query"/> and <see cref="QueryAsync"/> delegate to the owning
/// collection's <c>QueryIndexAsync</c>. <see cref="VectorSearch"/> throws
/// <see cref="NotSupportedException"/> until a dedicated RPC is available.
/// </para>
/// </summary>
internal sealed class RemoteCollectionIndex<TId, T> : ICollectionIndex<TId, T>
    where T : class
{
    private readonly Func<string, object?, object?, bool, CancellationToken, IAsyncEnumerable<T>> _queryDelegate;
    private readonly Func<string, float[], int, int, CancellationToken, IAsyncEnumerable<T>>? _vectorSearchDelegate;

    public string Name { get; }
    public string[] PropertyPaths { get; }
    public IndexType Type { get; }
    public bool IsUnique { get; }
    public int Dimensions { get; }
    public VectorMetric Metric { get; }

    internal RemoteCollectionIndex(
        string name,
        string[] propertyPaths,
        IndexType type,
        bool isUnique,
        Func<string, object?, object?, bool, CancellationToken, IAsyncEnumerable<T>> queryDelegate,
        int dimensions = 0,
        VectorMetric metric = VectorMetric.Cosine,
        Func<string, float[], int, int, CancellationToken, IAsyncEnumerable<T>>? vectorSearchDelegate = null)
    {
        Name                  = name;
        PropertyPaths         = propertyPaths;
        Type                  = type;
        IsUnique              = isUnique;
        Dimensions            = dimensions;
        Metric                = metric;
        _queryDelegate        = queryDelegate;
        _vectorSearchDelegate = vectorSearchDelegate;
    }

    public IEnumerable<T> Query(
        object? minKey = null, object? maxKey = null, bool ascending = true) =>
        throw new NotSupportedException("Use QueryAsync for remote collections.");

    public async IAsyncEnumerable<T> QueryAsync(
        object? minKey = null, object? maxKey = null, bool ascending = true,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _queryDelegate(Name, minKey, maxKey, ascending, ct)
                           .WithCancellation(ct))
            yield return item;
    }

    public IEnumerable<VectorSearchResult> VectorSearch(
        float[] query, int k, int efSearch = 100) =>
        throw new NotSupportedException("Use VectorSearchAsync for remote collections.");

    public async IAsyncEnumerable<T> VectorSearchAsync(
        float[] query, int k, int efSearch = 100,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_vectorSearchDelegate is null)
            throw new NotSupportedException("VectorSearch is only supported on vector indexes.");
        await foreach (var item in _vectorSearchDelegate(Name, query, k, efSearch, ct)
                           .WithCancellation(ct))
            yield return item;
    }
}
