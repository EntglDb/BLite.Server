// BLite.Client — RemoteDocumentCollection<TId, T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IDocumentCollection<TId,T> wrapper around RemoteCollection<TId,T>.
// Sync operations and local-engine-only features throw NotSupportedException.

using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Query;
using System.Linq.Expressions;

namespace BLite.Client.Collections;

/// <summary>
/// Remote implementation of <see cref="IDocumentCollection{TId,T}"/> that
/// delegates typed CRUD operations to a <see cref="RemoteCollection{TId,T}"/>
/// over gRPC.
///
/// <para>
/// Raw-scan operations (<c>Scan</c>, <c>ParallelScan</c>) are not supported
/// over gRPC and throw <see cref="NotSupportedException"/> at runtime.
/// <c>Watch</c> / CDC is fully supported via <c>DynamicService.Watch</c>.
/// </para>
/// </summary>
public sealed class RemoteDocumentCollection<TId, T> : IDocumentCollection<TId, T>
    where T : class
{
    private readonly RemoteCollection<TId, T> _inner;

    internal RemoteDocumentCollection(RemoteCollection<TId, T> inner) => _inner = inner;

    // ── Metadata ──────────────────────────────────────────────────────────────

    public SchemaVersion? CurrentSchemaVersion => null;

    // ── Insert ────────────────────────────────────────────────────────────────

    public Task<TId> InsertAsync(T entity, CancellationToken ct = default) =>
        _inner.InsertAsync(entity, null, ct);

    public async Task<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) =>
        (await _inner.InsertBulkAsync(entities, null, ct)).ToList();

    // ── Read ──────────────────────────────────────────────────────────────────

    public async ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default) =>
        await _inner.FindByIdAsync(id, ct);

    public IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default) =>
        _inner.FindAllAsync(ct);

    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        _inner.AsQueryable().Where(predicate).AsAsyncEnumerable();

    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        _inner.AsQueryable().FirstOrDefaultAsync(predicate, ct);

    public IBLiteQueryable<T> AsQueryable() => _inner.AsQueryable();

    // ── Update ────────────────────────────────────────────────────────────────

    public Task<bool> UpdateAsync(T entity, CancellationToken ct = default) =>
        _inner.UpdateAsync(entity, null, ct);

    public Task<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) =>
        _inner.UpdateBulkAsync(entities, null, ct);

    // ── Delete ────────────────────────────────────────────────────────────────

    public Task<bool> DeleteAsync(TId id, CancellationToken ct = default) =>
        _inner.DeleteAsync(id, null, ct);

    public Task<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default) =>
        _inner.DeleteBulkAsync(ids, null, ct);

    // ── Index management ──────────────────────────────────────────────────────

    public async Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false,
        CancellationToken ct = default)
    {
        var paths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var field = string.Join(".", paths);
        var indexName = name ?? $"idx_{string.Join("_", paths)}";
        await _inner.CreateIndexAsync(field, indexName, unique, ct);
        return new RemoteCollectionIndex<TId, T>(indexName, paths, IndexType.BTree, unique, QueryIndexAsync);
    }

    public async Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, int dimensions,
        VectorMetric metric = VectorMetric.Cosine, string? name = null,
        CancellationToken ct = default)
    {
        var paths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var field = string.Join(".", paths);
        var indexName = name ?? $"idx_{string.Join("_", paths)}";
        await _inner.CreateVectorIndexAsync(field, dimensions, metric.ToString(), indexName, ct);
        return new RemoteCollectionIndex<TId, T>(indexName, paths, IndexType.Vector, false, QueryIndexAsync, dimensions, metric, VectorSearchAsync);
    }

    public async Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false,
        CancellationToken ct = default)
    {
        var paths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        var indexName = name ?? $"idx_{string.Join("_", paths)}";
        var existing = await ListIndexesAsync(ct);
        var found = existing.FirstOrDefault(i => i.Name == indexName);
        if (found != null)
        {
            var vsDelegate = found.Type == IndexType.Vector ? (Func<string, float[], int, int, CancellationToken, IAsyncEnumerable<T>>?)VectorSearchAsync : null;
            return new RemoteCollectionIndex<TId, T>(found.Name, found.PropertyPaths, found.Type, found.IsUnique, QueryIndexAsync, vectorSearchDelegate: vsDelegate);
        }
        return await CreateIndexAsync(keySelector, name, unique, ct);
    }

    public Task<bool> DropIndexAsync(string name, CancellationToken ct = default) =>
        _inner.DropIndexAsync(name, ct);

    public IEnumerable<CollectionIndexInfo> GetIndexes() =>
        ListIndexesAsync().GetAwaiter().GetResult();

    public async Task<ICollectionIndex<TId, T>?> GetIndexAsync(string name)
    {
        var all = await ListIndexesAsync();
        var found = all.FirstOrDefault(i => i.Name == name);
        if (found is null) return null;
        var vsDelegate = found.Type == IndexType.Vector ? (Func<string, float[], int, int, CancellationToken, IAsyncEnumerable<T>>?)VectorSearchAsync : null;
        return new RemoteCollectionIndex<TId, T>(found.Name, found.PropertyPaths, found.Type, found.IsUnique, QueryIndexAsync, vectorSearchDelegate: vsDelegate);
    }

    public IEnumerable<T> QueryIndex(string indexName, object? minKey, object? maxKey, bool ascending = true)
    {
        async Task<List<T>> CollectAsync()
        {
            var list = new List<T>();
            await foreach (var item in QueryIndexAsync(indexName, minKey, maxKey, ascending))
                list.Add(item);
            return list;
        }
        return CollectAsync().GetAwaiter().GetResult();
    }

    public async IAsyncEnumerable<T> QueryIndexAsync(
        string indexName, object? minKey, object? maxKey, bool ascending = true,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _inner.QueryIndexAsync(indexName, minKey, maxKey, ascending, 0, 0, ct))
            yield return item;
    }

    public async IAsyncEnumerable<T> VectorSearchAsync(
        string indexName, float[] query, int k, int efSearch = 100,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _inner.VectorSearchAsync(indexName, query, k, efSearch, ct))
            yield return item;
    }

    /// <summary>
    /// Returns all indexes on this collection.
    /// </summary>
    public Task<IReadOnlyList<CollectionIndexInfo>> ListIndexesAsync(
        CancellationToken ct = default) =>
        _inner.ListIndexesAsync(ct);

    // ── Scan (not supported on remote) ───────────────────────────────────────

    public IEnumerable<T> Scan(BsonReaderPredicate predicate) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    public IAsyncEnumerable<T> ScanAsync(BsonReaderPredicate predicate, CancellationToken ct = default) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    public IEnumerable<TResult> Scan<TResult>(BsonReaderProjector<TResult> projector) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    public IAsyncEnumerable<TResult> ScanAsync<TResult>(
        BsonReaderProjector<TResult> projector, CancellationToken ct = default) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    public IEnumerable<T> ParallelScan(BsonReaderPredicate predicate, int degreeOfParallelism = -1) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    public IAsyncEnumerable<T> ParallelScanAsync(
        BsonReaderPredicate predicate, int degreeOfParallelism = -1,
        CancellationToken ct = default) =>
        throw new NotSupportedException("Scan is not supported on remote collections.");

    // ── TimeSeries (not supported on remote) ─────────────────────────────────

    public Task ForcePruneAsync() =>
        _inner.ForcePruneAsync(default);

    // ── Change Data Capture ───────────────────────────────────────────────────

    public IObservable<ChangeStreamEvent<TId, T>> Watch(bool capturePayload = false) =>
        _inner.WatchObservable(capturePayload);
}
