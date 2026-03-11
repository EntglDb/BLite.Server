// BLite.Client — RemoteDocumentCollection<TId, T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IDocumentCollection<TId,T> wrapper around RemoteCollection<TId,T>.
// Sync operations and local-engine-only features throw NotSupportedException.

using BLite.Bson;
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
/// Sync operations and local-engine-only features (indexes, scans, ForcePrune)
/// throw <see cref="NotSupportedException"/>. Use the <c>Async</c> overloads.
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

    public TId Insert(T entity) =>
        throw new NotSupportedException("Use InsertAsync for remote collections.");

    public Task<TId> InsertAsync(T entity, CancellationToken ct = default) =>
        _inner.InsertAsync(entity, null, ct);

    public List<TId> InsertBulk(IEnumerable<T> entities) =>
        throw new NotSupportedException("Use InsertBulkAsync for remote collections.");

    public async Task<List<TId>> InsertBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) =>
        (await _inner.InsertBulkAsync(entities, null, ct)).ToList();

    // ── Read ──────────────────────────────────────────────────────────────────

    public T? FindById(TId id) =>
        throw new NotSupportedException("Use FindByIdAsync for remote collections.");

    public async ValueTask<T?> FindByIdAsync(TId id, CancellationToken ct = default) =>
        await _inner.FindByIdAsync(id, ct);

    public IAsyncEnumerable<T> FindAllAsync(CancellationToken ct = default) =>
        _inner.FindAllAsync(ct);

    public IAsyncEnumerable<T> FindAsync(Func<T, bool> predicate, CancellationToken ct = default) =>
        _inner.FindAsync(predicate, ct);

    public IBLiteQueryable<T> AsQueryable() => _inner.AsQueryable();

    // ── Update ────────────────────────────────────────────────────────────────

    public bool Update(T entity) =>
        throw new NotSupportedException("Use UpdateAsync for remote collections.");

    public Task<bool> UpdateAsync(T entity, CancellationToken ct = default) =>
        _inner.UpdateAsync(entity, null, ct);

    public int UpdateBulk(IEnumerable<T> entities) =>
        throw new NotSupportedException("Use UpdateBulkAsync for remote collections.");

    public Task<int> UpdateBulkAsync(IEnumerable<T> entities, CancellationToken ct = default) =>
        _inner.UpdateBulkAsync(entities, null, ct);

    // ── Delete ────────────────────────────────────────────────────────────────

    public bool Delete(TId id) =>
        throw new NotSupportedException("Use DeleteAsync for remote collections.");

    public Task<bool> DeleteAsync(TId id, CancellationToken ct = default) =>
        _inner.DeleteAsync(id, null, ct);

    public int DeleteBulk(IEnumerable<TId> ids) =>
        throw new NotSupportedException("Use DeleteBulkAsync for remote collections.");

    public Task<int> DeleteBulkAsync(IEnumerable<TId> ids, CancellationToken ct = default) =>
        _inner.DeleteBulkAsync(ids, null, ct);

    // ── Index management (not supported on remote) ────────────────────────────

    public CollectionSecondaryIndex<TId, T> CreateIndex<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public Task<CollectionSecondaryIndex<TId, T>> CreateIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false,
        CancellationToken ct = default) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public CollectionSecondaryIndex<TId, T> CreateVectorIndex<TKey>(
        Expression<Func<T, TKey>> keySelector, int dimensions,
        VectorMetric metric = VectorMetric.Cosine, string? name = null) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public Task<CollectionSecondaryIndex<TId, T>> CreateVectorIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, int dimensions,
        VectorMetric metric = VectorMetric.Cosine, string? name = null,
        CancellationToken ct = default) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public CollectionSecondaryIndex<TId, T> EnsureIndex<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public Task<CollectionSecondaryIndex<TId, T>> EnsureIndexAsync<TKey>(
        Expression<Func<T, TKey>> keySelector, string? name = null, bool unique = false,
        CancellationToken ct = default) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public bool DropIndex(string name) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public Task<bool> DropIndexAsync(string name, CancellationToken ct = default) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public IEnumerable<CollectionIndexInfo> GetIndexes() =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public CollectionSecondaryIndex<TId, T>? GetIndex(string name) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public IEnumerable<T> QueryIndex(string indexName, object? minKey, object? maxKey, bool ascending = true) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

    public IAsyncEnumerable<T> QueryIndexAsync(
        string indexName, object? minKey, object? maxKey, bool ascending = true,
        CancellationToken ct = default) =>
        throw new NotSupportedException("Index operations are not supported on remote collections.");

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

    public void ForcePrune() =>
        throw new NotSupportedException("ForcePrune is not supported on remote collections.");
}
