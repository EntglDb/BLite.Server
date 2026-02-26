// BLite.Client — RemoteQueryable<T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IQueryable + IAsyncEnumerable wrapper for the remote query pipeline.
// Mirrors BTreeQueryable<T> from BLite.Core but routes execution through
// RemoteQueryProvider → gRPC streaming instead of the local BTree engine.

using System.Collections;
using System.Linq.Expressions;
using BLite.Core.Query;

namespace BLite.Client.Internal;

/// <summary>
/// Remote queryable that implements <see cref="IBLiteQueryable{T}"/> and
/// <see cref="IAsyncEnumerable{T}"/>, enabling both standard LINQ operators
/// and <c>await foreach</c> / <c>ToListAsync</c> on remote collections.
/// </summary>
internal sealed class RemoteQueryable<T> : IBLiteQueryable<T>, IAsyncEnumerable<T>
{
    private readonly IQueryProvider _provider;

    internal RemoteQueryable(IQueryProvider provider, Expression expression)
    {
        _provider  = provider;
        Expression = expression;
    }

    internal RemoteQueryable(IQueryProvider provider)
    {
        _provider  = provider;
        Expression = Expression.Constant(this);
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    // ── Sync enumeration (IQueryable contract) ────────────────────────────────

    public IEnumerator<T> GetEnumerator()
        => _provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // ── Async enumeration (gRPC streaming) ────────────────────────────────────

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken ct = default)
    {
        if (_provider is IRemoteQueryExecutor executor)
            return executor.ExecuteStreamingAsync<T>(Expression, ct);

        // Fallback: wrap sync results in async
        return WrapSyncAsync(ct);
    }

    private async IAsyncEnumerator<T> WrapSyncAsync(CancellationToken ct)
    {
        var results = await Task.Run(
            () => _provider.Execute<IEnumerable<T>>(Expression), ct);

        foreach (var item in results)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}

/// <summary>
/// Extended query execution interface implemented by <see cref="RemoteQueryProvider{TId,T}"/>
/// to support native async streaming over gRPC.
/// </summary>
internal interface IRemoteQueryExecutor
{
    IAsyncEnumerator<TElement> ExecuteStreamingAsync<TElement>(
        Expression expression, CancellationToken ct);
}
