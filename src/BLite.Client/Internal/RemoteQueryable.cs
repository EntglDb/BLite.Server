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

    // ── IBLiteQueryable<T> terminal operators ─────────────────────────────────

    public async Task<T?> FirstOrDefaultAsync(CancellationToken ct = default)
    {
        var limited = Queryable.Take(this, 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        return await e.MoveNextAsync() ? e.Current : default;
    }

    public async Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var limited = Queryable.Take(Queryable.Where(this, predicate), 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        return await e.MoveNextAsync() ? e.Current : default;
    }

    public async Task<List<T>> ToListAsync(CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            list.Add(item);
        return list;
    }

    public async Task<T?> SingleOrDefaultAsync(CancellationToken ct = default)
    {
        var limited = Queryable.Take(this, 2);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) return default;
        var found = e.Current;
        if (await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    public async Task<T?> SingleOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var limited = Queryable.Take(Queryable.Where(this, predicate), 2);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) return default;
        var found = e.Current;
        if (await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    public async Task<T> FirstAsync(CancellationToken ct = default)
    {
        var limited = Queryable.Take(this, 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains no elements.");
        return e.Current;
    }

    public async Task<T> FirstAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var limited = Queryable.Take(Queryable.Where(this, predicate), 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains no elements.");
        return e.Current;
    }

    public async Task<T> SingleAsync(CancellationToken ct = default)
    {
        var limited = Queryable.Take(this, 2);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains no elements.");
        var found = e.Current;
        if (await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    public async Task<T> SingleAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var limited = Queryable.Take(Queryable.Where(this, predicate), 2);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains no elements.");
        var found = e.Current;
        if (await e.MoveNextAsync()) throw new InvalidOperationException("Sequence contains more than one element.");
        return found;
    }

    public async Task<bool> AllAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var compiled = predicate.Compile();
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
        {
            if (!compiled(item)) return false;
        }
        return true;
    }

    public async Task<T[]> ToArrayAsync(CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            list.Add(item);
        return list.ToArray();
    }

    public async Task<int> CountAsync(CancellationToken ct = default)
    {
        int count = 0;
        await foreach (var _ in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            count++;
        return count;
    }

    public async Task<int> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var filtered = Queryable.Where(this, predicate);
        int count = 0;
        await foreach (var _ in ((IAsyncEnumerable<T>)filtered).WithCancellation(ct))
            count++;
        return count;
    }

    public async Task<bool> AnyAsync(CancellationToken ct = default)
    {
        var limited = Queryable.Take(this, 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        return await e.MoveNextAsync();
    }

    public async Task<bool> AnyAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var limited = Queryable.Take(Queryable.Where(this, predicate), 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        return await e.MoveNextAsync();
    }

    public async Task<T> LastAsync(CancellationToken ct = default)
    {
        T? found = default;
        bool seen = false;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct)) { found = item; seen = true; }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return found!;
    }

    public async Task<T> LastAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var filtered = Queryable.Where(this, predicate);
        T? found = default;
        bool seen = false;
        await foreach (var item in ((IAsyncEnumerable<T>)filtered).WithCancellation(ct)) { found = item; seen = true; }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return found!;
    }

    public async Task<T?> LastOrDefaultAsync(CancellationToken ct = default)
    {
        T? found = default;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            found = item;
        return found;
    }

    public async Task<T?> LastOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken ct = default)
    {
        var filtered = Queryable.Where(this, predicate);
        T? found = default;
        await foreach (var item in ((IAsyncEnumerable<T>)filtered).WithCancellation(ct))
            found = item;
        return found;
    }

    public async Task<T> ElementAtAsync(int index, CancellationToken ct = default)
    {
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        if (!await e.MoveNextAsync()) throw new ArgumentOutOfRangeException(nameof(index), "Index was out of range.");
        return e.Current;
    }

    public async Task<T?> ElementAtOrDefaultAsync(int index, CancellationToken ct = default)
    {
        if (index < 0) return default;
        var limited = Queryable.Take(Queryable.Skip(this, index), 1);
        await using var e = ((IAsyncEnumerable<T>)limited).GetAsyncEnumerator(ct);
        return await e.MoveNextAsync() ? e.Current : default;
    }

    public async Task ForEachAsync(Action<T> action, CancellationToken ct = default)
    {
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
        {
            ct.ThrowIfCancellationRequested();
            action(item);
        }
    }

    public IAsyncEnumerable<T> AsAsyncEnumerable() => this;

    // ─── OLAP aggregates (in-memory on streamed results) ─────────────────────

    public async Task<int> SumAsync(Expression<Func<T, int>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        int sum = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            sum += compiled(item);
        return sum;
    }

    public async Task<long> SumAsync(Expression<Func<T, long>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        long sum = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            sum += compiled(item);
        return sum;
    }

    public async Task<double> SumAsync(Expression<Func<T, double>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        double sum = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            sum += compiled(item);
        return sum;
    }

    public async Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        decimal sum = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
            sum += compiled(item);
        return sum;
    }

    public async Task<double> AverageAsync(Expression<Func<T, int>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        long sum = 0;
        int count = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct)) { sum += compiled(item); count++; }
        if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
        return (double)sum / count;
    }

    public async Task<double> AverageAsync(Expression<Func<T, long>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        long sum = 0;
        int count = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct)) { sum += compiled(item); count++; }
        if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
        return (double)sum / count;
    }

    public async Task<double> AverageAsync(Expression<Func<T, double>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        double sum = 0;
        int count = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct)) { sum += compiled(item); count++; }
        if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
        return sum / count;
    }

    public async Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        decimal sum = 0;
        int count = 0;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct)) { sum += compiled(item); count++; }
        if (count == 0) throw new InvalidOperationException("Sequence contains no elements.");
        return sum / count;
    }

    public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        TResult? min = default;
        bool seen = false;
        var comparer = Comparer<TResult>.Default;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
        {
            var val = compiled(item);
            if (!seen || comparer.Compare(val, min!) < 0) { min = val; seen = true; }
        }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return min!;
    }

    public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        TResult? max = default;
        bool seen = false;
        var comparer = Comparer<TResult>.Default;
        await foreach (var item in ((IAsyncEnumerable<T>)this).WithCancellation(ct))
        {
            var val = compiled(item);
            if (!seen || comparer.Compare(val, max!) > 0) { max = val; seen = true; }
        }
        if (!seen) throw new InvalidOperationException("Sequence contains no elements.");
        return max!;
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
