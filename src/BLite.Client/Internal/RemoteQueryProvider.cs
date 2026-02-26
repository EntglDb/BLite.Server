// BLite.Client — RemoteQueryProvider<TId, T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IQueryProvider that reuses BTreeExpressionVisitor from BLite.Core to parse
// the LINQ expression tree into a QueryModel, converts it to a QueryDescriptor,
// serializes it via MessagePack and sends it over gRPC via DynamicService.Query.
//
// The response is a server-streamed sequence of C-BSON documents that are
// deserialized on the client using the IDocumentMapper<TId, T>.

using System.Buffers;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Query;
using BLite.Proto;
using BLite.Proto.V1;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Client.Internal;

/// <summary>
/// LINQ query provider that translates expression trees into gRPC streaming calls.
/// </summary>
internal sealed class RemoteQueryProvider<TId, T> : IQueryProvider, IRemoteQueryExecutor
    where T : class
{
    private readonly string                                _collection;
    private readonly IDocumentMapper<TId, T>               _mapper;
    private readonly DynamicService.DynamicServiceClient    _dynStub;
    private readonly ClientKeyMap                          _keyMap;
    private readonly Metadata                              _headers;
    private readonly Func<CancellationToken, Task>         _ensureInit;

    internal RemoteQueryProvider(
        string collection,
        IDocumentMapper<TId, T> mapper,
        DynamicService.DynamicServiceClient dynStub,
        ClientKeyMap keyMap,
        Metadata headers,
        Func<CancellationToken, Task> ensureInit)
    {
        _collection = collection;
        _mapper     = mapper;
        _dynStub    = dynStub;
        _keyMap     = keyMap;
        _headers    = headers;
        _ensureInit = ensureInit;
    }

    // ── IQueryProvider ────────────────────────────────────────────────────────

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0];
        return (IQueryable)Activator.CreateInstance(
            typeof(RemoteQueryable<>).MakeGenericType(elementType),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
            null,
            [this, expression],
            null)!;
    }

    IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        => new RemoteQueryable<TElement>(this, expression);

    public object? Execute(Expression expression) => Execute<object>(expression);

    public TResult Execute<TResult>(Expression expression)
    {
        // 1. Visit the expression tree using the shared BTreeExpressionVisitor
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();

        // 2. Convert QueryModel → QueryDescriptor
        var descriptor = QueryModelToDescriptorConverter.Convert(model, _collection);

        // 3. Execute synchronously (materialise all results)
        //    Blocking here is acceptable — it's the IQueryProvider.Execute contract.
        //    Callers who want async should use ToListAsync / await foreach.
        _ensureInit(CancellationToken.None).GetAwaiter().GetResult();
        var results = StreamResultsSync(descriptor);

        // 4. Apply any post-query LINQ operators that weren't pushed down
        //    (e.g. Select projection, GroupBy, etc.)
        return ApplyInMemory<TResult>(expression, results);
    }

    // ── IRemoteQueryExecutor ──────────────────────────────────────────────────

    public IAsyncEnumerator<TElement> ExecuteStreamingAsync<TElement>(
        Expression expression, CancellationToken ct)
    {
        return ExecuteStreamingCoreAsync<TElement>(expression, ct).GetAsyncEnumerator(ct);
    }

    private async IAsyncEnumerable<TElement> ExecuteStreamingCoreAsync<TElement>(
        Expression expression,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var visitor = new BTreeExpressionVisitor();
        visitor.Visit(expression);
        var model = visitor.GetModel();

        var descriptor = QueryModelToDescriptorConverter.Convert(model, _collection);

        await _ensureInit(ct);

        // If TElement == T, stream directly without post-processing
        if (typeof(TElement) == typeof(T))
        {
            await foreach (var entity in StreamResultsAsync(descriptor, ct))
                yield return (TElement)(object)entity;
            yield break;
        }

        // Otherwise, materialise and apply Select/etc. in memory
        var results = new List<T>();
        await foreach (var entity in StreamResultsAsync(descriptor, ct))
            results.Add(entity);

        foreach (var item in ApplyInMemoryEnumerable<TElement>(expression, results))
            yield return item;
    }

    // ── gRPC streaming ────────────────────────────────────────────────────────

    private async IAsyncEnumerable<T> StreamResultsAsync(
        QueryDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var call = _dynStub.Query(
            new QueryRequest { QueryDescriptor = QueryDescriptorHelper.Serialize(descriptor) },
            _headers, cancellationToken: ct);

        await foreach (var response in call.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(response.Error))
                throw new InvalidOperationException($"Query failed: {response.Error}");

            yield return Deserialize(response.BsonPayload.ToByteArray());
        }
    }

    private List<T> StreamResultsSync(QueryDescriptor descriptor)
    {
        var results = new List<T>();
        var call = _dynStub.Query(
            new QueryRequest { QueryDescriptor = QueryDescriptorHelper.Serialize(descriptor) },
            _headers);

        // Synchronous enumeration of the async stream — needed for IQueryProvider.Execute<T>
        var stream = call.ResponseStream;
        while (stream.MoveNext(CancellationToken.None).GetAwaiter().GetResult())
        {
            var response = stream.Current;
            if (!string.IsNullOrEmpty(response.Error))
                throw new InvalidOperationException($"Query failed: {response.Error}");

            results.Add(Deserialize(response.BsonPayload.ToByteArray()));
        }

        return results;
    }

    // ── Deserialization ───────────────────────────────────────────────────────

    private T Deserialize(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes, _keyMap.Reverse);
        return _mapper.Deserialize(reader);
    }

    // ── In-memory post-processing ─────────────────────────────────────────────

    /// <summary>
    /// Takes the materialised <paramref name="source"/> data and replays the full
    /// expression tree against it using <see cref="EnumerableRewriter"/>, so that
    /// operators like <c>.Select()</c> and <c>.GroupBy()</c> are applied in memory.
    /// </summary>
    private TResult ApplyInMemory<TResult>(Expression expression, List<T> source)
    {
        // Find the root IQueryable constant in the expression tree
        var rootFinder = new RootFinder();
        rootFinder.Visit(expression);

        if (rootFinder.Root is null)
            throw new InvalidOperationException("Could not find root Queryable in expression.");

        // Rewrite Queryable.* calls → Enumerable.* calls against our source
        var rewriter = new EnumerableRewriter(rootFinder.Root, source);
        var rewritten = rewriter.Visit(expression);

        if (rewritten.Type != typeof(TResult))
            rewritten = Expression.Convert(rewritten, typeof(TResult));

        return Expression.Lambda<Func<TResult>>(rewritten).Compile()();
    }

    private IEnumerable<TElement> ApplyInMemoryEnumerable<TElement>(
        Expression expression, List<T> source)
    {
        var result = ApplyInMemory<object>(expression, source);
        if (result is IEnumerable<TElement> enumerable)
            return enumerable;

        throw new InvalidOperationException(
            $"Expected IEnumerable<{typeof(TElement).Name}> but got {result?.GetType().Name ?? "null"}.");
    }

    // ── Root finder ───────────────────────────────────────────────────────────

    private sealed class RootFinder : ExpressionVisitor
    {
        public IQueryable? Root { get; private set; }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (Root is null && node.Value is IQueryable q)
                Root = q;
            return base.VisitConstant(node);
        }
    }
}
