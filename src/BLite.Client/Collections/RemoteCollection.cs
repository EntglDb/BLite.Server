// BLite.Client — RemoteCollection<TId, T>
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Typed remote collection that mirrors DocumentCollection<TId,T>.
// Uses IDocumentMapper<TId,T> to serialize/deserialize entities using the
// C-BSON key map negotiated with the server via MetadataService.RegisterKeys.
//
// Key-map negotiation happens lazily on first use:
//   1. MetadataService.RegisterKeys(mapper.UsedKeys) → server assigns IDs
//   2. ClientKeyMap.Forward is populated with server-assigned ushort IDs
//   3. BsonSpanWriter(buf, Forward) serializes entities with the correct IDs
//   4. BsonSpanReader(bytes, Reverse) deserializes server bytes correctly
//   5. Server-side QueryDescriptor push-down works because both sides use
//      the same IDs for the same field names.

using System.Buffers;
using System.Runtime.CompilerServices;
using BLite.Bson;
using BLite.Client.Internal;
using BLite.Client.Transactions;
using BLite.Core.Collections;
using BLite.Core.Indexing;
using BLite.Core.Query;
using BLite.Proto;
using BLite.Proto.V1;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Client.Collections;

/// <summary>
/// Typed remote collection.  Mirrors <c>DocumentCollection&lt;TId, T&gt;</c>
/// from the BLite embedded engine.
/// </summary>
/// <typeparam name="TId">
/// Entity ID type.  Supported: <see cref="ObjectId"/>, <see cref="string"/>,
/// <see cref="int"/>, <see cref="long"/>, <see cref="Guid"/>.
/// </typeparam>
/// <typeparam name="T">Entity class decorated with <c>[BLiteMapper]</c>.</typeparam>
public sealed class RemoteCollection<TId, T>
    where T : class
{
    private readonly IDocumentMapper<TId, T>              _mapper;
    private readonly DynamicService.DynamicServiceClient  _dynStub;
    private readonly DocumentService.DocumentServiceClient _docStub;
    private readonly ClientKeyMap                          _keyMap;
    private readonly Metadata                              _headers;
    private readonly BsonIdType                            _idType;

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private volatile bool _initialized;

    /// <summary>The logical (namespace-stripped) collection name.</summary>
    public string Name => _mapper.CollectionName;

    internal RemoteCollection(
        IDocumentMapper<TId, T> mapper,
        DynamicService.DynamicServiceClient dynStub,
        DocumentService.DocumentServiceClient docStub,
        ClientKeyMap keyMap,
        Metadata headers)
    {
        _mapper  = mapper;
        _dynStub = dynStub;
        _docStub = docStub;
        _keyMap  = keyMap;
        _headers = headers;
        _idType  = ResolveIdType();
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    public async Task<TId> InsertAsync(T entity, RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var bytes = Serialize(entity);

        var response = await _docStub.InsertAsync(new TypedInsertRequest
        {
            Collection    = Name,
            BsonPayload   = ByteString.CopyFrom(bytes),
            TypeName      = typeof(T).FullName ?? typeof(T).Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(InsertAsync));
        return _mapper.FromIndexKey(
            new IndexKey(BsonIdConverter.FromProto(response.Id).ToBytes()));
    }

    public async Task<IReadOnlyList<TId>> InsertBulkAsync(
        IEnumerable<T> entities,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var typeName = typeof(T).FullName ?? typeof(T).Name;

        var request = new TypedBulkInsertRequest
        {
            Collection    = Name,
            TypeName      = typeName,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        request.Payloads.AddRange(
            entities.Select(e => ByteString.CopyFrom(Serialize(e))));

        var response = await _docStub.InsertBulkAsync(
            request, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(InsertBulkAsync));
        return [.. response.Ids.Select(p =>
            _mapper.FromIndexKey(new IndexKey(BsonIdConverter.FromProto(p).ToBytes())))];
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    public async Task<T?> FindByIdAsync(TId id, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        var response = await _dynStub.FindByIdAsync(new FindByIdRequest
        {
            Collection = Name,
            Id         = ToBsonIdBytes(id)
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(FindByIdAsync));
        return response.Found ? Deserialize(response.BsonPayload.ToByteArray()) : null;
    }

    public async IAsyncEnumerable<T> FindAllAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var descriptor = new QueryDescriptor { Collection = Name };
        await foreach (var entity in StreamQueryAsync(descriptor, ct))
            yield return entity;
    }

    /// <summary>
    /// Streams entities matching the predicate.  Predicate runs client-side.
    /// For server-side filtering, use <see cref="QueryAsync"/>.
    /// </summary>
    public async IAsyncEnumerable<T> FindAsync(
        Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var entity in FindAllAsync(ct))
        {
            if (predicate(entity))
                yield return entity;
        }
    }

    /// <summary>
    /// Executes a server-side query with push-down of WHERE/ORDER BY/SKIP/TAKE.
    /// Field names in the <see cref="QueryDescriptor"/> must be lowercase and
    /// match the mapper's <c>UsedKeys</c>.
    /// </summary>
    public async IAsyncEnumerable<T> QueryAsync(
        QueryDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        descriptor.Collection = Name;
        await foreach (var entity in StreamQueryAsync(descriptor, ct))
            yield return entity;
    }

    // ── LINQ Queryable ────────────────────────────────────────────────────────

    /// <summary>
    /// Returns an <see cref="IBLiteQueryable{T}"/> that translates LINQ expression
    /// trees into server-side <see cref="QueryDescriptor"/> queries over gRPC.
    /// <para>
    /// Supports <c>.Where()</c>, <c>.OrderBy()</c>, <c>.Take()</c>, <c>.Skip()</c>,
    /// <c>.Select()</c> push-down, plus all BLite async terminal operators
    /// (<c>ToListAsync</c>, <c>FirstOrDefaultAsync</c>, <c>CountAsync</c>, etc.).
    /// </para>
    /// <code>
    /// var results = await collection.AsQueryable()
    ///     .Where(u => u.Age > 25)
    ///     .OrderBy(u => u.Name)
    ///     .Take(10)
    ///     .ToListAsync();
    /// </code>
    /// </summary>
    public IBLiteQueryable<T> AsQueryable()
    {
        var provider = new RemoteQueryProvider<TId, T>(
            Name, _mapper, _dynStub, _keyMap, _headers,
            EnsureInitializedAsync);
        return new RemoteQueryable<T>(provider);
    }

    // ── Update ────────────────────────────────────────────────────────────────

    public async Task<bool> UpdateAsync(T entity, RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var id    = _mapper.GetId(entity);
        var bytes = Serialize(entity);

        var response = await _docStub.UpdateAsync(new TypedUpdateRequest
        {
            Collection    = Name,
            Id            = ToBsonIdBytes(id),
            BsonPayload   = ByteString.CopyFrom(bytes),
            TypeName      = typeof(T).FullName ?? typeof(T).Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(UpdateAsync));
        return response.Success;
    }

    public async Task<int> UpdateBulkAsync(
        IEnumerable<T> entities,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var typeName = typeof(T).FullName ?? typeof(T).Name;

        var request = new BulkUpdateRequest
        {
            Collection    = Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        foreach (var entity in entities)
        {
            request.Items.Add(new BulkUpdateItem
            {
                Id          = ToBsonIdBytes(_mapper.GetId(entity)),
                BsonPayload = ByteString.CopyFrom(Serialize(entity))
            });
        }

        var response = await _dynStub.UpdateBulkAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(UpdateBulkAsync));
        return response.AffectedCount;
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    public async Task<bool> DeleteAsync(TId id, RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        var response = await _docStub.DeleteAsync(new DeleteRequest
        {
            Collection    = Name,
            Id            = ToBsonIdBytes(id),
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(DeleteAsync));
        return response.Success;
    }

    public async Task<int> DeleteBulkAsync(
        IEnumerable<TId> ids,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        var request = new BulkDeleteRequest
        {
            Collection    = Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        request.Ids.AddRange(ids.Select(ToBsonIdBytes));

        var response = await _dynStub.DeleteBulkAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(DeleteBulkAsync));
        return response.AffectedCount;
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    private byte[] Serialize(T entity)
    {
        var buf     = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            var writer  = new BsonSpanWriter(buf, _keyMap.Forward);
            var written = _mapper.Serialize(entity, writer);
            return buf[..written];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    private T Deserialize(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes, _keyMap.Reverse);
        return _mapper.Deserialize(reader);
    }

    // ── ID conversion ─────────────────────────────────────────────────────────

    private BsonIdBytes ToBsonIdBytes(TId id)
    {
        var key = _mapper.ToIndexKey(id);
        return new BsonIdBytes
        {
            Value  = ByteString.CopyFrom(key.Data.ToArray()),
            IdType = (int)_idType
        };
    }

    private static BsonIdType ResolveIdType()
    {
        var t = typeof(TId);
        if (t == typeof(ObjectId)) return BsonIdType.ObjectId;
        if (t == typeof(string))   return BsonIdType.String;
        if (t == typeof(int))      return BsonIdType.Int32;
        if (t == typeof(long))     return BsonIdType.Int64;
        if (t == typeof(Guid))     return BsonIdType.Guid;
        throw new NotSupportedException(
            $"ID type '{t.Name}' is not supported by BLite.Client. " +
            "Use ObjectId, string, int, long, or Guid.");
    }

    // ── Lazy initialisation ───────────────────────────────────────────────────

    private async Task EnsureInitializedAsync(CancellationToken ct)
    {
        if (_initialized) return;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_initialized) return;
            await _keyMap.RegisterAsync(Name, _mapper.UsedKeys, ct);
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    // ── Streaming ─────────────────────────────────────────────────────────────

    private async IAsyncEnumerable<T> StreamQueryAsync(
        QueryDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Use DynamicService.Query: bytes are stored as-is, mapper deserializes them.
        var call = _dynStub.Query(
            new QueryRequest { QueryDescriptor = QueryDescriptorHelper.Serialize(descriptor) },
            _headers, cancellationToken: ct);

        await foreach (var response in call.ResponseStream.ReadAllAsync(ct))
        {
            ThrowIfError(response.Error, "Query");
            yield return Deserialize(response.BsonPayload.ToByteArray());
        }
    }

    private static void ThrowIfError(string error, string method)
    {
        if (!string.IsNullOrEmpty(error))
            throw new InvalidOperationException($"{method} failed: {error}");
    }
}
