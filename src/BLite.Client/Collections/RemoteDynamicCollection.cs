// BLite.Client — RemoteDynamicCollection
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Schema-less remote collection that mirrors the DynamicCollection API.
// Every method translates directly to a DynamicService gRPC call.
// Documents travel as raw C-BSON bytes; the ClientKeyMap bridges the gap
// between the server's global ushort ID dictionary and the client side.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using BLite.Bson;
using BLite.Client.Internal;
using BLite.Client.Transactions;
using BLite.Proto;
using BLite.Proto.V1;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Client.Collections;

/// <summary>
/// Schema-less remote collection.  Mirrors <c>DynamicCollection</c> from the
/// BLite embedded engine, routing every operation to the BLite Server via gRPC.
///
/// <para>
/// Documents are created with <see cref="NewDocumentAsync"/> which registers the
/// field names on the server and builds a C-BSON document using the server's
/// key assignments.  Inserting a document created with mismatched IDs will
/// result in unreadable data — always use <see cref="NewDocumentAsync"/> or
/// ensure fields have been pre-registered via <see cref="RefreshKeyMapAsync"/>.
/// </para>
/// </summary>
public sealed class RemoteDynamicCollection
{
    private readonly DynamicService.DynamicServiceClient _stub;
    private readonly ClientKeyMap                        _keyMap;
    private readonly Metadata                            _headers;

    /// <summary>The logical (namespace-stripped) collection name.</summary>
    public string Name { get; }

    internal RemoteDynamicCollection(
        string name,
        DynamicService.DynamicServiceClient stub,
        ClientKeyMap keyMap,
        Metadata headers)
    {
        Name     = name;
        _stub    = stub;
        _keyMap  = keyMap;
        _headers = headers;
    }

    // ── Document factory ──────────────────────────────────────────────────────

    /// <summary>
    /// Registers <paramref name="fieldNames"/> on the server (idempotent) and
    /// returns a <see cref="BsonDocument"/> built with the server-assigned IDs.
    ///
    /// All field names used inside <paramref name="build"/> MUST be listed in
    /// <paramref name="fieldNames"/> — unlisted names may receive temporary
    /// local IDs that differ from the server's assignments.
    /// </summary>
    public async Task<BsonDocument> NewDocumentAsync(
        string[] fieldNames,
        Action<BsonDocumentBuilder> build,
        CancellationToken ct = default)
    {
        await _keyMap.RegisterAsync(Name, fieldNames, ct);
        return BsonDocument.Create(_keyMap.Forward, _keyMap.Reverse, build);
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    public async Task<BsonId> InsertAsync(
        BsonDocument doc,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var response = await _stub.InsertAsync(new InsertRequest
        {
            Collection    = Name,
            BsonPayload   = ByteString.CopyFrom(doc.RawData.ToArray()),
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(InsertAsync));
        return BsonIdConverter.FromProto(response.Id);
    }

    public async Task<IReadOnlyList<BsonId>> InsertBulkAsync(
        IEnumerable<BsonDocument> docs,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var request = new BulkInsertRequest
        {
            Collection    = Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        request.Payloads.AddRange(
            docs.Select(d => ByteString.CopyFrom(d.RawData.ToArray())));

        var response = await _stub.InsertBulkAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(InsertBulkAsync));
        return [.. response.Ids.Select(BsonIdConverter.FromProto)];
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Finds a document by its ID.  Returns <c>null</c> when not found.
    /// </summary>
    public async Task<BsonDocument?> FindByIdAsync(
        BsonId id,
        CancellationToken ct = default)
    {
        await EnsureKeyMapLoadedAsync(ct);

        var response = await _stub.FindByIdAsync(new FindByIdRequest
        {
            Collection = Name,
            Id         = BsonIdConverter.ToProto(id)
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(FindByIdAsync));
        return response.Found
            ? new BsonDocument(response.BsonPayload.ToByteArray(), _keyMap.Reverse)
            : null;
    }

    /// <summary>
    /// Streams all documents in the collection.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> FindAllAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnsureKeyMapLoadedAsync(ct);

        var descriptor = new QueryDescriptor { Collection = Name };
        await foreach (var doc in StreamQueryAsync(descriptor, ct))
            yield return doc;
    }

    /// <summary>
    /// Streams documents matching the predicate.  The predicate runs
    /// client-side after full document retrieval (no push-down).
    /// For server-side filtering, use <see cref="QueryAsync"/>.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> FindAsync(
        Func<BsonDocument, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var doc in FindAllAsync(ct))
        {
            if (predicate(doc))
                yield return doc;
        }
    }

    /// <summary>
    /// Executes a server-side query from an explicit <see cref="QueryDescriptor"/>.
    /// Supports server-push-down of WHERE, ORDER BY, SKIP and TAKE.
    /// Field names in <see cref="FilterNode"/> must match the server's key map.
    /// </summary>
    public async IAsyncEnumerable<BsonDocument> QueryAsync(
        QueryDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnsureKeyMapLoadedAsync(ct);
        descriptor.Collection = Name;
        await foreach (var doc in StreamQueryAsync(descriptor, ct))
            yield return doc;
    }

    // ── Update ────────────────────────────────────────────────────────────────

    public async Task<bool> UpdateAsync(
        BsonId id,
        BsonDocument doc,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var response = await _stub.UpdateAsync(new UpdateRequest
        {
            Collection    = Name,
            Id            = BsonIdConverter.ToProto(id),
            BsonPayload   = ByteString.CopyFrom(doc.RawData.ToArray()),
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(UpdateAsync));
        return response.Success;
    }

    public async Task<int> UpdateBulkAsync(
        IEnumerable<(BsonId Id, BsonDocument Doc)> updates,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var request = new BulkUpdateRequest
        {
            Collection    = Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        request.Items.AddRange(updates.Select(u => new BulkUpdateItem
        {
            Id          = BsonIdConverter.ToProto(u.Id),
            BsonPayload = ByteString.CopyFrom(u.Doc.RawData.ToArray())
        }));

        var response = await _stub.UpdateBulkAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(UpdateBulkAsync));
        return response.AffectedCount;
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    public async Task<bool> DeleteAsync(
        BsonId id,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var response = await _stub.DeleteAsync(new DeleteRequest
        {
            Collection    = Name,
            Id            = BsonIdConverter.ToProto(id),
            TransactionId = tx?.TransactionId ?? string.Empty
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(DeleteAsync));
        return response.Success;
    }

    public async Task<int> DeleteBulkAsync(
        IEnumerable<BsonId> ids,
        RemoteTransaction? tx = null,
        CancellationToken ct = default)
    {
        var request = new BulkDeleteRequest
        {
            Collection    = Name,
            TransactionId = tx?.TransactionId ?? string.Empty
        };
        request.Ids.AddRange(ids.Select(BsonIdConverter.ToProto));

        var response = await _stub.DeleteBulkAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(DeleteBulkAsync));
        return response.AffectedCount;
    }

    // ── Index management ──────────────────────────────────────────────────────

    /// <summary>
    /// Creates a BTree secondary index on <paramref name="field"/>.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task CreateIndexAsync(
        string field,
        string? name = null,
        bool unique = false,
        CancellationToken ct = default)
    {
        var response = await _stub.CreateIndexAsync(new CreateIndexRequest
        {
            Collection = Name,
            Field      = field,
            Name       = name ?? string.Empty,
            Unique     = unique,
            IsVector   = false
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(CreateIndexAsync));
    }

    /// <summary>
    /// Creates a Vector (HNSW) index on <paramref name="field"/>.
    /// <paramref name="metric"/> accepts "Cosine" (default), "L2", or "DotProduct".
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task CreateVectorIndexAsync(
        string field,
        int dimensions,
        string metric = "Cosine",
        string? name = null,
        CancellationToken ct = default)
    {
        var response = await _stub.CreateIndexAsync(new CreateIndexRequest
        {
            Collection = Name,
            Field      = field,
            Name       = name ?? string.Empty,
            IsVector   = true,
            Dimensions = dimensions,
            Metric     = metric
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(CreateVectorIndexAsync));
    }

    /// <summary>
    /// Creates a Spatial (R-Tree) index on <paramref name="field"/>.
    /// The field must be stored as a coordinates array <c>[lat, lon]</c>.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task CreateSpatialIndexAsync(
        string field,
        string? name = null,
        CancellationToken ct = default)
    {
        var response = await _stub.CreateIndexAsync(new CreateIndexRequest
        {
            Collection = Name,
            Field      = field,
            Name       = name ?? string.Empty,
            IsSpatial  = true
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(CreateSpatialIndexAsync));
    }

    /// <summary>
    /// Drops a secondary index by name.
    /// Returns <c>false</c> when the index was not found.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task<bool> DropIndexAsync(
        string indexName, CancellationToken ct = default)
    {
        var response = await _stub.DropIndexAsync(new DropIndexRequest
        {
            Collection = Name,
            Name       = indexName
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(DropIndexAsync));
        return response.Success;
    }

    /// <summary>
    /// Returns descriptors for all secondary indexes on this collection.
    /// </summary>
    public async Task<IReadOnlyList<RemoteIndexInfo>> ListIndexesAsync(
        CancellationToken ct = default)
    {
        var response = await _stub.ListIndexesAsync(
            new CollectionRequest { Collection = Name },
            _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(ListIndexesAsync));
        return [.. response.Indexes.Select(i => new RemoteIndexInfo(
            i.Name, i.Field, i.Type, i.Unique, i.Dimensions, i.Metric))];
    }

    // ── VectorSource (embedding source configuration) ─────────────────────────

    /// <summary>
    /// Sets the VectorSource configuration for this collection.
    /// Pass an empty <paramref name="fields"/> list to clear the configuration.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task SetVectorSourceAsync(
        IEnumerable<(string Path, string? Prefix, string? Suffix)> fields,
        string separator = " ",
        CancellationToken ct = default)
    {
        var request = new SetVectorSourceRequest
        {
            Collection = Name,
            Separator  = separator
        };
        request.Fields.AddRange(fields.Select(f => new VectorFieldProto
        {
            Path   = f.Path,
            Prefix = f.Prefix ?? string.Empty,
            Suffix = f.Suffix ?? string.Empty
        }));

        var response = await _stub.SetVectorSourceAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(SetVectorSourceAsync));
    }

    /// <summary>
    /// Returns the current VectorSource configuration, or <c>null</c> when not set.
    /// </summary>
    public async Task<RemoteVectorSourceInfo?> GetVectorSourceAsync(
        CancellationToken ct = default)
    {
        var response = await _stub.GetVectorSourceAsync(
            new CollectionRequest { Collection = Name },
            _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(GetVectorSourceAsync));
        if (!response.Configured)
            return null;

        return new RemoteVectorSourceInfo(
            response.Separator,
            [.. response.Fields.Select(f =>
                (f.Path,
                 string.IsNullOrEmpty(f.Prefix) ? (string?)null : f.Prefix,
                 string.IsNullOrEmpty(f.Suffix) ? (string?)null : f.Suffix))]);
    }

    // ── Vector search ─────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the <paramref name="k"/> nearest documents to <paramref name="queryVector"/>
    /// using the HNSW vector index.
    /// </summary>
    /// <param name="queryVector">The query vector. Must match the index dimensionality.</param>
    /// <param name="k">Number of nearest neighbours to return (default 10).</param>
    /// <param name="indexName">Vector index name. Empty = use the first vector index found.</param>
    /// <param name="efSearch">HNSW efSearch parameter: higher = more recall, slower (default 100).</param>
    public async IAsyncEnumerable<BsonDocument> VectorSearchAsync(
        float[] queryVector,
        int k = 10,
        string? indexName = null,
        int efSearch = 100,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await EnsureKeyMapLoadedAsync(ct);

        var request = new VectorSearchRequest
        {
            Collection = Name,
            IndexName  = indexName ?? string.Empty,
            K          = k,
            EfSearch   = efSearch
        };
        request.QueryVector.AddRange(queryVector);

        var call = _stub.VectorSearch(request, _headers, cancellationToken: ct);
        await foreach (var response in call.ResponseStream.ReadAllAsync(ct))
        {
            ThrowIfError(response.Error, "VectorSearch");
            yield return new BsonDocument(
                response.BsonPayload.ToByteArray(), _keyMap.Reverse);
        }
    }

    // ── TimeSeries configuration ──────────────────────────────────────────────

    /// <summary>
    /// Configures this collection as a TimeSeries with TTL-based automatic pruning.
    /// This operation is irreversible once documents have been written.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task ConfigureTimeSeriesAsync(
        string ttlFieldName,
        TimeSpan retentionPolicy,
        CancellationToken ct = default)
    {
        var response = await _stub.ConfigureTimeSeriesAsync(new ConfigureTimeSeriesRequest
        {
            Collection   = Name,
            TtlFieldName = ttlFieldName,
            RetentionMs  = (long)retentionPolicy.TotalMilliseconds
        }, _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(ConfigureTimeSeriesAsync));
    }

    /// <summary>
    /// Returns the TimeSeries configuration for this collection.
    /// <see cref="RemoteTimeSeriesInfo.IsTimeSeries"/> is <c>false</c> when
    /// the collection has not been configured as a TimeSeries.
    /// </summary>
    public async Task<RemoteTimeSeriesInfo> GetTimeSeriesInfoAsync(
        CancellationToken ct = default)
    {
        var response = await _stub.GetTimeSeriesInfoAsync(
            new CollectionRequest { Collection = Name },
            _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(GetTimeSeriesInfoAsync));
        return new RemoteTimeSeriesInfo(
            response.IsTimeSeries,
            string.IsNullOrEmpty(response.TtlFieldName) ? null : response.TtlFieldName,
            response.RetentionMs);
    }

    // ── Schema management ─────────────────────────────────────────────────────

    /// <summary>
    /// Returns the latest schema version for this collection.
    /// <see cref="RemoteSchemaInfo.HasSchema"/> is <c>false</c> when no schema
    /// has been defined yet.
    /// </summary>
    public async Task<RemoteSchemaInfo> GetSchemaAsync(CancellationToken ct = default)
    {
        var response = await _stub.GetSchemaAsync(
            new CollectionRequest { Collection = Name },
            _headers, cancellationToken: ct);

        ThrowIfError(response.Error, nameof(GetSchemaAsync));
        if (!response.HasSchema)
            return new RemoteSchemaInfo(false, null, null, 0, []);

        var fields = response.Fields
            .Select(f => new RemoteSchemaFieldInfo(
                f.Name,
                f.Type,
                ((BLite.Bson.BsonType)f.Type).ToString(),
                f.Nullable))
            .ToList();

        return new RemoteSchemaInfo(
            true,
            string.IsNullOrEmpty(response.Title) ? null : response.Title,
            response.Version == 0 ? (int?)null : response.Version,
            response.VersionCount,
            fields);
    }

    /// <summary>
    /// Appends a new schema version to this collection.
    /// Each call creates an immutable version entry; previous versions are preserved.
    /// Requires <c>Admin</c> permission.
    /// </summary>
    public async Task SetSchemaAsync(
        IEnumerable<(string Name, BLite.Bson.BsonType Type, bool Nullable)> fields,
        string? title = null,
        CancellationToken ct = default)
    {
        var request = new SetSchemaRequest
        {
            Collection = Name,
            Title      = title ?? string.Empty
        };
        request.Fields.AddRange(fields.Select(f => new SchemaFieldProto
        {
            Name     = f.Name,
            Type     = (int)f.Type,
            Nullable = f.Nullable
        }));

        var response = await _stub.SetSchemaAsync(request, _headers, cancellationToken: ct);
        ThrowIfError(response.Error, nameof(SetSchemaAsync));
    }

    // ── Key-map management ────────────────────────────────────────────────────

    /// <summary>
    /// Forces a reload of the full global key map from the server.
    /// Call this if new fields have been added by another client.
    /// </summary>
    public Task RefreshKeyMapAsync(CancellationToken ct = default) =>
        _keyMap.RefreshAsync(Name, ct);

    // ── Internal helpers ──────────────────────────────────────────────────────

    private Task EnsureKeyMapLoadedAsync(CancellationToken ct) =>
        _keyMap.LoadFullMapAsync(Name, ct);

    private async IAsyncEnumerable<BsonDocument> StreamQueryAsync(
        QueryDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var call = _stub.Query(
            new QueryRequest { QueryDescriptor = QueryDescriptorHelper.Serialize(descriptor) },
            _headers, cancellationToken: ct);

        await foreach (var response in call.ResponseStream.ReadAllAsync(ct))
        {
            ThrowIfError(response.Error, "Query");
            yield return new BsonDocument(
                response.BsonPayload.ToByteArray(), _keyMap.Reverse);
        }
    }

    private static void ThrowIfError(string error, string method)
    {
        if (!string.IsNullOrEmpty(error))
            throw new InvalidOperationException($"{method} failed: {error}");
    }
}

// ── Data transfer objects ────────────────────────────────────────────────────

/// <summary>Describes the TimeSeries configuration of a remote collection.</summary>
public sealed record RemoteTimeSeriesInfo(
    bool IsTimeSeries,
    string? TtlFieldName,
    long RetentionMs);

/// <summary>Describes a single field in a remote collection schema version.</summary>
public sealed record RemoteSchemaFieldInfo(
    string Name,
    int TypeCode,
    string TypeName,
    bool IsNullable);

/// <summary>Describes the latest schema version of a remote collection.</summary>
public sealed record RemoteSchemaInfo(
    bool HasSchema,
    string? Title,
    int? Version,
    int VersionCount,
    IReadOnlyList<RemoteSchemaFieldInfo> Fields);
public sealed record RemoteIndexInfo(
    string Name,
    string FieldPath,
    string Type,      // "BTree" | "Vector" | "Spatial"
    bool Unique,
    int Dimensions,   // > 0 for Vector indexes
    string Metric);   // "Cosine" | "L2" | "DotProduct" for Vector indexes

/// <summary>Describes the VectorSource configuration of a remote collection.</summary>
public sealed record RemoteVectorSourceInfo(
    string Separator,
    IReadOnlyList<(string Path, string? Prefix, string? Suffix)> Fields);
