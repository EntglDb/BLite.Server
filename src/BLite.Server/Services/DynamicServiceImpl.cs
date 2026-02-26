// BLite.Server — DynamicService implementation (schema-less path)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Collections.Concurrent;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Storage;
using BLite.Proto;
using BLite.Proto.V1;
using BLite.Server.Auth;
using BLite.Server.Caching;
using BLite.Server.Execution;
using BLite.Server.Transactions;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Options;

namespace BLite.Server.Services;

/// <summary>
/// Implements the schema-less gRPC service.
/// All documents travel as raw BSON bytes — no compile-time type information required.
/// Every method checks the caller's permission, resolves the engine for the caller's
/// database, and applies namespace isolation.
/// </summary>
public sealed class DynamicServiceImpl : DynamicService.DynamicServiceBase
{
    private readonly EngineRegistry              _registry;
    private readonly AuthorizationService        _authz;
    private readonly TransactionManager          _txnManager;
    private readonly QueryCacheService           _cache;
    private readonly IOptions<QueryCacheOptions> _cacheOpts;
    private readonly ILogger<DynamicServiceImpl> _logger;

    public DynamicServiceImpl(
        EngineRegistry      registry,
        AuthorizationService authz,
        TransactionManager  txnManager,
        QueryCacheService cache,
        IOptions<QueryCacheOptions> cacheOpts,
        ILogger<DynamicServiceImpl> logger)
    {
        _registry   = registry;
        _authz      = authz;
        _txnManager = txnManager;
        _cache      = cache;
        _cacheOpts  = cacheOpts;
        _logger     = logger;
    }

    // -- Insert ----------------------------------------------------------------

    public override async Task<InsertResponse> Insert(
        InsertRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Insert);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var reverseKeys = ReverseKeys(engine);
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray(), reverseKeys);
            BsonId id;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                id = await session.Engine.GetOrCreateCollection(col).InsertAsync(doc, context.CancellationToken);
                session.MarkDirty(col);
            }
            else
            {
                id = await engine.InsertAsync(col, doc, context.CancellationToken);
                if (_cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            return new InsertResponse { Id = BsonIdSerializer.ToProto(id) };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Insert failed on collection {Col}", col);
            return new InsertResponse { Error = ex.Message };
        }
    }

    // -- FindById --------------------------------------------------------------

    public override async Task<DocumentResponse> FindById(
        FindByIdRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = await engine.FindByIdAsync(col, id, context.CancellationToken);

            if (doc is null)
                return new DocumentResponse { Found = false };

            return new DocumentResponse
            {
                BsonPayload = ByteString.CopyFrom(BsonPayloadSerializer.Serialize(doc)),
                Found       = true
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FindById failed on collection {Col}", col);
            return new DocumentResponse { Error = ex.Message };
        }
    }

    // -- Update ----------------------------------------------------------------

    public override async Task<MutationResponse> Update(
        UpdateRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Update);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var reverseKeys = ReverseKeys(engine);
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray(), reverseKeys);
            bool ok;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                ok = await session.Engine.GetOrCreateCollection(col).UpdateAsync(id, doc, context.CancellationToken);
                if (ok) session.MarkDirty(col);
            }
            else
            {
                ok = await engine.UpdateAsync(col, id, doc, context.CancellationToken);
                if (ok && _cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            return new MutationResponse { Success = ok };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Update failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // -- Delete ----------------------------------------------------------------

    public override async Task<MutationResponse> Delete(
        DeleteRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Delete);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var id = BsonIdSerializer.FromProto(request.Id);
            bool ok;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                ok = await session.Engine.GetOrCreateCollection(col).DeleteAsync(id, context.CancellationToken);
                if (ok) session.MarkDirty(col);
            }
            else
            {
                ok = await engine.DeleteAsync(col, id, context.CancellationToken);
                if (ok && _cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            return new MutationResponse { Success = ok };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Delete failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // -- Query (server-streaming) ----------------------------------------------

    public override async Task Query(
        QueryRequest request,
        IServerStreamWriter<DocumentResponse> responseStream,
        ServerCallContext context)
    {
        QueryDescriptor? descriptor;
        try
        {
            descriptor = QueryDescriptorSerializer.Deserialize(
                request.QueryDescriptor.ToByteArray());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize QueryDescriptor");
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                $"Invalid QueryDescriptor: {ex.Message}"));
        }

        var (col, user) = AuthorizeWithUser(context, descriptor.Collection, BLiteOperation.Query);
        descriptor.Collection = col;
        var engine = _registry.GetEngine(user.DatabaseId);
        var ct     = context.CancellationToken;

        var cacheKey = QueryCacheKeys.GrpcQuery(user.DatabaseId, col,
                                                request.QueryDescriptor.ToByteArray());

        if (_cache.Enabled && _cache.TryGet(cacheKey, out List<byte[]>? cachedPayloads))
        {
            foreach (var payload in cachedPayloads!)
                await responseStream.WriteAsync(
                    new DocumentResponse { BsonPayload = ByteString.CopyFrom(payload), Found = true },
                    ct);
            return;
        }

        var payloads = new List<byte[]>();
        await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(engine, descriptor, ct))
        {
            var payload = BsonPayloadSerializer.Serialize(doc);
            payloads.Add(payload);
            await responseStream.WriteAsync(
                new DocumentResponse { BsonPayload = ByteString.CopyFrom(payload), Found = true },
                ct);
        }

        if (_cache.Enabled && payloads.Count <= _cacheOpts.Value.MaxResultSetSize)
            _cache.Set(cacheKey, payloads, user.DatabaseId, col);
    }

    // -- InsertBulk ------------------------------------------------------------

    public override async Task<BulkInsertResponse> InsertBulk(
        BulkInsertRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Insert);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var reverseKeys = ReverseKeys(engine);
            var docs = request.Payloads
                .Select(p => BsonPayloadSerializer.Deserialize(p.ToByteArray(), reverseKeys))
                .ToList();

            List<BsonId> ids;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                ids = await session.Engine.GetOrCreateCollection(col).InsertBulkAsync(docs, context.CancellationToken);
                session.MarkDirty(col);
            }
            else
            {
                ids = await engine.InsertBulkAsync(col, docs, context.CancellationToken);
                if (_cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            var response = new BulkInsertResponse();
            response.Ids.AddRange(ids.Select(BsonIdSerializer.ToProto));
            return response;
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "InsertBulk failed on collection {Col}", col);
            return new BulkInsertResponse { Error = ex.Message };
        }
    }

    // -- UpdateBulk ------------------------------------------------------------

    public override async Task<BulkMutationResponse> UpdateBulk(
        BulkUpdateRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Update);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var reverseKeys = ReverseKeys(engine);
            var pairs = request.Items.Select(item =>
            {
                var id  = BsonIdSerializer.FromProto(item.Id);
                var doc = BsonPayloadSerializer.Deserialize(item.BsonPayload.ToByteArray(), reverseKeys);
                return (id, doc);
            });

            int count;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                count = await session.Engine.GetOrCreateCollection(col).UpdateBulkAsync(pairs, context.CancellationToken);
                if (count > 0) session.MarkDirty(col);
            }
            else
            {
                count = await engine.UpdateBulkAsync(col, pairs, context.CancellationToken);
                if (count > 0 && _cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            return new BulkMutationResponse { AffectedCount = count };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "UpdateBulk failed on collection {Col}", col);
            return new BulkMutationResponse { Error = ex.Message };
        }
    }

    // -- DeleteBulk ------------------------------------------------------------

    public override async Task<BulkMutationResponse> DeleteBulk(
        BulkDeleteRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Delete);
        var engine      = _registry.GetEngine(user.DatabaseId);
        try
        {
            var ids = request.Ids.Select(BsonIdSerializer.FromProto);
            int count;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                var session = _txnManager.RequireSession(request.TransactionId, user);
                count = await session.Engine.GetOrCreateCollection(col).DeleteBulkAsync(ids, context.CancellationToken);
                if (count > 0) session.MarkDirty(col);
            }
            else
            {
                count = await engine.DeleteBulkAsync(col, ids, context.CancellationToken);
                if (count > 0 && _cache.Enabled)
                    _cache.Invalidate(user.DatabaseId, col);
            }
            return new BulkMutationResponse { AffectedCount = count };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DeleteBulk failed on collection {Col}", col);
            return new BulkMutationResponse { Error = ex.Message };
        }
    }

    // -- VectorSearch ----------------------------------------------------------

    public override async Task VectorSearch(
        VectorSearchRequest request,
        IServerStreamWriter<DocumentResponse> responseStream,
        ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);
        try
        {
            var collection = engine.GetOrCreateCollection(col);
            var indexName = ResolveVectorIndexName(engine, col, request.IndexName);
            var k = request.K > 0 ? request.K : 10;
            var efSearch = request.EfSearch > 0 ? request.EfSearch : 100;
            var queryVector = request.QueryVector.ToArray();

            foreach (var doc in collection.VectorSearch(indexName, queryVector, k, efSearch))
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                var payload = BsonPayloadSerializer.Serialize(doc);
                await responseStream.WriteAsync(
                    new DocumentResponse { BsonPayload = ByteString.CopyFrom(payload), Found = true },
                    context.CancellationToken);
            }
        }
        catch (RpcException) { throw; }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "VectorSearch failed on collection {Col}", col);
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    private static string ResolveVectorIndexName(BLiteEngine engine, string collection, string requested)
    {
        if (!string.IsNullOrEmpty(requested))
            return requested;

        var indexes = engine.GetIndexDescriptors(collection);
        var vec = indexes.FirstOrDefault(d => d.Type == BLite.Core.Indexing.IndexType.Vector);
        if (vec == null)
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                $"Collection '{collection}' has no vector index."));

        return vec.Name;
    }

    // -- Index management -----------------------------------------------------

    public override async Task<MutationResponse> CreateIndex(
        CreateIndexRequest request, ServerCallContext context)
    {
        var (col, _) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Admin);
        var engine = _registry.GetEngine(BLiteServiceBase.GetCurrentUser(context).DatabaseId);
        try
        {
            var collection = engine.GetOrCreateCollection(col);
            var name = string.IsNullOrWhiteSpace(request.Name) ? null : request.Name.Trim();

            if (request.IsVector)
            {
                var metric = request.Metric.Trim().ToLowerInvariant() switch
                {
                    "l2"         => BLite.Core.Indexing.VectorMetric.L2,
                    "dotproduct" => BLite.Core.Indexing.VectorMetric.DotProduct,
                    _            => BLite.Core.Indexing.VectorMetric.Cosine
                };
                collection.CreateVectorIndex(request.Field, request.Dimensions, metric, name);
            }
            else if (request.IsSpatial)
            {
                collection.CreateSpatialIndex(request.Field, name);
            }
            else
            {
                collection.CreateIndex(request.Field, name, request.Unique);
            }

            await engine.CommitAsync(context.CancellationToken);
            return new MutationResponse { Success = true };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "CreateIndex failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    public override async Task<MutationResponse> DropIndex(
        DropIndexRequest request, ServerCallContext context)
    {
        var (col, _) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Admin);
        var engine = _registry.GetEngine(BLiteServiceBase.GetCurrentUser(context).DatabaseId);
        try
        {
            var collection = engine.GetOrCreateCollection(col);
            var ok = collection.DropIndex(request.Name);
            if (ok) await engine.CommitAsync(context.CancellationToken);
            return new MutationResponse { Success = ok };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DropIndex failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    public override Task<ListIndexesResponse> ListIndexes(
        CollectionRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);
        try
        {
            var descriptors = engine.GetIndexDescriptors(col);
            var response = new ListIndexesResponse();
            response.Indexes.AddRange(descriptors.Select(d => new IndexInfo
            {
                Name       = d.Name,
                Field      = d.FieldPath,
                Type       = d.Type.ToString(),
                Dimensions = d.Dimensions,
                Metric     = d.Metric.ToString()
            }));
            return Task.FromResult(response);
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ListIndexes failed on collection {Col}", col);
            return Task.FromResult(new ListIndexesResponse { Error = ex.Message });
        }
    }

    // -- VectorSource ---------------------------------------------------------

    public override Task<MutationResponse> SetVectorSource(
        SetVectorSourceRequest request, ServerCallContext context)
    {
        var (col, _) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Admin);
        var engine = _registry.GetEngine(BLiteServiceBase.GetCurrentUser(context).DatabaseId);
        try
        {
            VectorSourceConfig? config = null;
            if (request.Fields.Count > 0)
            {
                config = new VectorSourceConfig
                {
                    Separator = string.IsNullOrEmpty(request.Separator) ? " " : request.Separator
                };
                foreach (var f in request.Fields)
                {
                    config.Fields.Add(new VectorSourceField
                    {
                        Path   = f.Path,
                        Prefix = string.IsNullOrEmpty(f.Prefix) ? null : f.Prefix,
                        Suffix = string.IsNullOrEmpty(f.Suffix) ? null : f.Suffix
                    });
                }
            }
            engine.SetVectorSource(col, config);
            return Task.FromResult(new MutationResponse { Success = true });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SetVectorSource failed on collection {Col}", col);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    public override Task<GetVectorSourceResponse> GetVectorSource(
        CollectionRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);
        try
        {
            var config = engine.GetVectorSource(col);
            if (config == null)
                return Task.FromResult(new GetVectorSourceResponse { Configured = false });

            var response = new GetVectorSourceResponse
            {
                Configured = true,
                Separator  = config.Separator
            };
            response.Fields.AddRange(config.Fields.Select(f => new VectorFieldProto
            {
                Path   = f.Path,
                Prefix = f.Prefix ?? string.Empty,
                Suffix = f.Suffix ?? string.Empty
            }));
            return Task.FromResult(response);
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetVectorSource failed on collection {Col}", col);
            return Task.FromResult(new GetVectorSourceResponse { Error = ex.Message });
        }
    }

    // -- Schema ---------------------------------------------------------------

    public override Task<CollectionSchemaResponse> GetSchema(
        CollectionRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);
        try
        {
            var schemas = engine.GetSchemas(col);
            if (schemas.Count == 0)
                return Task.FromResult(new CollectionSchemaResponse { HasSchema = false });

            var latest = schemas[^1];
            var response = new CollectionSchemaResponse
            {
                HasSchema = true,
                Title = latest.Title ?? string.Empty,
                Version = latest.Version ?? 0,
                VersionCount = schemas.Count
            };
            response.Fields.AddRange(latest.Fields.Select(f => new SchemaFieldProto
            {
                Name = f.Name,
                Type = (int)f.Type,
                Nullable = f.IsNullable
            }));
            return Task.FromResult(response);
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetSchema failed on collection {Col}", col);
            return Task.FromResult(new CollectionSchemaResponse { Error = ex.Message });
        }
    }

    public override async Task<MutationResponse> SetSchema(
        SetSchemaRequest request, ServerCallContext context)
    {
        var (col, _) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Admin);
        var engine = _registry.GetEngine(BLiteServiceBase.GetCurrentUser(context).DatabaseId);
        try
        {
            var schema = new BLite.Bson.BsonSchema
            {
                Title = string.IsNullOrEmpty(request.Title) ? null : request.Title
            };
            foreach (var f in request.Fields)
            {
                if (!string.IsNullOrWhiteSpace(f.Name))
                    schema.Fields.Add(new BLite.Bson.BsonField
                    {
                        Name = f.Name.Trim(),
                        Type = (BLite.Bson.BsonType)f.Type,
                        IsNullable = f.Nullable
                    });
            }
            engine.SetSchema(col, schema);
            await engine.CommitAsync(context.CancellationToken);
            return new MutationResponse { Success = true };
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SetSchema failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // -- TimeSeries -----------------------------------------------------------

    public override Task<MutationResponse> ConfigureTimeSeries(
        ConfigureTimeSeriesRequest request, ServerCallContext context)
    {
        var (col, _) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Admin);
        var engine = _registry.GetEngine(BLiteServiceBase.GetCurrentUser(context).DatabaseId);
        try
        {
            if (string.IsNullOrWhiteSpace(request.TtlFieldName))
                return Task.FromResult(new MutationResponse { Success = false, Error = "ttl_field_name is required." });
            if (request.RetentionMs <= 0)
                return Task.FromResult(new MutationResponse { Success = false, Error = "retention_ms must be positive." });
            engine.SetTimeSeries(col, request.TtlFieldName, TimeSpan.FromMilliseconds(request.RetentionMs));
            return Task.FromResult(new MutationResponse { Success = true });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ConfigureTimeSeries failed on collection {Col}", col);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    public override Task<TimeSeriesInfoResponse> GetTimeSeriesInfo(
        CollectionRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);
        try
        {
            var (isTs, retentionMs, ttlField) = engine.GetTimeSeriesConfig(col);
            return Task.FromResult(new TimeSeriesInfoResponse
            {
                IsTimeSeries = isTs,
                TtlFieldName = ttlField ?? string.Empty,
                RetentionMs = retentionMs
            });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetTimeSeriesInfo failed on collection {Col}", col);
            return Task.FromResult(new TimeSeriesInfoResponse { Error = ex.Message });
        }
    }

    // -- Collection management -------------------------------------------------

    public override Task<CollectionListResponse> ListCollections(
        Empty request, ServerCallContext context)
    {
        var user   = BLiteServiceBase.GetCurrentUser(context);
        _authz.RequirePermission(user, "*", BLiteOperation.Query);
        var engine = _registry.GetEngine(user.DatabaseId);

        var all = engine.ListCollections();

        // Return only collections belonging to this user's namespace,
        // with the prefix stripped so the client sees logical names.
        var response = new CollectionListResponse();
        response.Names.AddRange(
            all.Where(n => NamespaceResolver.BelongsTo(user, n))
               .Select(n => NamespaceResolver.Strip(user, n)));

        return Task.FromResult(response);
    }

    public override Task<MutationResponse> DropCollection(
        DropCollectionRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Drop);
        var engine      = _registry.GetEngine(user.DatabaseId);
        var ok          = engine.DropCollection(col);
        return Task.FromResult(new MutationResponse { Success = ok });
    }

    // -- Auth helpers ----------------------------------------------------------

    private (string Col, BLiteUser User) AuthorizeWithUser(
        ServerCallContext ctx, string collection, BLiteOperation op)
    {
        var user = BLiteServiceBase.GetCurrentUser(ctx);
        _authz.RequirePermission(user, collection, op);
        return (NamespaceResolver.Resolve(user, collection), user);
    }

    // -- Engine helpers --------------------------------------------------------

    /// <summary>Returns the engine's ushort→name reverse key map for BSON deserialization.</summary>
    private static ConcurrentDictionary<ushort, string> ReverseKeys(BLiteEngine engine)
        => (ConcurrentDictionary<ushort, string>)engine.GetKeyReverseMap();
}
