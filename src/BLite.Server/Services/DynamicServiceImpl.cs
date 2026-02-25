// BLite.Server — DynamicService implementation (schema-less path)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using System.Collections.Concurrent;
using BLite.Bson;
using BLite.Core;
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
