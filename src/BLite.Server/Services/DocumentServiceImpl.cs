// BLite.Server — DocumentService implementation (typed path)
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
/// Implements the typed gRPC service endpoint.
/// Write operations accept pre-serialized BSON (produced by the BLite client mapper).
/// Read operations push projected bytes without deserializing T.
/// Every method checks the caller's permission, resolves the engine for the caller's
/// database, and applies namespace isolation.
/// </summary>
public sealed class DocumentServiceImpl : DocumentService.DocumentServiceBase
{
    private readonly EngineRegistry                _registry;
    private readonly AuthorizationService          _authz;
    private readonly TransactionManager            _txnManager;
    private readonly QueryCacheService             _cache;
    private readonly IOptions<QueryCacheOptions>   _cacheOpts;
    private readonly ILogger<DocumentServiceImpl>  _logger;

    public DocumentServiceImpl(
        EngineRegistry       registry,
        AuthorizationService authz,
        TransactionManager   txnManager,
        QueryCacheService cache,
        IOptions<QueryCacheOptions> cacheOpts,
        ILogger<DocumentServiceImpl> logger)
    {
        _registry   = registry;
        _authz      = authz;
        _txnManager = txnManager;
        _cache      = cache;
        _cacheOpts  = cacheOpts;
        _logger     = logger;
    }

    // -- Typed streaming query -------------------------------------------------

    public override async Task Query(
        QueryRequest request,
        IServerStreamWriter<TypedDocumentResponse> responseStream,
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
        var typeName = descriptor.Select?.ResultTypeName ?? string.Empty;
        var engine   = _registry.GetEngine(user.DatabaseId);
        var ct       = context.CancellationToken;

        var cacheKey = QueryCacheKeys.GrpcQuery(user.DatabaseId, col,
                                                request.QueryDescriptor.ToByteArray());

        if (_cache.Enabled && _cache.TryGet(cacheKey, out List<byte[]>? cachedPayloads))
        {
            foreach (var payload in cachedPayloads!)
                await responseStream.WriteAsync(
                    new TypedDocumentResponse { BsonPayload = ByteString.CopyFrom(payload), TypeName = typeName },
                    ct);
            return;
        }

        var payloads = new List<byte[]>();
        await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(engine, descriptor, ct))
        {
            var payload = BsonPayloadSerializer.Serialize(doc);
            payloads.Add(payload);
            var response = new TypedDocumentResponse
            {
                BsonPayload = ByteString.CopyFrom(payload),
                TypeName    = typeName
            };
            await responseStream.WriteAsync(response, ct);
        }

        if (_cache.Enabled && payloads.Count <= _cacheOpts.Value.MaxResultSetSize)
            _cache.Set(cacheKey, payloads, user.DatabaseId, col);
    }

    // -- Typed insert ---------------------------------------------------------

    public override async Task<InsertResponse> Insert(
        TypedInsertRequest request, ServerCallContext context)
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
            _logger.LogError(ex, "Typed Insert failed on collection {Col}", col);
            return new InsertResponse { Error = ex.Message };
        }
    }

    // -- Typed update ---------------------------------------------------------

    public override async Task<MutationResponse> Update(
        TypedUpdateRequest request, ServerCallContext context)
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
            _logger.LogError(ex, "Typed Update failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // -- Typed delete ---------------------------------------------------------

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
            _logger.LogError(ex, "Typed Delete failed on collection {Col}", col);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // -- Typed bulk insert -----------------------------------------------------

    public override async Task<BulkInsertResponse> InsertBulk(
        TypedBulkInsertRequest request, ServerCallContext context)
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
            _logger.LogError(ex, "TypedInsertBulk failed on collection {Col}", col);
            return new BulkInsertResponse { Error = ex.Message };
        }
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
