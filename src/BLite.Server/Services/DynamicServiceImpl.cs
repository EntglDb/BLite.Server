// BLite.Server � DynamicService implementation (schema-less path)
// Copyright (C) 2026 Luca Fabbri � AGPL-3.0

using BLite.Bson;
using BLite.Core;
using BLite.Proto;
using BLite.Proto.V1;
using BLite.Server.Auth;
using BLite.Server.Execution;
using BLite.Server.Transactions;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Implements the schema-less gRPC service.
/// All documents travel as raw BSON bytes � no compile-time type information required.
/// Every method checks the caller's permission and applies namespace isolation.
/// </summary>
public sealed class DynamicServiceImpl : DynamicService.DynamicServiceBase
{
    private readonly BLiteEngine               _engine;
    private readonly AuthorizationService      _authz;
    private readonly TransactionManager        _txnManager;
    private readonly ILogger<DynamicServiceImpl> _logger;

    public DynamicServiceImpl(
        BLiteEngine engine, AuthorizationService authz,
        TransactionManager txnManager,
        ILogger<DynamicServiceImpl> logger)
    {
        _engine     = engine;
        _authz      = authz;
        _txnManager = txnManager;
        _logger     = logger;
    }

    // -- Insert ----------------------------------------------------------------

    public override async Task<InsertResponse> Insert(
        InsertRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Insert);
        try
        {
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray());
            BsonId id;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                id = await _engine.GetOrCreateCollection(col).InsertAsync(doc, context.CancellationToken);
            }
            else
            {
                id = await _engine.InsertAsync(col, doc, context.CancellationToken);
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
        var col = Authorize(context, request.Collection, BLiteOperation.Query);
        try
        {
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = await _engine.FindByIdAsync(col, id, context.CancellationToken);

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
        try
        {
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray());
            bool ok;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                ok = await _engine.GetOrCreateCollection(col).UpdateAsync(id, doc, context.CancellationToken);
            }
            else
            {
                ok = await _engine.UpdateAsync(col, id, doc, context.CancellationToken);
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
        try
        {
            var id = BsonIdSerializer.FromProto(request.Id);
            bool ok;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                ok = await _engine.GetOrCreateCollection(col).DeleteAsync(id, context.CancellationToken);
            }
            else
            {
                ok = await _engine.DeleteAsync(col, id, context.CancellationToken);
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

        var col = Authorize(context, descriptor.Collection, BLiteOperation.Query);
        descriptor.Collection = col;
        var ct = context.CancellationToken;

        await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(
            _engine, descriptor, ct))
        {
            var response = new DocumentResponse
            {
                BsonPayload = ByteString.CopyFrom(BsonPayloadSerializer.Serialize(doc)),
                Found       = true
            };
            await responseStream.WriteAsync(response, ct);
        }
    }

    // -- InsertBulk ------------------------------------------------------------

    public override async Task<BulkInsertResponse> InsertBulk(
        BulkInsertRequest request, ServerCallContext context)
    {
        var (col, user) = AuthorizeWithUser(context, request.Collection, BLiteOperation.Insert);
        try
        {
            var docs = request.Payloads
                .Select(p => BsonPayloadSerializer.Deserialize(p.ToByteArray()))
                .ToList();

            List<BsonId> ids;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                ids = await _engine.GetOrCreateCollection(col).InsertBulkAsync(docs, context.CancellationToken);
            }
            else
            {
                ids = await _engine.InsertBulkAsync(col, docs, context.CancellationToken);
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
        try
        {
            var pairs = request.Items.Select(item =>
            {
                var id  = BsonIdSerializer.FromProto(item.Id);
                var doc = BsonPayloadSerializer.Deserialize(item.BsonPayload.ToByteArray());
                return (id, doc);
            });

            int count;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                count = await _engine.GetOrCreateCollection(col).UpdateBulkAsync(pairs, context.CancellationToken);
            }
            else
            {
                count = await _engine.UpdateBulkAsync(col, pairs, context.CancellationToken);
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
        try
        {
            var ids = request.Ids.Select(BsonIdSerializer.FromProto);
            int count;
            if (!string.IsNullOrEmpty(request.TransactionId))
            {
                _txnManager.RequireSession(request.TransactionId, user);
                count = await _engine.GetOrCreateCollection(col).DeleteBulkAsync(ids, context.CancellationToken);
            }
            else
            {
                count = await _engine.DeleteBulkAsync(col, ids, context.CancellationToken);
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
        var user = BLiteServiceBase.GetCurrentUser(context);
        _authz.RequirePermission(user, "*", BLiteOperation.Query);

        var all = _engine.ListCollections();

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
        var col = Authorize(context, request.Collection, BLiteOperation.Drop);
        var ok  = _engine.DropCollection(col);
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

    private string Authorize(ServerCallContext ctx, string collection, BLiteOperation op)
        => AuthorizeWithUser(ctx, collection, op).Col;
}

