// BLite.Server — DynamicService implementation (schema-less path)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Bson;
using BLite.Core;
using BLite.Proto;
using BLite.Proto.V1;
using BLite.Server.Execution;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Implements the schema-less gRPC service.
/// All documents travel as raw BSON bytes — no compile-time type information required.
/// </summary>
public sealed class DynamicServiceImpl : DynamicService.DynamicServiceBase
{
    private readonly BLiteEngine _engine;
    private readonly ILogger<DynamicServiceImpl> _logger;

    public DynamicServiceImpl(BLiteEngine engine, ILogger<DynamicServiceImpl> logger)
    {
        _engine = engine;
        _logger = logger;
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    public override Task<InsertResponse> Insert(InsertRequest request, ServerCallContext context)
    {
        try
        {
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray());
            var id  = _engine.Insert(request.Collection, doc);
            return Task.FromResult(new InsertResponse { Id = BsonIdSerializer.ToProto(id) });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Insert failed on collection {Col}", request.Collection);
            return Task.FromResult(new InsertResponse { Error = ex.Message });
        }
    }

    // ── FindById ──────────────────────────────────────────────────────────────

    public override async Task<DocumentResponse> FindById(FindByIdRequest request,
                                                           ServerCallContext context)
    {
        try
        {
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = await _engine.FindByIdAsync(request.Collection, id, context.CancellationToken);

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
            _logger.LogError(ex, "FindById failed on collection {Col}", request.Collection);
            return new DocumentResponse { Error = ex.Message };
        }
    }

    // ── Update ────────────────────────────────────────────────────────────────

    public override async Task<MutationResponse> Update(UpdateRequest request,
                                                         ServerCallContext context)
    {
        try
        {
            var id  = BsonIdSerializer.FromProto(request.Id);
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray());
            var ok  = await _engine.UpdateAsync(request.Collection, id, doc,
                                                context.CancellationToken);
            return new MutationResponse { Success = ok };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Update failed on collection {Col}", request.Collection);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    public override async Task<MutationResponse> Delete(DeleteRequest request,
                                                         ServerCallContext context)
    {
        try
        {
            var id = BsonIdSerializer.FromProto(request.Id);
            var ok = await _engine.DeleteAsync(request.Collection, id,
                                               context.CancellationToken);
            return new MutationResponse { Success = ok };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Delete failed on collection {Col}", request.Collection);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // ── Query (server-streaming) ──────────────────────────────────────────────

    public override async Task Query(QueryRequest request,
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

        var ct = context.CancellationToken;

        await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(_engine, descriptor, ct))
        {
            var response = new DocumentResponse
            {
                BsonPayload = ByteString.CopyFrom(BsonPayloadSerializer.Serialize(doc)),
                Found       = true
            };
            await responseStream.WriteAsync(response, ct);
        }
    }

    // ── InsertBulk ────────────────────────────────────────────────────────────

    public override Task<BulkInsertResponse> InsertBulk(BulkInsertRequest request,
                                                          ServerCallContext context)
    {
        try
        {
            var docs = request.Payloads
                .Select(p => BsonPayloadSerializer.Deserialize(p.ToByteArray()))
                .ToList();

            var ids = _engine.InsertBulk(request.Collection, docs);

            var response = new BulkInsertResponse();
            response.Ids.AddRange(ids.Select(BsonIdSerializer.ToProto));
            return Task.FromResult(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "InsertBulk failed on collection {Col}", request.Collection);
            return Task.FromResult(new BulkInsertResponse { Error = ex.Message });
        }
    }

    // ── UpdateBulk ────────────────────────────────────────────────────────────

    public override async Task<BulkMutationResponse> UpdateBulk(BulkUpdateRequest request,
                                                                  ServerCallContext context)
    {
        try
        {
            var pairs = request.Items.Select(item =>
            {
                var id  = BsonIdSerializer.FromProto(item.Id);
                var doc = BsonPayloadSerializer.Deserialize(item.BsonPayload.ToByteArray());
                return (id, doc);
            });

            var count = await _engine.UpdateBulkAsync(request.Collection, pairs,
                                                      context.CancellationToken);
            return new BulkMutationResponse { AffectedCount = count };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "UpdateBulk failed on collection {Col}", request.Collection);
            return new BulkMutationResponse { Error = ex.Message };
        }
    }

    // ── DeleteBulk ────────────────────────────────────────────────────────────

    public override async Task<BulkMutationResponse> DeleteBulk(BulkDeleteRequest request,
                                                                  ServerCallContext context)
    {
        try
        {
            var ids   = request.Ids.Select(BsonIdSerializer.FromProto);
            var count = await _engine.DeleteBulkAsync(request.Collection, ids,
                                                      context.CancellationToken);
            return new BulkMutationResponse { AffectedCount = count };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DeleteBulk failed on collection {Col}", request.Collection);
            return new BulkMutationResponse { Error = ex.Message };
        }
    }

    // ── Collection management ─────────────────────────────────────────────────

    public override Task<CollectionListResponse> ListCollections(Empty request,
                                                                  ServerCallContext context)
    {
        var response = new CollectionListResponse();
        response.Names.AddRange(_engine.ListCollections());
        return Task.FromResult(response);
    }

    public override Task<MutationResponse> DropCollection(DropCollectionRequest request,
                                                           ServerCallContext context)
    {
        var ok = _engine.DropCollection(request.Collection);
        return Task.FromResult(new MutationResponse { Success = ok });
    }
}
