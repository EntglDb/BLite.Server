// BLite.Server — DocumentService implementation (typed path)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// The typed path avoids instantiating T on the server.
// The client sends a QueryDescriptor with scalar SELECT fields;
// the server executes a push-down BSON scan and streams raw projected bytes back.

using BLite.Bson;
using BLite.Core;
using BLite.Proto;
using BLite.Proto.V1;
using BLite.Server.Execution;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Implements the typed gRPC service endpoint.
/// Write operations accept pre-serialized BSON (produced by the BLite client mapper).
/// Read operations push projected bytes without deserializing <c>T</c>.
/// </summary>
public sealed class DocumentServiceImpl : DocumentService.DocumentServiceBase
{
    private readonly BLiteEngine _engine;
    private readonly ILogger<DocumentServiceImpl> _logger;

    public DocumentServiceImpl(BLiteEngine engine, ILogger<DocumentServiceImpl> logger)
    {
        _engine = engine;
        _logger = logger;
    }

    // ── Typed streaming query ─────────────────────────────────────────────────

    public override async Task Query(QueryRequest request,
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

        var typeName = descriptor.Select?.ResultTypeName ?? string.Empty;
        var ct       = context.CancellationToken;

        await foreach (var doc in QueryDescriptorExecutor.ExecuteAsync(_engine, descriptor, ct))
        {
            var response = new TypedDocumentResponse
            {
                BsonPayload = ByteString.CopyFrom(BsonPayloadSerializer.Serialize(doc)),
                TypeName    = typeName
            };
            await responseStream.WriteAsync(response, ct);
        }
    }

    // ── Typed insert ─────────────────────────────────────────────────────────

    public override Task<InsertResponse> Insert(TypedInsertRequest request,
                                                 ServerCallContext context)
    {
        try
        {
            var doc = BsonPayloadSerializer.Deserialize(request.BsonPayload.ToByteArray());
            var id  = _engine.Insert(request.Collection, doc);
            return Task.FromResult(new InsertResponse { Id = BsonIdSerializer.ToProto(id) });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Typed Insert failed on collection {Col}", request.Collection);
            return Task.FromResult(new InsertResponse { Error = ex.Message });
        }
    }

    // ── Typed update ─────────────────────────────────────────────────────────

    public override async Task<MutationResponse> Update(TypedUpdateRequest request,
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
            _logger.LogError(ex, "Typed Update failed on collection {Col}", request.Collection);
            return new MutationResponse { Success = false, Error = ex.Message };
        }
    }

    // ── Typed delete ─────────────────────────────────────────────────────────

    public override Task<MutationResponse> Delete(DeleteRequest request,
                                                   ServerCallContext context)
    {
        try
        {
            var id = BsonIdSerializer.FromProto(request.Id);
            var ok = _engine.Delete(request.Collection, id.ToString()!);
            return Task.FromResult(new MutationResponse { Success = ok });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Typed Delete failed on collection {Col}", request.Collection);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    // ── Typed bulk insert ─────────────────────────────────────────────────────

    public override async Task<BulkInsertResponse> InsertBulk(TypedBulkInsertRequest request,
                                                               ServerCallContext context)
    {
        try
        {
            var docs = request.Payloads
                .Select(p => BsonPayloadSerializer.Deserialize(p.ToByteArray()))
                .ToList();

            var ids = await _engine.InsertBulkAsync(request.Collection, docs,
                                                    context.CancellationToken);
            var response = new BulkInsertResponse();
            response.Ids.AddRange(ids.Select(BsonIdSerializer.ToProto));
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TypedInsertBulk failed on collection {Col}", request.Collection);
            return new BulkInsertResponse { Error = ex.Message };
        }
    }
}
