// BLite.Server — KvService implementation (Key-Value store path)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Proto.V1;
using BLite.Server.Auth;
using Google.Protobuf;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Implements the gRPC KvService, exposing the page-backed Key-Value store
/// embedded in each BLite database file.
/// Keys are transparently namespaced for multi-tenant isolation using the same
/// <c>namespace:key</c> prefix scheme as collection names.
/// </summary>
public sealed class KvServiceImpl : KvService.KvServiceBase
{
    private readonly EngineRegistry      _registry;
    private readonly AuthorizationService _authz;
    private readonly ILogger<KvServiceImpl> _logger;

    public KvServiceImpl(
        EngineRegistry registry,
        AuthorizationService authz,
        ILogger<KvServiceImpl> logger)
    {
        _registry = registry;
        _authz    = authz;
        _logger   = logger;
    }

    // ── Auth + namespace helpers ──────────────────────────────────────────────

    private BLiteUser Authorize(ServerCallContext ctx, BLiteOperation op)
    {
        var user = BLiteServiceBase.GetCurrentUser(ctx);
        _authz.RequirePermission(user, "*", op);
        return user;
    }

    // KV keys use the same namespace:key prefix scheme as collection names.
    private static string PhysicalKey(BLiteUser user, string key)
        => NamespaceResolver.Resolve(user, key);

    // ── Read operations ───────────────────────────────────────────────────────

    public override Task<KvGetResponse> Get(KvGetRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Query);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            var value = kv.Get(PhysicalKey(user, request.Key));
            return Task.FromResult(value is not null
                ? new KvGetResponse { Value = ByteString.CopyFrom(value), Found = true }
                : new KvGetResponse { Found = false });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvGet failed for key {Key}", request.Key);
            return Task.FromResult(new KvGetResponse { Error = ex.Message });
        }
    }

    public override Task<KvExistsResponse> Exists(KvKeyRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Query);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            return Task.FromResult(new KvExistsResponse
            {
                Exists = kv.Exists(PhysicalKey(user, request.Key))
            });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvExists failed for key {Key}", request.Key);
            return Task.FromResult(new KvExistsResponse { Error = ex.Message });
        }
    }

    public override Task<KvScanResponse> ScanKeys(KvScanRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Query);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            // Physical prefix already includes the namespace, so only own keys are scanned.
            // For root users Resolve("") → "" which scans all keys (correct).
            var physicalPrefix = NamespaceResolver.Resolve(user, request.Prefix);
            var keys = kv.ScanKeys(physicalPrefix)
                         .Where(k  => NamespaceResolver.BelongsTo(user, k))
                         .Select(k => NamespaceResolver.Strip(user, k))
                         .ToList();

            var resp = new KvScanResponse();
            resp.Keys.AddRange(keys);
            return Task.FromResult(resp);
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvScanKeys failed for prefix {Prefix}", request.Prefix);
            return Task.FromResult(new KvScanResponse { Error = ex.Message });
        }
    }

    // ── Write operations ──────────────────────────────────────────────────────

    public override Task<MutationResponse> Set(KvSetRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Write);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            var ttl = request.TtlMs > 0 ? TimeSpan.FromMilliseconds(request.TtlMs) : (TimeSpan?)null;
            kv.Set(PhysicalKey(user, request.Key), request.Value.Span, ttl);
            return Task.FromResult(new MutationResponse { Success = true });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvSet failed for key {Key}", request.Key);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    public override Task<MutationResponse> Delete(KvDeleteRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Write);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            var ok = kv.Delete(PhysicalKey(user, request.Key));
            return Task.FromResult(new MutationResponse { Success = ok });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvDelete failed for key {Key}", request.Key);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    public override Task<MutationResponse> Refresh(KvRefreshRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Write);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            if (request.TtlMs <= 0)
                return Task.FromResult(new MutationResponse
                {
                    Success = false,
                    Error = "ttl_ms must be positive."
                });

            var ok = kv.Refresh(PhysicalKey(user, request.Key), TimeSpan.FromMilliseconds(request.TtlMs));
            return Task.FromResult(new MutationResponse { Success = ok });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvRefresh failed for key {Key}", request.Key);
            return Task.FromResult(new MutationResponse { Success = false, Error = ex.Message });
        }
    }

    public override Task<KvBatchResponse> Batch(KvBatchRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Write);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            var batch = kv.Batch();
            foreach (var op in request.Operations)
            {
                if (op.IsDelete)
                    batch.Delete(PhysicalKey(user, op.Key));
                else
                {
                    var ttl = op.TtlMs > 0 ? TimeSpan.FromMilliseconds(op.TtlMs) : (TimeSpan?)null;
                    batch.Set(PhysicalKey(user, op.Key), op.Value.ToByteArray(), ttl);
                }
            }

            return Task.FromResult(new KvBatchResponse { AffectedCount = batch.Execute() });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvBatch failed ({Count} ops)", request.Operations.Count);
            return Task.FromResult(new KvBatchResponse { Error = ex.Message });
        }
    }

    // ── Maintenance ───────────────────────────────────────────────────────────

    public override Task<KvPurgeResponse> PurgeExpired(KvDbRequest request, ServerCallContext context)
    {
        var user = Authorize(context, BLiteOperation.Admin);
        var kv   = _registry.GetEngine(user.DatabaseId).KvStore;
        try
        {
            return Task.FromResult(new KvPurgeResponse { PurgedCount = kv.PurgeExpired() });
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            _logger.LogError(ex, "KvPurgeExpired failed");
            return Task.FromResult(new KvPurgeResponse { Error = ex.Message });
        }
    }
}
