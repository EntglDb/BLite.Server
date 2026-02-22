// BLite.Server — MetadataService implementation (key-map negotiation)
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Core;
using BLite.Proto.V1;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Exposes the server's global C-BSON key dictionary over gRPC so that remote
/// clients can synchronise their local key map before sending typed payloads.
///
/// Permission model (per user request):
///   • <see cref="GetKeyMap"/>    — requires <see cref="BLiteOperation.Query"/>  (read)
///   • <see cref="RegisterKeys"/> — requires <see cref="BLiteOperation.Insert"/> (write)
/// </summary>
public sealed class MetadataServiceImpl : MetadataService.MetadataServiceBase
{
    private readonly BLiteEngine                  _engine;
    private readonly AuthorizationService         _authz;
    private readonly ILogger<MetadataServiceImpl> _logger;

    public MetadataServiceImpl(
        BLiteEngine engine,
        AuthorizationService authz,
        ILogger<MetadataServiceImpl> logger)
    {
        _engine = engine;
        _authz  = authz;
        _logger = logger;
    }

    // ── GetKeyMap ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the full global key→id map.
    /// Requires <see cref="BLiteOperation.Query"/> on the anchor collection.
    /// </summary>
    public override Task<KeyMapResponse> GetKeyMap(
        KeyMapRequest request, ServerCallContext context)
    {
        try
        {
            var user = BLiteServiceBase.GetCurrentUser(context);
            _authz.RequirePermission(user, request.Collection, BLiteOperation.Query);

            var map    = _engine.GetKeyMap();
            var result = new KeyMapResponse();
            foreach (var (name, id) in map)
                result.Entries[name] = id;

            _logger.LogDebug(
                "GetKeyMap: {Count} entries returned to '{User}' for collection '{Collection}'.",
                result.Entries.Count, user.Username, request.Collection);

            return Task.FromResult(result);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetKeyMap failed.");
            return Task.FromResult(new KeyMapResponse { Error = ex.Message });
        }
    }

    // ── RegisterKeys ──────────────────────────────────────────────────────────

    /// <summary>
    /// Registers the requested field names in the global dictionary (idempotent —
    /// existing names are left unchanged), then returns the assigned ID for each
    /// requested key only.
    /// Requires <see cref="BLiteOperation.Insert"/> on the anchor collection.
    /// </summary>
    public override Task<KeyMapResponse> RegisterKeys(
        RegisterKeysRequest request, ServerCallContext context)
    {
        try
        {
            var user = BLiteServiceBase.GetCurrentUser(context);
            _authz.RequirePermission(user, request.Collection, BLiteOperation.Insert);

            if (request.Keys.Count == 0)
                return Task.FromResult(new KeyMapResponse());

            // Register on the server (idempotent — assigns IDs to new names only)
            _engine.RegisterKeys(request.Keys);

            // Build response with the IDs of the requested keys only
            var map    = _engine.GetKeyMap();
            var result = new KeyMapResponse();
            foreach (var key in request.Keys)
            {
                var normalised = key.ToLowerInvariant();
                if (map.TryGetValue(normalised, out var id))
                    result.Entries[normalised] = id;
            }

            _logger.LogDebug(
                "RegisterKeys: {Count} key(s) resolved for '{User}' on collection '{Collection}'.",
                result.Entries.Count, user.Username, request.Collection);

            return Task.FromResult(result);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RegisterKeys failed.");
            return Task.FromResult(new KeyMapResponse { Error = ex.Message });
        }
    }
}
