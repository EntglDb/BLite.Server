// BLite.Server — Base class for gRPC service implementations
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using BLite.Core;
using BLite.Server.Auth;
using Grpc.Core;

namespace BLite.Server.Services;

/// <summary>
/// Provides authentication and namespace-resolution helpers to all BLite gRPC services.
/// </summary>
public abstract class BLiteServiceBase
{
    protected readonly EngineRegistry   _registry;
    protected readonly AuthorizationService _authz;
    protected readonly ILogger             _logger;

    protected BLiteServiceBase(
        EngineRegistry registry, AuthorizationService authz, ILogger logger)
    {
        _registry = registry;
        _authz    = authz;
        _logger   = logger;
    }

    /// <summary>
    /// Retrieves the authenticated <see cref="BLiteUser"/> from the gRPC call context.
    /// Throws <see cref="RpcException"/> Unauthenticated if no user is attached.
    /// </summary>
    public static BLiteUser GetCurrentUser(ServerCallContext ctx)
        => ctx.GetHttpContext().Items[nameof(BLiteUser)] as BLiteUser
           ?? throw new RpcException(new Status(
               StatusCode.Unauthenticated, "Request carries no valid API key."));

    /// <summary>
    /// Returns the <see cref="BLiteEngine"/> for the authenticated user's database.
    /// </summary>
    protected BLiteEngine GetEngine(BLiteUser user)
        => _registry.GetEngine(user.DatabaseId);

    /// <summary>
    /// Verifies that the caller holds <paramref name="op"/> permission on
    /// <paramref name="collection"/> and returns the namespace-resolved collection name
    /// (the physical name written to the engine).
    /// </summary>
    protected string AuthorizeAndResolve(
        ServerCallContext ctx, string collection, BLiteOperation op)
    {
        var user = GetCurrentUser(ctx);
        _authz.RequirePermission(user, collection, op);
        return NamespaceResolver.Resolve(user, collection);
    }
}
