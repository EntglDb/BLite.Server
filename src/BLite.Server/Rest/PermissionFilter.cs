// BLite.Server — Permission endpoint filter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IEndpointFilter applied per-endpoint (or per-group) to enforce operation-level
// permission checks after RestAuthFilter has already authenticated the caller.

using BLite.Server.Auth;

namespace BLite.Server.Rest;

/// <summary>
/// Enforces operation-level permission checks after <see cref="RestAuthFilter"/> has
/// placed a <see cref="BLiteUser"/> in <c>HttpContext.Items</c>.
/// </summary>
/// <param name="op">The operation required by this endpoint.</param>
/// <param name="collection">
/// Fixed collection name to check against (e.g. <c>"*"</c> for admin routes).
/// Pass <see langword="null"/> to read the value from the <c>{collection}</c> route
/// parameter, falling back to <c>"*"</c> when the route has no such segment.
/// </param>
/// <param name="checkDb">
/// When <see langword="true"/>, the <c>{dbId}</c> route segment is validated against
/// <see cref="BLiteUser.DatabaseId"/> if the user is restricted to a specific database.
/// </param>
internal sealed class PermissionFilter(
    BLiteOperation op,
    string? collection = null,
    bool checkDb = false) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext ctx, EndpointFilterDelegate next)
    {
        var http = ctx.HttpContext;

        if (http.Items[nameof(BLiteUser)] is not BLiteUser user)
            return BLiteErrors.MissingKey().ToResult();

        if (!user.Active)
            return BLiteErrors.InactiveUser(user.Username).ToResult();

        var col = collection ?? http.GetRouteValue("collection") as string ?? "*";
        var authz = http.RequestServices.GetRequiredService<AuthorizationService>();

        if (!authz.CheckPermission(user, col, op))
            return BLiteErrors.PermissionDenied(user.Username, op, col).ToResult();

        if (checkDb)
        {
            var dbId = http.GetRouteValue("dbId") as string ?? "";
            var normalized = RestApiExtensions.NullIfDefault(dbId);
            if (user.DatabaseId is not null &&
                !user.DatabaseId.Equals(normalized, StringComparison.OrdinalIgnoreCase))
            {
                return BLiteErrors.PermissionDenied(user.Username, op,
                    $"database '{dbId}' (user is restricted to '{user.DatabaseId}')").ToResult();
            }
        }

        return await next(ctx);
    }
}
