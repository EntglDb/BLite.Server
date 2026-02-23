// BLite.Server — REST API authentication filter
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// IEndpointFilter that validates x-api-key / Authorization: Bearer <key> on
// every REST request and stores the resolved BLiteUser in HttpContext.Items.

using BLite.Server.Auth;

namespace BLite.Server.Rest;

/// <summary>
/// Applied to all REST routes.  Resolves the caller's identity via
/// <see cref="ApiKeyValidator"/> and stores it in
/// <c>HttpContext.Items[nameof(BLiteUser)]</c> for downstream handlers.
/// Returns <c>401 Unauthorized</c> when the key is missing or invalid.
/// </summary>
public sealed class RestAuthFilter(ApiKeyValidator validator) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext ctx, EndpointFilterDelegate next)
    {
        var http = ctx.HttpContext;
        var key  = ExtractKey(http);
        var user = validator.Resolve(key);

        if (user is null)
            return Results.Problem(
                title:      "Unauthorized",
                detail:     "A valid x-api-key header (or Bearer token) is required.",
                statusCode: StatusCodes.Status401Unauthorized);

        http.Items[nameof(BLiteUser)] = user;
        return await next(ctx);
    }

    private static string? ExtractKey(HttpContext http)
    {
        // 1. x-api-key header (same as gRPC)
        var xKey = http.Request.Headers["x-api-key"].FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(xKey)) return xKey;

        // 2. Authorization: Bearer <key>
        var auth = http.Request.Headers.Authorization.FirstOrDefault();
        if (auth is not null && auth.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            return auth["Bearer ".Length..].Trim();

        return null;
    }
}
