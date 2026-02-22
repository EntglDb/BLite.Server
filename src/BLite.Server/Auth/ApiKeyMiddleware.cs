// BLite.Server — API Key middleware
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Reads the x-api-key gRPC metadata header, resolves it to a BLiteUser via
// ApiKeyValidator, and stores the user in HttpContext.Items[nameof(BLiteUser)].

using Grpc.Core;

namespace BLite.Server.Auth;

/// <summary>
/// Authenticates every incoming request via the <c>x-api-key</c> header.
/// On success, the resolved <see cref="BLiteUser"/> is stored in
/// <c>HttpContext.Items[nameof(BLiteUser)]</c> for downstream services.
/// </summary>
public sealed class ApiKeyMiddleware(
    RequestDelegate next,
    ApiKeyValidator validator,
    ILogger<ApiKeyMiddleware> logger)
{
    private const string HeaderName = "x-api-key";

    public async Task InvokeAsync(HttpContext context)
    {
        var key  = context.Request.Headers[HeaderName].FirstOrDefault();
        var user = validator.Resolve(key);

        if (user is null)
        {
            logger.LogWarning("Rejected request from {Remote} — invalid or missing API key",
                context.Connection.RemoteIpAddress);

            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.Headers.Append("grpc-status",
                ((int)StatusCode.Unauthenticated).ToString());
            context.Response.Headers.Append("grpc-message", "Invalid or missing API key");
            await context.Response.CompleteAsync();
            return;
        }

        context.Items[nameof(BLiteUser)] = user;
        await next(context);
    }
}
