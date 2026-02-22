// BLite.Server — API Key middleware
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

using Grpc.Core;

namespace BLite.Server.Auth;

/// <summary>
/// Reads the <c>x-api-key</c> gRPC metadata header and validates it.
/// Unauthenticated requests receive <see cref="StatusCode.Unauthenticated"/>.
/// </summary>
public sealed class ApiKeyMiddleware
{
    private const string HeaderName = "x-api-key";

    private readonly RequestDelegate   _next;
    private readonly ApiKeyValidator   _validator;
    private readonly ILogger<ApiKeyMiddleware> _logger;

    public ApiKeyMiddleware(RequestDelegate next, ApiKeyValidator validator,
                            ILogger<ApiKeyMiddleware> logger)
    {
        _next      = next;
        _validator = validator;
        _logger    = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var key = context.Request.Headers[HeaderName].FirstOrDefault();

        if (!_validator.IsValid(key))
        {
            _logger.LogWarning("Rejected request from {Remote} — invalid API key",
                context.Connection.RemoteIpAddress);

            context.Response.StatusCode = StatusCodes.Status401Unauthorized;

            // For gRPC (HTTP/2) the status must be in trailers
            context.Response.Headers.Append("grpc-status",
                ((int)StatusCode.Unauthenticated).ToString());
            context.Response.Headers.Append("grpc-message", "Invalid or missing API key");
            await context.Response.CompleteAsync();
            return;
        }

        await _next(context);
    }
}
