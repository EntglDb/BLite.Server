// BLite.Server — ASP.NET Core middleware that applies active restrictions to REST calls
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Server.License;

/// <summary>
/// Injects a per-request delay on all REST/API paths when an OperationDelayMs
/// restriction is active. Must be registered before authentication middleware
/// so it covers every request on the REST port.
/// </summary>
public sealed class RestrictionMiddleware(RequestDelegate next, RestrictionService restrictions)
{
    public async Task InvokeAsync(HttpContext context)
    {
        var delay = restrictions.Current.OperationDelayMs;
        if (delay > 0)
            await Task.Delay(delay, context.RequestAborted);

        await next(context);
    }
}
