// BLite.Server — gRPC interceptor that applies active server-side restrictions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Restrictions (delay, etc.) are set remotely by LicenseHub and stored in
// RestrictionService. This interceptor runs before TelemetryInterceptor.

using Grpc.Core;
using Grpc.Core.Interceptors;

namespace BLite.Server.License;

/// <summary>
/// gRPC server interceptor that enforces the operational restrictions received
/// from LicenseHub (delay per call, etc.).
/// </summary>
public sealed class RestrictionInterceptor(RestrictionService restrictions) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        await ApplyDelayAsync(context.CancellationToken);
        return await continuation(request, context);
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        await ApplyDelayAsync(context.CancellationToken);
        await continuation(request, responseStream, context);
    }

    private Task ApplyDelayAsync(CancellationToken ct)
    {
        var delay = restrictions.Current.OperationDelayMs;
        return delay > 0 ? Task.Delay(delay, ct) : Task.CompletedTask;
    }
}
