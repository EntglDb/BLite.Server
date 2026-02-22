using System.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace BLite.Server.Telemetry;

/// <summary>
/// gRPC server interceptor that automatically records traces and metrics
/// for every unary and server-streaming RPC handled by BLite.Server.
/// </summary>
public sealed class TelemetryInterceptor : Interceptor
{
    // ── Unary ─────────────────────────────────────────────────────────────────

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest                              request,
        ServerCallContext                     context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        using var activity = BLiteMetrics.Source.StartActivity(context.Method);
        var sw     = Stopwatch.StartNew();
        var status = "OK";

        try
        {
            var response = await continuation(request, context);
            return response;
        }
        catch (RpcException ex)
        {
            status = ex.StatusCode.ToString();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        catch (Exception ex)
        {
            status = "InternalError";
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        finally
        {
            RecordRpc(context.Method, status, sw.Elapsed.TotalMilliseconds);
        }
    }

    // ── Server streaming ──────────────────────────────────────────────────────

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest                                        request,
        IServerStreamWriter<TResponse>                  responseStream,
        ServerCallContext                               context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        using var activity = BLiteMetrics.Source.StartActivity(context.Method);
        var sw     = Stopwatch.StartNew();
        var status = "OK";

        try
        {
            await continuation(request, responseStream, context);
        }
        catch (RpcException ex)
        {
            status = ex.StatusCode.ToString();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        catch (Exception ex)
        {
            status = "InternalError";
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        finally
        {
            RecordRpc(context.Method, status, sw.Elapsed.TotalMilliseconds);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void RecordRpc(string method, string status, double elapsedMs)
    {
        var tags = new TagList
        {
            { "method", method },
            { "status", status }
        };
        BLiteMetrics.RpcTotal.Add(1, tags);
        BLiteMetrics.RpcDuration.Record(elapsedMs, tags);
    }
}
