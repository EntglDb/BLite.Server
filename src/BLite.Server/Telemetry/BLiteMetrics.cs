using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace BLite.Server.Telemetry;

/// <summary>
/// Central registry for OpenTelemetry ActivitySource and Meter instruments
/// used throughout BLite.Server.
/// </summary>
public static class BLiteMetrics
{
    public const string ServiceName    = "BLite.Server";
    public const string ServiceVersion = "1.0.0";

    // ── Tracing ──────────────────────────────────────────────────────────────

    /// <summary>ActivitySource for distributed traces.</summary>
    public static readonly ActivitySource Source = new(ServiceName, ServiceVersion);

    // ── Metrics ───────────────────────────────────────────────────────────────

    private static readonly Meter _meter = new(ServiceName, ServiceVersion);

    /// <summary>Total gRPC operations executed. Tags: method, status.</summary>
    public static readonly Counter<long> RpcTotal =
        _meter.CreateCounter<long>(
            "blite.server.rpc.total",
            unit: "operations",
            description: "Total gRPC operations executed");

    /// <summary>gRPC operation duration in milliseconds. Tags: method, status.</summary>
    public static readonly Histogram<double> RpcDuration =
        _meter.CreateHistogram<double>(
            "blite.server.rpc.duration",
            unit: "ms",
            description: "gRPC operation duration in milliseconds");

    /// <summary>Documents emitted by streaming RPCs. Tags: method.</summary>
    public static readonly Counter<long> DocumentsStreamed =
        _meter.CreateCounter<long>(
            "blite.server.documents.streamed",
            unit: "documents",
            description: "Documents emitted from streaming RPCs");

    /// <summary>Currently active server-managed transactions.</summary>
    public static readonly UpDownCounter<int> ActiveTransactions =
        _meter.CreateUpDownCounter<int>(
            "blite.server.active_transactions",
            unit: "transactions",
            description: "Currently active user-scoped transactions");
}
