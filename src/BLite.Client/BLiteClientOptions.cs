// BLite.Client — BLiteClientOptions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0

namespace BLite.Client;

/// <summary>
/// Configuration for connecting to a remote BLite Server.
/// </summary>
public sealed class BLiteClientOptions
{
    /// <summary>Server hostname or IP. Default: localhost.</summary>
    public string Host { get; set; } = "localhost";

    /// <summary>Server gRPC port. Default: 2626.</summary>
    public int Port { get; set; } = 2626;

    /// <summary>API key sent as <c>x-api-key</c> metadata on every call.</summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Whether to use TLS. Default: <c>true</c>.
    /// Set to <c>false</c> only for local development / testing.
    /// </summary>
    public bool UseTls { get; set; } = true;

    /// <summary>
    /// Optional: override the full address (e.g. "https://myserver:2626").
    /// When set, <see cref="Host"/>, <see cref="Port"/> and <see cref="UseTls"/>
    /// are ignored.
    /// </summary>
    public string? Address { get; set; }

    internal string ResolvedAddress =>
        Address ?? $"{(UseTls ? "https" : "http")}://{Host}:{Port}";
}
