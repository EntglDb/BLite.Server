// BLite.Server — singleton that holds the current server-side restrictions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Restrictions are set by the LicenseHub heartbeat response and applied
// across all gRPC calls, REST calls, query cache, and the Studio UI.
// All fields default to zero / false (no restrictions active).

namespace BLite.Server.License;

/// <summary>
/// Thread-safe holder for the operational restrictions received from LicenseHub.
/// Updated by <see cref="HeartbeatWorker"/> after every successful heartbeat.
/// </summary>
public sealed class RestrictionService
{
    // Volatile read is safe for reading a reference type on all current .NET
    // memory models; writes go through Interlocked.Exchange for atomicity.
    private volatile RestrictionSnapshot _current = RestrictionSnapshot.None;

    public RestrictionSnapshot Current => _current;

    /// <summary>Atomically replaces the active restriction set.</summary>
    public void Update(RestrictionSnapshot snapshot) =>
        Interlocked.Exchange(ref _current, snapshot);
}

/// <summary>
/// Immutable snapshot of the restrictions in force at a given point in time.
/// </summary>
public sealed class RestrictionSnapshot
{
    public static readonly RestrictionSnapshot None = new();

    /// <summary>
    /// Artificial delay added to every gRPC and REST API call (milliseconds).
    /// 0 = no delay. Even a small value (30–50 ms) is severe at production throughput.
    /// </summary>
    public int OperationDelayMs { get; init; } = 0;

    /// <summary>
    /// Hard cap on the number of documents any query may return.
    /// 0 = no cap. Overrides any higher limit set by the caller.
    /// </summary>
    public int QueryResultLimit { get; init; } = 0;

    /// <summary>
    /// When true the query result cache is fully bypassed.
    /// Forces every read to hit the storage engine, increasing latency and CPU usage.
    /// </summary>
    public bool DisableQueryCache { get; init; } = false;

    /// <summary>
    /// Non-null = a warning banner displayed in the Blazor Studio UI to alert
    /// the administrator about a license or compliance issue.
    /// </summary>
    public string? WarnBannerMessage { get; init; } = null;

    public bool HasAny =>
        OperationDelayMs > 0 ||
        QueryResultLimit > 0 ||
        DisableQueryCache    ||
        WarnBannerMessage is not null;
}
