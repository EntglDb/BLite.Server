// BLite.Server — singleton that holds the current server-side restrictions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Restrictions are set by the LicenseHub heartbeat response and applied
// across all gRPC calls, REST calls, query cache, and the Studio UI.
// All fields default to zero / false (no restrictions active).
//
// Local overrides (applied regardless of the Hub response):
//   • AGPL §13 violation  — SourceUrl not configured         → Severe
//   • Hub unreachable 7d  — no successful heartbeat in 7 days  → MediumSevere
//   • Hub unreachable 30d — no successful heartbeat in 30 days → Severe

namespace BLite.Server.License;

/// <summary>
/// Thread-safe holder for the operational restrictions.
/// May be updated by:
///   1. <see cref="HeartbeatWorker"/> with restrictions received from LicenseHub.
///   2. Local policy checks (SourceUrl missing, prolonged Hub unreachability).
/// </summary>
public sealed class RestrictionService
{
    // ── Preset snapshots ─────────────────────────────────────────────────────

    /// <summary>
    /// Medium-severe: applied after 7 consecutive days without a successful
    /// heartbeat to the LicenseHub.
    /// 50 ms per call is painful at any real-world RPS.
    /// </summary>
    public static readonly RestrictionSnapshot MediumSevere = new()
    {
        OperationDelayMs  = 50,
        DisableQueryCache = true,
        WarnBannerMessage = "⚠ This server has been unable to contact the BLite LicenseHub for 7+ days. " +
                            "Please ensure the server can reach licensehub.blitedb.com.",
    };

    /// <summary>
    /// Severe: applied after 30 consecutive days without heartbeat, OR when
    /// the AGPL-3.0 §13 source URL is not configured.
    /// 200 ms per call makes the server effectively unusable at production load.
    /// </summary>
    public static readonly RestrictionSnapshot Severe = new()
    {
        OperationDelayMs  = 200,
        QueryResultLimit  = 100,
        DisableQueryCache = true,
        WarnBannerMessage = "🚨 Critical compliance issue detected. " +
                            "This server is subject to severe operational restrictions. " +
                            "Contact support@blitedb.com.",
    };

    // ── State ────────────────────────────────────────────────────────────────

    // Volatile read is safe for reading a reference type on all current .NET
    // memory models; writes go through Interlocked.Exchange for atomicity.
    private volatile RestrictionSnapshot _current = RestrictionSnapshot.None;

    public RestrictionSnapshot Current => _current;

    /// <summary>Atomically replaces the active restriction set.</summary>
    public void Update(RestrictionSnapshot snapshot) =>
        Interlocked.Exchange(ref _current, snapshot);

    // ── Local policy helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Returns the restriction snapshot that must be applied locally due to
    /// prolonged Hub unreachability, ignoring any Hub-driven snapshot.
    /// Returns <see cref="RestrictionSnapshot.None"/> when within tolerance.
    /// </summary>
    public static RestrictionSnapshot FromMissedHeartbeatDays(int days) => days switch
    {
        >= 30 => Severe,
        >= 7  => MediumSevere,
        _     => RestrictionSnapshot.None,
    };
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
